package app

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform/pii"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/port"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type FastLaneJob struct {
	ShardID  string
	Delivery *domain.WebhookDelivery
	Merchant *domain.Merchant
}

type WebhookService struct {
	repo       port.Repository
	client     port.HTTPClient
	logger     *zap.Logger
	fastLaneCh chan FastLaneJob
}

func NewWebhookService(repo port.Repository, client port.HTTPClient, logger *zap.Logger) *WebhookService {
	return &WebhookService{
		repo:       repo,
		client:     client,
		logger:     logger,
		fastLaneCh: make(chan FastLaneJob, domain.FastLaneBufferSize),
	}
}

// StartFastLaneWorkers startsbackground goroutines that process the immediate, zero-latency HTTP webhooks.
// Blocks until all workers have completely drained on context cancellation.
func (s *WebhookService) StartFastLaneWorkers(ctx context.Context, workers int) func() error {
	return func() error {
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case job := <-s.fastLaneCh:
						s.executeFastLaneJob(ctx, job)
					case <-ctx.Done():
						// Drain any remaining buffered jobs in fastLaneCh before exiting
						detachedCtx := context.WithoutCancel(ctx)
						for {
							select {
							case job := <-s.fastLaneCh:
								s.executeFastLaneJob(detachedCtx, job)
							default:
								return
							}
						}
					}
				}
			}()
		}
		wg.Wait()
		return nil
	}
}

func (s *WebhookService) executeFastLaneJob(ctx context.Context, job FastLaneJob) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error(platform.LogEventPanicRecoveredDLQ, zap.Any(platform.LogFieldPanic, r))
			d := job.Delivery
			d.Status = domain.StatusDLQ
			errStr := fmt.Sprintf("%v: %v", domain.ErrPanic, r)
			if dbErr := s.repo.FailDeliveryAndRouteToDLQ(context.WithoutCancel(ctx), job.ShardID, d, errStr, pii.Mask(d.Payload), platform.NewEventID(), d.CreatedAt, time.Now()); dbErr != nil {
				s.logger.Error(platform.LogEventPanicDLQWriteFailed, zap.Error(dbErr))
			}
		}
	}()
	s.processFastLaneJob(ctx, job)
}

func (s *WebhookService) processFastLaneJob(ctx context.Context, job FastLaneJob) {
	merchant := job.Merchant
	var err error

	// If the job came from the retry scheduler, we need to fetch the merchant config
	if merchant == nil {
		merchant, err = s.repo.GetMerchantConfig(ctx, job.Delivery.MerchantID)
		if err != nil {
			platform.LoggerWithTrace(ctx, s.logger).Error(domain.LogEventMerchantRetry, zap.String(domain.JSONKeyMerchantID, job.Delivery.MerchantID), zap.Error(err))
			return
		}
		if merchant == nil {
			// Merchant was DELETED! Route to global DLQ and mark as DLQ in the shard to stop fetching.
			d := job.Delivery
			d.Status = domain.StatusDLQ

			if dbErr := s.repo.FailDeliveryAndRouteToDLQ(context.WithoutCancel(ctx), job.ShardID, d, domain.ErrorMerchantLookupFailed, pii.Mask(d.Payload), platform.NewEventID(), d.CreatedAt, time.Now()); dbErr != nil {
				platform.LoggerWithTrace(ctx, s.logger).Error(platform.LogEventDLQWriteFailed, zap.String(platform.LogFieldShardID, job.ShardID), zap.Error(dbErr))
			}
			return
		}
	}

	attempt := job.Delivery.AttemptCount + 1
	// 1. HTTP POST wrapped in Circuit Breaker (via decorator)
	start := time.Now()
	statusCode, err := s.attemptDelivery(ctx, merchant, job.Delivery.SourceEventID, job.Delivery.Payload, attempt)
	latencyMs := float64(time.Since(start).Milliseconds())

	// 2. State update based on result
	if err != nil {
		if errors.Is(err, circuitbreaker.ErrOpen) {
			if job.Delivery.LastError != nil {
				err = errors.New(*job.Delivery.LastError)
			} else {
				err = errors.New("circuit breaker is open (no previous error available)")
			}
		}
		firstFailed := time.Now()
		if !job.Delivery.CreatedAt.IsZero() {
			firstFailed = job.Delivery.CreatedAt
		}
		s.handleFailure(ctx, job.ShardID, merchant, job.Delivery.ID, job.Delivery.SourceEventID, job.Delivery.Payload, attempt, err, firstFailed, latencyMs)
	} else {
		s.recordSuccess(ctx, job.ShardID, merchant, job.Delivery.ID, job.Delivery.SourceEventID, job.Delivery.Payload, statusCode, attempt, latencyMs)
	}
}

// HandleMessage processes a fresh message from Kafka.
// Delivery is attempted once. If it fails, it is scheduled for retry in Postgres.
func (s *WebhookService) HandleMessage(ctx context.Context, merchantID string, payload []byte) error {
	var env eventsv1.EventEnvelope
	if err := proto.Unmarshal(payload, &env); err != nil {
		s.repo.RouteToGlobalDLQ(ctx, payload, err.Error())
		platform.LoggerWithTrace(ctx, s.logger).Error(domain.LogEventFailedUnmarshal, zap.Error(err))
		return nil
	}
	eventID := env.EventId

	merchant, err := s.repo.GetMerchantConfig(ctx, merchantID)
	if err != nil {
		return err
	}
	if merchant == nil {
		err = s.repo.RouteToGlobalDLQ(ctx, payload, domain.ErrorMerchantLookupFailed)
		if err != nil {
			return err
		}
		platform.LoggerWithTrace(ctx, s.logger).Warn(
			domain.LogEventMerchantLookup,
			zap.String(domain.JSONKeyMerchantID, merchantID),
			zap.String(platform.LogFieldEventID, eventID),
		)
		return nil
	}

	if merchant.Status != domain.StatusActive {
		return nil
	}

	deliveryID := platform.NewDeterministicDeliveryID(eventID, merchantID)
	now := time.Now()
	nextRetry := now.Add(domain.FastLaneGracePeriod)

	d := &domain.WebhookDelivery{
		ID:            deliveryID,
		MerchantID:    merchant.ID,
		SourceEventID: eventID,
		URL:           merchant.WebhookURL,
		Payload:       payload,
		Signature:     domain.ComputeHMACSHA256(merchant.WebhookSecret, "", payload),
		AttemptCount:  0, // Fast-lane worker will increment this to 1
		NextRetryAt:   &nextRetry,
		Status:        domain.StatusPending,
		CreatedAt:     now,
	}

	// 1. Synchronously persist to DB with infinite retries to prevent Kafka data loss
	err = s.repo.CreatePendingDelivery(ctx, merchant.ShardID, d)
	if err != nil {
		return err // Context canceled
	}

	// 2. Dispatch to Fast-Lane channel
	select {
	case s.fastLaneCh <- FastLaneJob{ShardID: merchant.ShardID, Delivery: d, Merchant: merchant}:
		// Successfully queued for immediate zero-latency delivery
	default:
		// Channel full (traffic spike). Safe to drop; RetryScheduler will pick it up.
	}

	return nil
}

func (s *WebhookService) attemptDelivery(ctx context.Context, m *domain.Merchant, eventID string, payload []byte, attempt int) (int, error) {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	sig := domain.ComputeHMACSHA256(m.WebhookSecret, timestamp, payload)
	return s.client.Post(ctx, m.ID, m.WebhookURL, payload, sig, timestamp, eventID, attempt)
}

func (s *WebhookService) handleFailure(ctx context.Context, shardID string, merchant *domain.Merchant, deliveryID, eventID string, payload []byte, attempt int, err error, firstFailedAt time.Time, latencyMs float64) {
	// Use detached context for terminal writes to prevent state corruption on shutdown
	detachedCtx := context.WithoutCancel(ctx)
	now := time.Now()
	log := platform.LoggerWithTrace(ctx, s.logger)
	lastErr := err.Error()

	d := domain.WebhookDelivery{
		ID:            deliveryID,
		MerchantID:    merchant.ID,
		SourceEventID: eventID,
		URL:           merchant.WebhookURL,
		Payload:       payload,
		Signature:     domain.ComputeHMACSHA256(merchant.WebhookSecret, "", payload),
		AttemptCount:  attempt,
		LastAttemptAt: &now,
		LastError:     &lastErr,
	}

	isTerminal := platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationTerminal

	if attempt >= domain.MaxDeliveryAttempts || isTerminal {
		d.Status = domain.StatusDLQ
		if dbErr := s.repo.FailDeliveryAndRouteToDLQ(detachedCtx, shardID, &d, lastErr, pii.Mask(payload), platform.NewEventID(), firstFailedAt, now); dbErr != nil {
			log.Error(platform.LogEventDLQWriteFailed, zap.Error(dbErr))
		}
		platform.LogCanonicalEvent(ctx, s.logger, platform.ServiceNameWebhookWorker, platform.CanonicalLogLine{
			Event:         platform.EventWebhookDLQ,
			Status:        platform.StatusFailed,
			MerchantID:    merchant.ID,
			TransferID:    eventID,
			HTTPLatencyMs: latencyMs,
			RetryCount:    attempt - 1,
			ErrorMessage:  lastErr,
		})
		return
	}

	// 2) Otherwise, schedule a retry
	d.Status = domain.StatusPending
	// exponential backoff with full jitter
	base := float64(domain.BaseRetryDelaySeconds)
	cap := float64(domain.CapRetryDelaySeconds)
	delaySeconds := base * math.Pow(2, float64(attempt-1))
	if delaySeconds > cap {
		delaySeconds = cap
	}
	// locally seeded PRNG to avoid global lock contention or unseeded deterministic fallback
	localRand := rand.New(rand.NewSource(now.UnixNano()))
	jitterDelay := localRand.Float64() * delaySeconds
	nextRetryAt := now.Add(time.Duration(jitterDelay) * time.Second)
	d.NextRetryAt = &nextRetryAt

	if dbErr := s.repo.ScheduleRetry(detachedCtx, shardID, &d, pii.Mask(payload), platform.NewEventID()); dbErr != nil {
		log.Error(platform.LogEventDLQWriteFailed, zap.Error(dbErr))
	}
	
	platform.LogCanonicalEvent(ctx, s.logger, platform.ServiceNameWebhookWorker, platform.CanonicalLogLine{
		Event:         platform.EventWebhookFailed,
		Status:        platform.StatusRetry,
		MerchantID:    merchant.ID,
		TransferID:    eventID,
		HTTPLatencyMs: latencyMs,
		RetryCount:    attempt - 1,
		ErrorMessage:  lastErr,
	})
}

func (s *WebhookService) recordSuccess(ctx context.Context, shardID string, merchant *domain.Merchant, deliveryID, eventID string, payload []byte, statusCode, attempt int, latencyMs float64) {
	log := platform.LoggerWithTrace(ctx, s.logger)
	now := time.Now()
	// Detached context ensures graceful shutdown does not corrupt state
	detachedCtx := context.WithoutCancel(ctx)
	delivery := &domain.WebhookDelivery{
		ID:            deliveryID,
		MerchantID:    merchant.ID,
		SourceEventID: eventID,
		URL:           merchant.WebhookURL,
		Payload:       payload,
		Signature:     domain.ComputeHMACSHA256(merchant.WebhookSecret, "", payload),
		AttemptCount:  attempt,
		LastAttemptAt: &now,
		Status:        domain.StatusDelivered,
		DeliveredAt:   &now,
	}

	if err := s.repo.CompleteDelivery(detachedCtx, shardID, delivery, pii.Mask(payload), platform.NewEventID()); err != nil {
		log.Error(platform.LogEventDLQWriteFailed, zap.Error(err))
	}

	// Canonical log: webhook delivered
	platform.LogCanonicalEvent(ctx, s.logger, platform.ServiceNameWebhookWorker, platform.CanonicalLogLine{
		Event:         platform.EventWebhookDelivered,
		Status:        platform.StatusSuccess,
		MerchantID:    merchant.ID,
		TransferID:    eventID,
		HTTPLatencyMs: latencyMs,
		RetryCount:    attempt - 1,
	})
}

// RetryScheduler polls Postgres for due retries and attempts them.
func (s *WebhookService) RetryScheduler(ctx context.Context) error {
	ticker := time.NewTicker(domain.SchedulerPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.processDueRetries(ctx)
		}
	}
}

func (s *WebhookService) processDueRetries(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error(platform.LogEventPanicRecovered, zap.Any(platform.LogFieldPanic, r))
		}
	}()

	for _, shardID := range s.repo.GetAvailableShardIDs() {
		deliveries, err := s.repo.FetchPendingRetries(ctx, shardID, domain.SchedulerBatchSize)
		if err != nil {
			platform.LoggerWithTrace(ctx, s.logger).Error(domain.LogEventFetchRetries, zap.Error(err), zap.String(platform.LogFieldShardID, shardID))
			continue
		}

		var wg sync.WaitGroup
		for _, d := range deliveries {
			d := d // Capture loop variable for goroutine

			wg.Add(1)
			go func() {
				defer wg.Done()

				// Panic isolation for individual retry
				defer func() {
					if r := recover(); r != nil {
						s.logger.Error(platform.LogEventPanicRecoveredDLQ, zap.Any(platform.LogFieldPanic, r))
						d.Status = domain.StatusDLQ
						errStr := fmt.Sprintf("%v: %v", domain.ErrPanic, r)
						if dbErr := s.repo.FailDeliveryAndRouteToDLQ(context.WithoutCancel(ctx), shardID, d, errStr, pii.Mask(d.Payload), platform.NewEventID(), d.CreatedAt, time.Now()); dbErr != nil {
							s.logger.Error(platform.LogEventPanicDLQWriteFailed, zap.Error(dbErr))
						}
					}
				}()

				// Use detached context with timeout to ensure drain completes on shutdown
				drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), domain.ServerShutdownTimeout)
				defer cancel()

				s.processFastLaneJob(drainCtx, FastLaneJob{ShardID: shardID, Delivery: d, Merchant: nil})
			}()
		}
		wg.Wait()
	}
}

// RouteToGlobalDLQ allows the consumer to write poison pills (e.g. panics) directly to the global DLQ
func (s *WebhookService) RouteToGlobalDLQ(ctx context.Context, payload []byte, errorMsg string) error {
	return s.repo.RouteToGlobalDLQ(ctx, payload, errorMsg)
}
