package app

import (
	"context"
	"encoding/json"
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform/pii"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/adapter/outbound/resilience"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/port"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type WebhookService struct {
	repo     port.Repository
	client   port.HTTPClient
	breakers *resilience.BreakerRegistry
	logger   *zap.Logger
}

func NewWebhookService(repo port.Repository, client port.HTTPClient, breakers *resilience.BreakerRegistry, logger *zap.Logger) *WebhookService {
	return &WebhookService{
		repo:     repo,
		client:   client,
		breakers: breakers,
		logger:   logger,
	}
}

// HandleMessage processes a fresh message from Kafka.
func (s *WebhookService) HandleMessage(ctx context.Context, merchantID string, payload []byte) error {
	// 1. Unmarshal to extract event ID.
	var env eventsv1.EventEnvelope
	if err := proto.Unmarshal(payload, &env); err != nil {
		return err
	}
	eventID := env.EventId

	merchant, err := s.repo.GetMerchantConfig(ctx, merchantID)
	if err != nil || merchant == nil {
		availableShards := s.repo.GetAvailableShardIDs()
		if len(availableShards) == 0 {
			return nil
		}
		shardID := availableShards[time.Now().UnixNano()%int64(len(availableShards))]

		return s.repo.RouteToDLQ(ctx, shardID, domain.EventSourceWebhook, pii.Mask(payload), domain.ErrorMerchantLookupFailed, domain.ZeroDeliveryAttempt, time.Now(), time.Now())
	}

	if merchant.Status != domain.StatusActive {
		return nil
	}

	breaker := s.breakers.For(merchantID)

	// Create canonical JSON bytes using protojson to send valid application/json.
	canonicalPayload, marshalErr := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: true,
	}.Marshal(&env)
	if marshalErr != nil {
		return marshalErr
	}

	deliveryID := platform.NewDeliveryID()

	result, err := breaker.Execute(func() (interface{}, error) {
		return s.attemptDelivery(ctx, merchant, eventID, canonicalPayload, domain.InitialDeliveryAttempt)
	})

	if err != nil {
		s.scheduleRetry(ctx, merchant, deliveryID, eventID, canonicalPayload, domain.InitialDeliveryAttempt, err)
	} else {
		statusCode := result.(int)
		s.recordSuccess(ctx, merchant.ShardID, deliveryID, merchant.ID, eventID, canonicalPayload, statusCode, domain.InitialDeliveryAttempt)
	}

	return nil
}

func (s *WebhookService) attemptDelivery(ctx context.Context, m *domain.Merchant, eventID string, payload []byte, attempt int) (int, error) {
	sig := domain.ComputeHMACSHA256(m.WebhookSecret, payload)
	return s.client.Post(ctx, m.WebhookURL, payload, sig, eventID, attempt)
}

func (s *WebhookService) scheduleRetry(ctx context.Context, m *domain.Merchant, deliveryID, eventID string, payload []byte, attempt int, err error) {
	now := time.Now()
	errStr := err.Error()
	if attempt >= domain.MaxDeliveryAttempts {
		log := platform.LoggerWithTrace(ctx, s.logger)
		if err := s.repo.RouteToDLQ(ctx, m.ShardID, domain.EventSourceWebhook, pii.Mask(payload), err.Error(), attempt, now, now); err != nil {
			log.Error("failed to route to DLQ", zap.String(platform.LogFieldEvent, "dlq_route_failed"), zap.Error(err))
		}

		failPayload, marshalErr := json.Marshal(map[string]interface{}{
			domain.JSONKeyDeliveryID:    deliveryID,
			domain.JSONKeyMerchantID:    m.ID,
			domain.JSONKeySourceEventID: eventID,
			domain.JSONKeyAttemptCount:  attempt,
			domain.JSONKeyLastError:     err.Error(),
		})
		if marshalErr != nil {
			log.Error("failed to marshal webhook failed event", zap.Error(marshalErr))
		}
		if err := s.repo.RecordEvent(ctx, m.ShardID, platform.NewEventID(), domain.EventTypeWebhookFailed, string(platform.AggregateTypeWebhook), eventID, failPayload); err != nil {
			log.Error("failed to record webhook failed event", zap.Error(err))
		}

		delivery := &domain.WebhookDelivery{
			ID:            deliveryID,
			MerchantID:    m.ID,
			SourceEventID: eventID,
			URL:           m.WebhookURL,
			Payload:       payload,
			Signature:     domain.ComputeHMACSHA256(m.WebhookSecret, payload),
			AttemptCount:  attempt,
			LastAttemptAt: &now,
			LastError:     &errStr,
			Status:        domain.StatusDLQ,
		}
		if err := s.repo.SaveDelivery(ctx, m.ShardID, delivery); err != nil {
			log.Error("failed to save delivery DLQ state", zap.Error(err))
		}
		return
	}

	// Full jitter calculation using platform package
	jitterDelay := platform.CalculateJitterBackoff(attempt, domain.BaseRetryDelaySeconds*time.Second, domain.CapRetryDelaySeconds*time.Second)
	nextRetry := now.Add(jitterDelay)

	delivery := &domain.WebhookDelivery{
		ID:            deliveryID,
		MerchantID:    m.ID,
		SourceEventID: eventID,
		URL:           m.WebhookURL,
		Payload:       payload,
		Signature:     domain.ComputeHMACSHA256(m.WebhookSecret, payload),
		AttemptCount:  attempt,
		LastAttemptAt: &now,
		NextRetryAt:   &nextRetry,
		LastError:     &errStr,
		Status:        domain.StatusPending,
	}

	if err := s.repo.SaveDelivery(ctx, m.ShardID, delivery); err != nil {
		platform.LoggerWithTrace(ctx, s.logger).Error("failed to save pending delivery", zap.Error(err))
	}
}

func (s *WebhookService) recordSuccess(ctx context.Context, shardID, deliveryID, merchantID, eventID string, payload []byte, statusCode, attempt int) {
	log := platform.LoggerWithTrace(ctx, s.logger)
	now := time.Now()
	delivery := &domain.WebhookDelivery{
		ID:            deliveryID,
		MerchantID:    merchantID,
		SourceEventID: eventID,
		URL:           "",
		Payload:       payload,
		Signature:     "",
		AttemptCount:  attempt,
		LastAttemptAt: &now,
		LastStatus:    &statusCode,
		Status:        domain.StatusDelivered,
		DeliveredAt:   &now,
	}
	if err := s.repo.SaveDelivery(ctx, shardID, delivery); err != nil {
		log.Error("failed to save delivered webhook state", zap.Error(err))
	}

	successPayload, marshalErr := json.Marshal(map[string]interface{}{
		domain.JSONKeyDeliveryID:    delivery.ID,
		domain.JSONKeyMerchantID:    merchantID,
		domain.JSONKeySourceEventID: eventID,
		domain.JSONKeyStatusCode:    statusCode,
		domain.JSONKeyAttemptCount:  attempt,
		domain.JSONKeyDeliveredAt:   now.Format(time.RFC3339),
	})
	if marshalErr != nil {
		log.Error("failed to marshal success payload", zap.Error(marshalErr))
	}
	if err := s.repo.RecordEvent(ctx, shardID, platform.NewEventID(), domain.EventTypeWebhookDelivered, string(platform.AggregateTypeWebhook), delivery.ID, successPayload); err != nil {
		log.Error("failed to record webhook delivered event", zap.Error(err))
	}

	// Canonical log: webhook delivered
	platform.LogCanonicalEvent(ctx, s.logger, platform.ServiceNameWebhookWorker, platform.CanonicalLogLine{
		Event:         platform.EventWebhookDelivered,
		Status:        platform.StatusSuccess,
		MerchantID:    merchantID,
		TransferID:    eventID,
		HTTPLatencyMs: 0, // Not tracked here
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
	for _, shardID := range s.repo.GetAvailableShardIDs() {
		deliveries, err := s.repo.FetchPendingRetries(ctx, shardID, domain.SchedulerBatchSize)
		if err != nil {
			platform.LoggerWithTrace(ctx, s.logger).Error(domain.LogEventFetchRetries, zap.Error(err), zap.String("shardID", shardID))
			continue
		}

		for _, d := range deliveries {
			merchant, err := s.repo.GetMerchantConfig(ctx, d.MerchantID)
			if err != nil || merchant == nil {
				platform.LoggerWithTrace(ctx, s.logger).Error(domain.LogEventMerchantRetry, zap.String(domain.JSONKeyMerchantID, d.MerchantID))
				continue
			}

			breaker := s.breakers.For(d.MerchantID)

			result, err := breaker.Execute(func() (interface{}, error) {
				return s.attemptDelivery(ctx, merchant, d.SourceEventID, d.Payload, d.AttemptCount+1)
			})

			if err != nil {
				s.scheduleRetry(ctx, merchant, d.ID, d.SourceEventID, d.Payload, d.AttemptCount+1, err)
			} else {
				statusCode := result.(int)
				s.recordSuccess(ctx, merchant.ShardID, d.ID, merchant.ID, d.SourceEventID, d.Payload, statusCode, d.AttemptCount+1)
			}
		}
	}
}
