package app

import (
	"context"
	"fmt"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/port"
	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.uber.org/zap"
)

type FraudService struct {
	logger      *zap.Logger
	repo        port.WalletRepository
	redis       port.RedisStore
	merchantDir port.MerchantDirectory
	rules       []domain.VelocityRule
}

var _ port.JobHandler = (*FraudService)(nil)

func NewFraudService(
	logger *zap.Logger,
	repo port.WalletRepository,
	redis port.RedisStore,
	merchantDir port.MerchantDirectory,
	rules []domain.VelocityRule,
) *FraudService {
	return &FraudService{
		logger:      logger,
		repo:        repo,
		redis:       redis,
		merchantDir: merchantDir,
		rules:       rules,
	}
}

func (s *FraudService) ProcessJob(ctx context.Context, payload *eventsv1.JobRequestedPayload, eventID string, occurredAt int64) error {
	// 1. Filter out non-transfer job types
	if payload.JobType != platform.JobTypeTransfer {
		return nil
	}

	// 2. Validate transfer payload
	transferData := payload.GetTransferData()
	if transferData == nil {
		return nil
	}

	// 3. Construct fraud evaluation event (handling both deposits and standard transfers)
	event := domain.Event{
		EventID:   eventID,
		WalletID:  transferData.FromWallet,
		Timestamp: occurredAt,
	}
	if transferData.FromWallet == "" {
		event.WalletID = transferData.ToWallet
	}

	// 4. Run velocity checks against all configured fraud rules
	return s.processEvent(ctx, payload.MerchantId, event)
}

func (s *FraudService) processEvent(ctx context.Context, merchantID string, event domain.Event) error {
	// 1. Resolve database shard for merchant
	shardID, err := s.merchantDir.ShardFor(ctx, merchantID)
	if err != nil {
		platform.LoggerWithTrace(ctx, s.logger).Error(platform.LogEventMerchantLookupFailed,
			zap.String(platform.LogFieldMerchantID, merchantID),
			zap.Error(err),
		)
		return err
	}

	// 2. Evaluate sliding-window transaction count in Redis for each velocity rule
	for _, rule := range s.rules {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		count, err := s.redis.UpdateVelocity(ctx, event.WalletID, event.EventID, event.Timestamp, rule.WindowMs)
		if err != nil {
			platform.RecordRedisFailClosed(ctx)
			platform.LoggerWithTrace(ctx, s.logger).Error(platform.LogEventRedisVelocityUpdateFailed,
				zap.String(platform.LogFieldWalletID, event.WalletID),
				zap.Error(err),
			)
			return err
		}

		// 3. Check if velocity threshold was exceeded
		if count >= rule.Threshold {
			platform.LoggerWithTrace(ctx, s.logger).Warn(platform.LogEventVelocityLimitExceeded,
				zap.String(platform.LogFieldMerchantID, merchantID),
				zap.String(platform.LogFieldWalletID, event.WalletID),
				zap.String(platform.LogFieldName, rule.Name),
				zap.Int(platform.LogFieldCount, count),
				zap.Int(platform.LogFieldThreshold, rule.Threshold),
			)

			// 4. Emit canonical log and record metrics
			platform.LogCanonicalEvent(ctx, s.logger, platform.ServiceNameFraudWorker, platform.CanonicalLogLine{
				Event:        platform.EventFraudVelocityCheck,
				Status:       platform.StatusFailed,
				MerchantID:   merchantID,
				WalletID:     event.WalletID,
				ErrorCode:    platform.ErrorCodeVelocityLimitExceeded,
				ErrorMessage: fmt.Sprintf("rule=%s count=%d threshold=%d", rule.Name, count, rule.Threshold),
			})

			platform.RecordVelocityLimitExceeded(ctx, event.WalletID, rule.Name)

			// 5. Freeze wallet if currently active
			if freezeErr := s.maybeFreezeWallet(ctx, shardID, event.WalletID, rule); freezeErr != nil {
				return freezeErr
			}
		}
	}
	return nil
}

func (s *FraudService) maybeFreezeWallet(ctx context.Context, shardID, walletID string, rule domain.VelocityRule) error {
	// 1. Fetch current wallet status from shard database
	status, err := s.repo.GetWalletStatus(ctx, shardID, walletID)
	if err != nil {
		platform.LoggerWithTrace(ctx, s.logger).Error(platform.LogEventWalletStatusCheckFailed,
			zap.String(platform.LogFieldWalletID, walletID),
			zap.Error(err),
		)
		return err
	}

	if status != platform.WalletStatusActive {
		return nil
	}

	// 2. Log freeze warning and update wallet status to frozen in database
	platform.LoggerWithTrace(ctx, s.logger).Warn(string(platform.EventFraudWalletFrozen),
		zap.String(platform.LogFieldWalletID, walletID),
		zap.String(platform.LogFieldName, rule.Name),
	)
	err = s.repo.FreezeWallet(ctx, shardID, walletID, rule.Reason)
	if err != nil {
		platform.LoggerWithTrace(ctx, s.logger).Error(platform.LogEventWalletFreezeFailed,
			zap.String(platform.LogFieldWalletID, walletID),
			zap.Error(err),
		)
		return err
	}
	return nil
}
