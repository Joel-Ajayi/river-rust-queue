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
	if payload.JobType != platform.JobTypeTransfer {
		return nil
	}

	transferData := payload.GetTransferData()
	if transferData == nil {
		return nil
	}

	// Account for deposit transfers, check destination wallet if source wallet is empty
	event := domain.Event{
		EventID:   eventID,
		WalletID:  transferData.FromWallet,
		Timestamp: occurredAt,
	}
	if transferData.FromWallet == "" {
		event.WalletID = transferData.ToWallet
	}

	return s.processEvent(ctx, payload.MerchantId, event)
}

func (s *FraudService) processEvent(ctx context.Context, merchantID string, event domain.Event) error {
	shardID, err := s.merchantDir.ShardFor(ctx, merchantID)
	if err != nil {
		s.logger.Error(platform.LogEventMerchantLookupFailed, zap.String(platform.MetricLabelMerchantID, merchantID), zap.Error(err))
		return err
	}

	for _, rule := range s.rules {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		count, err := s.redis.UpdateVelocity(ctx, event.WalletID, event.EventID, event.Timestamp, rule.WindowMs)
		if err != nil {
			// Record fail-closed metric so operators can distinguish Redis outages
			// from legitimate business-rule rejections (see issue 27).
			platform.RecordRedisFailClosed(ctx)
			s.logger.Error(platform.LogEventRedisVelocityUpdateFailed, zap.String(platform.MetricLabelWalletID, event.WalletID), zap.Error(err))
			return err
		}

		if count >= rule.Threshold {
			// Canonical log: velocity limit exceeded
			platform.LogCanonicalEvent(ctx, s.logger, platform.ServiceNameFraudWorker, platform.CanonicalLogLine{
				Event:        platform.EventFraudVelocityCheck,
				Status:       platform.StatusFailed,
				MerchantID:   merchantID,
				WalletID:     event.WalletID,
				ErrorCode:    platform.ErrorCodeVelocityLimitExceeded,
				ErrorMessage: fmt.Sprintf("rule=%s count=%d threshold=%d", rule.Name, count, rule.Threshold),
			})

			// Metric: velocity limit exceeded
			platform.RecordVelocityLimitExceeded(ctx, event.WalletID, rule.Name)

			if freezeErr := s.maybeFreezeWallet(ctx, shardID, event.WalletID, rule); freezeErr != nil {
				return freezeErr
			}
		}
	}
	return nil
}

func (s *FraudService) maybeFreezeWallet(ctx context.Context, shardID, walletID string, rule domain.VelocityRule) error {
	status, err := s.repo.GetWalletStatus(ctx, shardID, walletID)
	if err != nil {
		s.logger.Error(platform.LogEventWalletStatusCheckFailed, zap.String(platform.MetricLabelWalletID, walletID), zap.Error(err))
		return err
	}

	if status != platform.WalletStatusActive {
		return nil
	}

	s.logger.Warn(string(platform.EventFraudWalletFrozen), zap.String(platform.MetricLabelWalletID, walletID), zap.String(platform.LogFieldName, rule.Name))
	err = s.repo.FreezeWallet(ctx, shardID, walletID, rule.Reason)
	if err != nil {
		s.logger.Error(platform.LogEventWalletFreezeFailed, zap.String(platform.MetricLabelWalletID, walletID), zap.Error(err))
		return err
	}
	return nil
}
