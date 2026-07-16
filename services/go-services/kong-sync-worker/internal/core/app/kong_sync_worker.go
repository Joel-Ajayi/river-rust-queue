package app

import (
	"context"
	"sync"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/kong-sync-worker/internal/core/port"
	"go.uber.org/zap"
)

type KongSyncWorker struct {
	kongGateway       port.KongGateway
	merchantDirectory port.MerchantDirectory
	interval          time.Duration
	logger            *zap.Logger
	wg                sync.WaitGroup
}

func NewKongSyncWorker(kongGateway port.KongGateway, merchantDir port.MerchantDirectory, interval time.Duration, logger *zap.Logger) *KongSyncWorker {
	return &KongSyncWorker{
		kongGateway:       kongGateway,
		merchantDirectory: merchantDir,
		interval:          interval,
		logger:            logger,
	}
}

func (w *KongSyncWorker) Start(ctx context.Context) {
	w.logger.Info("Starting Kong Sync Worker...")
	w.wg.Add(1)
	defer w.wg.Done()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	// Run once immediately
	w.sync(ctx)

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Kong Sync Worker stopped via context cancellation")
			return
		case <-ticker.C:
			w.sync(ctx)
		}
	}
}

func (w *KongSyncWorker) Wait() {
	w.wg.Wait()
}

func (w *KongSyncWorker) sync(ctx context.Context) {
	w.logger.Info("Syncing merchants with Kong...")

	merchants, err := w.merchantDirectory.GetActiveMerchants(ctx)
	if err != nil {
		w.logger.Error("Failed to get active merchants", zap.Error(err))
		return
	}

	for _, m := range merchants {
		if err := w.kongGateway.SyncConsumer(ctx, m.ID); err != nil {
			w.logger.Error("Failed to sync consumer", zap.String("merchant_id", m.ID), zap.Error(err))
			continue
		}

		if err := w.kongGateway.SyncRateLimitPlugin(ctx, m.ID, m.Tier); err != nil {
			w.logger.Error("Failed to sync rate limit plugin", zap.String("merchant_id", m.ID), zap.Error(err))
			continue
		}

		if err := w.kongGateway.SyncJWTCredential(ctx, m.ID); err != nil {
			w.logger.Error("Failed to sync JWT credential", zap.String("merchant_id", m.ID), zap.Error(err))
		}
	}
}
