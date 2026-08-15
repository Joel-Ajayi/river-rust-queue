package postgres

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/port"
	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.uber.org/zap"
)

type dlqStore struct {
	pools       *platform.ShardPools
	logger      *zap.Logger
	dlqRetryCfg platform.RetryConfig
	component   string
}

var _ port.DLQStore = (*dlqStore)(nil)

func NewDLQStore(pools *platform.ShardPools, logger *zap.Logger, dlqRetryCfg platform.RetryConfig, component string) *dlqStore {
	return &dlqStore{
		pools:       pools,
		logger:      logger,
		dlqRetryCfg: dlqRetryCfg,
		component:   component,
	}
}

func (s *dlqStore) WriteDLQEntry(ctx context.Context, entry *eventsv1.DLQEntry) error {
	pool := s.pools.MerchantsPool()
	if pool == nil {
		return platform.ErrMerchantsPoolNotInitialized
	}
	return platform.WriteDLQEntryWithRetry(ctx, s.logger, pool, entry, s.dlqRetryCfg, s.component)
}
