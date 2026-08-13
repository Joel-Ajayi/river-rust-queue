package observability

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/port"
	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

type merchantDirectoryMetrics struct {
	next port.MerchantDirectory
}

func NewMerchantDirectoryMetrics(next port.MerchantDirectory) port.MerchantDirectory {
	return &merchantDirectoryMetrics{next: next}
}

func (m *merchantDirectoryMetrics) ShardFor(ctx context.Context, merchantID string) (string, error) {
	shardID, err := m.next.ShardFor(ctx, merchantID)
	if err != nil {
		if platform.ClassifyError(err, func(error) bool { return false }) == platform.ClassificationInfrastructure {
			platform.RecordInfrastructureError(ctx, platform.ComponentMerchantDirectory)
		}
	}
	return shardID, err
}

type walletRepositoryMetrics struct {
	next port.WalletRepository
}

func NewWalletRepositoryMetrics(next port.WalletRepository) port.WalletRepository {
	return &walletRepositoryMetrics{next: next}
}

func (w *walletRepositoryMetrics) GetWalletStatus(ctx context.Context, shardID string, walletID string) (string, error) {
	res, err := w.next.GetWalletStatus(ctx, shardID, walletID)
	if err != nil {
		if platform.ClassifyError(err, func(error) bool { return false }) == platform.ClassificationInfrastructure {
			platform.RecordInfrastructureError(ctx, platform.ComponentWalletDirectory)
		}
	}
	return res, err
}

func (w *walletRepositoryMetrics) FreezeWallet(ctx context.Context, shardID string, walletID string, reason string) error {
	err := w.next.FreezeWallet(ctx, shardID, walletID, reason)
	if err != nil {
		if platform.ClassifyError(err, func(error) bool { return false }) == platform.ClassificationInfrastructure {
			platform.RecordInfrastructureError(ctx, platform.ComponentWalletDirectory)
		}
	}
	return err
}

type jobHandlerMetrics struct {
	next port.JobHandler
}

func NewJobHandlerMetrics(next port.JobHandler) port.JobHandler {
	return &jobHandlerMetrics{next: next}
}

func (m *jobHandlerMetrics) ProcessJob(ctx context.Context, payload *eventsv1.JobRequestedPayload, eventID string, occurredAt int64) error {
	err := m.next.ProcessJob(ctx, payload, eventID, occurredAt)
	if err != nil {
		if platform.ClassifyError(err, func(error) bool { return false }) == platform.ClassificationInfrastructure {
			platform.RecordInfrastructureError(ctx, platform.ComponentJobHandler)
		}
	}
	return err
}

type redisStoreMetrics struct {
	next port.RedisStore
}

func NewRedisStoreMetrics(next port.RedisStore) port.RedisStore {
	return &redisStoreMetrics{next: next}
}

func (m *redisStoreMetrics) UpdateVelocity(ctx context.Context, walletID string, eventID string, timestampMs int64, windowMs int) (int, error) {
	count, err := m.next.UpdateVelocity(ctx, walletID, eventID, timestampMs, windowMs)
	if err != nil {
		if platform.ClassifyError(err, func(error) bool { return false }) == platform.ClassificationInfrastructure {
			platform.RecordInfrastructureError(ctx, platform.ComponentRedis)
		}
	}
	return count, err
}

type dlqStoreMetrics struct {
	next port.DLQStore
}

func NewDLQStoreMetrics(next port.DLQStore) port.DLQStore {
	return &dlqStoreMetrics{next: next}
}

func (m *dlqStoreMetrics) WriteDLQEntry(ctx context.Context, shardID string, entry domain.DLQEntry) error {
	err := m.next.WriteDLQEntry(ctx, shardID, entry)
	if err != nil {
		if platform.ClassifyError(err, func(error) bool { return false }) == platform.ClassificationInfrastructure {
			platform.RecordInfrastructureError(ctx, platform.ComponentDLQStore)
		}
	}
	return err
}
