package port

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/recon-worker/internal/core/domain"
)

type ReconRepository interface {
	AcquireLock(ctx context.Context) (bool, error)
	ReleaseLock(ctx context.Context) error
	GetShardSum(ctx context.Context, shardID string, cutoff time.Time) (int64, error)
	FindAffectedWallets(ctx context.Context, shardID string, start, cutoff time.Time) ([]string, error)
	CheckWallet(ctx context.Context, shardID string, walletID string, cutoff time.Time) (*domain.Discrepancy, error)
	CheckTransferLegs(ctx context.Context, shardID string, cutoff time.Time) ([]string, error)
	PersistReport(ctx context.Context, shardID string, report *domain.Report) error
}
