package ports

import (
	"context"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
)

// LedgerStore handles single-shard transactions.
type LedgerStore interface {
	PostTransfer(ctx context.Context, shardID string, transfer domain.Transfer) error
	FailTransfer(ctx context.Context, shardID string, transfer domain.Transfer, reason string) error
}

// CrossShardStore handles the multi-shard saga steps.
type CrossShardStore interface {
	DebitToClearingAccount(ctx context.Context, srcShard, dstShard, jobID string, transfer domain.Transfer) error
	CreditFromClearingAccount(ctx context.Context, intent *eventsv1.XShardTransferRequestedPayload) error
	SettleCrossShardTransfer(ctx context.Context, srcShard, transferID string) error
	ReverseCrossShardTransfer(ctx context.Context, srcShard, transferID, reason string) error
}

// DLQStore is for failed messages that can't be retried.
type DLQStore interface {
	WriteDLQEntry(ctx context.Context, shardID string, msg []byte) error
}

// MerchantDirectory tells us about merchants, used to enforce per-merchant rules.
type MerchantDirectory interface {
	ShardFor(ctx context.Context, merchantID string) (string, error)
}
