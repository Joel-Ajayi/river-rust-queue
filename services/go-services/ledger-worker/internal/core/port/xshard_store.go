package port

import (
	"context"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
)

// CrossShardStore handles the multi-shard saga steps.
type CrossShardStore interface {
	DebitToClearingAccount(ctx context.Context, srcShard, dstShard, jobID string, transfer domain.Transfer) error
	CreditFromClearingAccount(ctx context.Context, intent *eventsv1.XShardTransferRequestedPayload) error
	SettleCrossShardTransfer(ctx context.Context, srcShard, transferID string) error
	ReverseCrossShardTransfer(ctx context.Context, srcShard, transferID, reason string) error
}
