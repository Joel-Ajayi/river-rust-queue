package port

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
)

// LedgerStore handles single-shard transactions.
type LedgerStore interface {
	PostTransfer(ctx context.Context, shardID string, transfer domain.Transfer) error
	FailTransfer(ctx context.Context, shardID string, transfer domain.Transfer, reason string) error
}
