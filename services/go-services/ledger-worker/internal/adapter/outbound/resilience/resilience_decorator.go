package resilience

import (
	"context"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/port"
)

// Layer 3 CB decorators: pure shields keyed by pool identity. No retry.
// The daemon owns backoff, not the decorator.

// MerchantDirectory wraps the global merchants pool breaker.
type merchantDirResilience struct {
	next port.MerchantDirectory
	cbs  *platform.DBCircuitBreakers
}

func NewMerchantDirectoryResilience(next port.MerchantDirectory, cbs *platform.DBCircuitBreakers) port.MerchantDirectory {
	return &merchantDirResilience{next: next, cbs: cbs}
}

func (m *merchantDirResilience) ShardFor(ctx context.Context, merchantID string) (string, error) {
	res, err := m.cbs.Merchants().Execute(func() (interface{}, error) {
		return m.next.ShardFor(ctx, merchantID)
	})
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// LedgerStore wraps the per-shard DB pool breaker shared across stores.
type ledgerStoreResilience struct {
	next port.LedgerStore
	cbs  *platform.DBCircuitBreakers
}

func NewLedgerStoreResilience(next port.LedgerStore, cbs *platform.DBCircuitBreakers) port.LedgerStore {
	return &ledgerStoreResilience{next: next, cbs: cbs}
}

func (l *ledgerStoreResilience) PostTransfer(ctx context.Context, shardID string, transfer domain.Transfer) error {
	_, err := l.cbs.ShardRW(shardID).Execute(func() (interface{}, error) {
		return nil, l.next.PostTransfer(ctx, shardID, transfer)
	})
	return err
}

func (l *ledgerStoreResilience) FailTransfer(ctx context.Context, shardID string, transfer domain.Transfer, reason string) error {
	_, err := l.cbs.ShardRW(shardID).Execute(func() (interface{}, error) {
		return nil, l.next.FailTransfer(ctx, shardID, transfer, reason)
	})
	return err
}

// CrossShardStore debits/credits settle on srcShard or dstShard's pool.
type crossShardStoreResilience struct {
	next port.CrossShardStore
	cbs  *platform.DBCircuitBreakers
}

func NewCrossShardStoreResilience(next port.CrossShardStore, cbs *platform.DBCircuitBreakers) port.CrossShardStore {
	return &crossShardStoreResilience{next: next, cbs: cbs}
}

func (x *crossShardStoreResilience) DebitToClearingAccount(ctx context.Context, srcShard, dstShard, jobID string, transfer domain.Transfer) error {
	_, err := x.cbs.ShardRW(srcShard).Execute(func() (interface{}, error) {
		return nil, x.next.DebitToClearingAccount(ctx, srcShard, dstShard, jobID, transfer)
	})
	return err
}

func (x *crossShardStoreResilience) CreditFromClearingAccount(ctx context.Context, intent *eventsv1.XShardTransferRequestedPayload) error {
	_, err := x.cbs.ShardRW(intent.DstShard).Execute(func() (interface{}, error) {
		return nil, x.next.CreditFromClearingAccount(ctx, intent)
	})
	return err
}

func (x *crossShardStoreResilience) SettleCrossShardTransfer(ctx context.Context, srcShard, transferID string) error {
	_, err := x.cbs.ShardRW(srcShard).Execute(func() (interface{}, error) {
		return nil, x.next.SettleCrossShardTransfer(ctx, srcShard, transferID)
	})
	return err
}

func (x *crossShardStoreResilience) ReverseCrossShardTransfer(ctx context.Context, srcShard, transferID, reason string) error {
	_, err := x.cbs.ShardRW(srcShard).Execute(func() (interface{}, error) {
		return nil, x.next.ReverseCrossShardTransfer(ctx, srcShard, transferID, reason)
	})
	return err
}

// DLQStore shares the same per-shard breaker as LedgerStore & CrossShardStore.
type dlqStoreResilience struct {
	next port.DLQStore
	cbs  *platform.DBCircuitBreakers
}

func NewDLQStoreResilience(next port.DLQStore, cbs *platform.DBCircuitBreakers) port.DLQStore {
	return &dlqStoreResilience{next: next, cbs: cbs}
}

func (d *dlqStoreResilience) WriteDLQEntry(ctx context.Context, shardID string, entry domain.DLQEntry) error {
	_, err := d.cbs.ShardRW(shardID).Execute(func() (interface{}, error) {
		return nil, d.next.WriteDLQEntry(ctx, shardID, entry)
	})
	return err
}
