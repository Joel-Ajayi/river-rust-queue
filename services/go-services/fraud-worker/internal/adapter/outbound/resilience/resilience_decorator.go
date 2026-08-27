package resilience

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/port"
	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

type merchantDirResilience struct {
	next port.MerchantDirectory
	cbs  *platform.DBCircuitBreakers
}

func NewMerchantDirectoryResilience(next port.MerchantDirectory, cbs *platform.DBCircuitBreakers) port.MerchantDirectory {
	return &merchantDirResilience{next: next, cbs: cbs}
}

func (m *merchantDirResilience) ShardFor(ctx context.Context, merchantID string) (string, error) {
	res, err := m.cbs.Merchants().Execute(ctx, func() (interface{}, error) {
		return m.next.ShardFor(ctx, merchantID)
	})
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

type walletRepoResilience struct {
	next port.WalletRepository
	cbs  *platform.DBCircuitBreakers
}

func NewWalletRepositoryResilience(next port.WalletRepository, cbs *platform.DBCircuitBreakers) port.WalletRepository {
	return &walletRepoResilience{next: next, cbs: cbs}
}

func (w *walletRepoResilience) GetWalletStatus(ctx context.Context, shardID string, walletID string) (string, error) {
	res, err := w.cbs.ShardRO(shardID).Execute(ctx, func() (interface{}, error) {
		return w.next.GetWalletStatus(ctx, shardID, walletID)
	})
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

func (w *walletRepoResilience) FreezeWallet(ctx context.Context, shardID string, walletID string, reason string) error {
	_, err := w.cbs.ShardRW(shardID).Execute(ctx, func() (interface{}, error) {
		return nil, w.next.FreezeWallet(ctx, shardID, walletID, reason)
	})
	return err
}

type dlqStoreResilience struct {
	next port.DLQStore
	cbs  *platform.DBCircuitBreakers
}

func NewDLQStoreResilience(next port.DLQStore, cbs *platform.DBCircuitBreakers) port.DLQStore {
	return &dlqStoreResilience{next: next, cbs: cbs}
}

func (d *dlqStoreResilience) WriteDLQEntry(ctx context.Context, entry *eventsv1.DLQEntry) error {
	_, err := d.cbs.Merchants().Execute(ctx, func() (interface{}, error) {
		return nil, d.next.WriteDLQEntry(ctx, entry)
	})
	return err
}
