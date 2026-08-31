package port

import (
	"context"
)

type WalletRepository interface {
	GetWalletStatus(ctx context.Context, shardID string, walletID string) (status string, walletType string, err error)
	FreezeWallet(ctx context.Context, shardID string, walletID string, reason string) error
}
