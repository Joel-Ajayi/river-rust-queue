package port

import (
	"context"
)

type WalletRepository interface {
	GetWalletStatus(ctx context.Context, shardID string, walletID string) (string, error)
	FreezeWallet(ctx context.Context, shardID string, walletID string, reason string) error
}
