package domain

import "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"

type WalletStatus string
type WalletType string

const (
	WalletStatusFrozen WalletStatus = WalletStatus(platform.WalletStatusFrozen)
	WalletStatusClosed WalletStatus = WalletStatus(platform.WalletStatusClosed)

	WalletTypeSystem WalletType = WalletType(platform.WalletTypeSystem)
)

// IsSystemWallet returns true for wallet types that are allowed negative balances.
func IsSystemWallet(wt string) bool {
	return wt == platform.WalletTypeSystem || wt == platform.WalletTypeFiatVault
}
