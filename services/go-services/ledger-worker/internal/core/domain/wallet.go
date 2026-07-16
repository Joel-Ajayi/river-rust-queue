package domain

type WalletStatus string
type WalletType string

const (
	WalletStatusFrozen WalletStatus = "frozen"
	WalletStatusClosed WalletStatus = "closed"
	WalletStatusActive WalletStatus = "active"

	WalletTypeSystem              WalletType = "system"
	WalletTypeCustomer            WalletType = "customer"
	WalletTypeMerchantOperational WalletType = "merchant_operational"
)
