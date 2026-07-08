package domain

type WalletStatus string
type JobStatus string
type WalletType string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"

	WalletStatusFrozen WalletStatus = "frozen"
	WalletStatusClosed WalletStatus = "closed"
	WalletStatusActive WalletStatus = "active"

	WalletTypeSystem   WalletType = "system"
	WalletTypeCustomer WalletType = "customer"
)
