package domain

import "time"

const (
	DiscrepancyKindGlobalConservation = "global_conservation"
	DiscrepancyKindProjectionDrift    = "projection_drift"
	DiscrepancyKindBalanceAfter       = "balance_after"
	DiscrepancyKindLegImbalance       = "leg_imbalance"
)

type Discrepancy struct {
	Kind           string `json:"kind"`
	WalletID       string `json:"wallet_id,omitempty"`
	TransferID     string `json:"transfer_id,omitempty"`
	DerivedBalance int64  `json:"derived_balance,omitempty"`
	CachedBalance  int64  `json:"cached_balance,omitempty"`
	StoredBalance  int64  `json:"stored_balance,omitempty"`
	Delta          int64  `json:"delta,omitempty"`
}

type Report struct {
	RunID           string        `json:"run_id"`
	WindowStart     time.Time     `json:"window_start"`
	WindowEnd       time.Time     `json:"window_end"`
	GlobalSum       int64         `json:"global_sum"`
	WalletsChecked  int           `json:"wallets_checked"`
	Discrepancies   []Discrepancy `json:"discrepancies"`
	DurationSeconds float64       `json:"duration_seconds"`
}
