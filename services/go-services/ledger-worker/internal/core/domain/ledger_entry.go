package domain

import "time"

type Leg string

const (
	LegDebit  Leg = "debit"
	LegCredit Leg = "credit"
)

type LedgerEntry struct {
	ID           int64
	WalletID     string
	TransferID   string
	Leg          Leg
	Amount       int64
	BalanceAfter int64
	CreatedAt    time.Time
}
