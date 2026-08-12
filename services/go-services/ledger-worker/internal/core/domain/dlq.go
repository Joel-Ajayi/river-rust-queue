package domain

import (
	"time"
)

type DLQStatus string

const (
	DLQStatusOpen     DLQStatus = "open"
	DLQStatusReplayed DLQStatus = "replayed"
	DLQStatusResolved DLQStatus = "resolved"

	DLQSourceLedger  = "ledger"
	DLQSourceWebhook = "webhook"
	DLQSourceFraud   = "fraud"
	DLQSourceOutbox  = "outbox-relay"
)

type DLQEntry struct {
	ID                  string
	Source              string
	OriginalPayload     []byte
	ErrorMessage        string
	ErrorClassification string // "poison", "transient", "terminal", "infrastructure"
	AttemptCount        int
	FirstFailedAt       time.Time
	LastFailedAt        time.Time
	Status              DLQStatus
	TraceID             string
	SpanID              string
}
