package platform

const (
	// Job Statuses (Shared across API Gateway & Ledger)
	JobStatusPending   = "pending"
	JobStatusCompleted = "completed"
	JobStatusFailed    = "failed"

	AggregateTypeJob AggregateType = "job"

	EventTypeJobRequested EventType = "job.requested"

	// Job types
	JobTypeTransfer = "transfer"
	JobTypePayout   = "payout"
)
