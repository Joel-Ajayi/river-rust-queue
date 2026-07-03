package platform

type AggregateType string
type EventType string

const (
	// Event Job types
	EventTypeJobRequested EventType = "job.requested"

	// Event Transfer types
	EventTypeTransferCompleted  EventType = "transfer.completed"
	EventTypeTransferFailed     EventType = "transfer.failed"
	EventTypeTransferRolledBack EventType = "transfer.rolled_back"

	// Paths
	APIVersionV1     = "/v1"
	APIVersionV2     = "/v2"
	APIPathPrefix    = APIVersionV1
	APIJobPathPrefix = APIPathPrefix + "/jobs/"
	APITransfersPath = APIPathPrefix + "/transfers"
	APIAuthTokenPath = APIPathPrefix + "/auth/token"
	APIHealthPath    = "/health"
	APIReadyPath     = "/ready"

	// Job Statuses
	JobStatusPending   = "pending"
	JobStatusCompleted = "completed"
	JobStatusFailed    = "failed"

	// Aggregate types
	AggregateTypeJob      AggregateType = "job"
	AggregateTypeEvent    AggregateType = "ev"
	AggregateTypeTransfer AggregateType = "transfer"
)
