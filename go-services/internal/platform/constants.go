package platform

type AggregateType string
type EventType string

const (
	// Event types
	EventTypeJobRequested EventType = "job.requested"

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
	AggregateTypeJob   AggregateType = "job"
	AggregateTypeEvent AggregateType = "ev"
)
