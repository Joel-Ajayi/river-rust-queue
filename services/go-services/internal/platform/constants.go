package platform


type AggregateType string
type EventType string

const (
	// Paths
	APIVersionV1     = "/v1"
	APIVersionV2     = "/v2"
	APIPathPrefix    = APIVersionV1
	APIJobPathPrefix = APIPathPrefix + "/jobs/"
	APITransfersPath = APIPathPrefix + "/transfers"
	APIBalancesPath  = APIPathPrefix + "/balances"
	APIAuthTokenPath = APIPathPrefix + "/auth/token"
	APIHealthPath    = "/health"
	APIReadyPath     = "/ready"

	// Aggregate types
	AggregateTypeJob            AggregateType = "job"
	AggregateTypeEvent          AggregateType = "ev"
	AggregateTypeWallet         AggregateType = "wallet"
	AggregateTypeTransfer       AggregateType = "transfer"
	AggregateTypeXShardTransfer AggregateType = "xshard_transfer"

	// Event types
	EventTypeJobRequested            EventType = "job.requested"
	EventTypeTransferCompleted       EventType = "transfer.completed"
	EventTypeTransferFailed          EventType = "transfer.failed"
	EventTypeXShardTransferRequested EventType = "xshard.transfer.requested"
	EventTypeXShardTransferSettled   EventType = "xshard.transfer.settled"
	EventTypeXShardTransferFailed    EventType = "xshard.transfer.failed"
)
