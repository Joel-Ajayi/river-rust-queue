package platform

const (
	// Transfer Statuses (Shared across Ledger & API Gateway)
	TransferStatusPending   = "pending"
	TransferStatusCompleted = "completed"
	TransferStatusFailed    = "failed"

	// XShard Transfer Statuses (Shared across Ledger & API Gateway)
	XShardTransferStatusPending   = "pending"
	XShardTransferStatusCompleted = "completed"
	XShardTransferStatusReversed  = "reversed"

	AggregateTypeTransfer       AggregateType = "transfer"
	AggregateTypeXShardTransfer AggregateType = "xshard_transfer"

	EventTypeTransferCompleted       EventType = "transfer.completed"
	EventTypeTransferFailed          EventType = "transfer.failed"
	EventTypeXShardTransferRequested EventType = "xshard.transfer.requested"
	EventTypeXShardTransferSettled   EventType = "xshard.transfer.settled"
	EventTypeXShardTransferFailed    EventType = "xshard.transfer.failed"
)
