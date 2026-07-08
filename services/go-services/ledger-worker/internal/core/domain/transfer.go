package domain

import "time"

type TransferState string
type XShardTransferState string
const (
	TransferStatePending   TransferState = "pending"
	TransferStateCompleted TransferState = "completed"
	TransferStateFailed    TransferState = "failed"

	ReversalPrefix = "rev_"

	XShardTransferStatePending   XShardTransferState = "pending"
	XShardTransferStateCompleted XShardTransferState = "completed"
	XShardTransferStateReversed  XShardTransferState = "reversed"
)

type Transfer struct {
	ID            string
	JobID         string
	MerchantID    string
	FromWallet    string
	ToWallet      string
	Amount        int64
	Currency      string
	Status        TransferState
	FailureReason *string
	PostedAt      time.Time
}

type XShardTransfer struct {
	TransferID string
	JobID      string
	SrcShard   string
	DstShard   string
	FromWallet string
	ToWallet   string
	Amount     int64
	Currency   string
	State      XShardTransferState
	Reason     string
}


