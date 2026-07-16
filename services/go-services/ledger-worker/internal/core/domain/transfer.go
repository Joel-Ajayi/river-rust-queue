package domain

import "time"

const ReversalPrefix = "rev_"

type Transfer struct {
	ID            string
	JobID         string
	MerchantID    string
	FromWallet    string
	ToWallet      string
	Amount        int64
	Currency      string
	Status        string
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
	State      string
	Reason     string
}


