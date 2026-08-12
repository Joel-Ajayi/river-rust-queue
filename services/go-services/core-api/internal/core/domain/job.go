package domain

import "time"

type Job struct {
	ID             string
	MerchantID     string
	IdempotencyKey string
	Type           string
	PayloadHash    string
	Status         string
	FailureReason  *string
	CreatedAt      time.Time
	CompletedAt    *time.Time
	ShardID        string
}

type SubmitResult struct {
	Job            Job
	AlreadyExisted bool
	ShardID        string
}
