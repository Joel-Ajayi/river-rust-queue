package domain

import "time"

type JobType string
type JobStatus string

const (
	// Job types
	JobTypeTransfer JobType = "transfer"
	JobTypePayout   JobType = "payout"

	// Job statuses
	JobStatusPending   JobStatus = "pending"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
)

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
}

type SubmitResult struct {
	Job            Job
	AlreadyExisted bool
}
