package domain

// Job is the persisted record of an accepted request. The gateway only ever
// creates jobs in the "pending" state; workers advance them later.
type Job struct {
	ID     string
	Status string
}

// Principal is the authenticated identity behind a request.
type Principal struct {
	MerchantID string
	Tier       string
}

// SubmitResult is the outcome of submitting a transfer. AlreadyExisted is true
// when the idempotency key matched a prior job, in which case Job describes the
// existing job rather than a newly created one.
type SubmitResult struct {
	Job            Job
	AlreadyExisted bool
}
