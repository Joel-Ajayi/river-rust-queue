package domain

import "time"

type DLQEntry struct {
	ID                  string
	Source              string
	OriginalPayload     []byte
	ErrorMessage        string
	ErrorClassification string
	AttemptCount        int
	FirstFailedAt       time.Time
	LastFailedAt        time.Time
	Status              string
	TraceID             string
	SpanID              string
}
