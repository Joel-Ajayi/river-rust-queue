package port

import (
	"context"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
)

// JobHandler handles job execution requests from Kafka.
// The primary implementation routes and processes Transfer jobs.
type JobHandler interface {
	ProcessJob(ctx context.Context, payload *eventsv1.JobRequestedPayload) error
}
