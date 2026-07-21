package port

import (
	"context"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
)

type JobHandler interface {
	ProcessJob(ctx context.Context, payload *eventsv1.JobRequestedPayload, eventID string, occurredAt int64) error
}
