package port

import (
	"context"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
)

// SagaHandler handles the orchestration steps for Cross-Shard transfer Sagas.
type SagaHandler interface {
	HandleXShardRequested(ctx context.Context, payload *eventsv1.XShardTransferRequestedPayload) error
	HandleXShardSettled(ctx context.Context, payload *eventsv1.XShardTransferSettledPayload) error
	HandleXShardFailed(ctx context.Context, payload *eventsv1.XShardTransferFailedPayload) error
}
