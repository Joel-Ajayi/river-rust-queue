package observability

import (
	"context"
	"fmt"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/port"
	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type jobHandlerTraces struct {
	next port.JobHandler
}

func NewJobHandlerTraces(next port.JobHandler) port.JobHandler {
	return &jobHandlerTraces{next: next}
}

func (t *jobHandlerTraces) ProcessJob(ctx context.Context, payload *eventsv1.JobRequestedPayload, eventID string, occurredAt int64) error {
	spanCtx, span := platform.GetTracer().Start(ctx, platform.SpanProcessJob)
	defer span.End()

	span.SetAttributes(
		attribute.String(platform.MetricLabelJobID, payload.JobId),
		attribute.String(platform.MetricLabelMerchantID, payload.MerchantId),
	)
	if payload.GetTransferData() != nil {
		span.SetAttributes(attribute.String(platform.MetricLabelWalletID, payload.GetTransferData().FromWallet))
	}

	err := t.next.ProcessJob(spanCtx, payload, eventID, occurredAt)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(
			attribute.String(platform.MetricLabelErrorType, fmt.Sprintf("%T", err)),
			attribute.String(platform.MetricLabelErrorMessage, err.Error()),
		)
	}
	return err
}
