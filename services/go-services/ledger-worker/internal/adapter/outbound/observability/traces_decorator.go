package observability

import (
	"context"
	"fmt"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/port"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// -- Job Handler Decorator --

type jobHandlerTraces struct {
	next port.JobHandler
}

func NewJobHandlerTraces(next port.JobHandler) port.JobHandler {
	return &jobHandlerTraces{next: next}
}

func (t *jobHandlerTraces) ProcessJob(ctx context.Context, payload *eventsv1.JobRequestedPayload) error {
	spanCtx, span := platform.GetTracer().Start(ctx, platform.SpanProcessJob)
	defer span.End()

	span.SetAttributes(
		attribute.String(platform.MetricLabelJobID, payload.JobId),
		attribute.String(platform.MetricLabelMerchantID, payload.MerchantId),
	)
	if payload.GetTransferData() != nil {
		span.SetAttributes(attribute.String(platform.MetricLabelWalletID, payload.GetTransferData().FromWallet))
	}

	err := t.next.ProcessJob(spanCtx, payload)
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

// -- Saga Handler Decorator --

type sagaHandlerTraces struct {
	next port.SagaHandler
}

func NewSagaHandlerTraces(next port.SagaHandler) port.SagaHandler {
	return &sagaHandlerTraces{next: next}
}

func (t *sagaHandlerTraces) HandleXShardRequested(ctx context.Context, payload *eventsv1.XShardTransferRequestedPayload) error {
	spanCtx, span := platform.GetTracer().Start(ctx, platform.SpanHandleXShardRequested)
	defer span.End()

	span.SetAttributes(
		attribute.String(platform.MetricLabelJobID, payload.JobId),
		attribute.String(platform.MetricLabelMerchantID, payload.MerchantId),
	)

	err := t.next.HandleXShardRequested(spanCtx, payload)
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

func (t *sagaHandlerTraces) HandleXShardSettled(ctx context.Context, payload *eventsv1.XShardTransferSettledPayload) error {
	spanCtx, span := platform.GetTracer().Start(ctx, platform.SpanHandleXShardSettled)
	defer span.End()

	err := t.next.HandleXShardSettled(spanCtx, payload)
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

func (t *sagaHandlerTraces) HandleXShardFailed(ctx context.Context, payload *eventsv1.XShardTransferFailedPayload) error {
	spanCtx, span := platform.GetTracer().Start(ctx, platform.SpanHandleXShardFailed)
	defer span.End()

	err := t.next.HandleXShardFailed(spanCtx, payload)
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
