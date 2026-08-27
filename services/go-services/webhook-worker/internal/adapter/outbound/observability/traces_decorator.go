package observability

import (
	"context"
	"fmt"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/webhook-worker/internal/core/port"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// -- WebhookApp Decorator --

type webhookAppTraces struct {
	next port.WebhookApp
}

func NewWebhookAppTraces(next port.WebhookApp) port.WebhookApp {
	return &webhookAppTraces{next: next}
}

func (t *webhookAppTraces) HandleMessage(ctx context.Context, merchantID string, topic string, key string, payload []byte) error {
	spanCtx, span := platform.GetTracer().Start(ctx, platform.SpanHandleWebhookMessage)
	defer span.End()

	span.SetAttributes(
		attribute.String(platform.MetricLabelMerchantID, merchantID),
	)

	err := t.next.HandleMessage(spanCtx, merchantID, topic, key, payload)
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

func (t *webhookAppTraces) RetryScheduler(ctx context.Context) error {
	spanCtx, span := platform.GetTracer().Start(ctx, platform.SpanRetryScheduler)
	defer span.End()

	err := t.next.RetryScheduler(spanCtx)
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

func (t *webhookAppTraces) RouteToGlobalDLQ(ctx context.Context, payload []byte, topic string, key string, errorMsg string) error {
	spanCtx, span := platform.GetTracer().Start(ctx, platform.SpanRouteToGlobalDLQ)
	defer span.End()

	err := t.next.RouteToGlobalDLQ(spanCtx, payload, topic, key, errorMsg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}
