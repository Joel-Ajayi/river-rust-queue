package platform

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	meterProvider  *sdkmetric.MeterProvider
	tracerProvider *sdktrace.TracerProvider
)

const (
	SpanProcessJob                       = "kafka.process_transfer"
	SpanHandleWebhookMessage             = "webhook.handle_message"
	SpanProcessMessage                   = "kafka.process_message"
	SpanCommitOffsets                    = "kafka.commit_offsets"
	SpanHandleXShardRequested            = "xshard.handle_requested"
	SpanHandleXShardSettled              = "xshard.handle_settled"
	SpanHandleXShardFailed               = "xshard.handle_failed"
	SpanOutboxStoreRouteDLQ              = "outbox.store.route_dlq"
	SpanAPITransferSubmitterSubmit       = "api.transfer_submitter.submit"
	SpanAPITransferSubmitterGetBalance   = "api.transfer_submitter.get_balance"
	SpanAPIJobStoreClaimAndRecord        = "api.job_store.claim_and_record"
	SpanAPIJobStoreGetJob                = "api.job_store.get_job"
	SpanAPIMerchantDirectoryShardFor     = "api.merchant_directory.shard_for"
	SpanAPIWalletDirectoryCheckOwnership = "api.wallet_directory.check_ownership"
	SpanAPIWalletDirectoryGetBalance     = "api.wallet_directory.get_balance"
	SpanAPIWalletUseCaseCreateWallet     = "api.wallet_use_case.create_wallet"
	SpanAPIWalletUseCaseDeposit          = "api.wallet_use_case.deposit"
	SpanRetryScheduler                   = "webhook.retry_scheduler"
	SpanRouteToGlobalDLQ                 = "webhook.route_to_global_dlq"
)

// InitTelemetry initializes the OpenTelemetry SDK for metrics and traces.
func InitTelemetry(ctx context.Context, serviceName string) error {
	res, err := resource.New(
		ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %w", err)
	}

	// Initialize metrics exporter
	metricExporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return fmt.Errorf("failed to create metric exporter: %w", err)
	}

	meterProvider = sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
	)
	otel.SetMeterProvider(meterProvider)

	// Initialize traces exporter
	traceExporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return fmt.Errorf("failed to create trace exporter: %w", err)
	}

	tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExporter),
	)
	otel.SetTracerProvider(tracerProvider)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return nil
}

// ShutdownTelemetry should be called on application exit.
func ShutdownTelemetry(ctx context.Context) error {
	if err := meterProvider.Shutdown(ctx); err != nil {
		return err
	}
	return tracerProvider.Shutdown(ctx)
}

// ExtractTraceparent extracts the W3C traceparent string from the context.
// This allows propagating trace context across async boundaries.
func ExtractTraceparent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, carrier)
	return carrier.Get(TraceparentHeader)
}

// InjectTraceIntoContext takes a traceparent string extracted from Kafka headers
// and injects it into a new context so downstream spans are linked.
func InjectTraceIntoContext(ctx context.Context, msg *kafka.Message) context.Context {
	traceparentStr := ""
	for _, h := range msg.Headers {
		if h.Key == TraceparentHeader {
			traceparentStr = string(h.Value)
			break
		}
	}
	if traceparentStr == "" {
		return ctx
	}
	carrier := propagation.MapCarrier{
		TraceparentHeader: traceparentStr,
	}
	propagator := otel.GetTextMapPropagator()
	return propagator.Extract(ctx, carrier)
}

// GetTracer returns a standard Tracer to start manual spans in the app.
func GetTracer() trace.Tracer {
	return otel.Tracer("river-rust-queue/platform")
}

func ExtractTraceFromMessageHeaders(msg *kafka.Message) (string, string) {
	ctx := InjectTraceIntoContext(context.Background(), msg)
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		return sc.TraceID().String(), sc.SpanID().String()
	}
	return "", ""
}

func ExtractTraceFromParentString(traceparent string) (string, string) {
	if traceparent == "" {
		return "", ""
	}
	carrier := propagation.MapCarrier{
		TraceparentHeader: traceparent,
	}
	propagator := otel.GetTextMapPropagator()
	ctx := propagator.Extract(context.Background(), carrier)
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		return sc.TraceID().String(), sc.SpanID().String()
	}
	return "", ""
}
