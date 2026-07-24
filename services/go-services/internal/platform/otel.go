package platform

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"

	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	meterProvider  *sdkmetric.MeterProvider
	tracerProvider *sdktrace.TracerProvider
)

const (
	SpanProcessJob            = "ProcessJob"
	SpanHandleXShardRequested = "HandleXShardRequested"
	SpanHandleXShardSettled   = "HandleXShardSettled"
	SpanHandleXShardFailed    = "HandleXShardFailed"
	SpanHandleWebhookMessage  = "HandleWebhookMessage"
)

// InitTelemetry configures the global OpenTelemetry MeterProvider with an OTLP exporter.
// It explicitly attaches the service name as a Resource Attribute so that all metrics automatically
// contain the `service.name` label.
func InitTelemetry(serviceName string, endpoint string) error {
	ctx := context.Background()

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
	)

	// 2. Configure Metric Exporter targeting local OTel Collector proxy
	metricExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return err
	}

	meterProvider = sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)

	// 3. Configure Trace Exporter targeting local OTel Collector proxy
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return err
	}
	tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tracerProvider)

	// Register the W3C trace context and baggage propagators so they are automatically used.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

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
