package platform

import (
	"context"
	"strings"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// InitTelemetry configures the global OpenTelemetry MeterProvider with an OTLP exporter.
// It explicitly attaches the service name as a Resource Attribute so that all metrics automatically
// contain the `service.name` label.
func InitTelemetry(serviceName string) error {
	ctx := context.Background()
	// Auto picks up OTEL_EXPORTER_OTLP_ENDPOINT from the environment
	exporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return err
	}

	// Create Resource with the service.name attribute
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return err
	}

	// Setup the global MeterProvider with a periodic reader for OTLP
	provider := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(exporter)),
		metric.WithResource(res),
	)
	otel.SetMeterProvider(provider)

	return nil
}

// ShutdownTelemetry should be called on application exit.
func ShutdownTelemetry(ctx context.Context) error {
	if provider, ok := otel.GetMeterProvider().(*metric.MeterProvider); ok {
		return provider.Shutdown(ctx)
	}
	return nil
}

// ExtractTraceparent extracts the W3C traceparent string from the context.
// This allows propagating trace context across async boundaries.
func ExtractTraceparent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagator := propagation.TraceContext{}
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
	propagator := propagation.TraceContext{}
	return propagator.Extract(ctx, carrier)
}

func ExtractTraceFromMessageHeaders(msg *kafka.Message) (string, string) {
	traceID, spanID := "", ""
	for _, h := range msg.Headers {
		if h.Key == TraceparentHeader {
			parts := strings.Split(string(h.Value), "-")
			if len(parts) >= 3 {
				traceID, spanID = parts[1], parts[2]
			}
			break
		}
	}
	return traceID, spanID
}

func ExtractTraceFromParentString(traceparent string) (string, string) {
	parts := strings.Split(traceparent, "-")
	if len(parts) >= 3 {
		return parts[1], parts[2]
	}
	return "", ""
}
