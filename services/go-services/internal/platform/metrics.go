package platform

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	meter = otel.Meter(MetricMeterName)

	circuitBreakerOpenTotal       metric.Int64Counter
	circuitBreakerHalfOpenFailure metric.Int64Counter
	circuitBreakerState           metric.Int64Gauge
	dlqIngestionRate              metric.Int64Counter
	infrastructureErrorsTotal     metric.Int64Counter

	metricsOnce sync.Once
)

// init initializes the OpenTelemetry instruments on package load.
func init() {
	metricsOnce.Do(func() {
		var err error

		circuitBreakerOpenTotal, err = meter.Int64Counter(
			MetricCBOpenTotal,
			metric.WithDescription("Total number of times a circuit breaker has tripped to OPEN"),
		)
		if err != nil {
			panic("failed to initialize circuit breaker open metric: " + err.Error())
		}

		circuitBreakerHalfOpenFailure, err = meter.Int64Counter(
			MetricCBHalfOpenFailure,
			metric.WithDescription("Total number of times a circuit breaker failed its half-open trial"),
		)
		if err != nil {
			panic("failed to initialize circuit breaker half open failure metric: " + err.Error())
		}

		dlqIngestionRate, err = meter.Int64Counter(
			MetricDLQIngestionRate,
			metric.WithDescription("Total number of poison pills routed to the DLQ"),
		)
		if err != nil {
			panic("failed to initialize dlq ingestion rate metric: " + err.Error())
		}

		circuitBreakerState, err = meter.Int64Gauge(
			MetricCBState,
			metric.WithDescription("Current state of the circuit breaker (0=Closed, 1=HalfOpen, 2=Open)"),
		)
		if err != nil {
			panic("failed to initialize circuit breaker state metric: " + err.Error())
		}

		infrastructureErrorsTotal, err = meter.Int64Counter(
			MetricInfraErrorsTotal,
			metric.WithDescription("Total number of transient infrastructure errors encountered"),
		)
		if err != nil {
			panic("failed to initialize infrastructure errors metric: " + err.Error())
		}
	})
}

// RecordCircuitBreakerOpen increments the counter for tripped circuit breakers.
func RecordCircuitBreakerOpen(ctx context.Context, cbName string) {
	circuitBreakerOpenTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelCircuitBreaker, cbName),
	))
}

// RecordCircuitBreakerHalfOpenFailure increments the counter for half-open trial failures.
func RecordCircuitBreakerHalfOpenFailure(ctx context.Context, cbName string) {
	circuitBreakerHalfOpenFailure.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelCircuitBreaker, cbName),
	))
}

// RecordDLQIngestion increments the DLQ ingestion metric.
func RecordDLQIngestion(ctx context.Context, serviceName string) {
	dlqIngestionRate.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelService, serviceName),
	))
}

// RecordCircuitBreakerState sets the current state of a circuit breaker as a gauge.
func RecordCircuitBreakerState(ctx context.Context, cbName string, state int64) {
	circuitBreakerState.Record(ctx, state, metric.WithAttributes(
		attribute.String(MetricLabelCircuitBreaker, cbName),
	))
}

// RecordInfrastructureError increments the counter for transient infrastructure errors.
func RecordInfrastructureError(ctx context.Context) {
	infrastructureErrorsTotal.Add(ctx, 1)
}
