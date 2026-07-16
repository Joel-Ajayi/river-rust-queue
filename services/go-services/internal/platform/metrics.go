package platform

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// Observability Metrics
	// names
	MetricMeterName                  = "rrq/platform"
	MetricCBOpenTotal                = "rrq_circuit_breaker_open_total"
	MetricCBHalfOpenFailure          = "rrq_circuit_breaker_half_open_failure"
	MetricCBState                    = "rrq_circuit_breaker_state"
	MetricDLQIngestionRate           = "rrq_dlq_ingestion_rate"
	MetricInfraErrorsTotal           = "rrq_infrastructure_errors_total"
	MetricOutboxLagSeconds           = "rrq_outbox_lag_seconds"
	MetricOutboxEventsPublishedTotal = "rrq_outbox_events_published_total"
	MetricOutboxPublishDuration      = "rrq_outbox_publish_duration_seconds"
	MetricOutboxPurgedEventsTotal    = "rrq_outbox_purged_events_total"
	MetricOutboxPanicsTotal          = "rrq_outbox_panics_total"
	MetricIdempotencyConflictsTotal  = "rrq_idempotency_conflicts_total"
	MetricWeakAPIKeyAuth             = "rrq_weak_api_key_auth_total"
	MetricConsumerMsgDuration        = "rrq_consumer_message_duration_seconds"
	MetricConsumerBackoffDuration    = "rrq_consumer_backoff_duration_seconds"
	MetricConsumerCommitsTotal       = "rrq_consumer_commits_total"
	// label
	MetricLabelCircuitBreaker = "circuit_breaker"
	MetricLabelService        = "service"
	MetricLabelComponent      = "component"
	MetricLabelShard          = "shard"
	MetricLabelTopic          = "topic"
	MetricLabelMerchantID     = "merchant_id"
)

var (
	meter = otel.Meter(MetricMeterName)

	circuitBreakerOpenTotal       metric.Int64Counter
	circuitBreakerHalfOpenFailure metric.Int64Counter
	circuitBreakerState           metric.Int64Gauge
	dlqIngestionRate              metric.Int64Counter
	infrastructureErrorsTotal     metric.Int64Counter
	outboxLagGauge                metric.Float64Gauge
	outboxEventsPublishedTotal    metric.Int64Counter
	outboxPublishDuration         metric.Float64Histogram
	outboxPurgedEventsTotal       metric.Int64Counter
	outboxPanicsTotal             metric.Int64Counter
	idempotencyConflictsTotal     metric.Int64Counter
	weakAPIKeyAuthTotal           metric.Int64Counter
	consumerMsgDuration           metric.Float64Histogram
	consumerBackoffDuration       metric.Float64Histogram
	consumerCommitsTotal          metric.Int64Counter

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

		outboxLagGauge, err = meter.Float64Gauge(
			MetricOutboxLagSeconds,
			metric.WithDescription("Age in seconds of the oldest unpublished outbox event per shard"),
		)
		if err != nil {
			panic("failed to initialize outbox lag metric: " + err.Error())
		}

		infrastructureErrorsTotal, err = meter.Int64Counter(
			MetricInfraErrorsTotal,
			metric.WithDescription("Total number of transient infrastructure errors encountered"),
		)
		if err != nil {
			panic("failed to initialize infrastructure errors metric: " + err.Error())
		}

		outboxEventsPublishedTotal, err = meter.Int64Counter(
			MetricOutboxEventsPublishedTotal,
			metric.WithDescription("Total number of events successfully published by outbox relay"),
		)
		if err != nil {
			panic("failed to initialize outbox events published metric: " + err.Error())
		}

		outboxPublishDuration, err = meter.Float64Histogram(
			MetricOutboxPublishDuration,
			metric.WithDescription("Duration of outbox relay publish operations"),
		)
		if err != nil {
			panic("failed to initialize outbox publish duration metric: " + err.Error())
		}

		outboxPurgedEventsTotal, err = meter.Int64Counter(
			MetricOutboxPurgedEventsTotal,
			metric.WithDescription("Total number of successfully published events purged from database"),
		)
		if err != nil {
			panic("failed to initialize outbox purged events metric: " + err.Error())
		}

		outboxPanicsTotal, err = meter.Int64Counter(
			MetricOutboxPanicsTotal,
			metric.WithDescription("Total number of panics caught in outbox relay loop"),
		)
		if err != nil {
			panic("failed to initialize outbox panics metric: " + err.Error())
		}

		idempotencyConflictsTotal, err = meter.Int64Counter(
			MetricIdempotencyConflictsTotal,
			metric.WithDescription("Total number of idempotency key conflicts"),
		)
		if err != nil {
			panic("failed to initialize idempotency conflicts metric: " + err.Error())
		}

		weakAPIKeyAuthTotal, err = meter.Int64Counter(
			MetricWeakAPIKeyAuth,
			metric.WithDescription("Total number of authentications using a weak bcrypt factor API key"),
		)
		if err != nil {
			panic("failed to initialize weak api key metric: " + err.Error())
		}

		consumerMsgDuration, err = meter.Float64Histogram(
			MetricConsumerMsgDuration,
			metric.WithDescription("Duration of consumer message processing"),
			metric.WithUnit("s"),
		)
		if err != nil {
			panic("failed to initialize consumer message duration metric: " + err.Error())
		}

		consumerBackoffDuration, err = meter.Float64Histogram(
			MetricConsumerBackoffDuration,
			metric.WithDescription("Duration of consumer backoff sleeps"),
			metric.WithUnit("s"),
		)
		if err != nil {
			panic("failed to initialize consumer backoff duration metric: " + err.Error())
		}

		consumerCommitsTotal, err = meter.Int64Counter(
			MetricConsumerCommitsTotal,
			metric.WithDescription("Total number of consumer offset commits"),
		)
		if err != nil {
			panic("failed to initialize consumer commits metric: " + err.Error())
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

// RecordOutboxLag sets the age of the oldest unpublished event
func RecordOutboxLag(ctx context.Context, shardID string, lag time.Duration) {
	outboxLagGauge.Record(ctx, lag.Seconds(), metric.WithAttributes(
		attribute.String(MetricLabelShard, shardID),
	))
}

// RecordDLQIngestion increments the DLQ ingestion metric.
func RecordDLQIngestion(ctx context.Context, topic string) {
	dlqIngestionRate.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelTopic, topic),
	))
}

// RecordCircuitBreakerState sets the current state of a circuit breaker as a gauge.
func RecordCircuitBreakerState(ctx context.Context, cbName string, state int64) {
	circuitBreakerState.Record(ctx, state, metric.WithAttributes(
		attribute.String(MetricLabelCircuitBreaker, cbName),
	))
}

// RecordInfrastructureError increments the counter for transient infrastructure errors.
func RecordInfrastructureError(ctx context.Context, component string) {
	infrastructureErrorsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelComponent, component),
	))
}

// RecordOutboxEventsPublished increments the counter for successfully published outbox events.
func RecordOutboxEventsPublished(ctx context.Context, shardID string, topic string, count int) {
	outboxEventsPublishedTotal.Add(ctx, int64(count), metric.WithAttributes(
		attribute.String(MetricLabelShard, shardID),
		attribute.String(MetricLabelTopic, topic),
	))
}

// RecordOutboxPublishDuration records the duration of an outbox publishing operation.
func RecordOutboxPublishDuration(ctx context.Context, shardID string, duration time.Duration) {
	outboxPublishDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String(MetricLabelShard, shardID),
	))
}

// RecordOutboxPurgedEvents increments the counter for purged events.
func RecordOutboxPurgedEvents(ctx context.Context, shardID string, count int64) {
	outboxPurgedEventsTotal.Add(ctx, count, metric.WithAttributes(
		attribute.String(MetricLabelShard, shardID),
	))
}

// RecordOutboxPanic increments the counter for panics recovered in the outbox relay.
func RecordOutboxPanic(ctx context.Context, shardID string) {
	outboxPanicsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelShard, shardID),
	))
}

// RecordIdempotencyConflict increments the counter for business/idempotency conflicts.
func RecordIdempotencyConflict(ctx context.Context, merchantID, jobID, shardID string) {
	idempotencyConflictsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelMerchantID, merchantID),
		attribute.String("job_id", jobID),
		attribute.String(MetricLabelShard, shardID),
	))
}

// RecordMessageProcessingDuration records the duration of message processing with a custom label.
func RecordMessageProcessingDuration(ctx context.Context, duration time.Duration, label string) {
	consumerMsgDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("handler", label),
	))
}

// RecordConsumerMsgDuration records the duration of consumer message processing.
func RecordConsumerMsgDuration(ctx context.Context, topic string, duration time.Duration) {
	consumerMsgDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String(MetricLabelTopic, topic),
	))
}

// RecordConsumerBackoffDuration records the duration of consumer backoff sleep.
func RecordConsumerBackoffDuration(ctx context.Context, topic string, duration time.Duration) {
	consumerBackoffDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String(MetricLabelTopic, topic),
	))
}

// RecordConsumerCommit records a successful offset commit.
func RecordConsumerCommit(ctx context.Context, topic string) {
	consumerCommitsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelTopic, topic),
	))
}
