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
	MetricMeterName                   = "rrq/platform"
	MetricCBOpenTotal                 = "rrq.circuit.breaker.open_total"
	MetricCBHalfOpenFailure           = "rrq.circuit.breaker.half.open.failure"
	MetricCBState                     = "rrq.circuit.breaker.state"
	MetricDLQIngestionRate            = "rrq.dlq.ingestion_rate"
	MetricInfraErrorsTotal            = "rrq.infrastructure.errors.total"
	MetricOutboxLagSeconds            = "rrq.outbox.lag.seconds"
	MetricOutboxEventsPublishedTotal  = "rrq.outbox.events.published.total"
	MetricOutboxPublishDuration       = "rrq.outbox.publish.duration.seconds"
	MetricOutboxPanicsTotal           = "rrq.outbox.panics.total"
	MetricConsumerPanicsTotal         = "rrq.consumer.panics.total"
	MetricRedisFailClosed             = "rrq.redis.fail_closed.total"
	MetricDLQInfrastructureFlood      = "rrq.dlq.infrastructure_flood.total"
	MetricKafkaProducerBufferFill     = "rrq.kafka.producer.buffer.fill.ratio"
	MetricIdempotencyConflictsTotal   = "rrq.idempotency.conflicts.total"
	MetricIdempotencyHitsTotal        = "rrq.idempotency.hits.total"
	MetricWeakAPIKeyAuth              = "rrq.weak.api.key.auth.total"
	MetricConsumerMsgDuration         = "rrq.consumer.message.duration.seconds"
	MetricConsumerBackoffDuration     = "rrq.consumer.backoff.duration.seconds"
	MetricConsumerCommitsTotal        = "rrq.consumer.commits.total"
	MetricConsumerLagMessages         = "rrq.consumer.lag.messages"
	MetricCommitCoordinatorQueueDepth = "rrq.commit.coordinator.queue.depth"
	MetricTaskChannelFillRatio        = "rrq.task.channel.fill.ratio"
	MetricLedgerImbalanceTotal        = "rrq.ledger.imbalance.total"
	MetricSagaUnresolvedCount         = "rrq.saga.unresolved.count"
	MetricVelocityLimitExceededTotal  = "rrq.velocity.limit.exceeded.total"
	MetricAdminDLQReplayedTotal       = "rrq.admin.dlq.replayed.total"
	MetricBulkheadRejectionsTotal     = "rrq.bulkhead.rejections.total"
	MetricBulkheadInFlight            = "rrq.bulkhead.in.flight"

	// label
	MetricLabelCircuitBreaker = "circuit.breaker"
	MetricLabelService        = "service"
	MetricLabelComponent      = "component"
	MetricLabelShard          = "shard"
	MetricLabelTopic          = "topic"
	MetricLabelMerchantID     = "merchant.id"
	MetricLabelJobID          = "job.id"
	MetricLabelWalletID       = "wallet.id"
	MetricLabelErrorType      = "error.type"
	MetricLabelErrorMessage   = "error.message"
	MetricLabelHandler        = "handler"
	MetricLabelLimitType      = "limit.type"
	MetricLabelPartition      = "partition"
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
	outboxPanicsTotal             metric.Int64Counter
	consumerPanicsTotal           metric.Int64Counter
	redisFailClosedTotal          metric.Int64Counter
	dlqInfraFloodTotal            metric.Int64Counter
	kafkaProducerBufferFill       metric.Float64Gauge
	idempotencyConflictsTotal     metric.Int64Counter
	idempotencyHitsTotal          metric.Int64Counter
	consumerMsgDuration           metric.Float64Histogram
	consumerBackoffDuration       metric.Float64Histogram
	consumerCommitsTotal          metric.Int64Counter
	consumerLagMessages           metric.Int64Gauge
	commitCoordinatorQueueDepth   metric.Int64Gauge
	taskChannelFillRatio          metric.Float64Gauge
	ledgerImbalanceTotal          metric.Int64Gauge
	sagaUnresolvedCount           metric.Int64Gauge
	velocityLimitExceededTotal    metric.Int64Counter
	adminDLQReplayedTotal         metric.Int64Counter
	bulkheadRejectionsTotal       metric.Int64Counter
	bulkheadInFlight              metric.Int64UpDownCounter

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

		outboxPanicsTotal, err = meter.Int64Counter(
			MetricOutboxPanicsTotal,
			metric.WithDescription("Total number of panics caught in outbox relay loop"),
		)
		if err != nil {
			panic("failed to initialize outbox panics metric: " + err.Error())
		}

		consumerPanicsTotal, err = meter.Int64Counter(
			MetricConsumerPanicsTotal,
			metric.WithDescription("Total number of panics caught in any service's worker pool"),
		)
		if err != nil {
			panic("failed to initialize consumer panics metric: " + err.Error())
		}

		redisFailClosedTotal, err = meter.Int64Counter(
			MetricRedisFailClosed,
			metric.WithDescription("Number of operations rejected because Redis was unavailable (fail-closed)"),
		)
		if err != nil {
			panic("failed to initialize redis fail-closed metric: " + err.Error())
		}

		dlqInfraFloodTotal, err = meter.Int64Counter(
			MetricDLQInfrastructureFlood,
			metric.WithDescription("Number of messages routed to the DLQ specifically because an infrastructure dependency was down (vs. business-rule violations)"),
		)
		if err != nil {
			panic("failed to initialize DLQ infrastructure flood metric: " + err.Error())
		}

		kafkaProducerBufferFill, err = meter.Float64Gauge(
			MetricKafkaProducerBufferFill,
			metric.WithDescription("Kafka producer staging buffer fill ratio (0.0-1.0) computed from writer.BufferedBytes vs the 50KB allocation budget"),
		)
		if err != nil {
			panic("failed to initialize kafka producer buffer fill ratio metric: " + err.Error())
		}

		idempotencyConflictsTotal, err = meter.Int64Counter(
			MetricIdempotencyConflictsTotal,
			metric.WithDescription("Total number of idempotency conflicts"),
		)
		if err != nil {
			panic(err)
		}

		idempotencyHitsTotal, err = meter.Int64Counter(
			MetricIdempotencyHitsTotal,
			metric.WithDescription("Total number of idempotency key replay hits (duplicate submission resolved to prior job)"),
		)
		if err != nil {
			panic(err)
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

		consumerLagMessages, err = meter.Int64Gauge(
			MetricConsumerLagMessages,
			metric.WithDescription("Consumer lag in messages per partition"),
		)
		if err != nil {
			panic("failed to initialize consumer lag metric: " + err.Error())
		}

		commitCoordinatorQueueDepth, err = meter.Int64Gauge(
			MetricCommitCoordinatorQueueDepth,
			metric.WithDescription("Messages waiting in per-partition CommitCoordinator FIFO"),
		)
		if err != nil {
			panic("failed to initialize commit coordinator queue depth metric: " + err.Error())
		}

		taskChannelFillRatio, err = meter.Float64Gauge(
			MetricTaskChannelFillRatio,
			metric.WithDescription("taskChan fill ratio (0.0-1.0) per partition"),
		)
		if err != nil {
			panic("failed to initialize task channel fill ratio metric: " + err.Error())
		}

		ledgerImbalanceTotal, err = meter.Int64Gauge(
			MetricLedgerImbalanceTotal,
			metric.WithDescription("Total double-entry ledger imbalance sum"),
		)
		if err != nil {
			panic("failed to initialize ledger imbalance metric: " + err.Error())
		}

		sagaUnresolvedCount, err = meter.Int64Gauge(
			MetricSagaUnresolvedCount,
			metric.WithDescription("Total count of hanging cross-shard sagas unresolved > 120s"),
		)
		if err != nil {
			panic("failed to initialize saga unresolved count metric: " + err.Error())
		}

		velocityLimitExceededTotal, err = meter.Int64Counter(
			MetricVelocityLimitExceededTotal,
			metric.WithDescription("Total number of velocity limit violations"),
		)
		if err != nil {
			panic("failed to initialize velocity limit exceeded metric: " + err.Error())
		}

		adminDLQReplayedTotal, err = meter.Int64Counter(
			MetricAdminDLQReplayedTotal,
			metric.WithDescription("Total number of DLQ messages successfully replayed"),
		)
		if err != nil {
			panic("failed to initialize admin dlq replayed metric: " + err.Error())
		}

		bulkheadRejectionsTotal, err = meter.Int64Counter(
			MetricBulkheadRejectionsTotal,
			metric.WithDescription("Total number of requests rejected by the bulkhead"),
		)
		if err != nil {
			panic("failed to initialize bulkhead rejections metric: " + err.Error())
		}

		bulkheadInFlight, err = meter.Int64UpDownCounter(
			MetricBulkheadInFlight,
			metric.WithDescription("Current number of requests in flight through the bulkhead"),
		)
		if err != nil {
			panic("failed to initialize bulkhead in flight metric: " + err.Error())
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

// RecordOutboxPanic increments the counter for panics recovered in the outbox relay.
func RecordOutboxPanic(ctx context.Context, shardID string) {
	outboxPanicsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelShard, shardID),
	))
}

// RecordConsumerPanic increments the counter for panics recovered in any
// service's worker pool. The topic is required because the outbox relay uses
// shardID (which is also a topic for the relay's purposes).
func RecordConsumerPanic(ctx context.Context, topic string) {
	consumerPanicsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelTopic, topic),
	))
}

// RecordRedisFailClosed increments the counter for operations rejected because
// Redis was unavailable. Used by services that fail-closed on Redis errors
// (e.g., fraud-worker) so operators can distinguish "Redis down" from
// business-rule rejections in dashboards.
func RecordRedisFailClosed(ctx context.Context) {
	redisFailClosedTotal.Add(ctx, 1)
}

// RecordDLQInfraFlood increments the counter for messages routed to the DLQ
// because of an infrastructure failure (Redis, PG, Kafka down). Pair with
// RecordDLQIngestion to compute the ratio of infra-driven vs. business-driven
// DLQ entries. See issue 36.
func RecordDLQInfraFlood(ctx context.Context, serviceName string) {
	dlqInfraFloodTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelService, serviceName),
	))
}

// RecordKafkaProducerBufferFill records the Kafka staging buffer fill ratio (0.0-1.0).
func RecordKafkaProducerBufferFill(ctx context.Context, shardID string, ratio float64) {
	kafkaProducerBufferFill.Record(ctx, ratio, metric.WithAttributes(
		attribute.String(MetricLabelShard, shardID),
	))
}

// RecordIdempotencyConflict increments the counter for business/idempotency conflicts.
func RecordIdempotencyConflict(ctx context.Context, merchantID, jobID, shardID string) {
	idempotencyConflictsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelMerchantID, merchantID),
		attribute.String(MetricLabelJobID, jobID),
		attribute.String(MetricLabelShard, shardID),
	))
}

// RecordIdempotencyHit increments the counter for idempotency-key replay hits.
// A hit is a duplicate submission resolved to the prior job without a conflict error.
func RecordIdempotencyHit(ctx context.Context, merchantID, jobID, shardID string) {
	idempotencyHitsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelMerchantID, merchantID),
		attribute.String(MetricLabelJobID, jobID),
		attribute.String(MetricLabelShard, shardID),
	))
}

// RecordMessageProcessingDuration records the duration of message processing with a custom label.
func RecordMessageProcessingDuration(ctx context.Context, duration time.Duration, label string) {
	consumerMsgDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String(MetricLabelHandler, label),
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

// RecordLedgerImbalance records the absolute ledger imbalance sum.
func RecordLedgerImbalance(ctx context.Context, value int64) {
	ledgerImbalanceTotal.Record(ctx, value)
}

// RecordSagaUnresolvedCount records the unresolved saga count.
func RecordSagaUnresolvedCount(ctx context.Context, count int64) {
	sagaUnresolvedCount.Record(ctx, count)
}

// RecordVelocityLimitExceeded records a velocity limit violation.
func RecordVelocityLimitExceeded(ctx context.Context, walletID, limitType string) {
	velocityLimitExceededTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelWalletID, walletID),
		attribute.String(MetricLabelLimitType, limitType),
	))
}

// RecordAdminDLQReplayed increments the counter of successfully replayed DLQ messages.
func RecordAdminDLQReplayed(ctx context.Context, shardID string, count int64) {
	adminDLQReplayedTotal.Add(ctx, count, metric.WithAttributes(
		attribute.String(MetricLabelShard, shardID),
	))
}

// RecordBulkheadRejection increments the counter of requests rejected by the bulkhead.
func RecordBulkheadRejection(ctx context.Context) {
	bulkheadRejectionsTotal.Add(ctx, 1)
}

// AddBulkheadInFlight adjusts the current in-flight request count through the bulkhead.
func AddBulkheadInFlight(ctx context.Context, delta int64) {
	bulkheadInFlight.Add(ctx, delta)
}

// RecordConsumerLag records the number of messages behind the consumer per partition.
func RecordConsumerLag(ctx context.Context, topic string, partition int, lag int64) {
	consumerLagMessages.Record(ctx, lag, metric.WithAttributes(
		attribute.String(MetricLabelTopic, topic),
		attribute.Int(MetricLabelPartition, partition),
	))
}

// RecordCommitCoordinatorQueueDepth records the number of messages waiting in a partition's FIFO.
func RecordCommitCoordinatorQueueDepth(ctx context.Context, topic string, partition int, depth int64) {
	commitCoordinatorQueueDepth.Record(ctx, depth, metric.WithAttributes(
		attribute.String(MetricLabelTopic, topic),
		attribute.Int(MetricLabelPartition, partition),
	))
}

// RecordTaskChannelFillRatio records the fill ratio of a per-partition task channel.
func RecordTaskChannelFillRatio(ctx context.Context, topic string, partition int, ratio float64) {
	taskChannelFillRatio.Record(ctx, ratio, metric.WithAttributes(
		attribute.String(MetricLabelTopic, topic),
		attribute.Int(MetricLabelPartition, partition),
	))
}

// RecordConsumerCommitWithPartition records a successful offset commit with partition info.
func RecordConsumerCommitWithPartition(ctx context.Context, topic map[string]struct{}, partition int) {
	for t := range topic {
		consumerCommitsTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelTopic, t),
			attribute.Int(MetricLabelPartition, partition),
		))
	}
}
