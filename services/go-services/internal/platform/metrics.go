package platform

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// Observability Metrics
	// names
	MetricMeterName                   = "rrq/platform"
	MetricCBOpenTotal                 = "circuit_breaker.open.total"
	MetricCBHalfOpenFailure           = "circuit_breaker.half_open.failure"
	MetricCBState                     = "circuit_breaker.state"
	MetricDLQIngestionRate            = "dlq.ingestion.rate"
	MetricInfraErrorsTotal            = "infrastructure.errors.total"
	MetricOutboxLagSeconds            = "outbox.lag.seconds"
	MetricOutboxEventsPublishedTotal  = "outbox.events.published.total"
	MetricOutboxPanicsTotal           = "outbox.panics.total"
	MetricConsumerPanicsTotal         = "kafka_consumer.panics.total"
	MetricDLQInfrastructureFlood      = "dlq.infrastructure_flood.total"
	MetricKafkaProducerBufferFill     = "kafka.producer.buffer_fill.ratio"
	MetricIdempotencyConflictsTotal   = "idempotency.conflicts.total"
	MetricIdempotencyHitsTotal        = "idempotency.hits.total"
	MetricConsumerBackoffDuration     = "kafka_consumer.backoff_duration.seconds"
	MetricCommitCoordinatorQueueDepth = "commit_coordinator.queue.depth"
	MetricLedgerImbalance             = "ledger.imbalance"
	MetricSagaUnresolvedCount         = "saga.unresolved.count"
	MetricVelocityLimitExceededTotal  = "velocity.limit_exceeded.total"
	MetricAdminDLQReplayedTotal       = "admin.dlq_replayed.total"
	MetricBulkheadRejectionsTotal     = "bulkhead.rejections.total"
	MetricBulkheadInFlight            = "bulkhead.in_flight"
	MetricPGPoolAcquiredConns         = "pg.pool.acquired.conns"
	MetricPGPoolIdleConns             = "pg.pool.idle.conns"
	MetricPGPoolMaxConns              = "pg.pool.max.conns"
	MetricPGPoolEmptyAcquireCount     = "pg.pool.empty_acquire.count"
	MetricRetryBudgetExhaustedTotal   = "retry.budget.exhausted.total"
	MetricReconDiscrepanciesTotal     = "recon.discrepancies.total"
	MetricRedisFailClosed             = "redis.fail_closed.total"
	MetricTaskChannelFillRatio        = "task_channel.fill_ratio"

	// label
	MetricLabelCircuitBreaker = "circuit.breaker"
	MetricLabelService        = "service"
	MetricLabelComponent      = "component"
	MetricLabelShard          = "shard"
	MetricLabelDBPool         = "db.pool"
	MetricLabelTopic          = "topic"
	MetricLabelErrorType      = "error.type"
	MetricLabelErrorMessage   = "error.message"
	MetricLabelHandler        = "handler"
	MetricLabelLimitType      = "limit.type"
	MetricLabelPartition      = "partition"
	MetricLabelAmount         = "amount"
	MetricLabelIdempotencyKey = "idempotency.key"
	MetricLabelPayloadSize    = "payload.size"
	MetricLabelSignature      = "signature"
	MetricLabelKeyID          = "key.id"
	MetricLabelPort           = "port"
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
	outboxPanicsTotal             metric.Int64Counter
	consumerPanicsTotal           metric.Int64Counter
	redisFailClosedTotal          metric.Int64Counter
	dlqInfraFloodTotal            metric.Int64Counter
	kafkaProducerBufferFill       metric.Float64Gauge
	idempotencyConflictsTotal     metric.Int64Counter
	idempotencyHitsTotal          metric.Int64Counter
	consumerBackoffDuration       metric.Float64Histogram
	commitCoordinatorQueueDepth   metric.Int64Gauge
	taskChannelFillRatio          metric.Float64Gauge
	ledgerImbalanceTotal          metric.Int64Gauge
	sagaUnresolvedCount           metric.Int64Gauge
	velocityLimitExceededTotal    metric.Int64Counter
	adminDLQReplayedTotal         metric.Int64Counter
	bulkheadRejectionsTotal       metric.Int64Counter
	bulkheadInFlight              metric.Int64UpDownCounter

	pgPoolAcquiredConns     metric.Int64Gauge
	pgPoolIdleConns         metric.Int64Gauge
	pgPoolMaxConns          metric.Int64Gauge
	pgPoolEmptyAcquireCount metric.Int64Counter

	retryBudgetExhaustedTotal metric.Int64Counter
	reconDiscrepanciesTotal   metric.Int64Counter
)

// InitMetrics registers all platform OTel instruments. Call from main() after InitTelemetry.
func InitMetrics() error {
	var err error
	var initErr error

	register := func(name string, fn func() error) {
		if initErr != nil {
			return
		}
		if e := fn(); e != nil {
			initErr = fmt.Errorf("metric %s: %w", name, e)
		}
	}

	register("circuit_breaker_open_total", func() error {
		circuitBreakerOpenTotal, err = meter.Int64Counter(
			MetricCBOpenTotal,
			metric.WithDescription("Total number of times a circuit breaker has tripped to OPEN"),
		)
		return err
	})
	register("circuit_breaker_half_open_failure", func() error {
		circuitBreakerHalfOpenFailure, err = meter.Int64Counter(
			MetricCBHalfOpenFailure,
			metric.WithDescription("Total number of times a circuit breaker failed its half-open trial"),
		)
		return err
	})
	register("dlq_ingestion_rate", func() error {
		dlqIngestionRate, err = meter.Int64Counter(
			MetricDLQIngestionRate,
			metric.WithDescription("Total number of poison pills routed to the DLQ"),
		)
		return err
	})
	register("circuit_breaker_state", func() error {
		circuitBreakerState, err = meter.Int64Gauge(
			MetricCBState,
			metric.WithDescription("Current state of the circuit breaker (0=Closed, 1=HalfOpen, 2=Open)"),
		)
		return err
	})
	register("outbox_lag_seconds", func() error {
		outboxLagGauge, err = meter.Float64Gauge(
			MetricOutboxLagSeconds,
			metric.WithDescription("Age in seconds of the oldest unpublished outbox event per shard"),
		)
		return err
	})
	register("infrastructure_errors_total", func() error {
		infrastructureErrorsTotal, err = meter.Int64Counter(
			MetricInfraErrorsTotal,
			metric.WithDescription("Total number of transient infrastructure errors encountered"),
		)
		return err
	})
	register("outbox_events_published_total", func() error {
		outboxEventsPublishedTotal, err = meter.Int64Counter(
			MetricOutboxEventsPublishedTotal,
			metric.WithDescription("Total number of events successfully published by outbox relay"),
		)
		return err
	})
	register("outbox_panics_total", func() error {
		outboxPanicsTotal, err = meter.Int64Counter(
			MetricOutboxPanicsTotal,
			metric.WithDescription("Total number of panics caught in outbox relay loop"),
		)
		return err
	})
	register("consumer_panics_total", func() error {
		consumerPanicsTotal, err = meter.Int64Counter(
			MetricConsumerPanicsTotal,
			metric.WithDescription("Total number of panics caught in any service's worker pool"),
		)
		return err
	})
	register("redis_fail_closed_total", func() error {
		redisFailClosedTotal, err = meter.Int64Counter(
			MetricRedisFailClosed,
			metric.WithDescription("Number of operations rejected because Redis was unavailable (fail-closed)"),
		)
		return err
	})
	register("dlq_infrastructure_flood_total", func() error {
		dlqInfraFloodTotal, err = meter.Int64Counter(
			MetricDLQInfrastructureFlood,
			metric.WithDescription("Number of messages routed to the DLQ specifically because an infrastructure dependency was down (vs. business-rule violations)"),
		)
		return err
	})
	register("kafka_producer_buffer_fill", func() error {
		kafkaProducerBufferFill, err = meter.Float64Gauge(
			MetricKafkaProducerBufferFill,
			metric.WithDescription("Kafka producer staging buffer fill ratio (0.0-1.0) computed from writer.BufferedBytes vs the 50KB allocation budget"),
		)
		return err
	})
	register("idempotency_conflicts_total", func() error {
		idempotencyConflictsTotal, err = meter.Int64Counter(
			MetricIdempotencyConflictsTotal,
			metric.WithDescription("Total number of idempotency conflicts"),
		)
		return err
	})
	register("idempotency_hits_total", func() error {
		idempotencyHitsTotal, err = meter.Int64Counter(
			MetricIdempotencyHitsTotal,
			metric.WithDescription("Total number of idempotency key replay hits (duplicate submission resolved to prior job)"),
		)
		return err
	})
	register("consumer_backoff_duration", func() error {
		consumerBackoffDuration, err = meter.Float64Histogram(
			MetricConsumerBackoffDuration,
			metric.WithDescription("Duration of consumer backoff sleeps"),
			metric.WithUnit("s"),
		)
		return err
	})
	register("commit_coordinator_queue_depth", func() error {
		commitCoordinatorQueueDepth, err = meter.Int64Gauge(
			MetricCommitCoordinatorQueueDepth,
			metric.WithDescription("Messages waiting in per-partition CommitCoordinator FIFO"),
		)
		return err
	})
	register("task_channel_fill_ratio", func() error {
		taskChannelFillRatio, err = meter.Float64Gauge(
			MetricTaskChannelFillRatio,
			metric.WithDescription("taskChan fill ratio (0.0-1.0) per partition"),
		)
		return err
	})
	register("ledger_imbalance", func() error {
		ledgerImbalanceTotal, err = meter.Int64Gauge(
			MetricLedgerImbalance,
			metric.WithDescription("Total double-entry ledger imbalance sum"),
		)
		return err
	})
	register("saga_unresolved_count", func() error {
		sagaUnresolvedCount, err = meter.Int64Gauge(
			MetricSagaUnresolvedCount,
			metric.WithDescription("Total count of hanging cross-shard sagas unresolved > 120s"),
		)
		return err
	})
	register("velocity_limit_exceeded_total", func() error {
		velocityLimitExceededTotal, err = meter.Int64Counter(
			MetricVelocityLimitExceededTotal,
			metric.WithDescription("Total number of velocity limit violations"),
		)
		return err
	})
	register("admin_dlq_replayed_total", func() error {
		adminDLQReplayedTotal, err = meter.Int64Counter(
			MetricAdminDLQReplayedTotal,
			metric.WithDescription("Total number of DLQ messages successfully replayed"),
		)
		return err
	})
	register("bulkhead_rejections_total", func() error {
		bulkheadRejectionsTotal, err = meter.Int64Counter(
			MetricBulkheadRejectionsTotal,
			metric.WithDescription("Total number of requests rejected by the bulkhead"),
		)
		return err
	})
	register("bulkhead_in_flight", func() error {
		bulkheadInFlight, err = meter.Int64UpDownCounter(
			MetricBulkheadInFlight,
			metric.WithDescription("Current number of requests in flight through the bulkhead"),
		)
		return err
	})
	register("pg_pool_acquired_conns", func() error {
		pgPoolAcquiredConns, err = meter.Int64Gauge(
			MetricPGPoolAcquiredConns,
			metric.WithDescription("Number of currently acquired connections in the PG pool"),
		)
		return err
	})
	register("pg_pool_idle_conns", func() error {
		pgPoolIdleConns, err = meter.Int64Gauge(
			MetricPGPoolIdleConns,
			metric.WithDescription("Number of currently idle connections in the PG pool"),
		)
		return err
	})
	register("pg_pool_max_conns", func() error {
		pgPoolMaxConns, err = meter.Int64Gauge(
			MetricPGPoolMaxConns,
			metric.WithDescription("Maximum number of connections allowed in the PG pool"),
		)
		return err
	})
	register("pg_pool_empty_acquire_count", func() error {
		pgPoolEmptyAcquireCount, err = meter.Int64Counter(
			MetricPGPoolEmptyAcquireCount,
			metric.WithDescription("Cumulative count of successful acquires that waited for a connection to be released or established"),
		)
		return err
	})
	register("retry_budget_exhausted_total", func() error {
		retryBudgetExhaustedTotal, err = meter.Int64Counter(
			MetricRetryBudgetExhaustedTotal,
			metric.WithDescription("Total number of retry budget exhaustion events (Token Bucket denial) per service"),
		)
		return err
	})
	register("recon_discrepancies_total", func() error {
		reconDiscrepanciesTotal, err = meter.Int64Counter(
			MetricReconDiscrepanciesTotal,
			metric.WithDescription("Total number of reconciliation discrepancies detected across shards"),
		)
		return err
	})

	return initErr
}

// RecordCircuitBreakerOpen increments the counter for tripped circuit breakers.
func RecordCircuitBreakerOpen(ctx context.Context, cbName string) {
	if circuitBreakerOpenTotal != nil {
		circuitBreakerOpenTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelCircuitBreaker, cbName),
		))
	}
}

// RecordCircuitBreakerHalfOpenFailure increments the counter for half-open trial failures.
func RecordCircuitBreakerHalfOpenFailure(ctx context.Context, cbName string) {
	if circuitBreakerHalfOpenFailure != nil {
		circuitBreakerHalfOpenFailure.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelCircuitBreaker, cbName),
		))
	}
}

// RecordOutboxLag sets the age of the oldest unpublished event
func RecordOutboxLag(ctx context.Context, shardID string, lag time.Duration) {
	if outboxLagGauge != nil {
		outboxLagGauge.Record(ctx, lag.Seconds(), metric.WithAttributes(
			attribute.String(MetricLabelShard, shardID),
		))
	}
}

// RecordDLQIngestion increments the DLQ ingestion metric.
func RecordDLQIngestion(ctx context.Context, topic string) {
	if dlqIngestionRate != nil {
		dlqIngestionRate.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelTopic, topic),
		))
	}
}

// RecordCircuitBreakerState sets the current state of a circuit breaker as a gauge.
func RecordCircuitBreakerState(ctx context.Context, cbName string, state int64) {
	if circuitBreakerState != nil {
		circuitBreakerState.Record(ctx, state, metric.WithAttributes(
			attribute.String(MetricLabelCircuitBreaker, cbName),
		))
	}
}

// RecordInfrastructureError increments the counter for transient infrastructure errors.
func RecordInfrastructureError(ctx context.Context, component string) {
	if infrastructureErrorsTotal != nil {
		infrastructureErrorsTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelComponent, component),
		))
	}
}

// RecordOutboxEventsPublished increments the counter for successfully published outbox events.
func RecordOutboxEventsPublished(ctx context.Context, shardID string, topic string, count int) {
	if outboxEventsPublishedTotal != nil {
		outboxEventsPublishedTotal.Add(ctx, int64(count), metric.WithAttributes(
			attribute.String(MetricLabelShard, shardID),
			attribute.String(MetricLabelTopic, topic),
		))
	}
}

// RecordOutboxPanic increments the counter for panics recovered in the outbox relay.
func RecordOutboxPanic(ctx context.Context, shardID string) {
	if outboxPanicsTotal != nil {
		outboxPanicsTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelShard, shardID),
		))
	}
}

// RecordConsumerPanic increments the counter for panics recovered in any service's worker pool. Topic required (outbox relay uses shardID as topic).
func RecordConsumerPanic(ctx context.Context, topic string) {
	if consumerPanicsTotal != nil {
		consumerPanicsTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelTopic, topic),
		))
	}
}

// RecordRedisFailClosed increments the counter for operations rejected because Redis was unavailable (fail-closed).
func RecordRedisFailClosed(ctx context.Context) {
	if redisFailClosedTotal != nil {
		redisFailClosedTotal.Add(ctx, 1)
	}
}

// RecordDLQInfraFlood increments the counter for messages routed to the DLQ because of an infrastructure failure (Redis, PG, Kafka down).
func RecordDLQInfraFlood(ctx context.Context, serviceName string) {
	if dlqInfraFloodTotal != nil {
		dlqInfraFloodTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelService, serviceName),
		))
	}
}

// RecordKafkaProducerBufferFill records the Kafka staging buffer fill ratio (0.0-1.0).
func RecordKafkaProducerBufferFill(ctx context.Context, shardID string, ratio float64) {
	if kafkaProducerBufferFill != nil {
		kafkaProducerBufferFill.Record(ctx, ratio, metric.WithAttributes(
			attribute.String(MetricLabelShard, shardID),
		))
	}
}

// RecordIdempotencyConflict increments the counter for business/idempotency conflicts.
func RecordIdempotencyConflict(ctx context.Context, merchantID, jobID, shardID string) {
	if idempotencyConflictsTotal != nil {
		idempotencyConflictsTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelShard, shardID),
		))
	}
}

// RecordIdempotencyHit increments the counter for idempotency-key replay hits.
// A hit is a duplicate submission resolved to the prior job without a conflict error.
func RecordIdempotencyHit(ctx context.Context, merchantID, jobID, shardID string) {
	if idempotencyHitsTotal != nil {
		idempotencyHitsTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelShard, shardID),
		))
	}
}

// RecordConsumerBackoffDuration records the duration of consumer backoff sleep.
func RecordConsumerBackoffDuration(ctx context.Context, topic string, duration time.Duration) {
	if consumerBackoffDuration != nil {
		consumerBackoffDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
			attribute.String(MetricLabelTopic, topic),
		))
	}
}

// RecordLedgerImbalance records the absolute ledger imbalance sum.
func RecordLedgerImbalance(ctx context.Context, value int64) {
	if ledgerImbalanceTotal != nil {
		ledgerImbalanceTotal.Record(ctx, value)
	}
}

// RecordSagaUnresolvedCount records the unresolved saga count.
func RecordSagaUnresolvedCount(ctx context.Context, count int64) {
	if sagaUnresolvedCount != nil {
		sagaUnresolvedCount.Record(ctx, count)
	}
}

// RecordVelocityLimitExceeded records a velocity limit violation.
func RecordVelocityLimitExceeded(ctx context.Context, walletID, limitType string) {
	if velocityLimitExceededTotal != nil {
		velocityLimitExceededTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelLimitType, limitType),
		))
	}
}

// RecordAdminDLQReplayed increments the counter of successfully replayed DLQ messages.
func RecordAdminDLQReplayed(ctx context.Context, shardID string, count int64) {
	if adminDLQReplayedTotal != nil {
		adminDLQReplayedTotal.Add(ctx, count, metric.WithAttributes(
			attribute.String(MetricLabelShard, shardID),
		))
	}
}

// RecordBulkheadRejection increments the counter of requests rejected by the bulkhead.
func RecordBulkheadRejection(ctx context.Context) {
	if bulkheadRejectionsTotal != nil {
		bulkheadRejectionsTotal.Add(ctx, 1)
	}
}

// AddBulkheadInFlight adjusts the current in-flight request count through the bulkhead.
func AddBulkheadInFlight(ctx context.Context, delta int64) {
	if bulkheadInFlight != nil {
		bulkheadInFlight.Add(ctx, delta)
	}
}

// RecordCommitCoordinatorQueueDepth records the number of messages waiting in a partition's FIFO.
func RecordCommitCoordinatorQueueDepth(ctx context.Context, topic string, partition int, depth int64) {
	if commitCoordinatorQueueDepth != nil {
		commitCoordinatorQueueDepth.Record(ctx, depth, metric.WithAttributes(
			attribute.String(MetricLabelTopic, topic),
			attribute.Int(MetricLabelPartition, partition),
		))
	}
}

// RecordTaskChannelFillRatio records the fill ratio of a per-partition task channel.
func RecordTaskChannelFillRatio(ctx context.Context, topic string, partition int, ratio float64) {
	if taskChannelFillRatio != nil {
		taskChannelFillRatio.Record(ctx, ratio, metric.WithAttributes(
			attribute.String(MetricLabelTopic, topic),
			attribute.Int(MetricLabelPartition, partition),
		))
	}
}

// RecordPGPoolStats records the current pgxpool statistics.
func RecordPGPoolStats(ctx context.Context, poolName string, serviceName string, acquired, idle, maxConns int32, emptyAcquireCount int64) {
	attrs := metric.WithAttributes(
		attribute.String(MetricLabelService, serviceName),
		attribute.String(MetricLabelDBPool, poolName),
	)
	if pgPoolAcquiredConns != nil {
		pgPoolAcquiredConns.Record(ctx, int64(acquired), attrs)
	}
	if pgPoolIdleConns != nil {
		pgPoolIdleConns.Record(ctx, int64(idle), attrs)
	}
	if pgPoolMaxConns != nil {
		pgPoolMaxConns.Record(ctx, int64(maxConns), attrs)
	}
}

// AddPGPoolEmptyAcquireCount adds to the cumulative count of empty acquires.
func AddPGPoolEmptyAcquireCount(ctx context.Context, poolName string, serviceName string, delta int64) {
	if pgPoolEmptyAcquireCount != nil {
		pgPoolEmptyAcquireCount.Add(ctx, delta, metric.WithAttributes(
			attribute.String(MetricLabelService, serviceName),
			attribute.String(MetricLabelDBPool, poolName),
		))
	}
}

// RecordRetryBudgetExhausted increments the counter for retry budget exhaustion (Token Bucket denial).
// Service dimension lives on the OTel service.name resource attribute, not on the metric label.
func RecordRetryBudgetExhausted(ctx context.Context) {
	if retryBudgetExhaustedTotal != nil {
		retryBudgetExhaustedTotal.Add(ctx, 1)
	}
}

// RecordReconDiscrepancies increments the counter for reconciliation discrepancies across shards.
func RecordReconDiscrepancies(ctx context.Context, count int64) {
	if count <= 0 {
		return
	}
	if reconDiscrepanciesTotal != nil {
		reconDiscrepanciesTotal.Add(ctx, count)
	}
}

const (
	MetricLabelMerchantID = "merchant.id"
	MetricLabelJobID      = "job.id"
	MetricLabelWalletID   = "wallet.id"
)
