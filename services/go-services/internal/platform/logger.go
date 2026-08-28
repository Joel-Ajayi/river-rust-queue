package platform

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// Logging Fields
	LogFieldComponent              = "component"
	LogFieldEvent                  = "event"
	LogFieldJobID                  = "job_id"
	LogFieldDuration               = "duration_ms"
	LogFieldStatus                 = "status_code"
	LogFieldShardID                = "shard_id"
	LogFieldTopic                  = "topic"
	LogFieldStack                  = "stack"
	LogFieldSource                 = "source"
	LogFieldGroup                  = "group"
	LogFieldAddr                   = "addr"
	LogFieldPath                   = "path"
	LogFieldMethod                 = "method"
	LogFieldName                   = "name"
	LogFieldFrom                   = "from"
	LogFieldTo                     = "to"
	LogFieldPanic                  = "panic"
	LogFieldKey                    = "message_key"
	LogFieldSrcShard               = "src_shard"
	LogFieldDstShard               = "dst_shard"
	LogFieldJobStatus              = "job_status"
	LogFieldTransferID             = "transfer_id"
	LogFieldReason                 = "reason"
	LogFieldEntryID                = "entry_id"
	LogFieldCount                  = "count"
	LogFieldEventID                = "event_id"
	LogFieldSize                   = "size"
	LogFieldAttempt                = "attempt"
	LogFieldDLQID                  = "dlq_id"
	LogFieldCBName                 = "circuit_breaker_name"
	LogFieldWalletID               = "wallet_id"
	LogFieldMerchantID             = "merchant_id"
	LogFieldAmount                 = "amount"
	LogFieldCurrency               = "currency"
	LogFieldFromWallet             = "from_wallet"
	LogFieldToWallet               = "to_wallet"
	LogFieldIdempotencyKey         = "idempotency_key"
	LogFieldURL                    = "url"
	LogFieldStatusCode             = "status_code"
	LogFieldLatency                = "latency_ms"
	LogFieldTraceID                = "trace_id"
	LogFieldSpanID                 = "span_id"
	LogFieldLevel                  = "level"
	LogFieldService                = "service"
	LogFieldTimestamp              = "timestamp"
	LogFieldErrorCode              = "error_code"
	LogFieldErrorMessage           = "error_message"
	LogFieldDBCommitDurationMs     = "db_commit_duration_ms"
	LogFieldKafkaPublishDurationMs = "kafka_publish_duration_ms"
	LogFieldHTTPLatencyMs          = "http_latency_ms"
	LogFieldIdempotencyHit         = "idempotency_hit"
	LogFieldThreshold              = "threshold"
	LogFieldNextRetryAt            = "next_retry_at"

	// Consumer Pipeline Logging Fields
	LogFieldPartition    = "partition"
	LogFieldOffset       = "offset"
	LogFieldErrorType    = "error_type"
	LogFieldRetryCount   = "retry_count"
	LogFieldBatchSize    = "batch_size"
	LogFieldBatchTimeout = "batch_timeout"
	LogFieldChanFill     = "channel_fill_ratio"
	LogFieldWorkerPool   = "worker_pool_size"
	LogFieldPollMs       = "max_poll_interval_ms"
	LogFieldDelay        = "delay_ms"
	LogFieldMaxDelay     = "max_delay_ms"

	// Logging Events
	LogEventDLQReplayCompleted            = "dlq.replay_completed"
	LogEventDLQReplayFailed               = "dlq.replay_failed"
	LogEventDLQUpdateFailed               = "dlq.update_failed"
	LogEventAdminDLQReplayRequested       = "admin.dlq_replay_requested"
	LogEventMerchantsDBConnected          = "postgres.merchants_db_connected"
	LogEventShardDBConnected              = "postgres.shard_db_connected"
	LogEventKafkaWriterCreated            = "kafka.writer_created"
	LogEventKafkaReaderCreated            = "kafka.reader_created"
	LogEventRedisConnected                = "redis.connected"
	LogEventRateLimitExceeded             = "rate_limiter.exceeded"
	LogEventBulkheadRejected              = "bulkhead.rejected"
	LogEventHTTPRequestHandled            = "http.request_handled"
	LogEventServerStarted                 = "server.started"
	LogEventServerShutdown                = "server.shutdown"
	LogEventServerDraining                = "server.draining"
	LogEventJWTSigningFailed              = "jwt.signing_failed"
	LogEventStartupFailed                 = "server.startup_failed"
	LogEventShutdownSignalReceived        = "server.shutdown_signal_received"
	LogEventShutdownFailed                = "server.shutdown_failed"
	LogEventServerFailed                  = "server.failed"
	LogEventKafkaMessageHandled           = "kafka.message_handled"
	LogEventBatchProcessed                = "outbox.batch_processed"
	LogEventCanonicalLog                  = "canonical.log"
	LogEventTelemetryInitFailed           = "telemetry.init_failed"
	LogEventMetricsInitFailed             = "metrics.init_failed"
	LogEventBusinessMetricsInitFailed     = "business_metrics.init_failed"
	LogEventCircuitBreakerOpened          = "circuit_breaker.opened"
	LogEventCircuitBreakerClosed          = "circuit_breaker.closed"
	LogEventSagaScanFailed                = "saga.scan_failed"
	LogEventRetryBudgetExhaustedDLQ       = "kafka_consumer.retry_budget_exhausted_dlq"
	LogEventScheduleRetryFailed           = "webhook.schedule_retry_failed"
	LogEventDeliveryCompleteFailed        = "webhook.delivery_complete_failed"
	LogEventFastLaneEnqueued              = "webhook.fast_lane_enqueued"
	LogEventTransferFailed                = "transfer.failed"
	LogEventTransferSubmitted             = "transfer.submitted"
	LogEventTransferCompleted             = "transfer.completed"
	LogEventTransferValidated             = "transfer.validated"
	LogEventTransferPosted                = "transfer.posted"
	LogEventTransferDeclined              = "transfer.declined"
	LogEventJobNotFound                   = "job.not_found"
	LogEventDepositSubmitted              = "deposit.submitted"
	LogEventDepositFailed                 = "deposit.failed"
	LogEventConsumerPanic                 = "kafka_consumer.panic"
	LogEventInvalidPayload                = "kafka_consumer.invalid_payload"
	LogEventWalletCreateFailed            = "wallet.create_failed"
	LogEventAuthTokenFailed               = "auth.token_failed"
	LogEventMerchantCreateFailed          = "merchant.create_failed"
	LogEventOutboxLagCheckFailed          = "outbox.lag_check_failed"
	LogEventOutboxPublishFailed           = "outbox.publish_failed"
	LogEventSagaRequested                 = "saga.requested"
	LogEventSagaSettled                   = "saga.settled"
	LogEventSagaCompensated               = "saga.compensated"
	LogEventSagaFailed                    = "saga.failed"
	LogEventVelocityLimitExceeded         = "fraud.velocity_limit_exceeded"
	LogEventWebhookDeliveryAttemptFailed  = "webhook.delivery_attempt_failed"
	LogEventWebhookDeliverySucceeded      = "webhook.delivery_succeeded"
	LogEventWebhookDeliveryScheduled      = "webhook.delivery_scheduled"
	LogEventWebhookDeliveryDLQ            = "webhook.delivery_dlq"
	LogEventPostgresInitFailed            = "postgres.init_failed"
	LogEventServerFatalError              = "server.fatal_error"
	LogEventNoShardsAvailable             = "postgres.no_shards_available"
	LogEventKafkaConsumerStopped          = "kafka.consumer_stopped"
	LogEventRetrySchedulerStarted         = "webhook.retry_scheduler_started"
	LogEventRetrySchedulerStopped         = "webhook.retry_scheduler_stopped"
	LogEventKafkaFetchFailed              = "kafka.fetch_failed"
	LogEventKafkaCommitFailed             = "kafka.commit_failed"
	LogEventPanicRecovered                = "kafka_consumer.panic_recovered"
	LogEventPanicRecoveredDLQ             = "kafka_consumer.panic_recovered_dlq"
	LogEventTerminalBusinessError         = "kafka_consumer.terminal_business_error"
	LogEventPoisonPill                    = "kafka_consumer.poison_pill"
	LogEventDLQWriteFailed                = "dlq.write_failed"
	LogEventPoisonDLQWriteFailed          = "dlq.poison_write_failed"
	LogEventPanicDLQWriteFailed           = "dlq.panic_write_failed"
	LogEventCrossShardTerminalDLQ         = "xshard.terminal_dlq"
	LogEventCrossShardDLQWriteFailed      = "xshard.dlq_write_failed"
	LogEventDLQRetryFailed                = "dlq.retry_failed"
	LogEventDLQWriteExhausted             = "dlq.write_exhausted"
	LogEventRedisInitFailed               = "redis.init_failed"
	LogEventRedisVelocityUpdateFailed     = "redis.velocity_update_failed"
	LogEventMerchantLookupFailed          = "fraud.merchant_lookup_failed"
	LogEventWalletStatusCheckFailed       = "fraud.wallet_status_check_failed"
	LogEventWalletFreezeFailed            = "fraud.wallet_freeze_failed"
	LogEventReadinessCheckFailed          = "server.readiness_check_failed"
	LogEventJWTKeyNotFound                = "jwt.key_not_found"
	LogEventDBPoolsConnectFailed          = "postgres.db_pools_connect_failed"
	LogEventRSAKeyMarshalFailed           = "jwt.rsa_key_marshal_failed"
	LogEventActiveMerchantsFetchFailed    = "postgres.active_merchants_fetch_failed"
	LogEventRelayServiceStarted           = "outbox.relay_started"
	LogEventRelayServiceShutdown          = "outbox.relay_shutdown"
	LogEventRelayServiceDraining          = "outbox.relay_draining"
	LogEventRelayBatchProcessFailed       = "outbox.batch_process_failed"
	LogEventShardDBBreakerOpenPausingPoll = "outbox.shard_breaker_open_pausing"
	LogEventKafkaBufferFull               = "outbox.kafka_buffer_full_pausing"
	LogEventKafkaBufferResumed            = "outbox.kafka_buffer_drained_resuming"
	LogEventAllRelayersShutdown           = "outbox.all_relayers_shutdown"
	LogEventOutboxShutdownTimeout         = "outbox.shutdown_timeout"
	LogEventRelayWorkerError              = "outbox.relay_worker_error"
	LogEventKafkaWriterCloseFailed        = "kafka.writer_close_failed"
	LogEventWebhookShutdownTimeout        = "webhook.shutdown_timeout"
	LogEventAllConsumersShutdown          = "kafka_consumer.all_shutdown"
	LogEventConsumerShutdownTimeout       = "kafka_consumer.shutdown_timeout"
	LogEventReconRunFailed                = "recon.run_failed"
	LogEventReconCompletedSuccess         = "recon.completed_success"
	LogEventReconDiscrepanciesFound       = "recon.discrepancies_found"
	LogEventReconStarted                  = "recon.started"
	LogEventReconLockReleaseFailed        = "recon.lock_release_failed"
	LogEventReconConservationCheckFailed  = "recon.conservation_check_failed"
	LogEventReconLegImbalanceDetected     = "recon.leg_imbalance_detected"
	LogEventReconWalletCheckFailed        = "recon.wallet_check_failed"
	LogEventReconCompleted                = "recon.completed"
	LogEventWebhookReceived               = "webhook.received"
	LogEventRequestBodyReadFailed         = "http.request_body_read_failed"
	LogEventWorkerShutdownGraceful        = "kafka_consumer.shutdown_graceful"
	LogEventWorkerShutdownForce           = "kafka_consumer.shutdown_forceful"

	// Consumer Pipeline Logging Events
	LogEventConsumerCommitFailed    = "kafka_consumer.commit_failed"
	LogEventConsumerFetchRetry      = "kafka_consumer.fetch_retry"
	LogEventConsumerDrainTimeout    = "kafka_consumer.drain_timeout"
	LogEventConsumerChannelRefresh  = "kafka_consumer.channel_refresh"
	LogEventConsumerCoordinatorStop = "kafka_consumer.coordinator_stop"

	// Logging Components
	LogComponentKafka      = "kafka"
	LogComponentPostgres   = "postgres"
	LogComponentRedis      = "redis"
	LogComponentRESTServer = "rest_server"
)

// CanonicalEvent represents the event type for canonical log lines (single JSON at transaction boundary)
type CanonicalEvent string

const (
	// Transfer lifecycle
	EventTransferSubmitted CanonicalEvent = "transfer.submitted"
	EventTransferAccepted  CanonicalEvent = "transfer.accepted"
	EventTransferValidated CanonicalEvent = "transfer.validated"
	EventTransferPosted    CanonicalEvent = "transfer.posted"
	EventTransferCompleted CanonicalEvent = "transfer.completed"
	EventTransferFailed    CanonicalEvent = "transfer.failed"

	// Webhook lifecycle
	EventWebhookDelivered CanonicalEvent = "webhook.delivered"
	EventWebhookFailed    CanonicalEvent = "webhook.failed"
	EventWebhookDLQ       CanonicalEvent = "webhook.dlq"

	// Outbox lifecycle
	EventOutboxPublished CanonicalEvent = "outbox.published"
	EventOutboxFailed    CanonicalEvent = "outbox.failed"

	// Fraud
	EventFraudVelocityCheck CanonicalEvent = "fraud.velocity_check"
	EventFraudWalletFrozen  CanonicalEvent = "fraud.wallet_frozen"

	// Administration & Registration
	EventMerchantCreated CanonicalEvent = "merchant.created"
	EventWalletCreated   CanonicalEvent = "wallet.created"

	// Reconciliation
	EventReconStarted   CanonicalEvent = "recon.started"
	EventReconCompleted CanonicalEvent = "recon.completed"
	EventReconImbalance CanonicalEvent = "recon.imbalance_detected"

	// Saga
	EventSagaInitiated   CanonicalEvent = "saga.initiated"
	EventSagaCompleted   CanonicalEvent = "saga.completed"
	EventSagaCompensated CanonicalEvent = "saga.compensated"

	// System
	EventIdempotencyConflict  CanonicalEvent = "idempotency.conflict"
	EventCircuitBreakerOpened CanonicalEvent = "circuit_breaker.opened"
	EventCircuitBreakerClosed CanonicalEvent = "circuit_breaker.closed"
)

// CanonicalStatus represents the outcome status
type CanonicalStatus string

const (
	StatusSuccess CanonicalStatus = "success"
	StatusFailed  CanonicalStatus = "failed"
	StatusRetry   CanonicalStatus = "retry"
	StatusPending CanonicalStatus = "pending"
	StatusDLQ     CanonicalStatus = "dlq"
)

// CanonicalLogLine is the single structured log emitted at transaction boundaries
type CanonicalLogLine struct {
	Timestamp              time.Time       `json:"timestamp"`
	Level                  string          `json:"level"`
	Service                string          `json:"service"`
	TraceID                string          `json:"trace_id"`
	SpanID                 string          `json:"span_id"`
	MerchantID             string          `json:"merchant_id,omitempty"`
	JobID                  string          `json:"job_id,omitempty"`
	WalletID               string          `json:"wallet_id,omitempty"`
	TransferID             string          `json:"transfer_id,omitempty"`
	Event                  CanonicalEvent  `json:"event"`
	Status                 CanonicalStatus `json:"status"`
	Amount                 int64           `json:"amount,omitempty"`
	Currency               string          `json:"currency,omitempty"`
	ErrorCode              string          `json:"error_code,omitempty"`
	ErrorMessage           string          `json:"error_message,omitempty"`
	DurationMs             float64         `json:"duration_ms,omitempty"`
	DBCommitDurationMs     float64         `json:"db_commit_duration_ms,omitempty"`
	KafkaPublishDurationMs float64         `json:"kafka_publish_duration_ms,omitempty"`
	HTTPLatencyMs          float64         `json:"http_latency_ms,omitempty"`
	RetryCount             int             `json:"retry_count,omitempty"`
	IdempotencyHit         bool            `json:"idempotency_hit,omitempty"`
}

// NewLogger creates a structured zap logger at the specified level.
func NewLogger(level string) (*zap.Logger, error) {
	var cfg zap.Config
	if strings.EqualFold(level, "debug") {
		cfg = zap.NewDevelopmentConfig()
	} else {
		cfg = zap.NewProductionConfig()
	}

	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = zapcore.InfoLevel
	}
	cfg.Level.SetLevel(lvl)

	return cfg.Build()
}

// LoggerWithTrace injects the current OpenTelemetry trace_id and span_id into the zap.Logger
// to ensure perfect Trace-Log Correlation as mandated by the observability architecture.
func LoggerWithTrace(ctx context.Context, logger *zap.Logger) *zap.Logger {
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		return logger.With(
			zap.String(LogFieldTraceID, span.SpanContext().TraceID().String()),
			zap.String(LogFieldSpanID, span.SpanContext().SpanID().String()),
		)
	}
	return logger
}

// LogCanonicalEvent emits the canonical log line as flat zap fields (B3). Call at transaction boundaries: after DB commit, after Kafka publish, after HTTP delivery, etc.
func LogCanonicalEvent(ctx context.Context, logger *zap.Logger, serviceName string, line CanonicalLogLine) {
	// 1. Extract OpenTelemetry trace and span IDs if available
	span := trace.SpanFromContext(ctx)
	sc := span.SpanContext()

	if sc.HasTraceID() {
		line.TraceID = sc.TraceID().String()
	}
	if sc.HasSpanID() {
		line.SpanID = sc.SpanID().String()
	}

	line.Timestamp = time.Now().UTC()
	line.Level = "INFO"
	line.Service = serviceName

	// 2. Build flat structured fields using canonical constants
	fields := []zap.Field{
		zap.String(LogFieldEvent, string(LogEventCanonicalLog)),
		zap.Time(LogFieldTimestamp, line.Timestamp),
		zap.String(LogFieldLevel, line.Level),
		zap.String(LogFieldService, line.Service),
		zap.String(LogFieldTraceID, line.TraceID),
		zap.String(LogFieldSpanID, line.SpanID),
	}
	if line.MerchantID != "" {
		fields = append(fields, zap.String(LogFieldMerchantID, line.MerchantID))
	}
	if line.JobID != "" {
		fields = append(fields, zap.String(LogFieldJobID, line.JobID))
	}
	if line.WalletID != "" {
		fields = append(fields, zap.String(LogFieldWalletID, line.WalletID))
	}
	if line.TransferID != "" {
		fields = append(fields, zap.String(LogFieldTransferID, line.TransferID))
	}
	fields = append(fields,
		zap.String(LogFieldEvent, string(line.Event)),
		zap.String(LogFieldStatus, string(line.Status)),
	)
	if line.Amount != 0 {
		fields = append(fields, zap.Int64(LogFieldAmount, line.Amount))
	}
	if line.Currency != "" {
		fields = append(fields, zap.String(LogFieldCurrency, line.Currency))
	}
	if line.ErrorCode != "" {
		fields = append(fields, zap.String(LogFieldErrorCode, line.ErrorCode))
	}
	if line.ErrorMessage != "" {
		fields = append(fields, zap.String(LogFieldErrorMessage, line.ErrorMessage))
	}
	if line.DurationMs != 0 {
		fields = append(fields, zap.Float64(LogFieldDuration, line.DurationMs))
	}
	if line.DBCommitDurationMs != 0 {
		fields = append(fields, zap.Float64(LogFieldDBCommitDurationMs, line.DBCommitDurationMs))
	}
	if line.KafkaPublishDurationMs != 0 {
		fields = append(fields, zap.Float64(LogFieldKafkaPublishDurationMs, line.KafkaPublishDurationMs))
	}
	if line.HTTPLatencyMs != 0 {
		fields = append(fields, zap.Float64(LogFieldHTTPLatencyMs, line.HTTPLatencyMs))
	}
	if line.RetryCount != 0 {
		fields = append(fields, zap.Int(LogFieldRetryCount, line.RetryCount))
	}
	if line.IdempotencyHit {
		fields = append(fields, zap.Bool(LogFieldIdempotencyHit, line.IdempotencyHit))
	}

	// 3. Emit canonical line at Info level
	logger.Info(string(LogEventCanonicalLog), fields...)
}
