package platform

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// Logging Fields
	LogFieldComponent  = "component"
	LogFieldEvent      = "event"
	LogFieldJobID      = "job_id"
	LogFieldDuration   = "duration_ms"
	LogFieldStatus     = "status_code"
	LogFieldShardID    = "shard_id"
	LogFieldTopic      = "topic"
	LogFieldGroup      = "group"
	LogFieldAddr       = "addr"
	LogFieldPath       = "path"
	LogFieldMethod     = "method"
	LogFieldName       = "name"
	LogFieldFrom       = "from"
	LogFieldTo         = "to"
	LogFieldPanic      = "panic"
	LogFieldKey        = "message_key"
	LogFieldSrcShard   = "src_shard"
	LogFieldDstShard   = "dst_shard"
	LogFieldJobStatus  = "job_status"
	LogFieldTransferID = "transfer_id"
	LogFieldReason     = "reason"
	LogFieldEntryID    = "entry_id"
	LogFieldCount      = "count"
	LogFieldEventID    = "event_id"
	LogFieldSize       = "size"
	LogFieldAttempt    = "attempt"
	LogFieldDLQID      = "dlq_id"

	// Logging Events
	LogEventDLQReplayCompleted           = "dlq_replay_completed"
	LogEventDLQReplayFailed              = "dlq_replay_failed"
	LogEventDLQUpdateFailed              = "dlq_update_failed"
	LogEventAdminDLQReplayRequested      = "admin_dlq_replay_requested"
	LogEventMerchantsDBConnected         = "merchants_db_connected"
	LogEventShardDBConnected             = "shard_db_connected"
	LogEventKafkaWriterCreated           = "kafka_writer_created"
	LogEventKafkaReaderCreated           = "kafka_reader_created"
	LogEventRedisConnected               = "redis_connected"
	LogEventRateLimitExceeded            = "rate_limit_exceeded"
	LogEventBulkheadRejected             = "bulkhead_rejected"
	LogEventHTTPRequestHandled           = "http_request_handled"
	LogEventServerStarted                = "server_started"
	LogEventServerShutdown               = "server_shutdown"
	LogEventJWTSigningFailed             = "jwt_signing_failed"
	LogEventStartupFailed                = "startup_failed"
	LogEventShutdownSignalReceived       = "shutdown_signal_received"
	LogEventShutdownFailed               = "shutdown_failed"
	LogEventServerFailed                 = "server_failed"
	LogEventKafkaMessageHandled          = "kafka_message_handled"
	LogEventBatchProcessed               = "batch_processed"
	LogEventCanonicalLog                 = "canonical_log"
	LogEventTelemetryInitFailed          = "telemetry_init_failed"
	LogEventPostgresInitFailed           = "postgres_init_failed"
	LogEventServerFatalError             = "server_fatal_error"
	LogEventNoShardsAvailable            = "no_shards_available"
	LogEventKafkaConsumerStopped         = "kafka_consumer_stopped"
	LogEventRetrySchedulerStarted        = "retry_scheduler_started"
	LogEventRetrySchedulerStopped        = "retry_scheduler_stopped"
	LogEventKafkaFetchFailed             = "kafka_fetch_failed"
	LogEventKafkaCommitFailed            = "kafka_commit_failed"
	LogEventPanicRecovered               = "panic_recovered"
	LogEventPanicRecoveredDLQ            = "panic_recovered_dlq"
	LogEventTerminalBusinessError        = "terminal_business_error"
	LogEventPoisonPill                   = "poison_pill"
	LogEventDLQWriteFailed               = "dlq_write_failed"
	LogEventPoisonDLQWriteFailed         = "poison_dlq_write_failed"
	LogEventPanicDLQWriteFailed          = "panic_dlq_write_failed"
	LogEventCrossShardTerminalDLQ        = "cross_shard_terminal_dlq"
	LogEventCrossShardDLQWriteFailed     = "cross_shard_dlq_write_failed"
	LogEventDLQRetryFailed               = "dlq_retry_failed"
	LogEventDLQWriteExhausted            = "dlq_write_exhausted"
	LogEventRedisInitFailed              = "redis_init_failed"
	LogEventRedisVelocityUpdateFailed    = "redis_velocity_update_failed"
	LogEventMerchantLookupFailed         = "merchant_lookup_failed"
	LogEventWalletStatusCheckFailed      = "wallet_status_check_failed"
	LogEventWalletFreezeFailed           = "wallet_freeze_failed"
	LogEventReadinessCheckFailed         = "readiness_check_failed"
	LogEventJWTKeyNotFound               = "jwt_key_not_found"
	LogEventDBPoolsConnectFailed         = "db_pools_connect_failed"
	LogEventRSAKeyMarshalFailed          = "rsa_key_marshal_failed"
	LogEventActiveMerchantsFetchFailed   = "active_merchants_fetch_failed"
	LogEventRelayServiceStarted          = "relay_service_started"
	LogEventRelayServiceShutdown         = "relay_service_shutdown"
	LogEventRelayBatchProcessFailed      = "relay_batch_process_failed"
	LogEventAllRelayersShutdown          = "all_relayers_shut_down_gracefully"
	LogEventOutboxShutdownTimeout        = "outbox_shutdown_timeout_exceeded"
	LogEventWebhookShutdownTimeout       = "webhook_shutdown_timeout_exceeded"
	LogEventAllConsumersShutdown         = "all_consumers_shut_down_gracefully"
	LogEventConsumerShutdownTimeout      = "consumer_shutdown_timeout_exceeded"
	LogEventReconRunFailed               = "recon_run_failed"
	LogEventReconCompletedSuccess        = "recon_completed_successfully"
	LogEventReconDiscrepanciesFound      = "recon_discrepancies_found"
	LogEventReconStarted                 = "reconciliation_started"
	LogEventReconLockReleaseFailed       = "lock_release_failed"
	LogEventReconConservationCheckFailed = "conservation_check_failed"
	LogEventReconLegImbalanceDetected    = "leg_imbalance_detected"
	LogEventReconWalletCheckFailed       = "wallet_check_failed"
	LogEventReconCompleted               = "reconciliation_completed"
	LogEventWebhookReceived              = "webhook_received"
	LogEventRequestBodyReadFailed        = "request_body_read_failed"
	LogEventWorkerShutdownGraceful       = "worker_shutdown_graceful"
	LogEventWorkerShutdownForce          = "worker_shutdown_forceful"

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
	DBCommitDurationMs     float64         `json:"db_commit_duration_ms,omitempty"`
	KafkaPublishDurationMs float64         `json:"kafka_publish_duration_ms,omitempty"`
	HTTPLatencyMs          float64         `json:"http_latency_ms,omitempty"`
	RetryCount             int             `json:"retry_count,omitempty"`
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
			zap.String("trace_id", span.SpanContext().TraceID().String()),
			zap.String("span_id", span.SpanContext().SpanID().String()),
		)
	}
	return logger
}

// LogCanonicalEvent emits the canonical log line with trace correlation
// Call at transaction boundaries: after DB commit, after Kafka publish, after HTTP delivery, etc.
func LogCanonicalEvent(ctx context.Context, logger *zap.Logger, serviceName string, line CanonicalLogLine) {
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

	// Marshal to single JSON field to prevent field explosion in Elasticsearch
	jsonBytes, _ := json.Marshal(line)
	logger.Info(LogEventCanonicalLog, zap.ByteString("canonical_json", jsonBytes))
}
