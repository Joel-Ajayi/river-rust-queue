package platform


type AggregateType string
type EventType string

const (
	// Paths
	APIVersionV1     = "/v1"
	APIVersionV2     = "/v2"
	APIPathPrefix    = APIVersionV1
	APIJobPathPrefix = APIPathPrefix + "/jobs/"
	APITransfersPath = APIPathPrefix + "/transfers"
	APIBalancesPath  = APIPathPrefix + "/balances"
	APIAuthTokenPath = APIPathPrefix + "/auth/token"
	APIHealthPath    = "/health"
	APIReadyPath     = "/ready"

	// Aggregate types
	AggregateTypeJob            AggregateType = "job"
	AggregateTypeEvent          AggregateType = "ev"
	AggregateTypeWallet         AggregateType = "wallet"
	AggregateTypeTransfer       AggregateType = "transfer"
	AggregateTypeXShardTransfer AggregateType = "xshard_transfer"

	// Event types
	EventTypeJobRequested            EventType = "job.requested"
	EventTypeTransferCompleted       EventType = "transfer.completed"
	EventTypeTransferFailed          EventType = "transfer.failed"
	EventTypeXShardTransferRequested EventType = "xshard.transfer.requested"
	EventTypeXShardTransferSettled   EventType = "xshard.transfer.settled"
	EventTypeXShardTransferFailed    EventType = "xshard.transfer.failed"

	// Service Names
	ServiceNameAPIGateway   = "api-gateway"
	ServiceNameLedgerWorker = "ledger-worker"
	ServiceNameOutboxRelay  = "outbox-relay"

	// Circuit Breaker Names
	CBNameAPIGatewayKafkaPublisher = "KafkaPublisher"
	CBNameOutboxKafkaPublisher     = "OutboxKafkaPublisher"

	// Observability Metrics
	MetricMeterName               = "rrq/platform"
	MetricCBOpenTotal             = "rrq_circuit_breaker_open_total"
	MetricCBHalfOpenFailure       = "rrq_circuit_breaker_half_open_failure"
	MetricCBState                 = "rrq_circuit_breaker_state"
	MetricDLQIngestionRate        = "rrq_dlq_ingestion_rate"
	MetricInfraErrorsTotal        = "rrq_infrastructure_errors_total"
	MetricLabelCircuitBreaker     = "circuit_breaker"
	MetricLabelService            = "service"

	// Logging Fields
	LogFieldEvent     = "event"
	LogFieldTraceID   = "trace_id"
	LogFieldJobID     = "job_id"
	LogFieldDuration  = "duration_ms"
	LogFieldStatus    = "status_code"
	LogFieldShardID   = "shard_id"
	LogFieldTopic     = "topic"
	LogFieldGroup     = "group"
	LogFieldAddr      = "addr"
	LogFieldPath      = "path"
	LogFieldMethod    = "method"

	// Logging Events
	LogEventMerchantsDBConnected = "merchants_db_connected"
	LogEventShardDBConnected     = "shard_db_connected"
	LogEventKafkaWriterCreated   = "kafka_writer_created"
	LogEventKafkaReaderCreated   = "kafka_reader_created"
	LogEventRedisConnected       = "redis_connected"
	LogEventRateLimitExceeded    = "rate_limit_exceeded"
	LogEventBulkheadRejected     = "bulkhead_rejected"
	LogEventHTTPRequestHandled   = "http_request_handled"
	LogEventServerStarted          = "server_started"
	LogEventServerShutdown         = "server_shutdown"
	LogEventJWTSigningFailed       = "jwt_signing_failed"
	LogEventStartupFailed          = "startup_failed"
	LogEventShutdownSignalReceived = "shutdown_signal_received"
	LogEventShutdownFailed         = "shutdown_failed"
	LogEventServerFailed           = "server_failed"

	// Logging Components
	LogComponentKafka      = "kafka"
	LogComponentPostgres   = "postgres"
	LogComponentRedis      = "redis"
	LogComponentRESTServer = "rest_server"
)
