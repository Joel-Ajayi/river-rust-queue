package platform

import (
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// Logging Fields
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

	// Logging Events
	LogEventMerchantsDBConnected   = "merchants_db_connected"
	LogEventShardDBConnected       = "shard_db_connected"
	LogEventKafkaWriterCreated     = "kafka_writer_created"
	LogEventKafkaReaderCreated     = "kafka_reader_created"
	LogEventRedisConnected         = "redis_connected"
	LogEventRateLimitExceeded      = "rate_limit_exceeded"
	LogEventBulkheadRejected       = "bulkhead_rejected"
	LogEventHTTPRequestHandled     = "http_request_handled"
	LogEventServerStarted          = "server_started"
	LogEventServerShutdown         = "server_shutdown"
	LogEventJWTSigningFailed       = "jwt_signing_failed"
	LogEventStartupFailed          = "startup_failed"
	LogEventShutdownSignalReceived = "shutdown_signal_received"
	LogEventShutdownFailed         = "shutdown_failed"
	LogEventServerFailed           = "server_failed"
	LogEventKafkaMessageHandled    = "kafka_message_handled"
	LogEventBatchProcessed         = "batch_processed"

	// Logging Components
	LogComponentKafka      = "kafka"
	LogComponentPostgres   = "postgres"
	LogComponentRedis      = "redis"
	LogComponentRESTServer = "rest_server"
)

var sensitiveKeys = map[string]bool{
	"jwt_signing_key": true, "MERCHANTS_DB_URI": true,
	"SHARD_A_URI": true, "SHARD_B_URI": true,
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

// RedactedEnv returns os.Environ with sensitive values replaced by "***".
func RedactedEnv() []string {
	env := os.Environ()
	out := make([]string, 0, len(env))
	for _, e := range env {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) == 2 && sensitiveKeys[parts[0]] {
			out = append(out, parts[0]+"=***")
		} else {
			out = append(out, e)
		}
	}
	return out
}
