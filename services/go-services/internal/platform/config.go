package platform

import (
	"crypto/ed25519"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

const (
	DefaultKeyID  = "default"
	ShardIDPrefix = "shard-"
)

// Config holds environment-sourced configuration for an RRQ service.
type Config struct {
	ServiceName    string
	MerchantsDBURI string
	ShardURIs      map[string]string

	KafkaBrokers     []string
	KafkaTopicJobs   string
	KafkaTopicNotify string

	RedisDataHost     string
	RedisDataPort     string
	RedisDataPassword string

	JWTSigningKeys map[string]ed25519.PrivateKey
	JWTActiveKeyID string

	HTTPPort    int
	MetricsPort int

	LogLevel        string
	TraceSampleRate float64

	KubernetesNamespace string

	OtelExporterEndpoint string

	PlatformAdminKey string

	Capacity       *CapacityConfig
	GlobalCapacity *GlobalCapacityConfig
}

// CapacityConfig holds all variables derived by the SLO capacity engine.
type CapacityConfig struct {
	RequestTimeoutMs    int
	ServerTimeoutMs     int
	ShutdownTimeoutMs   int
	ServerIdleTimeoutMs int
	CBErrorThreshold    float64
	CBMinRequests       int
	CBTimeoutMs         int
	CBIntervalMs        int
	MaxRetries          int
	BackoffBaseMs       int
	BackoffCapMs        int
	RetryBudgetMinTokens int
	RetryBudgetMaxTokens int
	RetryBudgetFraction  float64

	// Memory request (MiB) derived by the capacity engine.
	PodMemRequestMiB int
	DBPoolSize       int
	WorkerPoolSize   int
	KafkaHeartbeatMs int
	KafkaSessionMs   int

	// HTTP Specific
	HTTPMaxIdleConns        int
	HTTPMaxIdleConnsPerHost int
	HTTPTimeoutMs           int

	// DLQ (dead-letter queue) retry policy
	DLQMaxRetries     int
	DLQBaseDelayMs    int
	DLQCapDelayMs     int
	DLQWriteTimeoutMs int // outer DLQ write deadline (engine-derived, == DLQ retry budget)

	// Other Specific
	JWTAccessHrs      int
	Argon2MemoryKib   int
	Argon2Iterations  int
	Argon2Parallelism int
	VelocityThreshold float64
	VelocityWindowMs  int
	FetchBatchSize    int
	AIMDThrottleFrac  float64
	AIMDPauseFrac     float64
	AIMDResumeFrac    float64

	// Per-pod per-shard RW and RO caps (engine-derived, not the DB's hard max_connections).
	// The DB's hard limit lives in the PG Cluster manifest, not in env vars.
	PGShardRWMaxConns     map[string]int
	PGShardROMaxConns     map[string]int
	PGMerchantsRWMaxConns int
	PGMerchantsROMaxConns int

	// Circuit breaker probes (engine-derived)
	CBMaxFails       int
	CBHalfOpenProbes int

	// Core-API
	MaxRequestBytes int

	// Webhook-API runtime
	VisibilityTimeoutMs         int
	DeliveryMaxAttempts         int
	DeliveryBackoffBaseMs       int
	DeliveryBackoffCapMs        int
	SchedulerPollIntervalMs     int
	SchedulerBatchSize          int
	FastLaneGracePeriodMs       int
	FastLaneBufferSize          int
	FastLaneWorkerPoolSize      int
	HTTPIdleConnTimeoutMs       int
	HTTPResponseHeaderTimeoutMs int
	HTTPTLSHandshakeTimeoutMs   int
	HTTPExpectContinueTimeoutMs int
	BreakerEvictionIntervalMs   int
	BreakerEvictionTTLMs        int
	WebhookMaxConcurrency       int

	// Outbox relay runtime
	RelayPoolIntervalMs          int
	RelayStagingKB               int // AIMD byte budget for the Kafka staging buffer
	RelayBatchMsgCount           int // kafka.Writer.BatchSize — message count, NOT bytes
	RelayBatchTimeoutMs          int
	RelayMaxPayloadBytes         int // producer-side payload validation cap; independent of Kafka reader fetch cap
	RelayBufferSampleIntervalMs  int
	RelayBufferMaxThrottleLevel  int
	RelayBufferMaxPollIntervalMs int
}

// Global DB pool bounds derived by the capacity engine.
type GlobalCapacityConfig struct {
	RedisMaxMemoryMiB             int
	KetamaVNodes                  int
	KafkaReaderMinBytes           int
	KafkaReaderMaxBytes           int
	KafkaReaderMaxWaitMs          int
	KafkaWriterMaxAttempts        int
	ConsumerMaxPendingBytes       int64
	ConsumerFetchGrabTimeoutMs    int
	ConsumerFetchBackoffMinMs     int
	ConsumerFetchBackoffMaxMs     int
	ConsumerChannelRefreshMs      int
	ConsumerDrainTimeoutMs        int
	ConsumerCommitFlushTimeoutMs  int
	ConsumerCommitFlushIntervalMs int
	ConsumerCommitBatchCapacity   int
	ConsumerPartitionChannelSize  int
	ConsumerMinCommitCapFrac      float64
	PGConnMaxIdleTimeMs           int
	PGConnMaxLifetimeMs           int
	RetryBudgetMinTokens          int
	RetryBudgetMaxTokens          int
	RetryBudgetFraction           float64
}

// LoadConfig reads configuration from env vars (injected by K8s ConfigMap + Secret).
func LoadConfig(servicePrefix string) *Config {
	cfg := &Config{
		ServiceName:    servicePrefix,
		MerchantsDBURI: os.Getenv("MERCHANTS_DB_URI"),
		ShardURIs:      make(map[string]string),

		KafkaBrokers:     strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopicJobs:   env("KAFKA_TOPIC_JOBS", TopicJobs),
		KafkaTopicNotify: env("KAFKA_TOPIC_NOTIFY", TopicNotify),

		RedisDataHost:     env("REDIS_DATA_HOST", "localhost"),
		RedisDataPort:     env("REDIS_DATA_PORT", "6379"),
		RedisDataPassword: os.Getenv("REDIS_DATA_PASSWORD"),

		JWTSigningKeys: parseJWTKeys(os.Getenv("JWT_SIGNING_KEYS")),
		JWTActiveKeyID: env("JWT_ACTIVE_KEY_ID", DefaultKeyID),

		HTTPPort:    envOrDefaultInt("HTTP_PORT", 8080),
		MetricsPort: envOrDefaultInt("METRICS_PORT", 9090),

		LogLevel:        env("LOG_LEVEL", "info"),
		TraceSampleRate: envOrDefaultFloat("TRACE_SAMPLE_RATE", 1.0),

		KubernetesNamespace: env("KUBERNETES_NAMESPACE", "rrq"),

		OtelExporterEndpoint: env("OTEL_EXPORTER_OTLP_ENDPOINT", "agent-exporter.observability.svc.cluster.local:4317"),

		Capacity:       LoadCapacityConfig(servicePrefix),
		GlobalCapacity: LoadGlobalCapacityConfig(),
	}

	// Determine which shard letters to load. The default set ("ABCDE") is
	// overridable via SHARD_LETTERS so the engine can plan for F+ when
	// traffic justifies a 6th shard (see issue 34).
	shardLetters := env("SHARD_LETTERS", DefaultShardLetters)
	for _, shard := range strings.Split(shardLetters, "") {
		if uri := os.Getenv("SHARD_" + shard + "_URI"); uri != "" {
			cfg.ShardURIs[ShardIDPrefix+strings.ToLower(shard)] = uri
		}
	}

	return cfg
}

func (c *Config) RedisAddr() string { return c.RedisDataHost + ":" + c.RedisDataPort }

// parseJWTKeys parses a comma-separated list of kid:key pairs.
func parseJWTKeys(keysEnv string) map[string]ed25519.PrivateKey {
	keys := make(map[string]ed25519.PrivateKey)
	if keysEnv != "" {
		for _, pair := range strings.Split(keysEnv, ",") {
			parts := strings.SplitN(pair, ":", 2)
			if len(parts) != 2 {
				continue
			}
			kid := strings.TrimSpace(parts[0])
			keyData := strings.TrimSpace(parts[1])
			keyData = strings.ReplaceAll(keyData, "\\n", "\n")
			if key, err := jwt.ParseEdPrivateKeyFromPEM([]byte(keyData)); err != nil {
				fmt.Fprintf(os.Stderr, "WARN: failed to parse JWT signing key %q: %v\n", kid, err)
			} else if edKey, ok := key.(ed25519.PrivateKey); ok {
				keys[kid] = edKey
			}
		}
	}
	return keys
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrDefaultInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envOrDefaultFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

// loadShardRWCaps loads per-shard RW connection caps from env vars for all
// configured shards (A–F, or as specified by SHARD_LETTERS). Each env var is
// of the form <PREFIX>PG_SHARD_<LETTER>_RW_MAX_CONNS and is rendered by the
// capacity engine.
func loadShardRWCaps(prefix string) map[string]int {
	shardLetters := env("SHARD_LETTERS", DefaultShardLetters)
	caps := make(map[string]int)
	for _, letter := range strings.Split(shardLetters, "") {
		if letter == "" {
			continue
		}
		envName := prefix + "PG_SHARD_" + strings.ToUpper(letter) + "_RW_MAX_CONNS"
		if v := envOrDefaultInt(envName, 0); v > 0 {
			shardID := ShardIDPrefix + strings.ToLower(letter)
			caps[shardID] = v
		}
	}
	return caps
}

func loadShardROCaps(prefix string) map[string]int {
	shardLetters := env("SHARD_LETTERS", DefaultShardLetters)
	caps := make(map[string]int)
	for _, letter := range strings.Split(shardLetters, "") {
		if letter == "" {
			continue
		}
		envName := prefix + "PG_SHARD_" + strings.ToUpper(letter) + "_RO_MAX_CONNS"
		if v := envOrDefaultInt(envName, 0); v > 0 {
			shardID := ShardIDPrefix + strings.ToLower(letter)
			caps[shardID] = v
		}
	}
	return caps
}

// LoadCapacityConfig loads the capacity engine's derived variables for a specific service.
func LoadCapacityConfig(prefix string) *CapacityConfig {
	return &CapacityConfig{
		RequestTimeoutMs:             envOrDefaultInt(prefix+"REQUEST_TIMEOUT_MS", 0),
		ServerTimeoutMs:              envOrDefaultInt(prefix+"SERVER_TIMEOUT_MS", 0),
		ShutdownTimeoutMs:            envOrDefaultInt(prefix+"SHUTDOWN_TIMEOUT_MS", 30000),
		ServerIdleTimeoutMs:          envOrDefaultInt(prefix+"SERVER_IDLE_TIMEOUT_MS", 60000),
		CBErrorThreshold:             envOrDefaultFloat(prefix+"CB_ERROR_THRESHOLD", 0.0),
		CBMinRequests:                envOrDefaultInt(prefix+"CB_MIN_REQUESTS", 0),
		CBTimeoutMs:                  envOrDefaultInt(prefix+"CB_TIMEOUT_MS", 0),
		CBIntervalMs:                 envOrDefaultInt(prefix+"CB_INTERVAL_MS", 10000),
		MaxRetries:                   envOrDefaultInt(prefix+"MAX_RETRIES", 0),
		BackoffBaseMs:                envOrDefaultInt(prefix+"BACKOFF_BASE_MS", 0),
		BackoffCapMs:                 envOrDefaultInt(prefix+"BACKOFF_CAP_MS", 0),
		RetryBudgetMinTokens:         envOrDefaultInt(prefix+"RETRY_BUDGET_MIN_TOKENS", 10),
		RetryBudgetMaxTokens:         envOrDefaultInt(prefix+"RETRY_BUDGET_MAX_TOKENS", 100),
		RetryBudgetFraction:          envOrDefaultFloat(prefix+"RETRY_BUDGET_FRACTION", 0.10),
		PodMemRequestMiB:             envOrDefaultInt(prefix+"POD_MEM_REQUEST_MIB", 0),
		DBPoolSize:                   envOrDefaultInt(prefix+"DB_POOL_SIZE", 0),
		WorkerPoolSize:               envOrDefaultInt(prefix+"WORKER_POOL_SIZE", 0),
		KafkaHeartbeatMs:             envOrDefaultInt(prefix+"KAFKA_HEARTBEAT_MS", 0),
		KafkaSessionMs:               envOrDefaultInt(prefix+"KAFKA_SESSION_MS", 0),
		HTTPMaxIdleConns:             envOrDefaultInt(prefix+"HTTP_MAX_IDLE_CONNS", 0),
		HTTPMaxIdleConnsPerHost:      envOrDefaultInt(prefix+"HTTP_MAX_IDLE_PER_HOST", 0),
		HTTPTimeoutMs:                envOrDefaultInt(prefix+"HTTP_TIMEOUT_MS", 0),
		DLQMaxRetries:                envOrDefaultInt(prefix+"DLQ_MAX_RETRIES", 0),
		DLQBaseDelayMs:               envOrDefaultInt(prefix+"DLQ_BASE_DELAY_MS", 0),
		DLQCapDelayMs:                envOrDefaultInt(prefix+"DLQ_CAP_DELAY_MS", 0),
		DLQWriteTimeoutMs:            envOrDefaultInt(prefix+"DLQ_WRITE_TIMEOUT_MS", 0),
		JWTAccessHrs:                 envOrDefaultInt(prefix+"JWT_ACCESS_HRS", 0),
		Argon2MemoryKib:              envOrDefaultInt(prefix+"ARGON2_MEMORY_KIB", 0),
		Argon2Iterations:             envOrDefaultInt(prefix+"ARGON2_ITERATIONS", 0),
		Argon2Parallelism:            envOrDefaultInt(prefix+"ARGON2_PARALLELISM", 0),
		VelocityThreshold:            envOrDefaultFloat(prefix+"VELOCITY_THRESHOLD", 0.0),
		VelocityWindowMs:             envOrDefaultInt(prefix+"VELOCITY_WINDOW_MS", 0),
		FetchBatchSize:               envOrDefaultInt(prefix+"FETCH_BATCH_SIZE", 0),
		AIMDThrottleFrac:             envOrDefaultFloat(prefix+"AIMD_THROTTLE_FRAC", 0.0),
		AIMDPauseFrac:                envOrDefaultFloat(prefix+"AIMD_PAUSE_FRAC", 0.0),
		AIMDResumeFrac:               envOrDefaultFloat(prefix+"AIMD_RESUME_FRAC", 0.0),
		PGShardRWMaxConns:            loadShardRWCaps(prefix),
		PGShardROMaxConns:            loadShardROCaps(prefix),
		PGMerchantsRWMaxConns:        envOrDefaultInt(prefix+"PG_MERCHANTS_RW_MAX_CONNS", 0),
		PGMerchantsROMaxConns:        envOrDefaultInt(prefix+"PG_MERCHANTS_RO_MAX_CONNS", 0),
		CBMaxFails:                   envOrDefaultInt(prefix+"CB_MAX_FAILS", 0),
		CBHalfOpenProbes:             envOrDefaultInt(prefix+"CB_HALF_OPEN_PROBES", 0),
		MaxRequestBytes:              envOrDefaultInt(prefix+"MAX_REQUEST_BYTES", 0),
		VisibilityTimeoutMs:          envOrDefaultInt(prefix+"VISIBILITY_TIMEOUT_MS", 0),
		DeliveryMaxAttempts:          envOrDefaultInt(prefix+"DELIVERY_MAX_ATTEMPTS", 0),
		DeliveryBackoffBaseMs:        envOrDefaultInt(prefix+"DELIVERY_BACKOFF_BASE_MS", 0),
		DeliveryBackoffCapMs:         envOrDefaultInt(prefix+"DELIVERY_BACKOFF_CAP_MS", 0),
		SchedulerPollIntervalMs:      envOrDefaultInt(prefix+"SCHEDULER_POLL_INTERVAL_MS", 0),
		SchedulerBatchSize:           envOrDefaultInt(prefix+"SCHEDULER_BATCH_SIZE", 0),
		FastLaneGracePeriodMs:        envOrDefaultInt(prefix+"FAST_LANE_GRACE_PERIOD_MS", 0),
		FastLaneBufferSize:           envOrDefaultInt(prefix+"FAST_LANE_BUFFER_SIZE", 0),
		FastLaneWorkerPoolSize:       envOrDefaultInt(prefix+"FAST_LANE_WORKER_POOL_SIZE", 0),
		HTTPIdleConnTimeoutMs:        envOrDefaultInt(prefix+"HTTP_IDLE_CONN_TIMEOUT_MS", 0),
		HTTPResponseHeaderTimeoutMs:  envOrDefaultInt(prefix+"HTTP_RESPONSE_HEADER_TIMEOUT_MS", 0),
		HTTPTLSHandshakeTimeoutMs:    envOrDefaultInt(prefix+"HTTP_TLS_HANDSHAKE_TIMEOUT_MS", 0),
		HTTPExpectContinueTimeoutMs:  envOrDefaultInt(prefix+"HTTP_EXPECT_CONTINUE_TIMEOUT_MS", 0),
		BreakerEvictionIntervalMs:    envOrDefaultInt(prefix+"BREAKER_EVICTION_INTERVAL_MS", 0),
		BreakerEvictionTTLMs:         envOrDefaultInt(prefix+"BREAKER_EVICTION_TTL_MS", 0),
		WebhookMaxConcurrency:        envOrDefaultInt(prefix+"WEBHOOK_MAX_CONCURRENCY", 0),
		RelayPoolIntervalMs:          envOrDefaultInt(prefix+"RELAY_POOL_INTERVAL_MS", 0),
		RelayStagingKB:               envOrDefaultInt(prefix+"STAGING_KB", 0),
		RelayBatchMsgCount:           envOrDefaultInt(prefix+"RELAY_BATCH_MSG_COUNT", 0),
		RelayBatchTimeoutMs:          envOrDefaultInt(prefix+"RELAY_BATCH_TIMEOUT_MS", 0),
		RelayMaxPayloadBytes:         envOrDefaultInt(prefix+"RELAY_MAX_PAYLOAD_BYTES", 0),
		RelayBufferSampleIntervalMs:  envOrDefaultInt(prefix+"RELAY_BUFFER_SAMPLE_INTERVAL_MS", 0),
		RelayBufferMaxThrottleLevel:  envOrDefaultInt(prefix+"RELAY_BUFFER_MAX_THROTTLE_LEVEL", 0),
		RelayBufferMaxPollIntervalMs: envOrDefaultInt(prefix+"RELAY_BUFFER_MAX_POLL_INTERVAL_MS", 0),
	}
}

// LoadGlobalCapacityConfig loads platform-level derived bounds.
// Per-shard/per-pod RW and RO caps are loaded in LoadCapacityConfig (per-service).
func LoadGlobalCapacityConfig() *GlobalCapacityConfig {
	return &GlobalCapacityConfig{
		RedisMaxMemoryMiB:             envOrDefaultInt("REDIS_MAXMEMORY_MIB", 0),
		KetamaVNodes:                  envOrDefaultInt("KETAMA_VNODES", 0),
		KafkaReaderMinBytes:           envOrDefaultInt("KAFKA_READER_MIN_BYTES", 0),
		KafkaReaderMaxBytes:           envOrDefaultInt("KAFKA_READER_MAX_BYTES", 0),
		KafkaReaderMaxWaitMs:          envOrDefaultInt("KAFKA_READER_MAX_WAIT_MS", 0),
		KafkaWriterMaxAttempts:        envOrDefaultInt("KAFKA_WRITER_MAX_ATTEMPTS", 0),
		ConsumerMaxPendingBytes:       int64(envOrDefaultInt("CONSUMER_MAX_PENDING_BYTES", 0)),
		ConsumerChannelRefreshMs:      envOrDefaultInt("CONSUMER_CHANNEL_REFRESH_MS", 0),
		ConsumerDrainTimeoutMs:        envOrDefaultInt("CONSUMER_DRAIN_TIMEOUT_MS", 0),
		ConsumerCommitFlushTimeoutMs:  envOrDefaultInt("CONSUMER_COMMIT_FLUSH_TIMEOUT_MS", 0),
		ConsumerCommitFlushIntervalMs: envOrDefaultInt("CONSUMER_COMMIT_FLUSH_INTERVAL_MS", 0),
		ConsumerCommitBatchCapacity:   envOrDefaultInt("CONSUMER_COMMIT_BATCH_CAPACITY", 0),
		ConsumerPartitionChannelSize:  envOrDefaultInt("CONSUMER_PARTITION_CHANNEL_SIZE", 0),
		ConsumerMinCommitCapFrac:      envOrDefaultFloat("CONSUMER_MIN_COMMIT_CAP_FRAC", 0.0),
		PGConnMaxIdleTimeMs:           envOrDefaultInt("PG_CONN_MAX_IDLE_TIME_MS", 0),
		PGConnMaxLifetimeMs:           envOrDefaultInt("PG_CONN_MAX_LIFETIME_MS", 0),
		RetryBudgetMinTokens:          envOrDefaultInt("RETRY_BUDGET_MIN_TOKENS", 10),
		RetryBudgetMaxTokens:          envOrDefaultInt("RETRY_BUDGET_MAX_TOKENS", 100),
		RetryBudgetFraction:           envOrDefaultFloat("RETRY_BUDGET_FRACTION", 0.10),
	}
}
