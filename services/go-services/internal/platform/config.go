package platform

import (
	"crypto/ed25519"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	DefaultKeyID  = "default"
	ShardIDPrefix = "shard-"
)

// Config holds environment-sourced configuration for an RRQ service.
type Config struct {
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
}

// LoadConfig reads configuration from env vars (injected by K8s ConfigMap + Secret).
func LoadConfig() (*Config, error) {
	cfg := &Config{
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
	}

	// dynamically load shard URIs from env vars
	for _, shard := range strings.Split(DefaultShardLetters, "") {
		if uri := os.Getenv("SHARD_" + shard + "_URI"); uri != "" {
			cfg.ShardURIs[ShardIDPrefix+strings.ToLower(shard)] = uri
		}
	}

	return cfg, nil
}

func (c *Config) RedisAddr() string             { return c.RedisDataHost + ":" + c.RedisDataPort }
func (c *Config) DefaultTimeout() time.Duration { return 30 * time.Second }

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
