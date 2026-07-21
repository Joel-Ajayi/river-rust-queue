package platform

import (
	"crypto/ed25519"
	"errors"
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

var (
	ErrMissingMerchantsDBURI = errors.New("MERCHANTS_DB_URI is required")
	ErrNoShardURIs           = errors.New("at least one SHARD_*_URI is required")
	ErrNoJWTSigningKey       = errors.New("JWT_SIGNING_KEYS or JWT_SIGNING_KEY is required")
	ErrActiveKeyIDNotFound   = errors.New("JWT_ACTIVE_KEY_ID not found in signing keys")
	ErrUnknownShard          = errors.New("unknown shard")
	ErrCBMerchantsOpen       = errors.New("merchants circuit breaker is open")
	ErrCBRWOpen              = errors.New("shard RW circuit breaker is open")
	ErrCBROpen               = errors.New("shard RO circuit breaker is open")
	ErrReconciliationHeld    = errors.New("reconciliation lock is already held by another runner")
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

	KongConfigAPIVersion    string
	KongAPIGroup            string
	KongAPIVersion          string
	KongResourceConsumers   string
	KongResourcePlugins     string
	KongResourceCredentials string
	KongKindConsumer        string
	KongKindPlugin          string
	KongKindCredential      string
	KongPluginRateLimiting  string
	KongPluginPolicy        string
	KongCredentialTypeJWT   string
	KongJWTAlgorithm        string

	PlatformAdminKey string
}

// LoadConfig reads configuration from env vars (injected by K8s ConfigMap + Secret).
func LoadConfig() (*Config, error) {
	cfg := &Config{
		MerchantsDBURI: os.Getenv("MERCHANTS_DB_URI"),
		ShardURIs:      make(map[string]string),

		KafkaBrokers:     strings.Split(envOrDefault("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopicJobs:   envOrDefault("KAFKA_TOPIC_JOBS", TopicJobs),
		KafkaTopicNotify: envOrDefault("KAFKA_TOPIC_NOTIFY", TopicNotify),

		RedisDataHost:     envOrDefault("REDIS_DATA_HOST", "localhost"),
		RedisDataPort:     envOrDefault("REDIS_DATA_PORT", "6379"),
		RedisDataPassword: os.Getenv("REDIS_DATA_PASSWORD"),

		JWTSigningKeys: parseJWTKeys(os.Getenv("JWT_SIGNING_KEYS")),
		JWTActiveKeyID: envOrDefault("JWT_ACTIVE_KEY_ID", DefaultKeyID),

		HTTPPort:    envOrDefaultInt("HTTP_PORT", 8080),
		MetricsPort: envOrDefaultInt("METRICS_PORT", 9090),

		LogLevel:        envOrDefault("LOG_LEVEL", "info"),
		TraceSampleRate: envOrDefaultFloat("TRACE_SAMPLE_RATE", 1.0),

		KubernetesNamespace: envOrDefault("KUBERNETES_NAMESPACE", "rrq"),

		KongConfigAPIVersion:    envOrDefault("KONG_CONFIG_API_VERSION", "configuration.konghq.com/v1"),
		KongAPIGroup:            envOrDefault("KONG_API_GROUP", "configuration.konghq.com"),
		KongAPIVersion:          envOrDefault("KONG_API_VERSION", "v1"),
		KongResourceConsumers:   envOrDefault("KONG_RESOURCE_CONSUMERS", "kongconsumers"),
		KongResourcePlugins:     envOrDefault("KONG_RESOURCE_PLUGINS", "kongplugins"),
		KongResourceCredentials: envOrDefault("KONG_RESOURCE_CREDENTIALS", "kongcredentials"),
		KongKindConsumer:        envOrDefault("KONG_KIND_CONSUMER", "KongConsumer"),
		KongKindPlugin:          envOrDefault("KONG_KIND_PLUGIN", "KongPlugin"),
		KongKindCredential:      envOrDefault("KONG_KIND_CREDENTIAL", "KongCredential"),
		KongPluginRateLimiting:  envOrDefault("KONG_PLUGIN_RATE_LIMITING", "rate-limiting"),
		KongPluginPolicy:        envOrDefault("KONG_PLUGIN_POLICY", "redis"),
		KongCredentialTypeJWT:   envOrDefault("KONG_CREDENTIAL_TYPE_JWT", "jwt"),
		KongJWTAlgorithm:        envOrDefault("KONG_JWT_ALGORITHM", "RS256"),

		PlatformAdminKey: os.Getenv("RRQ_PLATFORM_KEY"),
	}

	// dynamically load shard URIs from env vars
	for _, shard := range strings.Split(DefaultShardLetters, "") {
		if uri := os.Getenv("SHARD_" + shard + "_URI"); uri != "" {
			cfg.ShardURIs[ShardIDPrefix+strings.ToLower(shard)] = uri
		}
	}

	if cfg.MerchantsDBURI == "" {
		return nil, fmt.Errorf("%w", ErrMissingMerchantsDBURI)
	}
	if len(cfg.ShardURIs) == 0 {
		return nil, fmt.Errorf("%w", ErrNoShardURIs)
	}
	if len(cfg.JWTSigningKeys) == 0 {
		return nil, fmt.Errorf("%w", ErrNoJWTSigningKey)
	}
	if _, ok := cfg.JWTSigningKeys[cfg.JWTActiveKeyID]; !ok {
		return nil, fmt.Errorf("%w: kid=%s", ErrActiveKeyIDNotFound, cfg.JWTActiveKeyID)
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

func envOrDefault(key, fallback string) string {
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
