package platform

import (
	"crypto/rsa"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Config holds environment-sourced configuration for an RRQ service.
type Config struct {
	MerchantsDBURI string
	ShardURIs      map[string]string // e.g. {"shard-a": "postgres://..."}

	KafkaBrokers     []string
	KafkaTopicJobs   string
	KafkaTopicNotify string

	RedisDataHost string
	RedisDataPort string

	JWTSigningKeys map[string]*rsa.PrivateKey
	JWTActiveKeyID string

	HTTPPort    int
	MetricsPort int

	LogLevel        string
	TraceSampleRate float64
	
	BcryptCost int

	KubernetesNamespace string

	KongConfigAPIVersion   string
	KongAPIGroup           string
	KongAPIVersion         string
	KongResourceConsumers  string
	KongResourcePlugins    string
	KongResourceCredentials string
	KongKindConsumer       string
	KongKindPlugin         string
	KongKindCredential     string
	KongPluginRateLimiting string
	KongPluginPolicy       string
	KongCredentialTypeJWT  string
	KongJWTAlgorithm       string
}

// LoadConfig reads configuration from env vars (injected by K8s ConfigMap + Secret).
func LoadConfig() (*Config, error) {
	cfg := &Config{
		MerchantsDBURI: os.Getenv("MERCHANTS_DB_URI"),
		ShardURIs:      make(map[string]string),

		KafkaBrokers:     strings.Split(envOrDefault("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopicJobs:   envOrDefault("KAFKA_TOPIC_JOBS", "jobs"),
		KafkaTopicNotify: envOrDefault("KAFKA_TOPIC_NOTIFY", "notify"),

		RedisDataHost: envOrDefault("REDIS_DATA_HOST", "localhost"),
		RedisDataPort: envOrDefault("REDIS_DATA_PORT", "6379"),

		JWTSigningKeys: parseJWTKeys(os.Getenv("JWT_SIGNING_KEYS"), os.Getenv("JWT_SIGNING_KEY")),
		JWTActiveKeyID: envOrDefault("JWT_ACTIVE_KEY_ID", "default"),

		HTTPPort:    envOrDefaultInt("HTTP_PORT", 8080),
		MetricsPort: envOrDefaultInt("METRICS_PORT", 9090),

		LogLevel:        envOrDefault("LOG_LEVEL", "info"),
		TraceSampleRate: envOrDefaultFloat("TRACE_SAMPLE_RATE", 1.0),
		BcryptCost:      envOrDefaultInt("BCRYPT_COST", 12),

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
		KongPluginPolicy:        envOrDefault("KONG_PLUGIN_POLICY", "local"),
		KongCredentialTypeJWT:   envOrDefault("KONG_CREDENTIAL_TYPE_JWT", "jwt"),
		KongJWTAlgorithm:        envOrDefault("KONG_JWT_ALGORITHM", "RS256"),
	}

	if uri := os.Getenv("SHARD_A_URI"); uri != "" {
		cfg.ShardURIs["shard-a"] = uri
	}
	if uri := os.Getenv("SHARD_B_URI"); uri != "" {
		cfg.ShardURIs["shard-b"] = uri
	}

	if cfg.MerchantsDBURI == "" {
		return nil, fmt.Errorf("MERCHANTS_DB_URI is required")
	}
	if len(cfg.ShardURIs) == 0 {
		return nil, fmt.Errorf("at least one SHARD_*_URI is required")
	}
	if len(cfg.JWTSigningKeys) == 0 {
		return nil, fmt.Errorf("JWT_SIGNING_KEYS or JWT_SIGNING_KEY is required")
	}
	if _, ok := cfg.JWTSigningKeys[cfg.JWTActiveKeyID]; !ok {
		return nil, fmt.Errorf("JWT_ACTIVE_KEY_ID '%s' not found in signing keys", cfg.JWTActiveKeyID)
	}

	return cfg, nil
}

func (c *Config) RedisAddr() string             { return c.RedisDataHost + ":" + c.RedisDataPort }
func (c *Config) DefaultTimeout() time.Duration { return 30 * time.Second }

// parseJWTKeys parses a comma-separated list of kid:key pairs, falling back to a single key.
func parseJWTKeys(keysEnv, singleKeyEnv string) map[string]*rsa.PrivateKey {
	keys := make(map[string]*rsa.PrivateKey)
	if keysEnv != "" {
		for _, pair := range strings.Split(keysEnv, ",") {
			parts := strings.SplitN(pair, ":", 2)
			if len(parts) == 2 {
				kid := strings.TrimSpace(parts[0])
				keyData := strings.TrimSpace(parts[1])
				keyData = strings.ReplaceAll(keyData, "\\n", "\n")
				if key, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(keyData)); err == nil {
					keys[kid] = key
				}
			}
		}
	}
	if len(keys) == 0 && singleKeyEnv != "" {
		singleKeyEnv = strings.ReplaceAll(singleKeyEnv, "\\n", "\n")
		if key, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(singleKeyEnv)); err == nil {
			keys["default"] = key
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
