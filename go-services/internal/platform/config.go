package platform

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
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

	JWTSigningKey []byte

	HTTPPort    int
	MetricsPort int

	LogLevel        string
	TraceSampleRate float64
}

// LoadConfig reads configuration from env vars (injected by K8s ConfigMap + Secret).
func LoadConfig() (*Config, error) {
	cfg := &Config{
		MerchantsDBURI: os.Getenv("MERCHANTS_DB_URI"),
		ShardURIs:      make(map[string]string),

		KafkaBrokers:     strings.Split(envOrDefault("kafka_brokers", "localhost:9092"), ","),
		KafkaTopicJobs:   envOrDefault("kafka_topic_jobs", "jobs"),
		KafkaTopicNotify: envOrDefault("kafka_topic_notify", "notify"),

		RedisDataHost: envOrDefault("redis_data_host", "localhost"),
		RedisDataPort: envOrDefault("redis_data_port", "6379"),

		JWTSigningKey: []byte(os.Getenv("jwt_signing_key")),

		HTTPPort:    envOrDefaultInt("HTTP_PORT", 8080),
		MetricsPort: envOrDefaultInt("METRICS_PORT", 9090),

		LogLevel:        envOrDefault("log_level", "info"),
		TraceSampleRate: envOrDefaultFloat("trace_sample_rate", 1.0),
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
	if len(cfg.JWTSigningKey) == 0 {
		return nil, fmt.Errorf("jwt_signing_key is required")
	}

	return cfg, nil
}

func (c *Config) RedisAddr() string             { return c.RedisDataHost + ":" + c.RedisDataPort }
func (c *Config) DefaultTimeout() time.Duration { return 30 * time.Second }

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
