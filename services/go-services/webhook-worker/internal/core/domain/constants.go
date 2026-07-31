package domain

import "time"

// Delivery statuses
const (
	StatusActive  = "active"
	StatusFrozen  = "frozen"
	StatusClosed  = "closed"
	StatusOpen    = "open"
)

// Event Types and Sources
const (
	EventTypeWebhookFailed    = "webhook.failed"
	EventTypeWebhookDelivered = "webhook.delivered"
	EventSourceWebhook        = "webhook"
	AggregateTypeWebhook      = "webhook"
)

// Prefixes
const (
	PrefixDelivery = "wd_"
	PrefixEvent    = "ev_"
	PrefixWebhook  = "webhook-"
)

// Configuration Constants for Webhook Delivery
const (
	MaxDeliveryAttempts       = 10
	BaseRetryDelaySeconds     = 5
	CapRetryDelaySeconds      = 14400 // 4 hours
	SchedulerPollInterval             = 5 * time.Second
	SchedulerBatchSize                = 100
	SchedulerVisibilityTimeoutMinutes = 5
	FastLaneGracePeriod               = 5 * time.Second
	FastLaneBufferSize                = 1000
	FastLaneWorkerPoolSize            = 100

	// Server
	ServerShutdownTimeout     = 10 * time.Second

	// Kafka Resilience
	KafkaFetchMaxRetries      = -1
	KafkaCommitMaxRetries     = 3

	// Worker Resilience
	WorkerRetryBaseDelay = 5 * time.Second
	WorkerRetryMaxDelay  = 30 * time.Second

	// Delivery Attempts
	InitialDeliveryAttempt = 1
	ZeroDeliveryAttempt    = 0

	// JSON Keys
	JSONKeyDeliveryID    = "delivery_id"
	JSONKeyMerchantID    = "merchant_id"
	JSONKeySourceEventID = "source_event_id"
	JSONKeyAttemptCount  = "attempt_count"
	JSONKeyLastError     = "last_error"
	JSONKeyStatusCode    = "status_code"
	JSONKeyDeliveredAt   = "delivered_at"

	// Log Messages
	LogEventFailedUnmarshal  = "failed to unmarshal EventEnvelope"
	LogEventMerchantLookup   = "merchant config not found or error"
	LogEventMerchantInactive = "merchant is not active, skipping delivery"
	LogEventFetchRetries     = "failed to fetch pending retries"
	LogEventMerchantRetry    = "merchant config not found during retry"
)

// Circuit Breaker Constants
const (
	BreakerMaxRequests         = 5 // 5 consecutive probe trials in half-open state
	BreakerResetWindow         = 60 * time.Second
	BreakerCooldown            = 30 * time.Second
	BreakerConsecutiveFailures = 5
	BreakerEvictionInterval    = 15 * time.Minute
	BreakerEvictionTTL         = 1 * time.Hour
)

// HTTP Constants
const (
	HTTPClientTimeout         = 5 * time.Second
	HTTPMaxIdleConns          = 100
	HTTPMaxIdleConnsPerHost   = 100
	HTTPIdleConnTimeout       = 30 * time.Second
	HTTPMethodPost            = "POST"
	HTTPHeaderContentType     = "Content-Type"
	HTTPContentTypeJSON       = "application/json"
	HTTPHeaderSignature       = "X-RRQ-Signature"
	HTTPHeaderEventID         = "X-RRQ-Event-Id"
	HTTPHeaderDeliveryAttempt = "X-RRQ-Delivery-Attempt"
	HTTPHeaderUserAgent       = "User-Agent"
	HTTPUserAgentValue        = "rrq-webhook/1.0"
	HTTPHeaderTimestamp       = "Webhook-Timestamp"
	HTTPSignaturePrefix       = "sha256="
	
	ErrorMerchantLookupFailed = "merchant_lookup_failed"
)
