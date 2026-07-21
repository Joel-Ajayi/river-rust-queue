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
	BaseRetryDelaySeconds     = 1
	CapRetryDelaySeconds      = 300
	SchedulerPollInterval     = 5 * time.Second
	SchedulerBatchSize        = 100

	// Server
	ServerShutdownTimeout     = 10 * time.Second

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
	BreakerMaxRequests         = 1
	BreakerResetWindow         = 60 * time.Second
	BreakerCooldown            = 30 * time.Second
	BreakerConsecutiveFailures = 5
)

// HTTP Constants
const (
	HTTPClientTimeout         = 10 * time.Second
	HTTPMethodPost            = "POST"
	HTTPHeaderContentType     = "Content-Type"
	HTTPContentTypeJSON       = "application/json"
	HTTPHeaderSignature       = "X-RRQ-Signature"
	HTTPHeaderEventID         = "X-RRQ-Event-Id"
	HTTPHeaderDeliveryAttempt = "X-RRQ-Delivery-Attempt"
	HTTPHeaderUserAgent       = "User-Agent"
	HTTPUserAgentValue        = "rrq-webhook/1.0"
	HTTPSignaturePrefix       = "sha256="
	
	ErrorMerchantLookupFailed = "merchant_lookup_failed"
)
