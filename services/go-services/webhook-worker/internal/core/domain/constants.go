package domain

// Delivery statuses
const (
	StatusActive = "active"
	StatusFrozen = "frozen"
	StatusOpen   = "open"
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
	PrefixWebhook = "webhook-"
)

// JSON Keys
const (
	JSONKeyMerchantID = "merchant_id"
)

// Log Messages
const (
	LogEventFailedUnmarshal  = "failed to unmarshal EventEnvelope"
	LogEventMerchantLookup   = "merchant config not found or error"
	LogEventMerchantInactive = "merchant is not active, delivery routed to DLQ"
	LogEventFetchRetries     = "failed to fetch pending retries"
	LogEventMerchantRetry    = "merchant config not found during retry"
)

// HTTP Constants
const (
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
