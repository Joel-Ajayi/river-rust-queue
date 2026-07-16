package domain

import "time"

const (
	// Worker Timing
	WorkerSyncInterval    = 30 * time.Second
	WorkerShutdownTimeout = 5 * time.Second

	// Circuit Breaker (Low Volume / Background Sync)
	CBMaxRequests = 3
	CBTimeout     = 5 * time.Second
	CBMaxFails    = 3

	// Naming Patterns
	FormatRateLimitName = "rate-limit-%s"
	FormatJWTName       = "jwt-%s"
	CharUnderscore      = "_"
	CharDash            = "-"

	// Merchant Tiers & Limits
	TierPremium         = "premium"
	RateLimitStandard   = 60
	RateLimitPremium    = 600

	// Unstructured Object Fields
	FieldAPIVersion     = "apiVersion"
	FieldKind           = "kind"
	FieldMetadata       = "metadata"
	FieldName           = "name"
	FieldNamespace      = "namespace"
	FieldType           = "type"
	FieldUsername       = "username"
	FieldCustomID       = "custom_id"
	FieldPlugin         = "plugin"
	FieldConsumerRef    = "consumerRef"
	FieldConfig         = "config"
	FieldMinute         = "minute"
	FieldPolicy         = "policy"
	FieldAlgorithm      = "algorithm"
	FieldKey            = "key"
	FieldRSAPublicKey   = "rsa_public_key"

	// Metrics Constants
	MetricMerchantDirectoryLatency = "rrq_merchant_directory_latency_seconds"
	MetricKongGatewayLatency       = "rrq_kong_gateway_latency_seconds"
	MetricLabelOperation           = "operation"
	MetricLabelStatus              = "status"
	MetricStatusSuccess            = "success"
	MetricStatusError              = "error"

	// Operation Names
	OpGetActiveMerchants    = "GetActiveMerchants"
	OpSyncConsumer          = "SyncConsumer"
	OpSyncRateLimitPlugin   = "SyncRateLimitPlugin"
	OpSyncJWTCredential     = "SyncJWTCredential"
)
