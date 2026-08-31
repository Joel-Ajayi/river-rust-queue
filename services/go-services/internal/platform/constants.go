package platform

type AggregateType string
type EventType string

const (
	// Paths
	APIVersionV1             = "/v1"
	APIVersionV2             = "/v2"
	APIPathPrefix            = APIVersionV1
	APIJobPathPrefix         = APIPathPrefix + "/jobs/"
	APITransfersPath         = APIPathPrefix + "/transfers"
	APIBalancesPath          = APIPathPrefix + "/balances"
	APIAuthTokenPath         = APIPathPrefix + "/auth/token"
	APIMerchantsPath         = APIPathPrefix + "/merchants"
	APIWalletsPath           = APIPathPrefix + "/wallets"
	APIAdminDLQReplayPath    = APIPathPrefix + "/admin/dlq/replay"
	APIAdminDLQListPath      = APIPathPrefix + "/admin/dlq"
	APIAdminDLQReplayOnePath = APIPathPrefix + "/admin/dlq/replay-one"

	DefaultDLQBatchLimit = 100
	APIHealthPath        = "/health"
	APIReadyPath         = "/ready"

	// Service Names
	ServiceNameCoreAPI       = "core-api"
	ServiceNameLedgerWorker  = "ledger-worker"
	ServiceNameOutboxRelay   = "outbox-relay"
	ServiceNameWebhookWorker = "webhook-worker"
	ServiceNameWebhookEcho   = "webhook-echo"
	ServiceNameFraudWorker   = "fraud-worker"

	// Infrastructure Components
	ComponentMerchantDirectory  = "merchant_directory"
	ComponentWalletDirectory    = "wallet_directory"
	ComponentJobStore           = "job_store"
	ComponentEventStore         = "event_store"
	ComponentKafkaPublisher     = "kafka_publisher"
	ComponentLedgerStore        = "ledger_store"
	ComponentCrossShardStore    = "cross_shard_store"
	ComponentDLQStore           = "dlq_store"
	ComponentConsumerProcessing = "consumer_processing"
	ComponentConsumer           = "consumer"
	ComponentJobHandler         = "job_handler"
	ComponentSagaHandler        = "saga_handler"
	ComponentTransferService    = "transfer_service"
	ComponentWebhookStore       = "webhook_store"
	ComponentWebhookHandler     = "webhook_handler"
	ComponentRedis              = "redis"

	ComponentFormatShard  = "%s_%s"
	ComponentFormatGlobal = "%s_global"

	// Merchant Statuses
	MerchantStatusActive = "active"
	MerchantStatusFrozen = "frozen"
	MerchantStatusClosed = "closed"

	// Merchant Tiers
	MerchantTierPlatform = "platform"
	MerchantTierPremium  = "premium"
	MerchantTierStandard = "standard"

	// Wallet Types
	WalletTypeSystem    = "system"
	WalletTypeCustomer  = "customer"
	WalletTypeFiatVault = "system_fiat_vault"

	// Wallet Statuses
	WalletStatusActive = "active"
	WalletStatusFrozen = "frozen"
	WalletStatusClosed = "closed"

	// Actors
	ActorSystem = "system"

	// Shard Status
	DefaultShardID       = ShardIDPrefix + "a"
	ShardStatusActive    = "active"
	ShardStatusMigrating = "migrating"

	// DLQ Statuses
	DLQStatusOpen     = "open"
	DLQStatusReplayed = "replayed"
	DLQStatusResolved = "resolved"

	// DLQ Sources
	DLQSourceLedger  = "ledger"
	DLQSourceWebhook = "webhook"
	DLQSourceFraud   = "fraud"
	DLQSourceOutbox  = "outbox-relay"

	// DLQ entry id prefix used by hashDLQID.
	DLQIDPrefix = "dq_"

	// DLQOriginSep is the separator joining message-identity fields that seed the
	// deterministic DLQ id (see DeterministicDLQID / DLQEntryOrigin / KafkaOrigin).
	DLQOriginSep = "|"

	// DLQErrorPrefix is the namespace prefix for all DLQ error messages.
	DLQErrorPrefix = "dlq:"

	TraceparentHeader = "traceparent"
	HeaderEventID     = "event_id"
	HeaderEventType   = "event_type"

	// Shard Types (for business metrics)
	ShardTypeSame  = "same"
	ShardTypeCross = "cross"

	// Transfer Statuses (for business metrics — TSR computation)
	TransferMetricSuccess = "success"
	TransferMetricFailed  = "failed"
	TransferMetricPending = "pending"

	// Decline Reasons (for business metrics)
	DeclineReasonInsufficientBalance = "insufficient_balance"
	DeclineReasonWalletFrozen        = "wallet_frozen"
	DeclineReasonWalletClosed        = "wallet_closed"
	DeclineReasonWalletNotFound      = "wallet_not_found"
	DeclineReasonCurrencyMismatch    = "currency_mismatch"
	DeclineReasonSelfTransfer        = "self_transfer"
	DeclineReasonMerchantInactive    = "merchant_inactive"
	DeclineReasonSagaCompensated     = "saga_compensated"
	DeclineReasonUnknown             = "unknown"

	// Ledger Constants
	LedgerTransferWalletCount = 2

	// JWT Header Keys
	JWTHeaderKeyID = "kid"

	// Health Response Keys/Values
	HealthStatusKey         = "status"
	HealthStatusOK          = "ok"
	HealthStatusReady       = "ready"
	HealthStatusUnavailable = "Service Unavailable"

	// Redis Key Formats
	RedisKeyVelocity = "velocity:wallet:%s"

	// Canonical Error Codes (used in ErrorCode field of CanonicalLogLine)
	ErrorCodeSagaFailed            = "saga_failed"
	ErrorCodeVelocityLimitExceeded = "velocity_limit_exceeded"

	// API and Deposit Onboarding Constants
	APIKeySecretLength = 32
	APIKeyPrefix       = "rrq_live_"
	APIKeyFormat       = "rrq_live_%s_%s"
	APIKeySeparator    = "_"
	DepositReference   = "Fiat Deposit"
)
