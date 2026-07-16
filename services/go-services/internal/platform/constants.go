package platform

import (
	"github.com/sony/gobreaker"
)

type AggregateType string
type EventType string

const (
	// Paths
	APIVersionV1     = "/v1"
	APIVersionV2     = "/v2"
	APIPathPrefix    = APIVersionV1
	APIJobPathPrefix = APIPathPrefix + "/jobs/"
	APITransfersPath = APIPathPrefix + "/transfers"
	APIBalancesPath  = APIPathPrefix + "/balances"
	APIAuthTokenPath = APIPathPrefix + "/auth/token"
	APIHealthPath    = "/health"
	APIReadyPath     = "/ready"

	// Service Names
	ServiceNameAPIGateway     = "core-api"
	ServiceNameLedgerWorker   = "ledger-worker"
	ServiceNameOutboxRelay    = "outbox-relay"
	ServiceNameKongSyncWorker = "kong-sync-worker"

	// Circuit Breaker Names (legacy Kafka publisher names kept for back-compat)
	CBNameAPIGatewayKafkaPublisher = "KafkaPublisher"
	CBNameOutboxKafkaPublisher     = "OutboxKafkaPublisher"
	CBNameOutboxEventStore         = "OutboxEventStore"

	// Circuit Breaker defaults (shared, see platform.NewDBCircuitBreakers)
	// (Values removed in favor of service-specific profiles passed dynamically)

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
	ComponentKongGateway        = "kong_gateway"

	ComponentFormatShard  = "%s_%s"
	ComponentFormatGlobal = "%s_global"

	// Circuit Breaker State Values
	CBStateClosed   = gobreaker.StateClosed
	CBStateHalfOpen = gobreaker.StateHalfOpen
	CBStateOpen     = gobreaker.StateOpen

	// Resiliency Defaults

	// Kafka Limits
	KafkaMaxMessageBytes = 1000000 // 1MB

	// Default Timeouts

	// Merchant Statuses
	MerchantStatusActive = "active"
	MerchantStatusFrozen = "frozen"
	MerchantStatusClosed = "closed"

	// Shard Status
	ShardStatusActive    = "active"
	ShardStatusMigrating = "migrating"

	// DLQ Statuses
	DLQStatusOpen     = "open"
	DLQStatusReplayed = "replayed"
	DLQStatusResolved = "resolved"
	TraceparentHeader = "traceparent"
)
