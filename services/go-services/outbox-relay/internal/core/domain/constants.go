package domain

import (
	"time"
)

const (
	// Configuration Constants
	RelayPoolInterval    = 500 * time.Millisecond // 500ms pool interval across all shards
	RelayBackoffMinDelay = 500 * time.Millisecond
	RelayBackoffMaxDelay = 60 * time.Second
	RelayPublishTimeout  = 10 * time.Second
	RelayBatchSize       = 1000 // 1000 events per batch across all shards
	RelayProcessTimeout  = 15 * time.Second
	ServerShutdownTimeout= 30 * time.Second
	OutboxPurgeInterval  = 1 * time.Hour
	OutboxPurgeAge       = 168 * time.Hour // 7 days (7 * 24)

	// Circuit Breaker (Asynchronous / Background Worker)
	CBMaxRequests = 3                 // Max requests in half-open state
	CBTimeout     = 30 * time.Second  // Longer timeout for async workers
	CBMaxFails    = 5                 // 5 consecutive failures trips the breaker

	// Error / DLQ Reasons
	ReasonCorruptedPayload = "Corrupted JSON payload"
	ReasonMessageTooLarge  = "Message exceeds size limit"
	ReasonInvalidSchema    = "Invalid EventEnvelope schema"
	ReasonPanic            = "Panic recovered during processing"
)
