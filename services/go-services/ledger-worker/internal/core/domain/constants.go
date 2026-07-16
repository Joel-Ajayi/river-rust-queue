package domain

import "time"

const (
	ConsumerDLQAttemptCount   = 10
	ConsumerBackoffMinDelay   = 1 * time.Second
	ConsumerBackoffMaxDelay   = 60 * time.Second
	ConsumerProcessTimeout    = 30 * time.Second
	ConsumerDLQMaxRetries     = 3
	ConsumerDLQRetryBaseDelay = 100 * time.Millisecond
	ConsumerDLQMaxBackoff     = 5 * time.Second
	ServerShutdownTimeout     = 30 * time.Second

	// Circuit Breaker (Asynchronous / Background Worker)
	CBMaxRequests = 2                 // Max requests in half-open state
	CBTimeout     = 30 * time.Second  // Longer timeout for async workers
	CBMaxFails    = 5                 // 5 consecutive failures trips the breaker
)
