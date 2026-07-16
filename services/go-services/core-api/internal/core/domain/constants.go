package domain

import "time"

const (
	// Operational Limits
	MaxRequestBytes    = 512 * 1024      // 512KB
	BulkheadLimit      = 100             // semaphore count
	JWTLeeway          = 5 * time.Minute // 5min
	JWTExpiration      = 1 * time.Hour   // 1hr

	// Timeouts
	ServerShutdownTimeout = 15 * time.Second

	// Circuit Breaker (High Volume / Synchronous)
	CBMaxRequests = 1                // Max requests in half-open state
	CBTimeout     = 5 * time.Second  // Short timeout for fast recovery probing
	CBInterval    = 10 * time.Second // 10s sliding window for error rate calculation
	CBMinRequests = 100              // At least 100 requests in window before tripping
	CBErrorRate   = 0.50             // 50% error rate required to trip

	// JWT Claims
	ClaimSub  = "sub"
	ClaimIss  = "iss"
	ClaimIat  = "iat"
	ClaimExp  = "exp"
	ClaimTier = "tier"
)
