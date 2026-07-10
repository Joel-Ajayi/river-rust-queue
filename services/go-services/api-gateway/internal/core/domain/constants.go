package domain

import "time"

const (
	// Operational Limits
	MaxRequestBytes    = 512 * 1024      // 512KB
	BulkheadLimit      = 100             // semaphore count
	RateLimitReqPerSec = 500.0           // 500 rps
	RateLimitBurst     = 1000            // 1000 burst
	JWTLeeway          = 5 * time.Minute // 5min
	JWTExpiration      = 1 * time.Hour   // 1hr

	// JWT Claims
	ClaimSub  = "sub"
	ClaimIat  = "iat"
	ClaimExp  = "exp"
	ClaimTier = "tier"
)
