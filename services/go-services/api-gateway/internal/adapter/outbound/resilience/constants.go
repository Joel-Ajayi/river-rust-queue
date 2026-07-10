package resilience

import "time"

const (
	// Circuit breaker limits
	CircuitBreakerWriteName  = "DatabaseWrite"
	CircuitBreakerReadName   = "DatabaseRead"
	CircuitBreakerMaxRequest = 1               // requests allowed to pass through the breaker when it is in the Half-Open state.
	CircuitBreakerTimeout    = 5 * time.Second // 5sec Wait before Half-Open
	CircuitBreakerMaxFails   = 3               // 3 failures

	// Circuit breaker suffixes
	CBSuffixMerchantDir = "_MerchantDir"
	CBSuffixWalletDir   = "_WalletDir_"
	CBSuffixJobStore    = "_JobStore_"

	// Retry limits
	RetryMaxAttempts = 3
	RetryBaseDelay   = 10 * time.Millisecond  //10msec
	RetryMaxDelay    = 200 * time.Millisecond //200msec
)
