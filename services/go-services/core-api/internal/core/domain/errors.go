package domain

import "errors"

var (
	ErrIdempotencyConflict   = errors.New("idempotency key reused with different body")
	ErrMerchantInactive      = errors.New("merchant is not active")
	ErrInvalidCredentials    = errors.New("invalid credentials")
	ErrInvalidAPIKey         = errors.New("invalid Merchant Api key format")
	ErrWalletNotOwned        = errors.New("wallet does not belong to merchant")
	ErrWalletNotFound        = errors.New("destination wallet not found")
	ErrJobNotFound           = errors.New("job not found")
	ErrMsgTokenExpired       = errors.New("token is expired")
	ErrMissingIdempotencyKey = errors.New("missing idempotency key")
	ErrMissingDLQID          = errors.New("dlq_id is required")

	// Validation Errors
	ErrInvalidFromWallet = errors.New("from_wallet is required")
	ErrInvalidToWallet   = errors.New("to_wallet is required")
	ErrInvalidAmount     = errors.New("amount must be greater than 0")
	ErrInvalidCurrency   = errors.New("currency is required")
	ErrSameWallet        = errors.New("to_wallet must differ from from_wallet")

	ErrInternal                = errors.New("an unexpected error occurred")
	ErrServiceUnavailable      = errors.New("service unavailable")
	ErrInvalidBody             = errors.New("invalid json payload")
	ErrMissingAuthContext      = errors.New("missing authentication context")
	ErrMissingConsumerIdentity = errors.New("missing or invalid consumer identity header")
	ErrAdminForbidden          = errors.New("endpoint is restricted to the platform administrator")

	ErrMsgBulkheadExhausted = errors.New("service unavailable - connection pool exhausted")
	ErrMsgPayloadTooLarge   = errors.New("payload too large")

	ErrMsgQueryParamRequired = errors.New("query parameter is required")
	ErrMsgInValidJobID       = errors.New("job id is invalid")
	ErrMsgRequestBodyLarge   = errors.New("http: request body too large")
	ErrInvalidTier           = errors.New("tier must be one of: standard, premium")
	ErrPremiumRequiresAdmin  = errors.New("premium tier is reserved for the platform administrator")
)

// IsTerminalError returns true if the error is a business logic error
// that should fail the operation immediately without retries (and not trip circuit breakers).
func IsTerminalError(err error) bool {
	return errors.Is(err, ErrIdempotencyConflict) ||
		errors.Is(err, ErrMerchantInactive) ||
		errors.Is(err, ErrInvalidCredentials) ||
		errors.Is(err, ErrInvalidAPIKey) ||
		errors.Is(err, ErrWalletNotOwned) ||
		errors.Is(err, ErrWalletNotFound) ||
		errors.Is(err, ErrJobNotFound) ||
		errors.Is(err, ErrInvalidFromWallet) ||
		errors.Is(err, ErrInvalidToWallet) ||
		errors.Is(err, ErrInvalidAmount) ||
		errors.Is(err, ErrInvalidCurrency) ||
		errors.Is(err, ErrSameWallet) ||
		errors.Is(err, ErrMissingIdempotencyKey) ||
		errors.Is(err, ErrInvalidBody) ||
		errors.Is(err, ErrMissingAuthContext) ||
		errors.Is(err, ErrMissingConsumerIdentity) ||
		errors.Is(err, ErrAdminForbidden)
}
