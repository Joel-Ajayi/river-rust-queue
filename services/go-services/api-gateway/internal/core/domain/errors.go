package domain

import (
	"errors"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key reused with different body")
	ErrMerchantInactive    = errors.New("merchant is not active")
	ErrInvalidCredentials  = errors.New("invalid credentials")
	ErrInvalidAPIKey       = errors.New("invalid Merchant Api key format")
	ErrWalletNotOwned      = errors.New("wallet does not belong to merchant")
	ErrWalletNotFound      = errors.New("destination wallet not found")
	ErrJobNotFound         = errors.New("job not found")
	ErrMsgTokenExpired     = errors.New("token is expired")

	// Validation Errors
	ErrInvalidFromWallet = errors.New("from_wallet is required")
	ErrInvalidToWallet   = errors.New("to_wallet is required")
	ErrInvalidAmount     = errors.New("amount must be greater than 0")
	ErrInvalidCurrency   = errors.New("currency is required")
	ErrSameWallet        = errors.New("to_wallet must differ from from_wallet")

	ErrInternal           = errors.New("an unexpected error occurred")
	ErrServiceUnavailable = errors.New("service unavailable")
	ErrInvalidBody        = errors.New("invalid json payload")
	ErrMissingAuthContext = errors.New("missing authentication context")
	ErrMissingBearerToken = errors.New("missing or invalid authorization header")

	ErrMsgRateLimitExceeded = "Too Many Requests - Rate Limit Exceeded"
	ErrMsgBulkheadExhausted = "Service Unavailable - Connection Pool Exhausted"
	ErrMsgPayloadTooLarge   = "Payload Too Large"

	ErrMsgQueryParamRequired = "query parameter is required"
	ErrMsgJobIDRequired      = "job id is required"
	ErrMsgRequestBodyLarge   = "http: request body too large"
)

// These errors should not trip circuit breakers or trigger infrastructure alerts.
func IsTerminalError(err error) bool {
	if err == nil {
		return true
	}
	var appErr *platform.AppError
	if errors.As(err, &appErr) {
		return appErr.Status >= 400 && appErr.Status < 500
	}
	return errors.Is(err, ErrIdempotencyConflict) ||
		errors.Is(err, ErrMerchantInactive) ||
		errors.Is(err, ErrInvalidCredentials) ||
		errors.Is(err, ErrInvalidAPIKey) ||
		errors.Is(err, ErrWalletNotOwned) ||
		errors.Is(err, ErrWalletNotFound) ||
		errors.Is(err, ErrJobNotFound) ||
		errors.Is(err, ErrMsgTokenExpired) ||
		errors.Is(err, ErrInvalidFromWallet) ||
		errors.Is(err, ErrInvalidToWallet) ||
		errors.Is(err, ErrInvalidAmount) ||
		errors.Is(err, ErrInvalidCurrency) ||
		errors.Is(err, ErrSameWallet) ||
		errors.Is(err, ErrMissingAuthContext) ||
		errors.Is(err, ErrMissingBearerToken)
}
