package domain

import (
	"errors"
	"fmt"
)

type ValidationError struct {
	Field string
	Msg   string
}

func (e ValidationError) Error() string { return fmt.Sprintf("%s: %s", e.Field, e.Msg) }

var (
	ErrIdempotencyConflict = errors.New("idempotency key reused with different body")
	ErrMerchantInactive    = errors.New("merchant is not active")
	ErrInvalidCredentials  = errors.New("invalid credentials")
	ErrInvalidAPIKey       = errors.New("invalid Merchant Api key format")
	ErrWalletNotOwned      = errors.New("wallet does not belong to merchant")
	ErrWalletNotFound      = errors.New("destination wallet not found")
	ErrJobNotFound         = errors.New("job not found")

	ErrInternal           = errors.New("an unexpected error occurred")
	ErrInvalidBody        = errors.New("invalid json payload")
	ErrMissingAuthContext = errors.New("missing authentication context")
	ErrMissingBearerToken = errors.New("missing or invalid authorization header")
)
