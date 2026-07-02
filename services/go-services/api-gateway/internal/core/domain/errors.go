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
	ErrJobNotFound         = errors.New("job not found")

	ErrInternal           string = "an unexpected error occurred"
	ErrInvalidBody        string = "invalid json payload"
	ErrMissingAuthContext string = "missing authentication context"
	ErrMissingBearerToken        = "missing or invalid authorization header"
)
