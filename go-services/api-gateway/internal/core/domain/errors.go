package domain

import (
	"errors"
	"fmt"
)

// ValidationError is a field-level rule violation. The transport adapter decides
// how to render it (HTTP 422, a gRPC status, etc.); the core stays ignorant of that.
type ValidationError struct {
	Field string
	Msg   string
}

func (e ValidationError) Error() string { return fmt.Sprintf("%s %s", e.Field, e.Msg) }

// Sentinel domain errors. Adapters translate these to transport-specific codes.
var (
	// ErrMerchantInactive means the merchant is missing, frozen, or closed.
	ErrMerchantInactive = errors.New("merchant is not active")
	// ErrIdempotencyConflict means the key was reused with a different body.
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different body")
	// ErrInvalidCredentials means the presented API key matched no active merchant.
	ErrInvalidCredentials = errors.New("invalid credentials")
)
