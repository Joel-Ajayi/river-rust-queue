package platform

import "fmt"

// AppError is a structured error that carries a code, message, and HTTP status.
type AppError struct {
	Code    string
	Message string
	Status  int
	Field   string
}

func (e *AppError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

func ErrUnauthorized(msg string) *AppError {
	return &AppError{Code: "UNAUTHORIZED", Message: msg, Status: 401}
}

func ErrInvalidAPIKey(msg string) *AppError {
	return &AppError{Code: "INVALID_API_KEY", Message: msg, Status: 401}
}

func ErrMerchantFrozen() *AppError {
	return &AppError{Code: "MERCHANT_FROZEN", Message: "merchant account is not active", Status: 403}
}

func ErrMissingIdempotencyKey() *AppError {
	return &AppError{Code: "MISSING_IDEMPOTENCY_KEY", Message: "Idempotency-Key header required", Status: 400}
}

func ErrInvalidBody(msg string) *AppError {
	return &AppError{Code: "INVALID_BODY", Message: msg, Status: 400}
}

func ErrValidation(field, msg string) *AppError {
	return &AppError{Code: "VALIDATION_FAILED", Message: msg, Status: 422, Field: field}
}

func ErrIdempotencyMismatch() *AppError {
	return &AppError{Code: "IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_BODY", Message: "same key, different body", Status: 422}
}

func ErrLedgerUnavailable(msg string) *AppError {
	return &AppError{Code: "LEDGER_UNAVAILABLE", Message: msg, Status: 503}
}

func ErrInternal(msg string) *AppError {
	return &AppError{Code: "INTERNAL", Message: msg, Status: 500}
}
