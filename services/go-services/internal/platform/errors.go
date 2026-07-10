package platform

import (
	"fmt"
	"net/http"
)

// AppError is a structured error that carries a code, message, and HTTP status.
type AppError struct {
	Code    string
	Message string
	Status  int
	Field   string
}

func (e *AppError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

func ErrUnauthorized(msg string) *AppError {
	return &AppError{Code: "UNAUTHORIZED", Message: msg, Status: http.StatusUnauthorized}
}

func ErrInvalidAPIKey(msg string) *AppError {
	return &AppError{Code: "INVALID_API_KEY", Message: msg, Status: http.StatusUnauthorized}
}

func ErrMerchantFrozen() *AppError {
	return &AppError{Code: "MERCHANT_FROZEN", Message: "merchant account is not active", Status: http.StatusForbidden}
}

func ErrForeignWallet() *AppError {
	return &AppError{Code: "FOREIGN_WALLET", Message: "wallet does not belong to merchant", Status: http.StatusForbidden}
}

func ErrNotFound(resource string) *AppError {
	return &AppError{Code: "NOT_FOUND", Message: resource + " not found", Status: http.StatusNotFound}
}

func ErrMissingIdempotencyKey() *AppError {
	return &AppError{Code: "MISSING_IDEMPOTENCY_KEY", Message: "Idempotency-Key header required", Status: http.StatusBadRequest}
}

func ErrInvalidBody(msg string) *AppError {
	return &AppError{Code: "INVALID_BODY", Message: msg, Status: http.StatusBadRequest}
}

func ErrValidation(field, msg string) *AppError {
	return &AppError{Code: "VALIDATION_FAILED", Message: msg, Status: http.StatusUnprocessableEntity, Field: field}
}

func ErrIdempotencyMismatch() *AppError {
	return &AppError{Code: "IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_BODY", Message: "same key, different body", Status: http.StatusUnprocessableEntity}
}

func ErrLedgerUnavailable(msg string) *AppError {
	return &AppError{Code: "LEDGER_UNAVAILABLE", Message: msg, Status: http.StatusServiceUnavailable}
}

func ErrInternal(msg string) *AppError {
	return &AppError{Code: "INTERNAL", Message: msg, Status: http.StatusInternalServerError}
}

func ErrRateLimitExceeded(msg string) *AppError {
	return &AppError{Code: "RATE_LIMIT_EXCEEDED", Message: msg, Status: http.StatusTooManyRequests}
}

func ErrExpiredToken(msg string) *AppError {
	return &AppError{Code: "TOKEN_EXPIRED", Message: msg, Status: http.StatusBadRequest}
}

func ErrServiceUnavailable(msg string) *AppError {
	return &AppError{Code: "SERVICE_UNAVAILABLE", Message: msg, Status: http.StatusServiceUnavailable}
}

func ErrPayloadTooLarge(msg string) *AppError {
	return &AppError{Code: "PAYLOAD_TOO_LARGE", Message: msg, Status: http.StatusRequestEntityTooLarge}
}
