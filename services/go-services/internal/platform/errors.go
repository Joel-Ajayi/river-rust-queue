package platform

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"syscall"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"
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

func IsTransientError(err error) bool {
	if err == nil {
		return false
	}

	// 1. PostgreSQL Structural Error Inspection (pgx/v5)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case pgerrcode.DeadlockDetected, // 40P01: Concurrent transaction deadlock lock victim
			pgerrcode.SerializationFailure,  // 40001: SSI isolation failure (safe to replay transaction)
			pgerrcode.InsufficientResources, // 53000: DB out of memory/disk space (temporary spike)
			pgerrcode.TooManyConnections:    // 53300: Connection pool limit hit on Postgres
			return true
		// We explicitly DO NOT include 08000, 08003, 08006 (Connection drops)
		default:
			return false
		}
	}

	// 2. Kafka Network & State Failures (segmentio/kafka-go)
	var kErr kafka.Error
	if errors.As(err, &kErr) {
		if kErr.Temporary() || kErr.Timeout() {
			return true
		}
		switch kErr {
		case kafka.RequestTimedOut,
			kafka.LeaderNotAvailable,
			kafka.NotLeaderForPartition,
			kafka.NetworkException:
			return true
		}
	}

	// 3. Robust Network and Socket Level Timeouts
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout()) {
		// Differentiate between a transient network timeout and a hard dial failure (Connection Refused).
		// We explicitly do NOT want to retry dial failures or reset connections.
		var sysErr *net.OpError
		if errors.As(err, &sysErr) {
			if errors.Is(sysErr.Err, syscall.ECONNREFUSED) || errors.Is(sysErr.Err, syscall.ECONNRESET) {
				return false // Fail fast!
			}
		}
		return true
	}

	// 4. Go Context Deadlines
	// A context timeout (e.g., http client request timeout) is highly transient.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	return false
}
