package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"
)

// ErrorClassification represents the retry/handling strategy for an error.
// Exactly four categories: poison, transient, terminal, infrastructure.
type ErrorClassification string

const (
	ClassificationPoison         ErrorClassification = "poison"         // corrupted payload, unmarshal failure
	ClassificationTransient      ErrorClassification = "transient"      // retry with backoff
	ClassificationTerminal       ErrorClassification = "terminal"       // business rule violation
	ClassificationInfrastructure ErrorClassification = "infrastructure" // infra down (PG down, Kafka down)
)

// ClassifyError determines the retry/handling strategy for an error.
// This is the single source of truth for error classification across all services.
func ClassifyError(err error, isTerminalError func(err error) bool) ErrorClassification {
	if err == nil {
		return ""
	}

	// 1. Poison: unmarshal failures, JSON syntax errors, oversized messages
	// These are checked first in the consumer before calling ClassifyError,
	// but defensive check here:
	if isPoisonError(err) {
		return ClassificationPoison
	}

	// 2. Terminal: business rule violations
	// We check platform-level terminal errors here
	if isTerminalError != nil && isTerminalError(err) {
		return ClassificationTerminal
	}

	// 3. Transient: known retryable infra errors
	if isTransientError(err) {
		return ClassificationTransient
	}

	// 4. Infrastructure: PG down, Kafka down, Redis down
	if isInfrastructureError(err) {
		return ClassificationInfrastructure
	}

	// 5. HTTP status classification (webhook worker, etc.)
	var httpErr *HttpError
	if errors.As(err, &httpErr) {
		switch {
		case httpErr.Status == http.StatusTooManyRequests || httpErr.Status >= http.StatusInternalServerError:
			return ClassificationTransient
		case httpErr.Status >= http.StatusBadRequest && httpErr.Status < http.StatusInternalServerError:
			return ClassificationTerminal
		}
	}

	// 6. Kafka permanent errors
	var kErr kafka.Error
	if errors.As(err, &kErr) {
		if !kErr.Temporary() && !kErr.Timeout() {
			switch kErr {
			case kafka.UnknownTopicOrPartition, kafka.InvalidTopic:
				return ClassificationTerminal
			}
		}
	}

	// 7. Default: transient — limited retries then DLQ
	return ClassificationTransient
}

func isPoisonError(err error) bool {
	// 1. Low-Level Stream Corruption
	// Catches cut-off payloads or completely broken network packets
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return true
	}

	// 2. JSON Structural Malformation Errors
	var jsonSyntaxErr *json.SyntaxError
	if errors.As(err, &jsonSyntaxErr) {
		return true
	}

	// 3. JSON Schema / Type Mismatch Errors (e.g., String sent instead of Integer)
	var jsonTypeErr *json.UnmarshalTypeError
	return errors.As(err, &jsonTypeErr)
}

// isInfrastructureError determines if an error represents a fatal
// infrastructure failure that should trigger a deep sleep or circuit breaker rather than a fast retry.
func isInfrastructureError(err error) bool {
	if err == nil {
		return false
	}

	// 1. PostgreSQL Connection Errors (08xxx)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// pgerrcode.IsConnectionException handles all 08xxx classes (08000, 08003, 08006, etc)
		if pgerrcode.IsConnectionException(pgErr.Code) {
			return true
		}
	}

	// 2. Network connection refused / reset
	var sysErr *net.OpError
	if errors.As(err, &sysErr) {
		if errors.Is(sysErr.Err, syscall.ECONNREFUSED) || errors.Is(sysErr.Err, syscall.ECONNRESET) {
			return true
		}
	}

	// 3. Kafka Connection Refused
	var kErr kafka.Error
	if errors.As(err, &kErr) {
		if kErr == kafka.BrokerNotAvailable {
			return true
		}
	}

	// 4. Unexpected EOF (Connection Dropped)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	return false
}

// isTransientError determines if an error is transient and should be retried
func isTransientError(err error) bool {
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

// ErrUnmarshalFailed is a sentinel for protobuf/JSON unmarshal failures
var ErrUnmarshalFailed = errors.New("unmarshal failed")

// HttpError wraps an HTTP error with status code
type HttpError struct {
	Status int
	Err    error
}

func (e *HttpError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("http %d: %v", e.Status, e.Err)
	}
	return fmt.Sprintf("http %d", e.Status)
}

func (e *HttpError) Unwrap() error {
	return e.Err
}

func NewHttpError(status int, err error) *HttpError {
	return &HttpError{Status: status, Err: err}
}

// AppError is a structured error that carries a code, message, and HTTP status.
type AppError struct {
	Code    string
	Message string
	Status  int
	Field   string
	Err     error // wrapped error for errors.Is/As traversal
}

func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *AppError) Unwrap() error {
	return e.Err
}

const (
	ErrCodeUnauthorized          = "UNAUTHORIZED"
	ErrCodeInvalidAPIKey         = "INVALID_API_KEY"
	ErrCodeMerchantFrozen        = "MERCHANT_FROZEN"
	ErrCodeForeignWallet         = "FOREIGN_WALLET"
	ErrCodeNotFound              = "NOT_FOUND"
	ErrCodeMissingIdempotencyKey = "MISSING_IDEMPOTENCY_KEY"
	ErrCodeInvalidBody           = "INVALID_BODY"
	ErrCodeValidationFailed      = "VALIDATION_FAILED"
	ErrCodeIdempotencyMismatch   = "IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_BODY"
	ErrCodeLedgerUnavailable     = "LEDGER_UNAVAILABLE"
	ErrCodeInternal              = "INTERNAL"
	ErrCodeRateLimitExceeded     = "RATE_LIMIT_EXCEEDED"
	ErrCodeTokenExpired          = "TOKEN_EXPIRED"
	ErrCodeServiceUnavailable    = "SERVICE_UNAVAILABLE"
	ErrCodePayloadTooLarge       = "PAYLOAD_TOO_LARGE"
)

func ErrUnauthorized(err error) *AppError {
	return &AppError{Code: ErrCodeUnauthorized, Message: err.Error(), Status: http.StatusUnauthorized, Err: err}
}

func ErrInvalidAPIKey(err error) *AppError {
	return &AppError{Code: ErrCodeInvalidAPIKey, Message: err.Error(), Status: http.StatusUnauthorized, Err: err}
}

func ErrMerchantFrozen(err error) *AppError {
	return &AppError{Code: ErrCodeMerchantFrozen, Message: err.Error(), Status: http.StatusForbidden, Err: err}
}

func ErrForeignWallet(err error) *AppError {
	return &AppError{Code: ErrCodeForeignWallet, Message: err.Error(), Status: http.StatusForbidden, Err: err}
}

func ErrNotFound(err error) *AppError {
	return &AppError{Code: ErrCodeNotFound, Message: err.Error(), Status: http.StatusNotFound, Err: err}
}

func ErrMissingIdempotencyKey(err error) *AppError {
	return &AppError{Code: ErrCodeMissingIdempotencyKey, Message: err.Error(), Status: http.StatusBadRequest, Err: err}
}

func ErrInvalidBody(err error) *AppError {
	return &AppError{Code: ErrCodeInvalidBody, Message: err.Error(), Status: http.StatusBadRequest, Err: err}
}

func ErrValidation(field string, err error) *AppError {
	return &AppError{Code: ErrCodeValidationFailed, Message: err.Error(), Status: http.StatusUnprocessableEntity, Field: field, Err: err}
}

func ErrIdempotencyMismatch(err error) *AppError {
	return &AppError{Code: ErrCodeIdempotencyMismatch, Message: err.Error(), Status: http.StatusUnprocessableEntity, Err: err}
}

func ErrLedgerUnavailable(err error) *AppError {
	return &AppError{Code: ErrCodeLedgerUnavailable, Message: err.Error(), Status: http.StatusServiceUnavailable, Err: err}
}

func ErrInternal(err error) *AppError {
	return &AppError{Code: ErrCodeInternal, Message: err.Error(), Status: http.StatusInternalServerError, Err: err}
}

func ErrRateLimitExceeded(err error) *AppError {
	return &AppError{Code: ErrCodeRateLimitExceeded, Message: err.Error(), Status: http.StatusTooManyRequests, Err: err}
}

func ErrExpiredToken(err error) *AppError {
	return &AppError{Code: ErrCodeTokenExpired, Message: err.Error(), Status: http.StatusBadRequest, Err: err}
}

func ErrServiceUnavailable(err error) *AppError {
	return &AppError{Code: ErrCodeServiceUnavailable, Message: err.Error(), Status: http.StatusServiceUnavailable, Err: err}
}

func ErrPayloadTooLarge(err error) *AppError {
	return &AppError{Code: ErrCodePayloadTooLarge, Message: err.Error(), Status: http.StatusRequestEntityTooLarge, Err: err}
}
