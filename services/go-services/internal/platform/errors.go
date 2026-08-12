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

	"github.com/failsafe-go/failsafe-go/circuitbreaker"
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

// infrastructure failure that should trigger a long backoff,
// deep sleep, or circuit breaker rather than an immediate retry.
func isInfrastructureError(err error) bool {
	if err == nil {
		return false
	}

	// Caller cancelled the operation.
	if errors.Is(err, context.Canceled) {
		return false
	}

	// PostgreSQL infrastructure/resource failures.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgerrcode.IsConnectionException(pgErr.Code) {
			return true
		}

		switch pgErr.Code {
		case pgerrcode.CannotConnectNow, // 57P03
			pgerrcode.InsufficientResources, // 53000
			pgerrcode.DiskFull,              // 53100
			pgerrcode.OutOfMemory,           // 53200
			pgerrcode.TooManyConnections:    // 53300
			return true
		}
	}

	// Network failures.
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if errors.Is(opErr.Err, syscall.ECONNREFUSED) {
			return true
		}
	}

	// Kafka infrastructure failures.
	var kErr kafka.Error
	if errors.As(err, &kErr) {
		if !kErr.Temporary() {
			return true
		}
	}

	// Circuit breaker already open.
	if errors.Is(err, circuitbreaker.ErrOpen) {
		return true
	}

	return false
}

// isTransientError determines whether an error is expected to resolve
// quickly and should be retried with the normal retry policy.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	// Caller cancelled the operation.
	if errors.Is(err, context.Canceled) {
		return false
	}

	// Request deadline exceeded.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// PostgreSQL concurrency failures.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case pgerrcode.DeadlockDetected, // 40P01
			pgerrcode.SerializationFailure: // 40001
			return true
		default:
			return false
		}
	}

	// Kafka transient cluster state.
	var kErr kafka.Error
	if errors.As(err, &kErr) {
		if kErr.Temporary() {
			return true
		}
	}

	// Generic network timeouts.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	// Connection reset / dropped connection.
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if errors.Is(opErr.Err, syscall.ECONNRESET) {
			return true
		}
	}

	// Dropped stream.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	return false
}

// ErrConsumerPanic is wrapped when a worker goroutine panics during message processing.
var ErrConsumerPanic = errors.New("consumer panic")

// ErrConsumerFetchDeadline is returned when the fetcher exhausts its retry
// deadline (derived from SessionMs) without a successful FetchMessage.
var ErrConsumerFetchDeadline = errors.New("consumer fetch deadline exceeded")

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
	Err     error
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
