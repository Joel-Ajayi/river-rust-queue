package domain

import (
	"errors"
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

// IsTerminalError determines if an error is a terminal domain failure.
func IsTerminalError(err error) bool {
	if err == nil {
		return false
	}

	var httpErr *platform.HttpError
	if errors.As(err, &httpErr) {
		switch httpErr.Status {
		case http.StatusRequestTimeout, http.StatusTooManyRequests:
			return false // transient
		default:
			if httpErr.Status >= http.StatusBadRequest && httpErr.Status < http.StatusInternalServerError {
				return true // terminal
			}
			return false // transient (5xx)
		}
	}

	return false
}

// Application Errors
var (
	ErrDeliveryFailed     = errors.New("webhook delivery failed")
	ErrPanic              = errors.New("panic during processing")
	ErrorMerchantInactive = "merchant is not active" // terminal: route to DLQ
)

// WebhookMaxConcurrency limits concurrent HTTP requests per merchant via bulkhead.
const WebhookMaxConcurrency = 50
