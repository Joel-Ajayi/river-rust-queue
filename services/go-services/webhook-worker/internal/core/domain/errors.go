package domain

import (
	"errors"
)

// IsTerminalError determines if an error is a terminal domain failure.
func IsTerminalError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrDeliveryFailed)
}

// Application Errors
var (
	ErrDeliveryFailed = errors.New("webhook delivery failed")
	ErrPanic          = errors.New("panic during processing")
)

const (
	CBMaxRequests = 1
	CBTimeout     = 30
	CBMaxFails    = 5
)
