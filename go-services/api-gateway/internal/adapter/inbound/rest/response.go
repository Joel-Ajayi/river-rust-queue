// Package rest is the gateway's driving adapter: it turns HTTP requests into
// calls on the inbound ports and turns the results (and domain errors) back into
// HTTP responses. The core knows nothing about any of this.
package rest

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.uber.org/zap"
)

// apiError is the RFC 7807-style wire shape for an error.
type apiError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// toAppError maps an abstract domain error onto a transport-level platform.AppError.
// This is the single seam where business vocabulary becomes HTTP status codes.
func toAppError(err error) *platform.AppError {
	var ve domain.ValidationError
	switch {
	case errors.As(err, &ve):
		return platform.ErrValidation(ve.Field, ve.Msg)
	case errors.Is(err, domain.ErrMerchantInactive):
		return platform.ErrMerchantFrozen()
	case errors.Is(err, domain.ErrInvalidCredentials):
		return platform.ErrInvalidAPIKey("API key does not match")
	case errors.Is(err, domain.ErrIdempotencyConflict):
		return platform.ErrIdempotencyMismatch()
	}
	if appErr, ok := err.(*platform.AppError); ok {
		return appErr
	}
	return platform.ErrInternal("internal server error")
}

func handleError(w http.ResponseWriter, log *zap.Logger, err error) {
	appErr := toAppError(err)
	if appErr.Status >= 500 {
		log.Error("server error", zap.Error(err))
	}
	writeJSON(w, appErr.Status, apiError{
		Error:   appErr.Code,
		Message: appErr.Message,
		Field:   appErr.Field,
	})
}
