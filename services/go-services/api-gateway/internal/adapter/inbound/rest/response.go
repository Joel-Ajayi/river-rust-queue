package rest

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set(string(ContentType), string(ApplicationJSON))
	w.WriteHeader(status)
	if data != nil {
		_ = json.NewEncoder(w).Encode(data)
	}
}

// writeError translates domain errors into HTTP platform errors.
func writeError(w http.ResponseWriter, err error) {
	var appErr *platform.AppError

	if errors.As(err, &appErr) {
		writeJSON(w, appErr.Status, appErr)
		return
	}

	switch {
	case errors.Is(err, domain.ErrMerchantInactive):
		appErr = platform.ErrMerchantFrozen()
	case errors.Is(err, domain.ErrIdempotencyConflict):
		appErr = platform.ErrIdempotencyMismatch()
	case errors.Is(err, domain.ErrJobNotFound):
		appErr = platform.ErrNotFound("job")
	case errors.Is(err, domain.ErrInvalidAPIKey):
		appErr = platform.ErrInvalidAPIKey(err.Error())
	case errors.Is(err, domain.ErrWalletNotOwned):
		appErr = platform.ErrForeignWallet()
	default:
		// Check for Validation errors
		var valErr domain.ValidationError
		if errors.As(err, &valErr) {
			appErr = platform.ErrValidation(valErr.Field, valErr.Msg)
		} else {
			// Catch-all for unexpected errors (e.g., database connection dropped)
			appErr = platform.ErrInternal(domain.ErrInternal)
		}
	}

	writeJSON(w, appErr.Status, appErr)
}
