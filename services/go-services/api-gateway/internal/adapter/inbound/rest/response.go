package rest

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set(string(ContentType), string(ApplicationJSON))
	w.WriteHeader(status)
	if data != nil {
		if pbMsg, ok := data.(proto.Message); ok {
			marshaler := protojson.MarshalOptions{EmitUnpopulated: true}
			b, _ := marshaler.Marshal(pbMsg)
			w.Write(b)
		} else {
			_ = json.NewEncoder(w).Encode(data)
		}
	}
}

// writeError translates domain errors into HTTP platform errors.
func writeError(w http.ResponseWriter, err error) {
	var appErr *platform.AppError

	if errors.As(err, &appErr) {
		pbErr := &apiv1.ErrorResponse{
			Error:   appErr.Code,
			Message: appErr.Message,
			Field:   appErr.Field,
		}
		writeJSON(w, appErr.Status, pbErr)
		return
	}

	switch {
	case errors.Is(err, domain.ErrMerchantInactive):
		appErr = platform.ErrMerchantFrozen()
	case errors.Is(err, domain.ErrIdempotencyConflict):
		appErr = platform.ErrIdempotencyMismatch()
	case errors.Is(err, domain.ErrJobNotFound):
		appErr = platform.ErrNotFound(string(platform.AggregateTypeJob))
	case errors.Is(err, domain.ErrInvalidAPIKey):
		appErr = platform.ErrInvalidAPIKey(err.Error())
	case errors.Is(err, domain.ErrWalletNotOwned):
		appErr = platform.ErrForeignWallet()
	case errors.Is(err, domain.ErrWalletNotFound):
		appErr = platform.ErrValidation(string(platform.AggregateTypeWallet), domain.ErrWalletNotFound.Error())
	default:
		// Check for Validation errors
		var valErr domain.ValidationError
		if errors.As(err, &valErr) {
			appErr = platform.ErrValidation(valErr.Field, valErr.Msg)
		} else {
			// Catch-all for unexpected errors (e.g., database connection dropped)
			appErr = platform.ErrInternal(domain.ErrInternal.Error())
		}
	}

	pbErr := &apiv1.ErrorResponse{
		Error:   appErr.Code,
		Message: appErr.Message,
		Field:   appErr.Field,
	}
	writeJSON(w, appErr.Status, pbErr)
}
