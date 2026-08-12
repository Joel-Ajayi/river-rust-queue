package rest

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set(string(ContentType), string(ApplicationJSON))
	if data != nil {
		if pbMsg, ok := data.(proto.Message); ok {
			marshaler := protojson.MarshalOptions{EmitUnpopulated: false}
			b, err := marshaler.Marshal(pbMsg)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(status)
			w.Write(b)
		} else {
			w.WriteHeader(status)
			if err := json.NewEncoder(w).Encode(data); err != nil {
				return
			}
		}
	} else {
		w.WriteHeader(status)
	}
}

// SetMaxRequestBodyBytes overrides the HTTP body limit from the capacity engine (CORE_API_MAX_REQUEST_BYTES).
func SetMaxRequestBodyBytes(n int64) {
	maxRequestBodyBytes = n
}

// maxRequestBodyBytes defaults to the engine-derived 512 KiB (see slo-input payload bytes × 64).
var maxRequestBodyBytes int64 = 512 * 1024

// decodeProtoBody reads the HTTP request body safely and decodes it into a protobuf message.
func decodeProtoBody(w http.ResponseWriter, r *http.Request, msg proto.Message) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeError(w, platform.ErrPayloadTooLarge(domain.ErrMsgPayloadTooLarge))
			return false
		}
		writeError(w, platform.ErrInvalidBody(domain.ErrInvalidBody))
		return false
	}
	unmarshaler := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := unmarshaler.Unmarshal(bodyBytes, msg); err != nil {
		writeError(w, platform.ErrInvalidBody(domain.ErrInvalidBody))
		return false
	}
	return true
}

// writeError translates domain errors into HTTP platform errors.
func writeError(w http.ResponseWriter, err error) {
	var appErr *platform.AppError

	if !errors.As(err, &appErr) {
		switch {
		case errors.Is(err, domain.ErrInvalidAPIKey), errors.Is(err, domain.ErrInvalidCredentials):
			appErr = platform.ErrInvalidAPIKey(err)
		case errors.Is(err, domain.ErrWalletNotOwned):
			appErr = platform.ErrForeignWallet(err)
		case errors.Is(err, domain.ErrWalletNotFound):
			appErr = platform.ErrValidation(ParamWalletID, err)
		case errors.Is(err, domain.ErrServiceUnavailable):
			appErr = platform.ErrLedgerUnavailable(err)
		case errors.Is(err, domain.ErrInvalidFromWallet):
			appErr = platform.ErrValidation(ParamFromWallet, err)
		case errors.Is(err, domain.ErrInvalidToWallet):
			appErr = platform.ErrValidation(ParamToWallet, err)
		case errors.Is(err, domain.ErrInvalidAmount):
			appErr = platform.ErrValidation(ParamAmount, err)
		case errors.Is(err, domain.ErrInvalidCurrency):
			appErr = platform.ErrValidation(ParamCurrency, err)
		case errors.Is(err, domain.ErrSameWallet):
			appErr = platform.ErrValidation(ParamToWallet, err)
		case errors.Is(err, domain.ErrJobNotFound):
			appErr = platform.ErrNotFound(err)
		case errors.Is(err, domain.ErrIdempotencyConflict):
			appErr = platform.ErrIdempotencyMismatch(err)
		default:
			// Catch-all for unexpected errors (e.g., database connection dropped)
			appErr = platform.ErrInternal(err)
		}
	}

	pbErr := &apiv1.ErrorResponse{
		Error:   appErr.Code,
		Message: appErr.Message,
		Field:   appErr.Field,
	}
	writeJSON(w, appErr.Status, pbErr)
}
