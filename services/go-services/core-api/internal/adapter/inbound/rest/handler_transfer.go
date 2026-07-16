package rest

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
)

func (s *Server) handleCreateTransfer(w http.ResponseWriter, r *http.Request) {
	// Get idempotency key
	idempKey := r.Header.Get(string(HeaderIdempotencyKey))
	if idempKey == "" {
		writeError(w, platform.ErrMissingIdempotencyKey(domain.ErrMissingIdempotencyKey))
		return
	}

	// Decode JSON to Transfer struct (body limited to prevent OOM)
	r.Body = http.MaxBytesReader(w, r.Body, domain.MaxRequestBytes)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			writeError(w, platform.ErrPayloadTooLarge(domain.ErrMsgPayloadTooLarge))
			return
		}
		writeError(w, platform.ErrInvalidBody(domain.ErrInvalidBody))
		return
	}

	var req apiv1.CreateTransferRequest
	if err := protojson.Unmarshal(bodyBytes, &req); err != nil {
		writeError(w, platform.ErrInvalidBody(domain.ErrInvalidBody))
		return
	}
	t := domain.Transfer{
		FromWallet: req.FromWallet,
		ToWallet:   req.ToWallet,
		Amount:     req.Amount,
		Currency:   req.Currency,
		Reference:  req.Reference,
	}

	// Get Merchant Identity from Context
	principal, ok := r.Context().Value(ContextPrincipal).(domain.Principal)
	if !ok {
		writeError(w, platform.ErrUnauthorized(domain.ErrMissingAuthContext))
		return
	}

	// Call service inside the single Layer 1 retry boundary.
	t.MerchantID = principal.MerchantID
	var res domain.SubmitResult
	err = retryBoundary(r.Context(), func() error {
		var fnErr error
		res, fnErr = s.transfers.Submit(r.Context(), t, idempKey)
		return fnErr
	})
	if err != nil {
		err = mapHTTPError(err)
		if errors.Is(err, domain.ErrIdempotencyConflict) {
			platform.RecordIdempotencyConflict(r.Context(), principal.MerchantID, res.Job.ID, res.Job.ShardID)
		}
		writeError(w, err)
		return
	}

	// Respond
	resp := apiv1.CreateTransferResponse{
		JobId:  res.Job.ID,
		Status: string(res.Job.Status),
		Links: &apiv1.JobLinks{
			Self: fmt.Sprintf("%s%s", platform.APIJobPathPrefix, res.Job.ID),
		},
	}

	statusCode := http.StatusAccepted
	if res.AlreadyExisted {
		statusCode = http.StatusOK
	}
	writeJSON(w, statusCode, &resp)
}
