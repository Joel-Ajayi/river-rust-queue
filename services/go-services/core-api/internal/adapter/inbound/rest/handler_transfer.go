package rest

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"

	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
)

func (s *Server) handleCreateTransfer(w http.ResponseWriter, r *http.Request) {
	// Get idempotency key
	idempKey := r.Header.Get(string(HeaderIdempotencyKey))
	if idempKey == "" {
		writeError(w, platform.ErrMissingIdempotencyKey(domain.ErrMissingIdempotencyKey))
		return
	}

	var req apiv1.CreateTransferRequest
	if !decodeProtoBody(w, r, &req) {
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
	err := retryBoundary(r.Context(), func() error {
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

	// Canonical log: transfer submitted (after DB commit)
	platform.LogCanonicalEvent(r.Context(), s.log, platform.ServiceNameCoreAPI, platform.CanonicalLogLine{
		Event:      platform.EventTransferSubmitted,
		Status:     platform.StatusSuccess,
		MerchantID: principal.MerchantID,
		JobID:      res.Job.ID,
		Amount:     t.Amount,
		Currency:   t.Currency,
	})

	statusCode := http.StatusAccepted
	if res.AlreadyExisted {
		statusCode = http.StatusOK
	}
	writeJSON(w, statusCode, &resp)
}
