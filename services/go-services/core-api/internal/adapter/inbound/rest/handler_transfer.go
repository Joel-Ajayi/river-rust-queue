package rest

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/failsafe-go/failsafe-go"

	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
)

func (s *Server) handleCreateTransfer(w http.ResponseWriter, r *http.Request) {
	// Get idempotency key
	idempKey := r.Header.Get(string(HeaderIdempotencyKey))
	if idempKey == "" {
		writeError(w, platform.ErrMissingIdempotencyKey(domain.ErrMissingIdempotencyKey))
		return
	}

	// Get request body from request
	var req apiv1.CreateTransferRequest
	if !decodeProtoBody(w, r, &req) {
		return
	}

	// Get Merchant Identity from Context
	principal, ok := r.Context().Value(ContextPrincipal).(domain.Principal)
	if !ok {
		writeError(w, platform.ErrUnauthorized(domain.ErrMissingAuthContext))
		return
	}

	// Call service inside the single Layer 1 retry boundary.
	t := domain.Transfer{
		FromWallet: req.FromWallet,
		ToWallet:   req.ToWallet,
		Amount:     req.Amount,
		Currency:   req.Currency,
		Reference:  req.Reference,
	}
	t.MerchantID = principal.MerchantID
	var res domain.SubmitResult
	var attempts int
	start := time.Now()
	err := s.retryBoundary(r.Context(), func(attemptCtx context.Context, exec failsafe.Execution[any]) error {
		var fnErr error
		attempts = exec.Executions() + 1
		// Handle Deposit
		if req.FromWallet == "" {
			res, fnErr = s.wallets.Deposit(attemptCtx, t, idempKey)
		} else {
			res, fnErr = s.transfers.Transfer(attemptCtx, t, idempKey)
		}
		return fnErr
	})
	dbDuration := time.Since(start)
	if err != nil {
		err = mapHTTPError(err)
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
		Event:          platform.EventTransferSubmitted,
		Status:         platform.StatusSuccess,
		MerchantID:     principal.MerchantID,
		JobID:          res.Job.ID,
		Amount:         t.Amount,
		Currency:       t.Currency,
		DurationMs:     float64(dbDuration.Milliseconds()),
		RetryCount:     attempts,
		IdempotencyHit: res.AlreadyExisted,
	})

	statusCode := http.StatusAccepted
	if res.AlreadyExisted {
		statusCode = http.StatusOK
	}
	writeJSON(w, statusCode, &resp)
}
