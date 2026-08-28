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
	"go.uber.org/zap"
)

func (s *Server) handleCreateTransfer(w http.ResponseWriter, r *http.Request) {
	// 1. Validate and extract mandatory idempotency key from header
	idempKey := r.Header.Get(string(HeaderIdempotencyKey))
	if idempKey == "" {
		writeError(w, platform.ErrMissingIdempotencyKey(domain.ErrMissingIdempotencyKey))
		return
	}

	// 2. Decode and validate protobuf request body
	var req apiv1.CreateTransferRequest
	if !decodeProtoBody(w, r, &req) {
		return
	}

	// 3. Extract authenticated merchant principal from request context
	principal, ok := r.Context().Value(ContextPrincipal).(domain.Principal)
	if !ok {
		writeError(w, platform.ErrUnauthorized(domain.ErrMissingAuthContext))
		return
	}

	// 4. Construct transfer domain model
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

	// 5. Execute transaction within single Layer 1 retry boundary
	err := s.retryBoundary(r.Context(), func(attemptCtx context.Context, exec failsafe.Execution[any]) error {
		var fnErr error
		attempts = exec.Executions() + 1
		if req.FromWallet == "" {
			res, fnErr = s.wallets.Deposit(attemptCtx, t, idempKey)
		} else {
			res, fnErr = s.transfers.Transfer(attemptCtx, t, idempKey)
		}
		return fnErr
	})
	dbDuration := time.Since(start)

	// 6. Handle failure and emit structured error log
	if err != nil {
		platform.LoggerWithTrace(r.Context(), s.log).Warn(
			platform.LogEventTransferFailed,
			zap.String(platform.LogFieldMerchantID, principal.MerchantID),
			zap.String(platform.LogFieldFromWallet, t.FromWallet),
			zap.String(platform.LogFieldToWallet, t.ToWallet),
			zap.Int64(platform.LogFieldAmount, t.Amount),
			zap.String(platform.LogFieldCurrency, t.Currency),
			zap.String(platform.LogFieldIdempotencyKey, idempKey),
			zap.Error(err),
		)
		err = mapHTTPError(err)
		writeError(w, err)
		return
	}

	// 7. Construct API response payload
	resp := apiv1.CreateTransferResponse{
		JobId:  res.Job.ID,
		Status: string(res.Job.Status),
		Links: &apiv1.JobLinks{
			Self: fmt.Sprintf("%s%s", platform.APIJobPathPrefix, res.Job.ID),
		},
	}

	// 8. Emit canonical transaction log upon successful DB commit
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

	// 9. Write JSON response (202 Accepted for new jobs, 200 OK for idempotency replays)
	statusCode := http.StatusAccepted
	if res.AlreadyExisted {
		statusCode = http.StatusOK
	}
	writeJSON(w, statusCode, &resp)
}
