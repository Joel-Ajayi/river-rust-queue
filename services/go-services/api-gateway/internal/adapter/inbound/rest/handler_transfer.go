package rest

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"

	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
)

func (s *Server) handleCreateTransfer(w http.ResponseWriter, r *http.Request) {
	// a. Get idempotency key
	idempKey := r.Header.Get(string(HeaderIdempotencyKey))
	if idempKey == "" {
		writeError(w, platform.ErrMissingIdempotencyKey())
		return
	}

	// b. Decode JSON to Transfer Struct
	var req apiv1.CreateTransferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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

	// c. Get Merchant Identity from Context
	principal, ok := r.Context().Value(ContextPrincipal).(domain.Principal)
	if !ok {
		writeError(w, platform.ErrUnauthorized(domain.ErrMissingAuthContext))
		return
	}

	// d. Call application service
	t.MerchantID = principal.MerchantID
	res, err := s.svc.Submit(r.Context(), t, idempKey)
	if err != nil {
		writeError(w, err)
		return
	}

	// e. Respond
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
