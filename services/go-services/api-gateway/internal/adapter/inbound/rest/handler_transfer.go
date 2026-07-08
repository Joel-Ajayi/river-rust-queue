package rest

import (
	"fmt"
	"io"
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"google.golang.org/protobuf/encoding/protojson"

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
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, platform.ErrInvalidBody(domain.ErrInvalidBody.Error()))
		return
	}

	var req apiv1.CreateTransferRequest
	if err := protojson.Unmarshal(bodyBytes, &req); err != nil {
		writeError(w, platform.ErrInvalidBody(domain.ErrInvalidBody.Error()))
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
		writeError(w, platform.ErrUnauthorized(domain.ErrMissingAuthContext.Error()))
		return
	}

	// d. Call application service
	t.MerchantID = principal.MerchantID
	res, err := s.transfers.Submit(r.Context(), t, idempKey)
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
	writeJSON(w, statusCode, &resp)
}
