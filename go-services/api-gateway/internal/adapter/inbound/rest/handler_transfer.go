package rest

import (
	"encoding/json"
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

// transferRequest is the adapter's wire DTO. It is mapped into a domain.Transfer
// and never leaks past this package.
type transferRequest struct {
	FromWallet string `json:"from_wallet"`
	ToWallet   string `json:"to_wallet"`
	Amount     int64  `json:"amount"`
	Currency   string `json:"currency"`
	Reference  string `json:"reference"`
}

type jobResponse struct {
	JobID  string            `json:"job_id"`
	Status string            `json:"status"`
	Links  map[string]string `json:"_links"`
}

func (s *Server) handleCreateTransfer(w http.ResponseWriter, r *http.Request) {
	merchantID := merchantFromCtx(r.Context())

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		handleError(w, s.log, platform.ErrMissingIdempotencyKey())
		return
	}

	var body transferRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		handleError(w, s.log, platform.ErrInvalidBody("invalid JSON"))
		return
	}

	transfer := domain.Transfer{
		MerchantID: merchantID,
		FromWallet: body.FromWallet,
		ToWallet:   body.ToWallet,
		Amount:     body.Amount,
		Currency:   body.Currency,
		Reference:  body.Reference,
	}

	res, err := s.submitter.Submit(r.Context(), transfer, idempotencyKey)
	if err != nil {
		handleError(w, s.log, err)
		return
	}

	status := http.StatusAccepted
	if res.AlreadyExisted {
		status = http.StatusOK
	}
	writeJSON(w, status, jobResponse{
		JobID:  res.Job.ID,
		Status: res.Job.Status,
		Links:  map[string]string{"self": "/v1/jobs/" + res.Job.ID},
	})
}
