package rest

import (
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

// handleCreateWallet handles wallet creation requests.
func (s *Server) handleCreateWallet(w http.ResponseWriter, r *http.Request) {
	// 1. Validation
	principal, ok := r.Context().Value(ContextPrincipal).(domain.Principal)
	if !ok {
		writeError(w, platform.ErrUnauthorized(domain.ErrMissingAuthContext))
		return
	}

	var req apiv1.CreateWalletRequest
	if !decodeProtoBody(w, r, &req) {
		return
	}

	// Create wallet
	walletID, err := s.wallets.CreateWallet(r.Context(), principal.MerchantID, req.Currency)
	if err != nil {
		writeError(w, err)
		return
	}

	resp := apiv1.CreateWalletResponse{
		WalletId: walletID,
		Currency: req.Currency,
	}

	writeJSON(w, http.StatusCreated, &resp)
}

// handleDeposit handles wallet funding requests from the fiat vault.
func (s *Server) handleDeposit(w http.ResponseWriter, r *http.Request) {
	// 1. Validation
	idempKey := r.Header.Get(string(HeaderIdempotencyKey))
	if idempKey == "" {
		writeError(w, platform.ErrMissingIdempotencyKey(domain.ErrMissingIdempotencyKey))
		return
	}

	principal, ok := r.Context().Value(ContextPrincipal).(domain.Principal)
	if !ok {
		writeError(w, platform.ErrUnauthorized(domain.ErrMissingAuthContext))
		return
	}

	walletID := r.PathValue("wallet_id")
	if walletID == "" {
		writeError(w, platform.ErrValidation(ParamWalletID, domain.ErrInvalidToWallet))
		return
	}

	var req apiv1.DepositRequest
	if !decodeProtoBody(w, r, &req) {
		return
	}

	// 2. Deposit
	res, err := s.wallets.Deposit(r.Context(), principal.MerchantID, walletID, req.Amount, req.Currency, idempKey)
	if err != nil {
		writeError(w, err)
		return
	}

	resp := apiv1.DepositResponse{
		JobId:  res.Job.ID,
		Status: string(res.Job.Status),
	}

	statusCode := http.StatusAccepted
	if res.AlreadyExisted {
		statusCode = http.StatusOK
	}
	writeJSON(w, statusCode, &resp)
}
