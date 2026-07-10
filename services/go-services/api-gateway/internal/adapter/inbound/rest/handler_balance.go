package rest

import (
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

func (s *Server) handleGetBalance(w http.ResponseWriter, r *http.Request) {
	// a. Get Merchant Identity from Context
	principal, ok := r.Context().Value(ContextPrincipal).(domain.Principal)
	if !ok {
		writeError(w, platform.ErrUnauthorized(domain.ErrMissingAuthContext.Error()))
		return
	}

	walletID := r.URL.Query().Get(ParamWalletID)
	if walletID == "" {
		writeError(w, platform.ErrValidation(ParamWalletID, domain.ErrMsgQueryParamRequired))
		return
	}

	// b. Get balance
	balance, currency, err := s.transfers.GetBalance(r.Context(), walletID, principal.MerchantID)
	if err != nil {
		writeError(w, err)
		return
	}

	// c. Respond
	resp := &apiv1.GetBalanceResponse{
		WalletId: walletID,
		Balance:  balance,
		Currency: currency,
	}
	writeJSON(w, http.StatusOK, resp)
}
