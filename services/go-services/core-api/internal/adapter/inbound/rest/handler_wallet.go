package rest

import (
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.uber.org/zap"
)

// handleCreateWallet handles wallet creation requests.
func (s *Server) handleCreateWallet(w http.ResponseWriter, r *http.Request) {
	// 1. Extract and validate authenticated merchant principal from request context
	principal, ok := r.Context().Value(ContextPrincipal).(domain.Principal)
	if !ok {
		writeError(w, platform.ErrUnauthorized(domain.ErrMissingAuthContext))
		return
	}

	// 2. Decode and validate protobuf request body
	var req apiv1.CreateWalletRequest
	if !decodeProtoBody(w, r, &req) {
		return
	}

	// 3. Create wallet in shard database via wallet usecase
	walletID, err := s.wallets.CreateWallet(r.Context(), principal.MerchantID, req.Currency)
	if err != nil {
		platform.LoggerWithTrace(r.Context(), s.log).Error(platform.LogEventWalletCreateFailed,
			zap.String(platform.LogFieldCurrency, req.Currency),
			zap.Error(err),
		)
		writeError(w, err)
		return
	}

	// 4. Construct and return response
	resp := apiv1.CreateWalletResponse{
		WalletId: walletID,
		Currency: req.Currency,
	}

	writeJSON(w, http.StatusCreated, &resp)
}
