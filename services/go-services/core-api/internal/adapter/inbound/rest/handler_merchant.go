package rest

import (
	"net/http"
	"strings"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
)

// handleAuthToken exchanges an API key for a short-lived JWT. Authentication
func (s *Server) handleAuthToken(w http.ResponseWriter, r *http.Request) {
	authHeader := r.Header.Get(HeaderAuthorization)
	if !strings.HasPrefix(authHeader, HeaderValBearer) {
		writeError(w, domain.ErrInvalidAPIKey)
		return
	}
	apiKey := strings.TrimPrefix(authHeader, HeaderValBearer)
	apiKey = strings.TrimSpace(apiKey)
	principal, err := s.merchants.Authenticate(r.Context(), apiKey)
	if err != nil {
		writeError(w, err)
		return
	}

	now := time.Now()
	claims := jwt.MapClaims{
		domain.ClaimSub:  principal.MerchantID,
		domain.ClaimIss:  platform.ServiceNameCoreAPI,
		domain.ClaimIat:  now.Unix(),
		domain.ClaimExp:  now.Add(s.jwtExpiration).Unix(),
		domain.ClaimTier: principal.Tier,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header[platform.JWTHeaderKeyID] = s.jwtActiveKeyID

	activeKey, ok := s.jwtKeys[s.jwtActiveKeyID]
	if !ok {
		s.log.Error(platform.LogEventJWTKeyNotFound, zap.String(platform.MetricLabelKeyID, s.jwtActiveKeyID))
		writeError(w, platform.ErrInternal(nil))
		return
	}

	signed, err := token.SignedString(activeKey)
	if err != nil {
		s.log.Error(platform.LogEventJWTSigningFailed, zap.Error(err))
		writeError(w, platform.ErrInternal(err))
		return
	}

	resp := &apiv1.AuthTokenResponse{
		Token:     signed,
		ExpiresIn: int32(s.jwtExpiration.Seconds()),
		Tier:      principal.Tier,
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleCreateMerchant creates a new merchant and registers them on a shard.
func (s *Server) handleCreateMerchant(w http.ResponseWriter, r *http.Request) {
	var req apiv1.CreateMerchantRequest
	if !decodeProtoBody(w, r, &req) {
		return
	}

	// Registration is self-service: only standard merchants can be created
	// this way. The premium tier belongs exclusively to the seeded platform
	// merchant, so the system always has exactly one premium merchant.
	if req.Tier == platform.MerchantTierPremium {
		writeError(w, platform.ErrForbidden(domain.ErrPremiumRequiresAdmin))
		return
	}

	merchantID, apiKeyPlain, shardID, err := s.merchants.CreateMerchant(
		r.Context(),
		req.Name,
		req.WebhookUrl,
		req.WebhookSecret,
		req.Tier,
	)
	if err != nil {
		writeError(w, err)
		return
	}

	resp := apiv1.CreateMerchantResponse{
		MerchantId: merchantID,
		ApiKey:     apiKeyPlain,
		ShardId:    shardID,
	}

	writeJSON(w, http.StatusCreated, &resp)
}
