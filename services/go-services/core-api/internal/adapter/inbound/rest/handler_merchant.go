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

// handleAuthToken exchanges an API key for a short-lived JWT.
func (s *Server) handleAuthToken(w http.ResponseWriter, r *http.Request) {
	// 1. Extract Bearer API key from Authorization header
	authHeader := r.Header.Get(HeaderAuthorization)
	if !strings.HasPrefix(authHeader, HeaderValBearer) {
		writeError(w, domain.ErrInvalidAPIKey)
		return
	}
	apiKey := strings.TrimPrefix(authHeader, HeaderValBearer)
	apiKey = strings.TrimSpace(apiKey)

	// 2. Authenticate merchant via Argon2 password verification
	principal, err := s.merchants.Authenticate(r.Context(), apiKey)
	if err != nil {
		platform.LoggerWithTrace(r.Context(), s.log).Warn(platform.LogEventAuthTokenFailed, zap.Error(err))
		return
	}

	// 3. Build JWT Claims with sub, issuer, expiration, and tier
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

	// 4. Retrieve active Ed25519 signing key
	activeKey, ok := s.jwtKeys[s.jwtActiveKeyID]
	if !ok {
		platform.LoggerWithTrace(r.Context(), s.log).Error(platform.LogEventJWTKeyNotFound, zap.String(platform.MetricLabelKeyID, s.jwtActiveKeyID))
		writeError(w, platform.ErrInternal(nil))
		return
	}

	// 5. Sign JWT token
	signed, err := token.SignedString(activeKey)
	if err != nil {
		platform.LoggerWithTrace(r.Context(), s.log).Error(platform.LogEventJWTSigningFailed, zap.Error(err))
		writeError(w, platform.ErrInternal(err))
		return
	}

	// 6. Write JSON response
	resp := &apiv1.AuthTokenResponse{
		Token:     signed,
		ExpiresIn: int32(s.jwtExpiration.Seconds()),
		Tier:      principal.Tier,
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleCreateMerchant creates a new merchant and registers them on a shard.
func (s *Server) handleCreateMerchant(w http.ResponseWriter, r *http.Request) {
	// 1. Decode protobuf registration request body
	var req apiv1.CreateMerchantRequest
	if !decodeProtoBody(w, r, &req) {
		return
	}

	// 2. Enforce self-service registration tier policy (standard only)
	if req.Tier == platform.MerchantTierPremium {
		writeError(w, platform.ErrForbidden(domain.ErrPremiumRequiresAdmin))
		return
	}

	// 3. Create merchant in global database and allocate to a database shard
	merchantID, apiKeyPlain, shardID, err := s.merchants.CreateMerchant(
		r.Context(),
		req.Name,
		req.WebhookUrl,
		req.WebhookSecret,
		req.Tier,
	)
	if err != nil {
		platform.LoggerWithTrace(r.Context(), s.log).Error(platform.LogEventMerchantCreateFailed,
			zap.String(platform.LogFieldName, req.Name),
		)
		writeError(w, err)
		return
	}

	// 4. Construct and return response with plaintext API key (shown once)
	resp := apiv1.CreateMerchantResponse{
		MerchantId: merchantID,
		ApiKey:     apiKeyPlain,
		ShardId:    shardID,
	}

	writeJSON(w, http.StatusCreated, &resp)
}
