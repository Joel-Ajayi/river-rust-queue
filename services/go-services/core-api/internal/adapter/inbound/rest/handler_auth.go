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
// (the API-key lookup) is delegated to the inbound port; minting the JWT is a
// transport detail and stays here.
func (s *Server) handleAuthToken(w http.ResponseWriter, r *http.Request) {
	authHeader := r.Header.Get(HeaderAuthorization)
	if !strings.HasPrefix(authHeader, HeaderValBearer) {
		writeError(w, domain.ErrInvalidAPIKey)
		return
	}
	apiKey := strings.TrimPrefix(authHeader, HeaderValBearer)
	apiKey = strings.TrimSpace(apiKey)
	principal, err := s.auth.Authenticate(r.Context(), apiKey)
	if err != nil {
		writeError(w, err)
		return
	}

	now := time.Now()
	claims := jwt.MapClaims{
		domain.ClaimSub:  principal.MerchantID,
		domain.ClaimIss:  principal.MerchantID,
		domain.ClaimIat:  now.Unix(),
		domain.ClaimExp:  now.Add(domain.JWTExpiration).Unix(),
		domain.ClaimTier: principal.Tier,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = s.jwtActiveKeyID
	
	activeKey, ok := s.jwtKeys[s.jwtActiveKeyID]
	if !ok {
		s.log.Error("Active JWT signing key not found in config", zap.String("kid", s.jwtActiveKeyID))
		writeError(w, platform.ErrInternal(nil))
		return
	}
	
	signed, err := token.SignedString(activeKey)
	if err != nil {
		s.log.Error("Failed to sign JWT token", zap.String(platform.LogFieldEvent, platform.LogEventJWTSigningFailed), zap.Error(err))
		writeError(w, platform.ErrInternal(err))
		return
	}

	resp := &apiv1.AuthTokenResponse{
		Token:     signed,
		ExpiresIn: int32(domain.JWTExpiration.Seconds()),
	}
	writeJSON(w, http.StatusOK, resp)
}
