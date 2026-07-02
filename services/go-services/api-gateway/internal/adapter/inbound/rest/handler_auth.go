package rest

import (
	"net/http"
	"strings"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
)

// handleAuthToken exchanges an API key for a short-lived JWT. Authentication
// (the API-key lookup) is delegated to the inbound port; minting the JWT is a
// transport detail and stays here.
func (s *Server) handleAuthToken(w http.ResponseWriter, r *http.Request) {
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, string(HeaderValBearer)) {
		writeError(w, domain.ErrInvalidAPIKey)
		return
	}
	apiKey := strings.TrimPrefix(authHeader, string(HeaderValBearer))
	apiKey = strings.TrimSpace(apiKey)
	principal, err := s.svc.Authenticate(r.Context(), apiKey)
	if err != nil {
		writeError(w, err)
		return
	}

	now := time.Now()
	claims := jwt.MapClaims{
		"sub":  principal.MerchantID,
		"iat":  now.Unix(),
		"exp":  now.Add(1 * time.Hour).Unix(),
		"tier": principal.Tier,
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.jwtKey)
	if err != nil {
		s.log.Error("jwt signing failed", zap.Error(err))
		writeError(w, platform.ErrInternal(domain.ErrInternal))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"token":      signed,
		"expires_in": 3600,
	})
}
