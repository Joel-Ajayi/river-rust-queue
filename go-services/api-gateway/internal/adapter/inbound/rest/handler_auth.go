package rest

import (
	"net/http"
	"strings"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
)

// handleAuthToken exchanges an API key for a short-lived JWT. Authentication
// (the API-key lookup) is delegated to the inbound port; minting the JWT is a
// transport detail and stays here.
func (s *Server) handleAuthToken(w http.ResponseWriter, r *http.Request) {
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, "Bearer ") {
		handleError(w, s.log, platform.ErrInvalidAPIKey("API key missing"))
		return
	}
	apiKey := strings.TrimPrefix(authHeader, "Bearer ")

	principal, err := s.auth.Authenticate(r.Context(), apiKey)
	if err != nil {
		handleError(w, s.log, err)
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
		handleError(w, s.log, platform.ErrInternal("internal error"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"token":      signed,
		"expires_in": 3600,
	})
}
