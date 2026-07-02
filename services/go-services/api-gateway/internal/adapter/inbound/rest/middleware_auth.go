package rest

import (
	"context"
	"net/http"
	"strings"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/golang-jwt/jwt/v5"
)

func (s *Server) VerifyJWTkeyFunc(token *jwt.Token) (interface{}, error) {
	if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
		return nil, platform.ErrUnauthorized("unexpected signing method")
	}
	return s.jwtKey, nil
}

func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// a. Extract Bearer Token
		authHeader := r.Header.Get(string(HeaderAuthorization))

		if authHeader == "" || !strings.HasPrefix(authHeader, string(HeaderValBearer)) {
			writeError(w, platform.ErrUnauthorized(domain.ErrMissingBearerToken))
			return
		}
		tokenStr := strings.TrimPrefix(authHeader, string(HeaderValBearer))
		tokenStr = strings.TrimSpace(tokenStr)

		// b. Verify Token signature and extract claims
		token, err := jwt.Parse(tokenStr, s.VerifyJWTkeyFunc)
		if err != nil || !token.Valid {
			writeError(w, platform.ErrInvalidAPIKey(domain.ErrInvalidAPIKey.Error()))
			return
		}

		// c. Extract the merchant_id from the token claims
		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			writeError(w, platform.ErrUnauthorized(domain.ErrInvalidAPIKey.Error()))
			return
		}
		merchantID, ok := claims["sub"].(string)
		if !ok || merchantID == "" {
			writeError(w, platform.ErrUnauthorized(domain.ErrInvalidAPIKey.Error()))
			return
		}

		// d. Check active status
		_, err = s.directory.ShardFor(r.Context(), merchantID)
		if err != nil {
			writeError(w, err) // Translates domain.ErrMerchantInactive -> 403 Frozen
			return
		}

		// e. Create a Principal and inject into context
		principal := domain.Principal{MerchantID: merchantID, Status: string(platform.MerchantStatusActive)}
		ctx := context.WithValue(r.Context(), ContextPrincipal, principal)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
