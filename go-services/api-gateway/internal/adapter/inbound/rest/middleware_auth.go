package rest

import (
	"context"
	"net/http"
	"strings"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
)

type contextKey string

const merchantIDKey contextKey = "merchant_id"

// requireAuth verifies the bearer JWT (a transport-security concern, so it lives
// in the adapter, not the core) and stores the merchant id on the request context.
func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			handleError(w, s.log, platform.ErrUnauthorized("missing or invalid token"))
			return
		}
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

		token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, jwt.ErrSignatureInvalid
			}
			return s.jwtKey, nil
		})
		if err != nil || !token.Valid {
			s.log.Debug("jwt validation failed", zap.Error(err))
			handleError(w, s.log, platform.ErrUnauthorized("missing or invalid token"))
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			handleError(w, s.log, platform.ErrUnauthorized("invalid claims"))
			return
		}
		merchantID, _ := claims["sub"].(string)
		if merchantID == "" {
			handleError(w, s.log, platform.ErrUnauthorized("missing subject"))
			return
		}

		ctx := context.WithValue(r.Context(), merchantIDKey, merchantID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func merchantFromCtx(ctx context.Context) string {
	v, _ := ctx.Value(merchantIDKey).(string)
	return v
}
