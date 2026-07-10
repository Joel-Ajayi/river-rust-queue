package rest

import (
	"context"
	"net/http"
	"strings"

	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"go.uber.org/zap"
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
		authHeader := r.Header.Get(HeaderAuthorization)

		if authHeader == "" || !strings.HasPrefix(authHeader, HeaderValBearer) {
			writeError(w, platform.ErrUnauthorized(domain.ErrMissingBearerToken.Error()))
			return
		}
		tokenStr := strings.TrimPrefix(authHeader, HeaderValBearer)
		tokenStr = strings.TrimSpace(tokenStr)

		// b. Verify Token signature and extract claims
		token, err := jwt.Parse(tokenStr, s.VerifyJWTkeyFunc, jwt.WithLeeway(domain.JWTLeeway))
		if err != nil || !token.Valid {
			if err != nil && strings.Contains(err.Error(), domain.ErrMsgTokenExpired.Error()) {
				// HTTP 400 Reject replay attacks where JWT or timestamp is >5 mins skewed
				writeError(w, platform.ErrExpiredToken(domain.ErrMsgTokenExpired.Error()))
				return
			}
			writeError(w, platform.ErrInvalidAPIKey(domain.ErrInvalidAPIKey.Error()))
			return
		}

		// c. Extract the merchant_id from the token claims
		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			writeError(w, platform.ErrUnauthorized(domain.ErrInvalidAPIKey.Error()))
			return
		}
		merchantID, ok := claims[domain.ClaimSub].(string)
		if !ok || merchantID == "" {
			writeError(w, platform.ErrUnauthorized(domain.ErrInvalidAPIKey.Error()))
			return
		}

		// d. Delegate to the Core Domain to verify the merchant and build the Principal profile
		principal, err := s.auth.GetPrincipal(r.Context(), merchantID)
		if err != nil {
			writeError(w, err) // Translates domain.ErrMerchantInactive -> 403 Frozen
			return
		}

		// e. Inject Principal into context
		ctx := context.WithValue(r.Context(), ContextPrincipal, principal)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) withRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqLog := platform.LoggerFromContext(r.Context(), s.log)
		if !s.limiter.Allow() {
			reqLog.Warn("Rate limit exceeded", zap.String(platform.LogFieldEvent, platform.LogEventRateLimitExceeded), zap.String(platform.LogFieldPath, r.URL.Path))
			writeError(w, platform.ErrRateLimitExceeded(domain.ErrMsgRateLimitExceeded))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) withBulkhead(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqLog := platform.LoggerFromContext(r.Context(), s.log)
		if !s.bulkhead.TryAcquire(1) {
			reqLog.Warn("Bulkhead full", zap.String(platform.LogFieldEvent, platform.LogEventBulkheadRejected), zap.String(platform.LogFieldPath, r.URL.Path))
			writeError(w, platform.ErrServiceUnavailable(domain.ErrMsgBulkheadExhausted))
			return
		}
		defer s.bulkhead.Release(1)
		next.ServeHTTP(w, r)
	})
}

// statusRecorder is a custom ResponseWriter that tracks the HTTP status code
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (s *Server) withStructuredLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		traceID := uuid.NewString()

		// Inject traceID into context
		ctx := platform.InjectTraceID(r.Context(), traceID)
		r = r.WithContext(ctx)

		recorder := &statusRecorder{
			ResponseWriter: w,
			status:         http.StatusOK, // Default if WriteHeader is never called
		}

		next.ServeHTTP(recorder, r)

		// Canonical Log Line (Stripe Style)
		duration := time.Since(start).Milliseconds()
		s.log.Info("Handled HTTP request",
			zap.String(platform.LogFieldEvent, platform.LogEventHTTPRequestHandled),
			zap.String(platform.LogFieldTraceID, traceID),
			zap.String(platform.LogFieldMethod, r.Method),
			zap.String(platform.LogFieldPath, r.URL.Path),
			zap.Int(platform.LogFieldStatus, recorder.status),
			zap.Int64(platform.LogFieldDuration, duration),
		)
	})
}
