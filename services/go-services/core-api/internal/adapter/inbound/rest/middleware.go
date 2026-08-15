package rest

import (
	"context"
	"net/http"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

func (s *Server) extractMerchant(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract merchantID and tier from Envoy Gateway's injected headers (claimsToHeaders)
		merchantID := r.Header.Get(HeaderMerchantID)
		if merchantID == "" {
			writeError(w, platform.ErrUnauthorized(domain.ErrMissingConsumerIdentity))
			return
		}
		tier := r.Header.Get(HeaderMerchantTier)

		// Trust Envoy Gateway's edge JWT authentication
		principal := domain.Principal{
			MerchantID: merchantID,
			Tier:       tier,
			Status:     platform.MerchantStatusActive,
		}

		// e. Inject Principal into context
		ctx := context.WithValue(r.Context(), ContextPrincipal, principal)

		// f. Inject merchant_id into OpenTelemetry trace span
		span := trace.SpanFromContext(ctx)
		if span.IsRecording() {
			span.SetAttributes(attribute.String(platform.MetricLabelMerchantID, principal.MerchantID))
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// requirePlatformAdmin restricts an endpoint to the platform administrator.
// It requires the request to have arrived via the Envoy gateway's admin route
// (which stamps X-RRQ-Edge) and the authenticated merchant to be the seeded
// platform merchant.
func (s *Server) requirePlatformAdmin(next http.Handler) http.Handler {
	return s.extractMerchant(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(HeaderEdgeOrigin) != HeaderEdgeOriginValue {
			writeError(w, platform.ErrForbidden(domain.ErrAdminForbidden))
			return
		}
		principal, ok := r.Context().Value(ContextPrincipal).(domain.Principal)
		if !ok || principal.MerchantID != platform.PlatformMerchantID {
			writeError(w, platform.ErrForbidden(domain.ErrAdminForbidden))
			return
		}
		next.ServeHTTP(w, r)
	}))
}

func (s *Server) withBulkhead(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if !s.bulkhead.TryAcquire(1) {
			platform.RecordBulkheadRejection(ctx)
			s.log.Warn(platform.LogEventBulkheadRejected,
				zap.String(platform.LogFieldMethod, r.Method),
				zap.String(platform.LogFieldPath, r.URL.Path),
			)
			writeError(w, platform.ErrServiceUnavailable(domain.ErrMsgBulkheadExhausted))
			return
		}
		platform.AddBulkheadInFlight(ctx, 1)
		defer func() {
			s.bulkhead.Release(1)
			platform.AddBulkheadInFlight(ctx, -1)
		}()
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

func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		logger := platform.LoggerWithTrace(r.Context(), s.log)
		route := r.Pattern
		if route == "" {
			route = r.URL.Path
		}
		logger.Info(platform.LogEventHTTPRequestHandled,
			zap.String(platform.LogFieldMethod, r.Method),
			zap.String(platform.LogFieldPath, route),
			zap.Int(platform.LogFieldStatus, rec.status),
			zap.Duration(platform.LogFieldDuration, time.Since(start)),
		)
	})
}
