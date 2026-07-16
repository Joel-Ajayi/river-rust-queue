package rest

import (
	"context"
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// a. Extract merchantID from Kong's injected header
		merchantID := r.Header.Get(HeaderKongConsumerCustomID)
		if merchantID == "" {
			writeError(w, platform.ErrUnauthorized(domain.ErrMissingConsumerIdentity))
			return
		}

		// d. Trust Kong's authentication and bypass DB lookup
		principal := domain.Principal{
			MerchantID: merchantID,
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

func (s *Server) withBulkhead(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.bulkhead.TryAcquire(1) {
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
