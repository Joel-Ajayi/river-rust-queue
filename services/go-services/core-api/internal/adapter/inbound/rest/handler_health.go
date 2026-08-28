package rest

import (
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.uber.org/zap"
)

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	// 1. Return 200 OK for Kubernetes liveness probe
	writeJSON(w, http.StatusOK, map[string]string{platform.HealthStatusKey: platform.HealthStatusOK})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	// 1. If server is draining on SIGTERM, fail readiness with 503 to evict pod from ingress
	if s.IsDraining() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			platform.HealthStatusKey: platform.HealthStatusUnavailable,
		})
		return
	}

	// 2. Ping database pools and verify circuit breakers are not open
	if err := s.ready(r.Context()); err != nil {
		platform.LoggerWithTrace(r.Context(), s.log).Error(platform.LogEventReadinessCheckFailed, zap.Error(err))
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			platform.HealthStatusKey: platform.HealthStatusUnavailable,
		})
		return
	}

	// 3. Return 200 OK ready status
	writeJSON(w, http.StatusOK, map[string]string{platform.HealthStatusKey: platform.HealthStatusReady})
}
