package rest

import (
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.uber.org/zap"
)

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{platform.HealthStatusKey: platform.HealthStatusOK})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	// While draining (SIGTERM received), return 503 so Kubernetes removes the pod
	// from Service endpoints and stops routing new traffic to it.
	if s.IsDraining() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			platform.HealthStatusKey: platform.HealthStatusUnavailable,
		})
		return
	}
	if err := s.ready(r.Context()); err != nil {
		s.log.Error(platform.LogEventReadinessCheckFailed, zap.Error(err))
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			platform.HealthStatusKey: platform.HealthStatusUnavailable,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{platform.HealthStatusKey: platform.HealthStatusReady})
}
