package rest

import (
	"net/http"

	"go.uber.org/zap"
)

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if err := s.ready(r.Context()); err != nil {
		s.log.Error("Readiness check failed", zap.Error(err))
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "Service Unavailable",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
