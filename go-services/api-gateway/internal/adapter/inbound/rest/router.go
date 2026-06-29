package rest

import "net/http"

// registerRoutes wires all HTTP routes onto the mux.
func (s *Server) registerRoutes(mux *http.ServeMux) {
	// Health (no auth).
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)

	// Token exchange (API key -> JWT).
	mux.HandleFunc("POST /v1/auth/token", s.handleAuthToken)

	// Protected routes (require a valid JWT).
	mux.Handle("POST /v1/transfers", s.requireAuth(http.HandlerFunc(s.handleCreateTransfer)))
}
