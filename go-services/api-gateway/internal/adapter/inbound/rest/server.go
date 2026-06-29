package rest

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/port"
	"go.uber.org/zap"
)

// ReadinessFunc reports whether the service's dependencies are reachable.
// The composition root supplies one that pings Postgres and Redis, keeping this
// adapter free of any direct infrastructure knowledge.
type ReadinessFunc func(ctx context.Context) error

// Server is the HTTP front door. It depends only on the inbound ports and a few
// transport concerns (JWT signing key, readiness probe) — never on the database.
type Server struct {
	httpSrv   *http.Server
	submitter port.TransferSubmitter
	auth      port.Authenticator
	jwtKey    []byte
	ready     ReadinessFunc
	log       *zap.Logger
}

// NewServer builds the HTTP server and wires routes.
func NewServer(
	addr string,
	submitter port.TransferSubmitter,
	auth port.Authenticator,
	jwtKey []byte,
	ready ReadinessFunc,
	log *zap.Logger,
) *Server {
	s := &Server{
		submitter: submitter,
		auth:      auth,
		jwtKey:    jwtKey,
		ready:     ready,
		log:       log,
	}

	mux := http.NewServeMux()
	s.registerRoutes(mux)

	s.httpSrv = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return s
}

// Start begins serving and blocks until the server stops.
func (s *Server) Start() error {
	s.log.Info("starting api-gateway", zap.String("addr", s.httpSrv.Addr))
	return s.httpSrv.ListenAndServe()
}

// Shutdown gracefully drains in-flight requests.
func (s *Server) Shutdown(ctx context.Context) error {
	s.log.Info("shutting down api-gateway")
	return s.httpSrv.Shutdown(ctx)
}

// Addr formats a :port listen address.
func Addr(port int) string { return fmt.Sprintf(":%d", port) }
