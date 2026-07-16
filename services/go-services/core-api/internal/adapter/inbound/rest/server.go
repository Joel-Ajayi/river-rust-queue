package rest

import (
	"context"
	"crypto/rsa"
	"net/http"
	"strconv"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

// ReadinessFunc provides the function to check liveness/readiness
type ReadinessFunc func(ctx context.Context) error
type Server struct {
	httpSrv        *http.Server
	auth           port.Authenticator
	transfers      port.TransferSubmitter
	jobs           port.JobReader
	jwtKeys        map[string]*rsa.PrivateKey
	jwtActiveKeyID string
	ready          ReadinessFunc
	log            *zap.Logger
	bulkhead       *semaphore.Weighted // Bulkhead isolation (Netflix Pattern)
}

func NewServer(
	httpPort int,
	jwtKeys map[string]*rsa.PrivateKey,
	jwtActiveKeyID string,
	auth port.Authenticator,
	transfers port.TransferSubmitter,
	jobs port.JobReader,
	ready ReadinessFunc,
	log *zap.Logger,
) *Server {
	s := &Server{
		auth:           auth,
		transfers:      transfers,
		jobs:           jobs,
		jwtKeys:        jwtKeys,
		jwtActiveKeyID: jwtActiveKeyID,
		ready:          ready,
		log:            log.Named(platform.LogComponentRESTServer),
		bulkhead:       semaphore.NewWeighted(domain.BulkheadLimit),
	}

	mux := http.NewServeMux()
	s.RegisterRoutes(mux)

	s.httpSrv = &http.Server{
		Addr:              ":" + strconv.Itoa(httpPort),
		Handler:           mux,
		ReadTimeout:       ServerReadTimeout,
		ReadHeaderTimeout: ServerReadTimeout,
		WriteTimeout:      ServerWriteTimeout,
		IdleTimeout:       ServerIdleTimeout,
	}

	return s
}

// ServeHTTP delegates to the underlying HTTP server's handler. Useful for testing.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.httpSrv.Handler.ServeHTTP(w, r)
}

// Start begins serving and blocks until the server stops.
func (s *Server) Start() error {
	s.log.Info("Starting API Gateway server", zap.String(platform.LogFieldEvent, platform.LogEventServerStarted), zap.String(platform.LogFieldAddr, s.httpSrv.Addr))
	return s.httpSrv.ListenAndServe()
}

// Shutdown gracefully drains in-flight requests.
func (s *Server) Shutdown(ctx context.Context) error {
	s.log.Info("Shutting down API Gateway server", zap.String(platform.LogFieldEvent, platform.LogEventServerShutdown))
	return s.httpSrv.Shutdown(ctx)
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	// Health (no auth).
	mux.HandleFunc("GET "+platform.APIHealthPath, s.handleHealth)
	mux.HandleFunc("GET "+platform.APIReadyPath, s.handleReady)

	mux.Handle("POST "+platform.APIAuthTokenPath, s.withBulkhead(http.HandlerFunc(s.handleAuthToken)))

	mux.Handle("POST "+platform.APITransfersPath, s.withBulkhead(s.requireAuth(http.HandlerFunc(s.handleCreateTransfer))))
	mux.Handle("GET "+platform.APIJobPathPrefix+"{id}", s.withBulkhead(s.requireAuth(http.HandlerFunc(s.handleGetJob))))
	mux.Handle("GET "+platform.APIBalancesPath, s.withBulkhead(s.requireAuth(http.HandlerFunc(s.handleGetBalance))))
}
