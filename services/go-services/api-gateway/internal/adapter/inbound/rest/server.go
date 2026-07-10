package rest

import (
	"context"
	"net/http"
	"strconv"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)



// ReadinessFunc provides the function to check liveness/readiness
type ReadinessFunc func(ctx context.Context) error
type Server struct {
	httpSrv   *http.Server
	auth      port.Authenticator
	transfers port.TransferSubmitter
	jobs      port.JobReader
	jwtKey    []byte
	ready     ReadinessFunc
	log       *zap.Logger
	bulkhead  *semaphore.Weighted // Bulkhead isolation (Netflix Pattern)
	limiter   *rate.Limiter       // Rate limiting against thundering herds
}

func NewServer(
	httpPort int,
	jwtKey []byte,
	auth port.Authenticator,
	transfers port.TransferSubmitter,
	jobs port.JobReader,
	ready ReadinessFunc,
	log *zap.Logger,
) *Server {
	s := &Server{
		auth:      auth,
		transfers: transfers,
		jobs:      jobs,
		jwtKey:    jwtKey,
		ready:     ready,
		log:       log,
		bulkhead:  semaphore.NewWeighted(domain.BulkheadLimit),
		limiter:   rate.NewLimiter(rate.Limit(domain.RateLimitReqPerSec), domain.RateLimitBurst),
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
	s.log.Info("starting api-gateway", zap.String("addr", s.httpSrv.Addr))
	return s.httpSrv.ListenAndServe()
}

// Shutdown gracefully drains in-flight requests.
func (s *Server) Shutdown(ctx context.Context) error {
	s.log.Info("shutting down api-gateway")
	return s.httpSrv.Shutdown(ctx)
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	// Health (no auth).
	mux.HandleFunc("GET "+platform.APIHealthPath, s.handleHealth)
	mux.HandleFunc("GET "+platform.APIReadyPath, s.handleReady)

	// Token exchange (API key -> JWT).
	mux.Handle("POST "+platform.APIAuthTokenPath, s.withRateLimit(s.withBulkhead(http.HandlerFunc(s.handleAuthToken))))

	// Protected routes (require a valid JWT).
	// We apply RateLimit -> Bulkhead -> Auth
	mux.Handle("POST "+platform.APITransfersPath, s.withRateLimit(s.withBulkhead(s.requireAuth(http.HandlerFunc(s.handleCreateTransfer)))))
	mux.Handle("GET "+platform.APIJobPathPrefix+"{id}", s.withRateLimit(s.withBulkhead(s.requireAuth(http.HandlerFunc(s.handleGetJob)))))
	mux.Handle("GET "+platform.APIBalancesPath, s.withRateLimit(s.withBulkhead(s.requireAuth(http.HandlerFunc(s.handleGetBalance)))))
}


