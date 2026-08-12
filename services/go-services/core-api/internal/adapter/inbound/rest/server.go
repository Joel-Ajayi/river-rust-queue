package rest

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

// ReadinessFunc provides the function to check liveness/readiness
type ReadinessFunc func(ctx context.Context) error
type Server struct {
	httpSrv        *http.Server
	transfers      port.TransferSubmitter
	jobs           port.JobReader
	merchants      port.MerchantUseCase
	wallets        port.WalletUseCase
	admin          port.AdminUseCase
	jwtKeys        map[string]ed25519.PrivateKey
	jwtActiveKeyID string
	ready          ReadinessFunc
	log            *zap.Logger
	bulkhead       *semaphore.Weighted // Bulkhead isolation (Netflix Pattern).
	draining       atomic.Bool         // Set on SIGTERM; readiness returns 503 while draining.
	retryConfig    platform.RetryConfig
	attemptTimeout time.Duration
	jwtExpiration  time.Duration
}

func NewServer(
	cfg *platform.Config,
	transfers port.TransferSubmitter,
	jobs port.JobReader,
	merchants port.MerchantUseCase,
	wallets port.WalletUseCase,
	admin port.AdminUseCase,
	ready ReadinessFunc,
	log *zap.Logger,
) *Server {
	s := &Server{
		transfers:      transfers,
		jobs:           jobs,
		merchants:      merchants,
		wallets:        wallets,
		admin:          admin,
		jwtKeys:        cfg.JWTSigningKeys,
		jwtActiveKeyID: cfg.JWTActiveKeyID,
		ready:          ready,
		log:            log.Named(platform.LogComponentRESTServer),
		bulkhead:       semaphore.NewWeighted(int64(cfg.Capacity.WorkerPoolSize)),
		retryConfig: platform.RetryConfig{
			MaxRetries: cfg.Capacity.MaxRetries,
			BaseDelay:  time.Duration(cfg.Capacity.BackoffBaseMs) * time.Millisecond,
			MaxDelay:   time.Duration(cfg.Capacity.BackoffCapMs) * time.Millisecond,
		},
		attemptTimeout: time.Duration(cfg.Capacity.RequestTimeoutMs) * time.Millisecond,
		jwtExpiration:  time.Duration(cfg.Capacity.JWTAccessHrs) * time.Hour,
	}

	mux := http.NewServeMux()
	s.RegisterRoutes(mux)

	timeoutMsg := `{"error":"gateway_timeout"}`
	timeoutHandler := http.TimeoutHandler(mux, time.Duration(cfg.Capacity.ServerTimeoutMs)*time.Millisecond, timeoutMsg)

	s.httpSrv = &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.HTTPPort),
		Handler:           timeoutHandler,
		ReadTimeout:       time.Duration(cfg.Capacity.RequestTimeoutMs) * time.Millisecond,
		ReadHeaderTimeout: time.Duration(cfg.Capacity.RequestTimeoutMs) * time.Millisecond,
		WriteTimeout:      time.Duration(cfg.Capacity.ServerTimeoutMs+500) * time.Millisecond,
		IdleTimeout:       time.Duration(cfg.Capacity.ServerIdleTimeoutMs) * time.Millisecond,
	}

	return s
}

// ServeHTTP delegates to the underlying HTTP server's handler. Useful for testing.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.httpSrv.Handler.ServeHTTP(w, r)
}

// Start begins serving and blocks until the server stops.
func (s *Server) Start() error {
	s.log.Info(platform.LogEventServerStarted, zap.String(platform.LogFieldAddr, s.httpSrv.Addr))
	return s.httpSrv.ListenAndServe()
}

// Shutdown gracefully drains in-flight requests.
func (s *Server) Shutdown(ctx context.Context) error {
	s.log.Info(platform.LogEventServerShutdown)
	return s.httpSrv.Shutdown(ctx)
}

// BeginDrain marks the server as draining so the readiness probe returns 503,
// removing the pod from Service endpoints before SIGTERM-driven shutdown.
func (s *Server) BeginDrain() {
	s.draining.Store(true)
}

// IsDraining reports whether the server is in the draining state.
func (s *Server) IsDraining() bool {
	return s.draining.Load()
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	// Health (no auth).
	mux.HandleFunc("GET "+platform.APIHealthPath, s.handleHealth)
	mux.HandleFunc("GET "+platform.APIReadyPath, s.handleReady)
	mux.HandleFunc("GET /.well-known/jwks.json", s.handleJWKS)

	mux.Handle("POST "+platform.APIAuthTokenPath, s.withLogging(s.withBulkhead(http.HandlerFunc(s.handleAuthToken))))

	mux.Handle("POST "+platform.APITransfersPath, s.withLogging(s.withBulkhead(s.extractMerchant(http.HandlerFunc(s.handleCreateTransfer)))))

	mux.Handle("GET "+platform.APIJobPathPrefix+"{id}", s.withLogging(s.withBulkhead(s.extractMerchant(http.HandlerFunc(s.handleGetJob)))))
	mux.Handle("GET "+platform.APIBalancesPath, s.withLogging(s.withBulkhead(s.extractMerchant(http.HandlerFunc(s.handleGetBalance)))))

	mux.Handle("POST "+platform.APIMerchantsPath, s.withLogging(s.withBulkhead(http.HandlerFunc(s.handleCreateMerchant))))
	mux.Handle("POST "+platform.APIAdminDLQReplayPath, s.withLogging(s.withBulkhead(s.extractMerchant(http.HandlerFunc(s.handleAdminDLQReplay)))))
	mux.Handle("POST "+platform.APIWalletsPath, s.withLogging(s.withBulkhead(s.extractMerchant(http.HandlerFunc(s.handleCreateWallet)))))
}
