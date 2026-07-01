package rest

import (
	"context"
	"net/http"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.uber.org/zap"
)

type HeaderKey string
type ContextKey string
type HeaderVal string

const (
	HeaderIdempotencyKey HeaderKey  = "Idempotency-Key"
	ContextPrincipal     ContextKey = "principal"
	HeaderAuthorization  HeaderKey  = "Authorization"

	HeaderValBearer HeaderVal = "Bearer"
	ApplicationJSON HeaderVal = "application/json"

	ContentType HeaderKey = "Content-Type"

	PORT string = ":8080"
)

// ReadinessFunc provides the function to check liveness/readiness
type ReadinessFunc func(ctx context.Context) error
type Server struct {
	httpSrv   *http.Server
	svc       port.APIGatewayService
	directory port.MerchantDirectory
	jwtKey    []byte
	ready     ReadinessFunc
	log       *zap.Logger
}

func NewServer(
	svc port.APIGatewayService,
	directory port.MerchantDirectory,
	jwtKey string,
	ready ReadinessFunc,
	log *zap.Logger,
) *Server {
	s := &Server{
		svc:       svc,
		directory: directory,
		jwtKey:    []byte(jwtKey),
		ready:     ready,
		log:       log,
	}

	mux := http.NewServeMux()
	s.RegisterRoutes(mux)

	s.httpSrv = &http.Server{
		Addr:              PORT,
		Handler:           mux,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       15 * time.Second,
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
	mux.HandleFunc("POST "+platform.APIAuthTokenPath, s.handleAuthToken)

	// Protected routes (require a valid JWT).
	mux.Handle("POST "+platform.APITransfersPath, s.requireAuth(http.HandlerFunc(s.handleCreateTransfer)))
	mux.Handle("GET "+platform.APIJobPathPrefix+"{id}", s.requireAuth(http.HandlerFunc(s.handleGetJob)))
}
