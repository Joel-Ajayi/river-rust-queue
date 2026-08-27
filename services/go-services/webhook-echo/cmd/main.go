package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform/pii"
	"go.uber.org/zap"
)

const (
	DefaultLogLevel = "info"
	EnvVarPort      = "HTTP_PORT"
	EnvVarLogLevel  = "LOG_LEVEL"
	RootPath        = "/"
	ContentTypeKey  = "Content-Type"
	ContentTypeJSON = "application/json"
	SignatureHeader = "X-RRQ-Signature"
	OkResponse      = `{"status":"ok"}`
	ShutdownTimeout = 5 * time.Second
)

func main() {
	//  -- Logger --
	logLevel := os.Getenv(EnvVarLogLevel)
	logger, err := platform.NewLogger(logLevel)
	if err != nil {
		panic("failed to initialize logger: " + err.Error())
	}
	defer logger.Sync()

	// -- Port --
	port := os.Getenv(EnvVarPort)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := platform.InitTelemetry(ctx, "webhook-echo"); err != nil {
		logger.Panic("Failed to initialize telemetry", zap.Error(err))
	}

	// -- New Server --
	mux := http.NewServeMux()
	mux.HandleFunc(RootPath, handleWebhook(logger))

	srv := &http.Server{
		Addr:    net.JoinHostPort("", port),
		Handler: mux,
	}

	go func() {
		logger.Info(platform.LogEventServerStarted, zap.String("port", port))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal(platform.LogEventServerFailed, zap.Error(err))
		}
	}()

	<-ctx.Done()

	logger.Info(platform.LogEventShutdownSignalReceived)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error(platform.LogEventShutdownFailed, zap.Error(err))
	} else {
		logger.Info(platform.LogEventServerShutdown)
	}

	_ = platform.ShutdownTelemetry(context.Background())
}

// handleWebhook echoes incoming webhook payloads to logs.
func handleWebhook(logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Error(platform.LogEventRequestBodyReadFailed, zap.Error(err))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		sig := r.Header.Get(SignatureHeader)
		if sig == "" {
			sig = r.Header.Get("X-Webhook-Signature")
		}

		maskedPayload := pii.Mask(body)
		logger.Info(platform.LogEventWebhookReceived,
			zap.String("payload_size", fmt.Sprintf("%d", len(body))),
			zap.ByteString("masked_payload", maskedPayload),
			zap.String("signature", sig),
		)

		w.Header().Set(ContentTypeKey, ContentTypeJSON)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(OkResponse))
	}
}
