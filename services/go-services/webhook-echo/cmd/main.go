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
	SignatureHeader = "X-Webhook-Signature"
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

	if err := platform.InitTelemetry(platform.ServiceNameWebhookWorker, "http://agent-collector.observability.svc.cluster.local:4317"); err != nil {
		logger.Panic(platform.LogEventTelemetryInitFailed, zap.Error(err))
	}

	// -- Port --
	port := os.Getenv(EnvVarPort)

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

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info(platform.LogEventShutdownSignalReceived)
	ctx, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
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

		maskedPayload := pii.Mask(body)
		logger.Info(platform.LogEventWebhookReceived,
			zap.String("payload_size", fmt.Sprintf("%d", len(body))),
			zap.ByteString("masked_payload", maskedPayload),
		)

		w.Header().Set(ContentTypeKey, ContentTypeJSON)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(OkResponse))
	}
}
