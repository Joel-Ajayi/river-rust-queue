package main

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/recon-worker/internal/adapter/outbound/postgres"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/recon-worker/internal/core/app"
	"go.uber.org/zap"
)

const (
	DefaultLogLevel         = "info"
	ReconTimeout            = 10 * time.Minute
	DefaultReconWindowHours = 24
)

func main() {
	cfg, err := platform.LoadConfig()
	if err != nil {
		panic("config: " + err.Error())
	}

	logger, err := platform.NewLogger(cfg.LogLevel)
	if err != nil {
		panic("logger: " + err.Error())
	}
	defer logger.Sync()

	logger.Info(platform.LogEventServerStarted)

	if err := platform.InitTelemetry(platform.ServiceNameReconWorker, cfg.OtelExporterEndpoint); err != nil {
		logger.Panic(platform.LogEventTelemetryInitFailed, zap.Error(err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), ReconTimeout)
	defer cancel()

	pools, err := platform.NewShardPools(ctx, cfg, logger)
	if err != nil {
		logger.Panic(platform.LogEventPostgresInitFailed, zap.Error(err))
	}
	defer pools.Close()

	repo := postgres.NewReconRepository(pools)
	runner := app.NewRunner(logger, repo, cfg.HTTPPort, pools) // HTTPPort reused for parallelism limit

	now := time.Now()
	windowEnd := now
	windowStart := now.Add(-DefaultReconWindowHours * time.Hour)

	report, err := runner.Run(ctx, windowStart, windowEnd)
	if err != nil {
		logger.Fatal(platform.LogEventReconRunFailed, zap.Error(err))
	}

	logger.Info(platform.LogEventReconCompletedSuccess,
		zap.String("run_id", report.RunID),
		zap.Int64("global_sum", report.GlobalSum),
		zap.Int("wallets_checked", report.WalletsChecked),
		zap.Int("discrepancies_count", len(report.Discrepancies)),
		zap.Float64("duration_seconds", report.DurationSeconds),
	)

	if len(report.Discrepancies) > 0 {
		logger.Warn(platform.LogEventReconDiscrepanciesFound, zap.Int("count", len(report.Discrepancies)))
	}
}
