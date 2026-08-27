package testutil

import (
	"context"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

func init() {
	_ = platform.InitTelemetry(context.Background(), "test")
	_ = platform.InitMetrics()
	_ = platform.InitBusinessMetrics()
}
