package main

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/adapter/outbound/kafka"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/outbox-relay/internal/core/app"
	"go.uber.org/zap"
)

// monitorKafkaBuffer samples in-flight publish bytes and throttles/pauses relay polling.
// AIMD thresholds and buffer bounds come from the capacity engine (OUTBOX_RELAY_AIMD_*, RELAY_BUFFER_*).
// Per-shard monitoring (Issue 10): the global staging budget applies across all
// shards, but each shard's poll interval is throttled independently based on
// its own in-flight bytes relative to the shared staging budget.
func monitorKafkaBuffer(ctx context.Context, publisher *kafka.EventPublisher, relays []*app.RelayService, cfg *platform.Config, log *zap.Logger) {
	ticker := time.NewTicker(time.Duration(cfg.Capacity.RelayBufferSampleIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	throttleFrac := cfg.Capacity.AIMDThrottleFrac
	pauseFrac := cfg.Capacity.AIMDPauseFrac
	resumeFrac := cfg.Capacity.AIMDResumeFrac
	stagingBytes := float64(cfg.Capacity.RelayStagingKB * 1024)

	// Per-shard throttle state. Keyed by shardID (matches RelayService.shardID).
	type shmState struct {
		throttleLevel int
		paused        bool
	}
	states := make(map[string]*shmState)
	for _, r := range relays {
		states[r.ShardID()] = &shmState{}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Record global buffer fill for aggregate observability.
			totalRatio := float64(publisher.InFlightBytes()) / stagingBytes
			platform.RecordKafkaProducerBufferFill(ctx, "", totalRatio)

			for _, r := range relays {
				sid := r.ShardID()
				st := states[sid]
				if st == nil {
					continue
				}

				// Per-shard in-flight ratio relative to the shared staging budget.
				shardRatio := float64(publisher.InFlightBytesForShard(sid)) / stagingBytes

				switch {
				case shardRatio >= pauseFrac:
					// Buffer full: pause polling until it drains below resume threshold.
					if !st.paused {
						st.paused = true
						st.throttleLevel = 0
						r.SetPollInterval(time.Hour)
						log.Warn(platform.LogEventKafkaBufferFull,
							zap.String(platform.LogFieldShardID, sid),
						)
					}
				case st.paused && shardRatio < resumeFrac:
					// Buffer drained: resume normal polling.
					st.paused = false
					r.SetPollInterval(time.Duration(cfg.Capacity.RelayPoolIntervalMs) * time.Millisecond)
					log.Info(platform.LogEventKafkaBufferResumed,
						zap.String(platform.LogFieldShardID, sid),
					)
				case !st.paused && shardRatio > throttleFrac:
					// Throttle: double the interval up to the max cap.
					if st.throttleLevel < cfg.Capacity.RelayBufferMaxThrottleLevel {
						st.throttleLevel++
						interval := time.Duration(cfg.Capacity.RelayPoolIntervalMs) * time.Millisecond << st.throttleLevel
						if interval > time.Duration(cfg.Capacity.RelayBufferMaxPollIntervalMs)*time.Millisecond {
							interval = time.Duration(cfg.Capacity.RelayBufferMaxPollIntervalMs) * time.Millisecond
						}
						r.SetPollInterval(interval)
					}
				case !st.paused && st.throttleLevel > 0 && shardRatio <= throttleFrac:
					// Recovered: reset to normal polling.
					st.throttleLevel = 0
					r.SetPollInterval(time.Duration(cfg.Capacity.RelayPoolIntervalMs) * time.Millisecond)
				}
			}
		}
	}
}
