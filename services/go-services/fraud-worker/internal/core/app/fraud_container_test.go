//go:build integration

package app

import (
	"context"
	"testing"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/adapter/outbound/redis"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/fraud-worker/internal/core/domain"
	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/testutil"
	"go.uber.org/zap"
)

func TestFraud_Container_ProcessJob_WithRealRedis(t *testing.T) {
	rdb, _ := testutil.StartRedis(t)
	redisStore := redis.NewRedisStore(rdb)

	logger, _ := zap.NewDevelopment()
	rules := []domain.VelocityRule{
		{
			Name:      "redis_container_velocity_rule",
			WindowMs:  60000,
			Threshold: 3,
			Reason:    "high transfer velocity",
		},
	}

	svc := NewFraudService(logger, &mockWalletRepository{}, redisStore, &mockMerchantDirectory{}, rules)

	walletID := "merch-redis.wallet1"
	payload := &eventsv1.JobRequestedPayload{
		JobId:      platform.NewJobID(),
		JobType:    platform.JobTypeTransfer,
		MerchantId: "merch-redis",
		Data: &eventsv1.JobRequestedPayload_TransferData{
			TransferData: &eventsv1.TransferData{
				FromWallet: walletID,
				ToWallet:   "merch-redis.wallet2",
				Amount:     500,
				Currency:   "USD",
			},
		},
	}

	// 1st & 2nd events under threshold
	err := svc.ProcessJob(context.Background(), payload, platform.NewEventID(), time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("unexpected error on 1st event: %v", err)
	}

	err = svc.ProcessJob(context.Background(), payload, platform.NewEventID(), time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("unexpected error on 2nd event: %v", err)
	}
}
