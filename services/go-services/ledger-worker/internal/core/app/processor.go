package app

import (
	"context"
	"fmt"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/ports"
	"go.uber.org/zap"
)

type Processor struct {
	logger      *zap.Logger
	ledger      ports.LedgerStore
	xshardStore ports.CrossShardStore
	directory   ports.MerchantDirectory
}

func NewProcessor(
	logger *zap.Logger,
	ledger ports.LedgerStore,
	xshardStore ports.CrossShardStore,
	directory ports.MerchantDirectory,
) *Processor {
	return &Processor{
		logger:      logger,
		ledger:      ledger,
		xshardStore: xshardStore,
		directory:   directory,
	}
}

func (p *Processor) ProcessJob(ctx context.Context, payload *eventsv1.JobRequestedPayload) error {
	p.logger.Info("Processing JobRequested", zap.String("job_id", payload.JobId))

	if payload.JobType != string(platform.AggregateTypeTransfer) {
		return fmt.Errorf("unsupported job type: %s", payload.JobType)
	}

	transferData := payload.GetTransferData()
	if transferData == nil {
		return fmt.Errorf("missing transfer data")
	}

	// 1. Determine Source Shard
	srcShard, err := p.directory.ShardFor(ctx, payload.MerchantId)
	if err != nil {
		return fmt.Errorf("failed to get src shard: %w", err)
	}

	// 2. Determine Destination Shard
	dstShard, err := p.directory.ShardFor(ctx, transferData.ToMerchantId)
	if err != nil {
		return fmt.Errorf("failed to get dst shard: %w", err)
	}

	transfer := domain.Transfer{
		ID:         payload.JobId,
		JobID:      payload.JobId,
		MerchantID: payload.MerchantId,
		FromWallet: transferData.FromWallet,
		ToWallet:   transferData.ToWallet,
		Amount:     transferData.Amount,
		Currency:   transferData.Currency,
	}

	// 3. Route to the appropriate store
	if srcShard == dstShard {
		p.logger.Info("Routing to local LedgerStore", zap.String("shard", srcShard))
		return p.ledger.PostTransfer(ctx, srcShard, transfer)
	}

	p.logger.Info("Routing to CrossShardStore", zap.String("src_shard", srcShard), zap.String("dst_shard", dstShard))
	return p.xshardStore.DebitToClearingAccount(ctx, srcShard, dstShard, payload.JobId, transfer)
}
