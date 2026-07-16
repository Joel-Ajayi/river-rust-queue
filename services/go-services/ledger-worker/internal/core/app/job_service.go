package app

import (
	"context"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/port"
	"go.uber.org/zap"
)

type JobService struct {
	ledger      port.LedgerStore
	xshardStore port.CrossShardStore
	directory   port.MerchantDirectory
}

func NewJobService(
	_ *zap.Logger,
	ledger port.LedgerStore,
	xshardStore port.CrossShardStore,
	directory port.MerchantDirectory,
) *JobService {
	return &JobService{
		ledger:      ledger,
		xshardStore: xshardStore,
		directory:   directory,
	}
}

func (p *JobService) Directory() port.MerchantDirectory {
	return p.directory
}

func (p *JobService) ProcessJob(ctx context.Context, payload *eventsv1.JobRequestedPayload) error {
	if payload.JobType != platform.JobTypeTransfer {
		return domain.ErrInvalidJobType
	}

	transferData := payload.GetTransferData()
	if transferData == nil {
		return domain.ErrMissingTransferData
	}

	// 1. Determine Source Shard
	srcShard, err := p.directory.ShardFor(ctx, payload.MerchantId)
	if err != nil {
		return err
	}

	// 2. Determine Destination Shard
	dstShard, err := p.directory.ShardFor(ctx, transferData.ToMerchantId)
	if err != nil {
		return err
	}

	transfer := domain.Transfer{
		ID:         platform.NewTransferID(),
		JobID:      payload.JobId,
		MerchantID: payload.MerchantId,
		FromWallet: transferData.FromWallet,
		ToWallet:   transferData.ToWallet,
		Amount:     transferData.Amount,
		Currency:   transferData.Currency,
	}

	// 3. Route to the appropriate store
	var errPost error
	if srcShard == dstShard {
		errPost = p.ledger.PostTransfer(ctx, srcShard, transfer)
	} else {
		errPost = p.xshardStore.DebitToClearingAccount(ctx, srcShard, dstShard, payload.JobId, transfer)
	}

	if errPost != nil {
		if domain.IsTerminalError(errPost) {
			if failErr := p.ledger.FailTransfer(ctx, srcShard, transfer, errPost.Error()); failErr != nil {
				return failErr
			}
			return nil // Business failure recorded successfully, job is considered "processed"
		}
		return errPost // Transient error, return to consumer for retry
	}

	return nil
}
