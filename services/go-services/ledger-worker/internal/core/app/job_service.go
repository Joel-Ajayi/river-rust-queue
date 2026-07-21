package app

import (
	"context"
	"errors"

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
	logger      *zap.Logger
}

func NewJobService(
	logger *zap.Logger,
	ledger port.LedgerStore,
	xshardStore port.CrossShardStore,
	directory port.MerchantDirectory,
) *JobService {
	return &JobService{
		ledger:      ledger,
		xshardStore: xshardStore,
		directory:   directory,
		logger:      logger,
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
			// Business metrics: decline + failed transfer
			shardType := platform.ShardTypeSame
			if srcShard != dstShard {
				shardType = platform.ShardTypeCross
			}
			platform.RecordBusinessDecline(ctx, reasonFromError(errPost))
			platform.RecordBusinessTransfer(ctx, platform.TransferMetricFailed, shardType)
			return nil // Business failure recorded successfully, job is considered "processed"
		}
		return errPost // Transient error, return to consumer for retry
	}

	// Canonical log + business metrics: different events for same-shard vs cross-shard
	if srcShard == dstShard {
		// Same-shard: transfer fully completed in one transaction
		platform.RecordBusinessGTV(ctx, transferData.Amount, transferData.Currency)
		platform.RecordBusinessTransfer(ctx, platform.TransferMetricSuccess, platform.ShardTypeSame)

		platform.LogCanonicalEvent(ctx, p.logger, platform.ServiceNameLedgerWorker, platform.CanonicalLogLine{
			Event:      platform.EventTransferCompleted,
			Status:     platform.StatusSuccess,
			MerchantID: payload.MerchantId,
			JobID:      payload.JobId,
			WalletID:   transferData.FromWallet,
			TransferID: transfer.ID,
			Amount:     transferData.Amount,
			Currency:   transferData.Currency,
		})
	} else {
		// Cross-shard: saga initiated (debit posted, credit pending on destination shard)
		platform.RecordBusinessSagaInitiated(ctx)
		platform.RecordBusinessTransfer(ctx, platform.TransferMetricPending, platform.ShardTypeCross)

		platform.LogCanonicalEvent(ctx, p.logger, platform.ServiceNameLedgerWorker, platform.CanonicalLogLine{
			Event:      platform.EventSagaInitiated,
			Status:     platform.StatusSuccess,
			MerchantID: payload.MerchantId,
			JobID:      payload.JobId,
			WalletID:   transferData.FromWallet,
			TransferID: transfer.ID,
			Amount:     transferData.Amount,
			Currency:   transferData.Currency,
		})
	}

	return nil
}

// reasonFromError maps a terminal error to a decline reason string for business metrics.
func reasonFromError(err error) string {
	switch {
	case errors.Is(err, domain.ErrInsufficientBalance):
		return platform.DeclineReasonInsufficientBalance
	case errors.Is(err, domain.ErrWalletFrozen):
		return platform.DeclineReasonWalletFrozen
	case errors.Is(err, domain.ErrWalletClosed):
		return platform.DeclineReasonWalletClosed
	case errors.Is(err, domain.ErrWalletNotFound):
		return platform.DeclineReasonWalletNotFound
	case errors.Is(err, domain.ErrCurrencyMismatch):
		return platform.DeclineReasonCurrencyMismatch
	case errors.Is(err, domain.ErrSelfTransfer):
		return platform.DeclineReasonSelfTransfer
	case errors.Is(err, domain.ErrMerchantInactive):
		return platform.DeclineReasonMerchantInactive
	default:
		return platform.DeclineReasonUnknown
	}
}
