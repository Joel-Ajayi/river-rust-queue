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
	// 1. Validate job type and ensure transfer data payload exists
	if payload.JobType != platform.JobTypeTransfer {
		platform.LoggerWithTrace(ctx, p.logger).Warn(platform.LogEventTerminalBusinessError,
			zap.String(platform.LogFieldJobID, payload.JobId),
			zap.String(platform.LogFieldReason, "invalid job type"),
		)
		return domain.ErrInvalidJobType
	}

	transferData := payload.GetTransferData()
	if transferData == nil {
		platform.LoggerWithTrace(ctx, p.logger).Warn(platform.LogEventTerminalBusinessError,
			zap.String(platform.LogFieldJobID, payload.JobId),
			zap.String(platform.LogFieldReason, "missing transfer data"),
		)
		return domain.ErrMissingTransferData
	}

	// 2. Resolve destination database shard from merchant directory
	dstShard, err := p.directory.ShardFor(ctx, transferData.ToMerchantId)
	if err != nil {
		platform.LoggerWithTrace(ctx, p.logger).Error(platform.LogEventMerchantLookupFailed,
			zap.String(platform.LogFieldMerchantID, transferData.ToMerchantId),
			zap.Error(err),
		)
		return err
	}

	// 3. Resolve source database shard (vault deposits and same-merchant transfers execute locally on dstShard)
	var srcShard string
	if transferData.FromWallet == "" || payload.MerchantId == transferData.ToMerchantId {
		srcShard = dstShard
	} else {
		srcShard, err = p.directory.ShardFor(ctx, payload.MerchantId)
		if err != nil {
			// If source merchant has no dedicated shard (e.g. system vault entity), route locally on dstShard
			srcShard = dstShard
		}
	}

	// 4. Construct deterministic transfer entity
	transfer := domain.Transfer{
		ID:         platform.NewDeterministicTransferID(payload.JobId),
		JobID:      payload.JobId,
		MerchantID: payload.MerchantId,
		FromWallet: transferData.FromWallet,
		ToWallet:   transferData.ToWallet,
		Amount:     transferData.Amount,
		Currency:   transferData.Currency,
	}

	// 5. Route double-entry transaction (same-shard direct transfer vs cross-shard clearing debit)
	var errPost error
	if srcShard == dstShard {
		errPost = p.ledger.PostTransfer(ctx, srcShard, transfer)
	} else {
		errPost = p.xshardStore.DebitToClearingAccount(ctx, srcShard, dstShard, payload.JobId, transfer)
	}

	// 6. Handle posting outcome: record business decline on terminal error, or return for retry
	if errPost != nil {
		if domain.IsTerminalError(errPost) {
			platform.LoggerWithTrace(ctx, p.logger).Warn(platform.LogEventTransferDeclined,
				zap.String(platform.LogFieldJobID, payload.JobId),
				zap.String(platform.LogFieldMerchantID, payload.MerchantId),
				zap.String(platform.LogFieldTransferID, transfer.ID),
				zap.String(platform.LogFieldReason, errPost.Error()),
			)
			if failErr := p.ledger.FailTransfer(ctx, srcShard, transfer, errPost.Error()); failErr != nil {
				platform.LoggerWithTrace(ctx, p.logger).Error(platform.LogEventTransferFailed, zap.Error(failErr))
				return failErr
			}
			shardType := platform.ShardTypeSame
			if srcShard != dstShard {
				shardType = platform.ShardTypeCross
			}
			platform.RecordBusinessDecline(ctx, reasonFromError(errPost))
			platform.RecordBusinessTransfer(ctx, platform.TransferMetricFailed, shardType)
			return nil
		}
		platform.LoggerWithTrace(ctx, p.logger).Error(platform.LogEventTransferFailed,
			zap.String(platform.LogFieldJobID, payload.JobId),
			zap.String(platform.LogFieldShardID, srcShard),
			zap.Error(errPost),
		)
		return errPost
	}

	// 7. Emit canonical event and business metrics upon successful posting
	if srcShard == dstShard {
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
