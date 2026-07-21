package observability

import (
	"context"
	"fmt"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Trace wrapper for TransferSubmitter
type transferSubmitterTraces struct {
	next port.TransferSubmitter
}

func NewTransferSubmitterTraces(next port.TransferSubmitter) port.TransferSubmitter {
	return &transferSubmitterTraces{next: next}
}

func (t *transferSubmitterTraces) Submit(ctx context.Context, tr domain.Transfer, idempKey string) (domain.SubmitResult, error) {
	ctx, span := platform.GetTracer().Start(ctx, "api.transfer_submitter.submit",
		trace.WithAttributes(
			attribute.String(platform.MetricLabelMerchantID, tr.MerchantID),
			attribute.Int64("amount", tr.Amount),
			attribute.String("currency", tr.Currency),
			attribute.String("idempotency_key", idempKey)))
	defer span.End()

	result, err := t.next.Submit(ctx, tr, idempKey)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(
			attribute.String(platform.MetricLabelErrorType, fmt.Sprintf("%T", err)),
			attribute.String(platform.MetricLabelErrorMessage, err.Error()),
		)
		return domain.SubmitResult{}, err
	}

	span.SetAttributes(attribute.String(platform.MetricLabelJobID, result.Job.ID))
	return result, nil
}

func (t *transferSubmitterTraces) GetBalance(ctx context.Context, walletID, merchantID string) (int64, string, error) {
	ctx, span := platform.GetTracer().Start(ctx, "api.transfer_submitter.get_balance",
		trace.WithAttributes(
			attribute.String(platform.MetricLabelWalletID, walletID),
			attribute.String(platform.MetricLabelMerchantID, merchantID)))
	defer span.End()

	bal, curr, err := t.next.GetBalance(ctx, walletID, merchantID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return bal, curr, err
}

// Trace wrapper for JobStore
type jobStoreTraces struct {
	next port.JobStore
}

func NewJobStoreTraces(next port.JobStore) port.JobStore {
	return &jobStoreTraces{next: next}
}

func (j *jobStoreTraces) ClaimAndRecord(ctx context.Context, shardID string, job domain.Job, tr domain.Transfer, idempKey string) (domain.SubmitResult, error) {
	ctx, span := platform.GetTracer().Start(ctx, "api.job_store.claim_and_record",
		trace.WithAttributes(
			attribute.String(platform.MetricLabelShard, shardID),
			attribute.String(platform.MetricLabelJobID, job.ID),
			attribute.String(platform.MetricLabelMerchantID, job.MerchantID),
			attribute.String("idempotency_key", idempKey)))
	defer span.End()

	result, err := j.next.ClaimAndRecord(ctx, shardID, job, tr, idempKey)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(
			attribute.String(platform.MetricLabelErrorType, fmt.Sprintf("%T", err)),
			attribute.String(platform.MetricLabelErrorMessage, err.Error()),
		)
		return domain.SubmitResult{}, err
	}

	span.SetAttributes(attribute.String(platform.MetricLabelJobID, result.Job.ID))
	return result, nil
}

func (j *jobStoreTraces) GetJob(ctx context.Context, shardID, jobID string) (domain.Job, error) {
	ctx, span := platform.GetTracer().Start(ctx, "api.job_store.get_job",
		trace.WithAttributes(
			attribute.String(platform.MetricLabelShard, shardID),
			attribute.String(platform.MetricLabelJobID, jobID)))
	defer span.End()

	job, err := j.next.GetJob(ctx, shardID, jobID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return job, err
}

// Trace wrapper for MerchantDirectory
type merchantDirectoryTraces struct {
	next port.MerchantDirectory
}

func NewMerchantDirectoryTraces(next port.MerchantDirectory) port.MerchantDirectory {
	return &merchantDirectoryTraces{next: next}
}

func (m *merchantDirectoryTraces) ShardFor(ctx context.Context, merchantID string) (string, error) {
	ctx, span := platform.GetTracer().Start(ctx, "api.merchant_directory.shard_for",
		trace.WithAttributes(
			attribute.String(platform.MetricLabelMerchantID, merchantID)))
	defer span.End()

	shardID, err := m.next.ShardFor(ctx, merchantID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(
			attribute.String(platform.MetricLabelErrorType, fmt.Sprintf("%T", err)),
			attribute.String(platform.MetricLabelErrorMessage, err.Error()),
		)
		return "", err
	}
	return shardID, nil
}

// Trace wrapper for WalletDirectory
type walletDirectoryTraces struct {
	next port.WalletDirectory
}

func NewWalletDirectoryTraces(next port.WalletDirectory) port.WalletDirectory {
	return &walletDirectoryTraces{next: next}
}

func (w *walletDirectoryTraces) CheckWalletOwnership(ctx context.Context, shardID, walletID, merchantID string) error {
	ctx, span := platform.GetTracer().Start(ctx, "api.wallet_directory.check_ownership",
		trace.WithAttributes(
			attribute.String(platform.MetricLabelShard, shardID),
			attribute.String(platform.MetricLabelWalletID, walletID),
			attribute.String(platform.MetricLabelMerchantID, merchantID)))
	defer span.End()

	err := w.next.CheckWalletOwnership(ctx, shardID, walletID, merchantID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(
			attribute.String(platform.MetricLabelErrorType, fmt.Sprintf("%T", err)),
			attribute.String(platform.MetricLabelErrorMessage, err.Error()),
		)
		return err
	}
	return nil
}

func (w *walletDirectoryTraces) GetBalance(ctx context.Context, shardID, walletID string) (int64, string, error) {
	ctx, span := platform.GetTracer().Start(ctx, "api.wallet_directory.get_balance",
		trace.WithAttributes(
			attribute.String(platform.MetricLabelShard, shardID),
			attribute.String(platform.MetricLabelWalletID, walletID)))
	defer span.End()

	bal, curr, err := w.next.GetBalance(ctx, shardID, walletID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(
			attribute.String(platform.MetricLabelErrorType, fmt.Sprintf("%T", err)),
			attribute.String(platform.MetricLabelErrorMessage, err.Error()),
		)
	}
	return bal, curr, err
}
