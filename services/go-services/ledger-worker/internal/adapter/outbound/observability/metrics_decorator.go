package observability

import (
	"context"
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/ledger-worker/internal/core/port"
)

// -- Ledger Store Decorator --

type ledgerStoreMetrics struct {
	next port.LedgerStore
}

func NewLedgerStoreMetrics(next port.LedgerStore) port.LedgerStore {
	return &ledgerStoreMetrics{next: next}
}

func (m *ledgerStoreMetrics) PostTransfer(ctx context.Context, shardID string, transfer domain.Transfer) error {
	err := m.next.PostTransfer(ctx, shardID, transfer)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentLedgerStore)
	}
	return err
}

func (m *ledgerStoreMetrics) FailTransfer(ctx context.Context, shardID string, transfer domain.Transfer, reason string) error {
	err := m.next.FailTransfer(ctx, shardID, transfer, reason)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentLedgerStore)
	}
	return err
}

// -- CrossShard Store Decorator --

type crossShardStoreMetrics struct {
	next port.CrossShardStore
}

func NewCrossShardStoreMetrics(next port.CrossShardStore) port.CrossShardStore {
	return &crossShardStoreMetrics{next: next}
}

func (m *crossShardStoreMetrics) DebitToClearingAccount(ctx context.Context, srcShard, dstShard, jobID string, transfer domain.Transfer) error {
	err := m.next.DebitToClearingAccount(ctx, srcShard, dstShard, jobID, transfer)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentCrossShardStore)
	}
	return err
}

func (m *crossShardStoreMetrics) CreditFromClearingAccount(ctx context.Context, intent *eventsv1.XShardTransferRequestedPayload) error {
	err := m.next.CreditFromClearingAccount(ctx, intent)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentCrossShardStore)
	}
	return err
}

func (m *crossShardStoreMetrics) SettleCrossShardTransfer(ctx context.Context, srcShard, transferID string) error {
	err := m.next.SettleCrossShardTransfer(ctx, srcShard, transferID)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentCrossShardStore)
	}
	return err
}

func (m *crossShardStoreMetrics) ReverseCrossShardTransfer(ctx context.Context, srcShard, transferID, reason string) error {
	err := m.next.ReverseCrossShardTransfer(ctx, srcShard, transferID, reason)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentCrossShardStore)
	}
	return err
}

// -- DLQ Store Decorator --

type dlqStoreMetrics struct {
	next port.DLQStore
}

func NewDLQStoreMetrics(next port.DLQStore) port.DLQStore {
	return &dlqStoreMetrics{next: next}
}

func (m *dlqStoreMetrics) WriteDLQEntry(ctx context.Context, shardID string, entry domain.DLQEntry) error {
	err := m.next.WriteDLQEntry(ctx, shardID, entry)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentDLQStore)
	}
	return err
}

// -- Merchant Directory Decorator --

type merchantDirMetrics struct {
	next port.MerchantDirectory
}

func NewMerchantDirectoryMetrics(next port.MerchantDirectory) port.MerchantDirectory {
	return &merchantDirMetrics{next: next}
}

func (m *merchantDirMetrics) ShardFor(ctx context.Context, merchantID string) (string, error) {
	shardID, err := m.next.ShardFor(ctx, merchantID)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentMerchantDirectory)
	}
	return shardID, err
}

// -- Job Handler Decorator --

type jobHandlerMetrics struct {
	next port.JobHandler
}

func NewJobHandlerMetrics(next port.JobHandler) port.JobHandler {
	return &jobHandlerMetrics{next: next}
}

func (m *jobHandlerMetrics) ProcessJob(ctx context.Context, payload *eventsv1.JobRequestedPayload) error {
	start := time.Now()
	err := m.next.ProcessJob(ctx, payload)
	duration := time.Since(start)

	platform.RecordConsumerMsgDuration(ctx, platform.TopicJobs, duration)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentJobHandler)
	}
	return err
}

// -- Saga Handler Decorator --

type sagaHandlerMetrics struct {
	next port.SagaHandler
}

func NewSagaHandlerMetrics(next port.SagaHandler) port.SagaHandler {
	return &sagaHandlerMetrics{next: next}
}

func (m *sagaHandlerMetrics) HandleXShardRequested(ctx context.Context, payload *eventsv1.XShardTransferRequestedPayload) error {
	start := time.Now()
	err := m.next.HandleXShardRequested(ctx, payload)
	duration := time.Since(start)

	platform.RecordConsumerMsgDuration(ctx, platform.TopicXShardRequested, duration)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentSagaHandler)
	}
	return err
}

func (m *sagaHandlerMetrics) HandleXShardSettled(ctx context.Context, payload *eventsv1.XShardTransferSettledPayload) error {
	start := time.Now()
	err := m.next.HandleXShardSettled(ctx, payload)
	duration := time.Since(start)

	platform.RecordConsumerMsgDuration(ctx, platform.TopicXShardSettled, duration)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentSagaHandler)
	}
	return err
}

func (m *sagaHandlerMetrics) HandleXShardFailed(ctx context.Context, payload *eventsv1.XShardTransferFailedPayload) error {
	start := time.Now()
	err := m.next.HandleXShardFailed(ctx, payload)
	duration := time.Since(start)

	platform.RecordConsumerMsgDuration(ctx, platform.TopicXShardFailed, duration)
	if err != nil && platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure {
		platform.RecordInfrastructureError(ctx, platform.ComponentSagaHandler)
	}
	return err
}
