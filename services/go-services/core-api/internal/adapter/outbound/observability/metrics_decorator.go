package observability

import (
	"context"
	"errors"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

// -- Merchant Directory Decorator --

type merchantDirMetrics struct {
	next port.MerchantDirectory
}

func NewMerchantDirectoryMetrics(next port.MerchantDirectory) port.MerchantDirectory {
	return &merchantDirMetrics{next: next}
}

func (m *merchantDirMetrics) ShardFor(ctx context.Context, merchantID string) (string, error) {
	shardID, err := m.next.ShardFor(ctx, merchantID)
	if err != nil {
		if platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure || errors.Is(err, domain.ErrServiceUnavailable) {
			platform.RecordInfrastructureError(ctx, platform.ComponentMerchantDirectory)
		}
	}
	return shardID, err
}

// -- Wallet Directory Decorator --

type walletDirMetrics struct {
	next port.WalletDirectory
}

func NewWalletDirectoryMetrics(next port.WalletDirectory) port.WalletDirectory {
	return &walletDirMetrics{next: next}
}

func (m *walletDirMetrics) CheckWalletOwnership(ctx context.Context, shardID, walletID, merchantID string) error {
	err := m.next.CheckWalletOwnership(ctx, shardID, walletID, merchantID)
	if err != nil {
		if platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure || errors.Is(err, domain.ErrServiceUnavailable) {
			platform.RecordInfrastructureError(ctx, platform.ComponentWalletDirectory)
		}
	}
	return err
}

func (m *walletDirMetrics) GetBalance(ctx context.Context, shardID, walletID string) (int64, string, error) {
	balance, currency, err := m.next.GetBalance(ctx, shardID, walletID)
	if err != nil {
		if platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure || errors.Is(err, domain.ErrServiceUnavailable) {
			platform.RecordInfrastructureError(ctx, platform.ComponentWalletDirectory)
		}
	}
	return balance, currency, err
}

// -- Job Store Decorator --

type jobStoreMetrics struct {
	next port.JobStore
}

func NewJobStoreMetrics(next port.JobStore) port.JobStore {
	return &jobStoreMetrics{next: next}
}

func (m *jobStoreMetrics) GetJob(ctx context.Context, shardID, jobID string) (domain.Job, error) {
	job, err := m.next.GetJob(ctx, shardID, jobID)
	if err != nil {
		if platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure || errors.Is(err, domain.ErrServiceUnavailable) {
			platform.RecordInfrastructureError(ctx, platform.ComponentJobStore)
		}
	}
	return job, err
}

func (m *jobStoreMetrics) ClaimAndRecord(
	ctx context.Context,
	shardID string,
	job domain.Job,
	t domain.Transfer,
	idempKey string,
) (domain.SubmitResult, error) {
	res, err := m.next.ClaimAndRecord(ctx, shardID, job, t, idempKey)
	if err != nil {
		if platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure || errors.Is(err, domain.ErrServiceUnavailable) {
			platform.RecordInfrastructureError(ctx, platform.ComponentJobStore)
		}
	}
	return res, err
}

// -- Transfer Service Decorator --

type transferServiceMetrics struct {
	next port.TransferSubmitter
}

func NewTransferServiceMetrics(next port.TransferSubmitter) port.TransferSubmitter {
	return &transferServiceMetrics{next: next}
}

func (m *transferServiceMetrics) Transfer(ctx context.Context, t domain.Transfer, idempKey string) (domain.SubmitResult, error) {
	res, err := m.next.Transfer(ctx, t, idempKey)
	if err != nil {
		if errors.Is(err, domain.ErrIdempotencyConflict) {
			platform.RecordIdempotencyConflict(ctx, t.MerchantID, res.ShardID, res.Job.ID)
		} else if platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure || errors.Is(err, domain.ErrServiceUnavailable) {
			platform.RecordInfrastructureError(ctx, platform.ComponentTransferService)
		}
	} else if res.AlreadyExisted {
		platform.RecordIdempotencyHit(ctx, t.MerchantID, res.Job.ID, res.ShardID)
	}
	return res, err
}

func (m *transferServiceMetrics) GetBalance(ctx context.Context, walletID, merchantID string) (int64, string, error) {
	return m.next.GetBalance(ctx, walletID, merchantID)
}

// -- Wallet Use Case Decorator --

type walletUseCaseMetrics struct {
	next port.WalletUseCase
}

func NewWalletUseCaseMetrics(next port.WalletUseCase) port.WalletUseCase {
	return &walletUseCaseMetrics{next: next}
}

func (m *walletUseCaseMetrics) CreateWallet(ctx context.Context, merchantID, currency string) (string, error) {
	walletID, err := m.next.CreateWallet(ctx, merchantID, currency)
	if err == nil {
		platform.RecordWalletCreated(ctx, merchantID, currency)
	} else if platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure || errors.Is(err, domain.ErrServiceUnavailable) {
		platform.RecordInfrastructureError(ctx, platform.ComponentJobStore)
	}
	return walletID, err
}

func (m *walletUseCaseMetrics) Deposit(ctx context.Context, t domain.Transfer, idempKey string) (domain.SubmitResult, error) {
	res, err := m.next.Deposit(ctx, t, idempKey)

	if err != nil {
		platform.RecordDepositRequest(ctx, t.MerchantID, t.Currency, t.Amount, platform.TransferMetricFailed)
		if errors.Is(err, domain.ErrIdempotencyConflict) {
			platform.RecordIdempotencyConflict(ctx, t.MerchantID, res.ShardID, res.Job.ID)
		} else if platform.ClassifyError(err, domain.IsTerminalError) == platform.ClassificationInfrastructure || errors.Is(err, domain.ErrServiceUnavailable) {
			platform.RecordInfrastructureError(ctx, platform.ComponentTransferService)
		}
	} else {
		platform.RecordDepositRequest(ctx, t.MerchantID, t.Currency, t.Amount, platform.TransferMetricSuccess)
		if res.AlreadyExisted {
			platform.RecordIdempotencyHit(ctx, t.MerchantID, res.Job.ID, res.ShardID)
		}
	}
	return res, err
}
