package app

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/port"
)

var _ port.TransferSubmitter = (*TransferService)(nil)

type TransferService struct {
	merchantsDir port.MerchantDirectory
	walletsDir   port.WalletDirectory
	jobs         port.JobStore
	getNewJobID  func() string
}

func NewTransferService(mDir port.MerchantDirectory, wDir port.WalletDirectory, jobs port.JobStore, idGen func() string) *TransferService {
	return &TransferService{
		merchantsDir: mDir,
		walletsDir:   wDir,
		jobs:         jobs,
		getNewJobID:  idGen,
	}
}

func (s *TransferService) Submit(ctx context.Context, t domain.Transfer, idempKey string) (domain.SubmitResult, error) {
	if err := t.Validate(); err != nil {
		return domain.SubmitResult{}, err
	}

	toMerchantID, err := s.walletsDir.LookupMerchantForWallet(ctx, t.ToWallet)
	if err != nil {
		return domain.SubmitResult{}, err
	}
	t.ToMerchantID = toMerchantID

	hash := t.Hash()

	shard, err := s.merchantsDir.ShardFor(ctx, t.MerchantID)
	if err != nil {
		return domain.SubmitResult{}, err
	}

	if err := s.walletsDir.CheckWalletOwnership(ctx, shard, t.FromWallet, t.MerchantID); err != nil {
		return domain.SubmitResult{}, err
	}

	job := domain.Job{
		ID:             s.getNewJobID(),
		MerchantID:     t.MerchantID,
		PayloadHash:    hash,
		IdempotencyKey: idempKey,
		Type:           domain.JobTypeTransfer,
		Status:         domain.JobStatusPending,
	}

	return s.jobs.ClaimAndRecord(ctx, shard, job, t, idempKey)
}
