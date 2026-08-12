package app

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
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

func (s *TransferService) Transfer(ctx context.Context, t domain.Transfer, idempKey string) (domain.SubmitResult, error) {
	// Validate and return destination MerchantID
	toMerchantID, err := t.Validate(false)
	if err != nil {
		return domain.SubmitResult{}, err
	}
	t.ToMerchantID = toMerchantID

	// hash the transfer to create a payload hash for the job
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
		Type:           platform.JobTypeTransfer,
		Status:         platform.JobStatusPending,
		ShardID:        shard,
	}

	res, err := s.jobs.ClaimAndRecord(ctx, shard, job, t, idempKey)
	if err != nil {
		return domain.SubmitResult{}, err
	}

	res.ShardID = shard
	return res, nil
}

func (s *TransferService) GetBalance(ctx context.Context, walletID, merchantID string) (int64, string, error) {
	shard, err := s.merchantsDir.ShardFor(ctx, merchantID)
	if err != nil {
		return 0, "", err
	}

	if err := s.walletsDir.CheckWalletOwnership(ctx, shard, walletID, merchantID); err != nil {
		return 0, "", err
	}

	return s.walletsDir.GetBalance(ctx, shard, walletID)
}
