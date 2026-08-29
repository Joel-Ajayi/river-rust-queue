package app

import (
	"context"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

type WalletService struct {
	merchantsDir port.MerchantDirectory
	walletsDir   port.WalletDirectory
	walletsStore port.WalletStore
	jobStore     port.JobStore
	getNewJobID  func() string
}

func NewWalletService(
	mDir port.MerchantDirectory,
	wDir port.WalletDirectory,
	wStore port.WalletStore,
	jStore port.JobStore,
	idGen func() string,
) *WalletService {
	return &WalletService{
		merchantsDir: mDir,
		walletsDir:   wDir,
		walletsStore: wStore,
		jobStore:     jStore,
		getNewJobID:  idGen,
	}
}

// CreateWallet registers a new customer wallet on the merchant's shard.
func (s *WalletService) CreateWallet(ctx context.Context, merchantID, currency string) (string, error) {
	if currency == "" {
		return "", domain.ErrInvalidCurrency
	}

	shard, err := s.merchantsDir.ShardFor(ctx, merchantID)
	if err != nil {
		return "", err
	}

	walletID := platform.NewWalletID(merchantID)
	err = s.walletsStore.CreateWallet(ctx, shard, walletID, merchantID, currency, platform.WalletTypeCustomer)
	if err != nil {
		return "", err
	}

	return walletID, nil
}

// Deposit deposits funds into a merchant's customer wallet from the platform fiat vault.
func (s *WalletService) Deposit(ctx context.Context, t domain.Transfer, idempKey string) (domain.SubmitResult, error) {
	// Validate and return destination MerchantID
	toMerchantID, err := t.Validate(true)
	if err != nil {
		return domain.SubmitResult{}, err
	}
	t.ToMerchantID = toMerchantID

	// 1. Locate the fiat vault wallet on the shard.
	shard, err := s.merchantsDir.ShardFor(ctx, t.ToMerchantID)
	if err != nil {
		return domain.SubmitResult{}, err
	}

	fiatVault, err := s.walletsStore.FindFiatVault(ctx, shard, t.Currency)
	if err != nil {
		return domain.SubmitResult{}, err
	}
	t.FromWallet = fiatVault

	job := domain.Job{
		ID:             s.getNewJobID(),
		MerchantID:     t.MerchantID,
		PayloadHash:    t.Hash(),
		IdempotencyKey: idempKey,
		Type:           platform.JobTypeTransfer,
		Status:         platform.JobStatusPending,
		ShardID:        shard,
		CreatedAt:      time.Now(),
	}

	res, err := s.jobStore.ClaimAndRecord(ctx, shard, job, t, idempKey)
	if err != nil {
		return domain.SubmitResult{}, err
	}

	res.ShardID = shard
	return res, nil
}
