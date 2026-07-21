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

	platform.RecordWalletCreated(ctx, merchantID, currency)
	return walletID, nil
}

// Deposit deposits funds into a merchant's customer wallet from the platform fiat vault.
func (s *WalletService) Deposit(ctx context.Context, merchantID, walletID string, amount int64, currency, idempKey string) (domain.SubmitResult, error) {
	if amount <= 0 {
		return domain.SubmitResult{}, domain.ErrInvalidAmount
	}
	if currency == "" {
		return domain.SubmitResult{}, domain.ErrInvalidCurrency
	}

	shard, err := s.merchantsDir.ShardFor(ctx, merchantID)
	if err != nil {
		return domain.SubmitResult{}, err
	}

	// Verify recipient wallet is owned by the requesting merchant.
	if err := s.walletsDir.CheckWalletOwnership(ctx, shard, walletID, merchantID); err != nil {
		return domain.SubmitResult{}, err
	}

	// Locate the fiat vault wallet on the shard.
	fiatVault, err := s.walletsStore.FindFiatVault(ctx, shard, currency)
	if err != nil {
		return domain.SubmitResult{}, err
	}

	t := domain.Transfer{
		MerchantID:   merchantID,
		ToMerchantID: merchantID,
		FromWallet:   fiatVault,
		ToWallet:     walletID,
		Amount:       amount,
		Currency:     currency,
		Reference:    platform.DepositReference,
	}

	job := domain.Job{
		ID:             s.getNewJobID(),
		MerchantID:     merchantID,
		PayloadHash:    t.Hash(),
		IdempotencyKey: idempKey,
		Type:           platform.JobTypeTransfer,
		Status:         platform.JobStatusPending,
		ShardID:        shard,
		CreatedAt:      time.Now(),
	}

	res, err := s.jobStore.ClaimAndRecord(ctx, shard, job, t, idempKey)
	if err != nil {
		platform.RecordDepositRequest(ctx, merchantID, currency, amount, platform.TransferMetricFailed)
		return domain.SubmitResult{}, err
	}

	res.ShardID = shard
	platform.RecordDepositRequest(ctx, merchantID, currency, amount, platform.TransferMetricSuccess)
	return res, nil
}
