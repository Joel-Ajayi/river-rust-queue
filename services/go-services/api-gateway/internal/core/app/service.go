package app

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/port"
)

// compile time check that Service implements TransferSubmitter
var (
	_ port.TransferSubmitter = (*Service)(nil)
	_ port.Authenticator     = (*Service)(nil)
)

type Service struct {
	merchantsDir port.MerchantDirectory
	walletsDir   port.WalletDirectory
	jobs         port.JobStore
	getNewJobID  func() string
}

func NewService(mDir port.MerchantDirectory, wDir port.WalletDirectory, jobs port.JobStore, idGen func() string) *Service {
	return &Service{
		merchantsDir: mDir,
		walletsDir:   wDir,
		jobs:         jobs,
		getNewJobID:  idGen,
	}
}

func (s *Service) Submit(ctx context.Context, t domain.Transfer, idempKey string) (domain.SubmitResult, error) {
	// a. Validate the transfer payload first
	if err := t.Validate(); err != nil {
		return domain.SubmitResult{}, err
	}

	// b. Hash request to test for idempotency
	hash := t.Hash()

	// c. check shard of merchant
	shard, err := s.merchantsDir.ShardFor(ctx, t.MerchantID)
	if err != nil {
		return domain.SubmitResult{}, err
	}

	// d. Verify that the merchant owns the from_wallet
	if err := s.walletsDir.CheckWalletOwnership(ctx, shard, t.FromWallet, t.MerchantID); err != nil {
		return domain.SubmitResult{}, err
	}

	// e. Create Job record
	job := domain.Job{
		ID:             s.getNewJobID(),
		MerchantID:     t.MerchantID,
		PayloadHash:    hash,
		IdempotencyKey: idempKey,
		Type:           string(domain.JobTypeTransfer),
		Status:         string(domain.JobStatusPending),
	}

	return s.jobs.ClaimAndRecord(ctx, shard, job, t, idempKey)
}

func (s *Service) Authenticate(ctx context.Context, apiKey string) (domain.Principal, error) {
	return s.merchantsDir.AuthenticateAPIKey(ctx, apiKey)
}

func (s *Service) GetJobStatus(ctx context.Context, merchantID, jobID string) (domain.Job, error) {
	shard, err := s.merchantsDir.ShardFor(ctx, merchantID)
	if err != nil {
		return domain.Job{}, err
	}

	job, err := s.jobs.GetJob(ctx, shard, jobID)
	if err != nil {
		return domain.Job{}, err
	}

	if job.MerchantID != merchantID {
		return domain.Job{}, domain.ErrJobNotFound
	}

	return job, nil
}
