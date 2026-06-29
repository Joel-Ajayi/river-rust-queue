package app

import (
	"context"
	"errors"
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
)

// fakeDirectory and fakeJobStore are in-memory fakes for the driven ports. The
// use-case is tested with zero infrastructure — no Postgres, no network.
type fakeDirectory struct {
	shard      string
	shardErr   error
	principal  domain.Principal
	authErr    error
	lastLookup string
}

func (f *fakeDirectory) ShardFor(_ context.Context, merchantID string) (string, error) {
	f.lastLookup = merchantID
	return f.shard, f.shardErr
}

func (f *fakeDirectory) AuthenticateAPIKey(_ context.Context, _ string) (domain.Principal, error) {
	return f.principal, f.authErr
}

type fakeJobStore struct {
	gotShard string
	gotJob   domain.Job
	result   domain.SubmitResult
	err      error
}

func (f *fakeJobStore) ClaimAndRecord(_ context.Context, shardID string, job domain.Job, _ domain.Transfer, _, _ string) (domain.SubmitResult, error) {
	f.gotShard = shardID
	f.gotJob = job
	if f.err != nil {
		return domain.SubmitResult{}, f.err
	}
	if (f.result == domain.SubmitResult{}) {
		return domain.SubmitResult{Job: job}, nil
	}
	return f.result, nil
}

func validTransfer() domain.Transfer {
	return domain.Transfer{
		MerchantID: "mer_1", FromWallet: "w_a", ToWallet: "w_b",
		Amount: 100, Currency: "USD",
	}
}

func TestSubmit_RoutesToOwningShardAndCreatesJob(t *testing.T) {
	dir := &fakeDirectory{shard: "shard-b"}
	jobs := &fakeJobStore{}
	svc := NewService(dir, jobs, func() string { return "job_fixed" })

	res, err := svc.Submit(context.Background(), validTransfer(), "key-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dir.lastLookup != "mer_1" {
		t.Errorf("expected shard lookup for mer_1, got %q", dir.lastLookup)
	}
	if jobs.gotShard != "shard-b" {
		t.Errorf("job written to wrong shard: %q", jobs.gotShard)
	}
	if res.Job.ID != "job_fixed" || res.Job.Status != "pending" {
		t.Errorf("unexpected job: %+v", res.Job)
	}
}

func TestSubmit_RejectsInvalidTransferBeforeAnyIO(t *testing.T) {
	dir := &fakeDirectory{shard: "shard-a"}
	jobs := &fakeJobStore{}
	svc := NewService(dir, jobs, func() string { return "job_x" })

	bad := validTransfer()
	bad.Amount = 0 // violates a domain rule

	_, err := svc.Submit(context.Background(), bad, "key-2")

	var ve domain.ValidationError
	if !errors.As(err, &ve) || ve.Field != "amount" {
		t.Fatalf("expected amount ValidationError, got %v", err)
	}
	if dir.lastLookup != "" || jobs.gotShard != "" {
		t.Error("validation must short-circuit before touching driven ports")
	}
}

func TestSubmit_PropagatesInactiveMerchant(t *testing.T) {
	dir := &fakeDirectory{shardErr: domain.ErrMerchantInactive}
	jobs := &fakeJobStore{}
	svc := NewService(dir, jobs, func() string { return "job_x" })

	_, err := svc.Submit(context.Background(), validTransfer(), "key-3")
	if !errors.Is(err, domain.ErrMerchantInactive) {
		t.Fatalf("expected ErrMerchantInactive, got %v", err)
	}
	if jobs.gotShard != "" {
		t.Error("must not write a job for an inactive merchant")
	}
}
