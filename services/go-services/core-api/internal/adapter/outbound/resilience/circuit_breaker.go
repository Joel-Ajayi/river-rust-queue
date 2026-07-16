package resilience

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

// Layer 3 CB decorators are pure shields: cb.Execute(fn) only — no retry.
// Retry lives at Layer 1 (rest.RetryBoundary).

type balanceResult struct {
	bal int64
	cur string
}

// -- Merchant Directory (global merchants pool) --

type merchantDirCB struct {
	next port.MerchantDirectory
	cbs  *platform.DBCircuitBreakers
}

// NewMerchantDirectoryCB wraps with the merchants-global DB circuit breaker.
func NewMerchantDirectoryCB(next port.MerchantDirectory, cbs *platform.DBCircuitBreakers) port.MerchantDirectory {
	return &merchantDirCB{next: next, cbs: cbs}
}

func (c *merchantDirCB) ShardFor(ctx context.Context, merchantID string) (string, error) {
	res, err := c.cbs.Merchants().Execute(func() (interface{}, error) {
		return c.next.ShardFor(ctx, merchantID)
	})
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

func (c *merchantDirCB) AuthenticateAPIKey(ctx context.Context, apiKey string) (domain.Principal, error) {
	res, err := c.cbs.Merchants().Execute(func() (interface{}, error) {
		return c.next.AuthenticateAPIKey(ctx, apiKey)
	})
	if err != nil {
		return domain.Principal{}, err
	}
	return res.(domain.Principal), nil
}

// -- Wallet Directory (per-shard for Check/GetBalance; merchants-global lookup) --

type walletDirCB struct {
	next port.WalletDirectory
	cbs  *platform.DBCircuitBreakers
}

func NewWalletDirectoryCB(next port.WalletDirectory, cbs *platform.DBCircuitBreakers) port.WalletDirectory {
	return &walletDirCB{next: next, cbs: cbs}
}

func (c *walletDirCB) CheckWalletOwnership(ctx context.Context, shardID, walletID, merchantID string) error {
	_, err := c.cbs.ShardRO(shardID).Execute(func() (interface{}, error) {
		return nil, c.next.CheckWalletOwnership(ctx, shardID, walletID, merchantID)
	})
	return err
}

func (c *walletDirCB) GetBalance(ctx context.Context, shardID, walletID string) (int64, string, error) {
	res, err := c.cbs.ShardRO(shardID).Execute(func() (interface{}, error) {
		balance, currency, err := c.next.GetBalance(ctx, shardID, walletID)
		return balanceResult{bal: balance, cur: currency}, err
	})
	if err != nil {
		return 0, "", err
	}
	w := res.(balanceResult)
	return w.bal, w.cur, nil
}

// -- Job Store (per-shard) --

type jobStoreCB struct {
	next port.JobStore
	cbs  *platform.DBCircuitBreakers
}

func NewJobStoreCB(next port.JobStore, cbs *platform.DBCircuitBreakers) port.JobStore {
	return &jobStoreCB{next: next, cbs: cbs}
}

func (c *jobStoreCB) GetJob(ctx context.Context, shardID, jobID string) (domain.Job, error) {
	res, err := c.cbs.ShardRO(shardID).Execute(func() (interface{}, error) {
		return c.next.GetJob(ctx, shardID, jobID)
	})
	if err != nil {
		return domain.Job{}, err
	}
	return res.(domain.Job), nil
}

func (c *jobStoreCB) ClaimAndRecord(ctx context.Context, shardID string, job domain.Job, t domain.Transfer, idempKey string) (domain.SubmitResult, error) {
	res, err := c.cbs.ShardRW(shardID).Execute(func() (interface{}, error) {
		return c.next.ClaimAndRecord(ctx, shardID, job, t, idempKey)
	})
	if err != nil {
		return domain.SubmitResult{}, err
	}
	return res.(domain.SubmitResult), nil
}
