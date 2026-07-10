package resilience

import (
	"context"
	"errors"
	"sync"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/sony/gobreaker"
)

// mapCBError translates gobreaker state errors into domain-specific service unavailable errors.
func mapCBError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return domain.ErrServiceUnavailable
	}
	return err
}

func newCB(name string) *gobreaker.CircuitBreaker {
	return platform.NewCircuitBreaker(platform.CircuitBreakerConfig{
		Name:        name,
		MaxRequests: CircuitBreakerMaxRequest,
		Timeout:     CircuitBreakerTimeout,
		MaxFails:    CircuitBreakerMaxFails,
		IsSuccessful: func(err error) bool {
			if err == nil {
				return true
			}
			return domain.IsNonRetryableError(err)
		},
	})
}

// -- Merchant Directory Decorator --

type merchantDirCB struct {
	next   port.MerchantDirectory
	readCB *gobreaker.CircuitBreaker
}

func NewMerchantDirectoryCB(next port.MerchantDirectory) port.MerchantDirectory {
	return &merchantDirCB{
		next:   next,
		readCB: newCB(CircuitBreakerReadName + "_MerchantDir"),
	}
}

func (c *merchantDirCB) ShardFor(ctx context.Context, merchantID string) (string, error) {
	var res interface{}

	err := platform.ExecuteWithJitter(ctx, platform.RetryConfig{
		MaxRetries: RetryMaxAttempts,
		BaseDelay:  RetryBaseDelay,
		MaxDelay:   RetryMaxDelay,
	}, domain.IsNonRetryableError, func() error {
		var err error
		res, err = c.readCB.Execute(func() (interface{}, error) {
			return c.next.ShardFor(ctx, merchantID)
		})
		return err
	})

	if err != nil {
		return "", mapCBError(err)
	}
	return res.(string), nil
}

func (c *merchantDirCB) AuthenticateAPIKey(ctx context.Context, apiKey string) (domain.Principal, error) {
	var res interface{}
	err := platform.ExecuteWithJitter(ctx, platform.RetryConfig{
		MaxRetries: RetryMaxAttempts,
		BaseDelay:  RetryBaseDelay,
		MaxDelay:   RetryMaxDelay,
	}, domain.IsNonRetryableError, func() error {
		var err error
		res, err = c.readCB.Execute(func() (interface{}, error) {
			return c.next.AuthenticateAPIKey(ctx, apiKey)
		})
		return err
	})
	if err != nil {
		return domain.Principal{}, mapCBError(err)
	}
	return res.(domain.Principal), nil
}

// -- Wallet Directory Decorator --

type walletDirCB struct {
	next    port.WalletDirectory
	readCBs sync.Map
}

func NewWalletDirectoryCB(next port.WalletDirectory) port.WalletDirectory {
	return &walletDirCB{
		next: next,
	}
}

func (c *walletDirCB) getReadCB(shardID string) *gobreaker.CircuitBreaker {
	name := CircuitBreakerReadName + "_WalletDir_" + shardID
	if cb, ok := c.readCBs.Load(name); ok {
		return cb.(*gobreaker.CircuitBreaker)
	}
	cb := newCB(name)
	actual, _ := c.readCBs.LoadOrStore(name, cb)
	return actual.(*gobreaker.CircuitBreaker)
}

func (c *walletDirCB) CheckWalletOwnership(ctx context.Context, shardID, walletID, merchantID string) error {
	cb := c.getReadCB(shardID)
	err := platform.ExecuteWithJitter(ctx, platform.RetryConfig{
		MaxRetries: RetryMaxAttempts,
		BaseDelay:  RetryBaseDelay,
		MaxDelay:   RetryMaxDelay,
	}, domain.IsNonRetryableError, func() error {
		_, e := cb.Execute(func() (interface{}, error) {
			return nil, c.next.CheckWalletOwnership(ctx, shardID, walletID, merchantID)
		})
		return e
	})
	return mapCBError(err)
}

func (c *walletDirCB) LookupMerchantForWallet(ctx context.Context, walletID string) (string, error) {
	return c.next.LookupMerchantForWallet(ctx, walletID)
}

// We need a custom struct to return two values through Execute
type balanceResult struct {
	bal int64
	cur string
}

func (c *walletDirCB) GetBalance(ctx context.Context, shardID, walletID string) (int64, string, error) {
	cb := c.getReadCB(shardID)
	var res interface{}
	err := platform.ExecuteWithJitter(ctx, platform.RetryConfig{
		MaxRetries: RetryMaxAttempts,
		BaseDelay:  RetryBaseDelay,
		MaxDelay:   RetryMaxDelay,
	}, domain.IsNonRetryableError, func() error {
		var err error
		res, err = cb.Execute(func() (interface{}, error) {
			bal, cur, e := c.next.GetBalance(ctx, shardID, walletID)
			return balanceResult{bal, cur}, e
		})
		return err
	})
	if err != nil {
		return 0, "", mapCBError(err)
	}
	br := res.(balanceResult)
	return br.bal, br.cur, nil
}

// -- Job Store Decorator --

type jobStoreCB struct {
	next     port.JobStore
	readCBs  sync.Map
	writeCBs sync.Map
}

func NewJobStoreCB(next port.JobStore) port.JobStore {
	return &jobStoreCB{
		next: next,
	}
}

func (c *jobStoreCB) getReadCB(shardID string) *gobreaker.CircuitBreaker {
	name := CircuitBreakerReadName + "_JobStore_" + shardID
	if cb, ok := c.readCBs.Load(name); ok {
		return cb.(*gobreaker.CircuitBreaker)
	}
	cb := newCB(name)
	actual, _ := c.readCBs.LoadOrStore(name, cb)
	return actual.(*gobreaker.CircuitBreaker)
}

func (c *jobStoreCB) getWriteCB(shardID string) *gobreaker.CircuitBreaker {
	name := CircuitBreakerWriteName + "_JobStore_" + shardID
	if cb, ok := c.writeCBs.Load(name); ok {
		return cb.(*gobreaker.CircuitBreaker)
	}
	cb := newCB(name)
	actual, _ := c.writeCBs.LoadOrStore(name, cb)
	return actual.(*gobreaker.CircuitBreaker)
}

func (c *jobStoreCB) GetJob(ctx context.Context, shardID, jobID string) (domain.Job, error) {
	cb := c.getReadCB(shardID)
	var res interface{}
	err := platform.ExecuteWithJitter(ctx, platform.RetryConfig{
		MaxRetries: RetryMaxAttempts,
		BaseDelay:  RetryBaseDelay,
		MaxDelay:   RetryMaxDelay,
	}, domain.IsNonRetryableError, func() error {
		var err error
		res, err = cb.Execute(func() (interface{}, error) {
			return c.next.GetJob(ctx, shardID, jobID)
		})
		return err
	})
	if err != nil {
		return domain.Job{}, mapCBError(err)
	}
	return res.(domain.Job), nil
}

func (c *jobStoreCB) ClaimAndRecord(ctx context.Context, shardId string, job domain.Job, t domain.Transfer, idempKey string) (domain.SubmitResult, error) {
	cb := c.getWriteCB(shardId)
	var res interface{}
	err := platform.ExecuteWithJitter(ctx, platform.RetryConfig{
		MaxRetries: RetryMaxAttempts,
		BaseDelay:  RetryBaseDelay,
		MaxDelay:   RetryMaxDelay,
	}, domain.IsNonRetryableError, func() error {
		var err error
		res, err = cb.Execute(func() (interface{}, error) {
			return c.next.ClaimAndRecord(ctx, shardId, job, t, idempKey)
		})
		return err
	})
	if err != nil {
		return domain.SubmitResult{}, mapCBError(err)
	}
	return res.(domain.SubmitResult), nil
}
