package app

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/recon-worker/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/recon-worker/internal/core/port"
	"go.uber.org/zap"
)

const (
	SafetyMargin = 60 * time.Second
	BalanceZero  = 0
)

type Runner struct {
	logger      *zap.Logger
	repo        port.ReconRepository
	parallelism int
	pools       *platform.ShardPools
}

func NewRunner(
	logger *zap.Logger,
	repo port.ReconRepository,
	parallelism int,
	pools *platform.ShardPools,
) *Runner {
	return &Runner{
		logger:      logger,
		repo:        repo,
		parallelism: parallelism,
		pools:       pools,
	}
}

func (r *Runner) Run(ctx context.Context, start, end time.Time) (*domain.Report, error) {
	runID := platform.NewEventID()
	r.logger.Info(platform.LogEventReconStarted, zap.String("run_id", runID), zap.Time("start", start), zap.Time("end", end))

	// 1. Acquire Lock
	locked, err := r.repo.AcquireLock(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire reconciliation lock: %w", err)
	}
	if !locked {
		return nil, fmt.Errorf("%w", platform.ErrReconciliationHeld)
	}
	defer func() {
		if err := r.repo.ReleaseLock(ctx); err != nil {
			r.logger.Error(platform.LogEventReconLockReleaseFailed, zap.Error(err))
		}
	}()

	startTime := time.Now()
	cutoff := end.Add(-SafetyMargin)

	report := &domain.Report{
		RunID:       runID,
		WindowStart: start,
		WindowEnd:   end,
	}

	shardIDs := r.pools.GetAvailableShardIDs()
	var globalDiscrepancies []domain.Discrepancy

	// 2. Run Shard Checks
	for _, shardID := range shardIDs {
		// A. Per-shard sum check
		sum, err := r.repo.GetShardSum(ctx, shardID, cutoff)
		if err != nil {
			return nil, fmt.Errorf("failed to get shard sum for %s: %w", shardID, err)
		}
		if sum != BalanceZero {
			var absSum int64 = sum
			if sum < BalanceZero {
				absSum = -sum
			}
			platform.RecordLedgerImbalance(ctx, absSum)
			r.logger.Error(platform.LogEventReconConservationCheckFailed, zap.String("shard_id", shardID), zap.Int64("sum", sum))
			platform.LogCanonicalEvent(ctx, r.logger, platform.ServiceNameReconWorker, platform.CanonicalLogLine{
				Event:      platform.EventReconImbalance,
				Status:     platform.StatusFailed,
				ErrorCode:  "conservation",
				Amount:     absSum,
			})
			globalDiscrepancies = append(globalDiscrepancies, domain.Discrepancy{
				Kind:           domain.DiscrepancyKindGlobalConservation,
				DerivedBalance: sum,
			})
			report.GlobalSum += sum
		} else {
			platform.RecordLedgerImbalance(ctx, BalanceZero)
		}

		// B. Leg imbalance check
		badTransfers, err := r.repo.CheckTransferLegs(ctx, shardID, cutoff)
		if err != nil {
			return nil, fmt.Errorf("failed to check transfer legs on %s: %w", shardID, err)
		}
		for _, tfID := range badTransfers {
			r.logger.Error(platform.LogEventReconLegImbalanceDetected, zap.String("transfer_id", tfID), zap.String("shard_id", shardID))
			globalDiscrepancies = append(globalDiscrepancies, domain.Discrepancy{
				Kind:       domain.DiscrepancyKindLegImbalance,
				TransferID: tfID,
			})
		}
	}

	// 3. Fanned-out per-wallet check
	var allWallets []string
	walletShardMap := make(map[string]string)
	for _, shardID := range shardIDs {
		wallets, err := r.repo.FindAffectedWallets(ctx, shardID, start, cutoff)
		if err != nil {
			return nil, fmt.Errorf("failed to find affected wallets on %s: %w", shardID, err)
		}
		for _, w := range wallets {
			allWallets = append(allWallets, w)
			walletShardMap[w] = shardID
		}
	}

	report.WalletsChecked = len(allWallets)

	discrepanciesChan := make(chan domain.Discrepancy, len(allWallets))
	semaphore := make(chan struct{}, r.parallelism)
	var wg sync.WaitGroup

	WalletLoop:
	for _, wID := range allWallets {
		select {
		case <-ctx.Done():
			break WalletLoop
		case semaphore <- struct{}{}:
			wg.Add(1)
			go func(walletID, shard string) {
				defer wg.Done()
				defer func() { <-semaphore }()

				if ctx.Err() != nil {
					return
				}

				disc, err := r.repo.CheckWallet(ctx, shard, walletID, cutoff)
				if err != nil {
					r.logger.Error(platform.LogEventReconWalletCheckFailed, zap.String("wallet_id", walletID), zap.Error(err))
					return
				}
				if disc != nil {
					select {
					case discrepanciesChan <- *disc:
					case <-ctx.Done():
					}
				}
			}(wID, walletShardMap[wID])
		}
	}

	wg.Wait()
	close(discrepanciesChan)

	var walletDiscrepancies []domain.Discrepancy
	for d := range discrepanciesChan {
		walletDiscrepancies = append(walletDiscrepancies, d)
	}

	report.Discrepancies = append(globalDiscrepancies, walletDiscrepancies...)
	report.DurationSeconds = time.Since(startTime).Seconds()

	// 4. Persist Report to a random shard so outbox relay picks up events
	if len(shardIDs) > 0 {
		idx := int(time.Now().UnixNano() & math.MaxInt32) % len(shardIDs)
		targetShard := shardIDs[idx]
		err = r.repo.PersistReport(ctx, targetShard, report)
		if err != nil {
			return nil, fmt.Errorf("failed to persist report to database: %w", err)
		}
	}

	r.logger.Info(platform.LogEventReconCompleted,
		zap.String("run_id", runID),
		zap.Int("wallets_checked", report.WalletsChecked),
		zap.Int("discrepancies_found", len(report.Discrepancies)),
		zap.Float64("duration_sec", report.DurationSeconds),
	)

	platform.LogCanonicalEvent(ctx, r.logger, platform.ServiceNameReconWorker, platform.CanonicalLogLine{
		Event:      platform.EventReconCompleted,
		Status:     platform.StatusSuccess,
		Amount:     report.GlobalSum,
		RetryCount: len(report.Discrepancies),
	})

	return report, nil
}
