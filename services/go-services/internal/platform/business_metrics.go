package platform

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// Business Metrics
	MetricBusinessGTV            = "business.gtv.total"
	MetricBusinessTransfers      = "business.transfers.total"
	MetricBusinessDeclines       = "business.declines.total"
	MetricBusinessSagaDuration   = "business.saga_duration.seconds"
	MetricBusinessSagaInitiated  = "business.saga.initiated.total"
	MetricBusinessSagaCompleted  = "business.saga.completed.total"
	MetricBusinessRefunds        = "business.refunds.total"
	MetricBusinessDisputes       = "business.disputes.total"
	MetricBusinessWalletsCreated = "business.wallets_created.total"
	MetricBusinessDeposits       = "business.deposits.total"
	MetricBusinessDepositsAmount = "business.deposits_amount.total"

	// Business Metric Labels
	MetricLabelCurrency      = "currency"
	MetricLabelStatus        = "status"
	MetricLabelShardType     = "shard.type"
	MetricLabelReason        = "reason"
	MetricLabelDisputeStatus = "dispute.status"
)

var (
	businessGTVTotal       metric.Int64Counter
	businessTransfersTotal metric.Int64Counter
	businessDeclinesTotal  metric.Int64Counter
	businessSagaDuration   metric.Float64Histogram
	businessSagaInitiated  metric.Int64Counter
	businessSagaCompleted  metric.Int64Counter
	businessRefundsTotal   metric.Int64Counter
	businessDisputesTotal  metric.Int64Counter
	businessWalletsCreated metric.Int64Counter
	businessDepositsTotal  metric.Int64Counter
	businessDepositsAmount metric.Int64Counter
)

// InitBusinessMetrics registers all business OTel instruments. B1: returns an error instead of panicking.
func InitBusinessMetrics() error {
	var err error
	var initErr error

	register := func(name string, fn func() error) {
		if initErr != nil {
			return
		}
		if e := fn(); e != nil {
			initErr = fmt.Errorf("business metric %s: %w", name, e)
		}
	}

	register("business_gtv", func() error {
		businessGTVTotal, err = meter.Int64Counter(
			MetricBusinessGTV,
			metric.WithDescription("Gross Transaction Value — total amount of successfully processed transfers"),
		)
		return err
	})
	register("business_transfers", func() error {
		businessTransfersTotal, err = meter.Int64Counter(
			MetricBusinessTransfers,
			metric.WithDescription("Total number of transfers processed by status (success/failed) and shard type (same/cross)"),
		)
		return err
	})
	register("business_declines", func() error {
		businessDeclinesTotal, err = meter.Int64Counter(
			MetricBusinessDeclines,
			metric.WithDescription("Total number of declined transfers by reason"),
		)
		return err
	})
	register("business_saga_duration", func() error {
		businessSagaDuration, err = meter.Float64Histogram(
			MetricBusinessSagaDuration,
			metric.WithDescription("Duration of cross-shard saga from initiation to completion"),
			metric.WithUnit("s"),
		)
		return err
	})
	register("business_saga_initiated", func() error {
		businessSagaInitiated, err = meter.Int64Counter(
			MetricBusinessSagaInitiated,
			metric.WithDescription("Total number of cross-shard sagas initiated"),
		)
		return err
	})
	register("business_saga_completed", func() error {
		businessSagaCompleted, err = meter.Int64Counter(
			MetricBusinessSagaCompleted,
			metric.WithDescription("Total number of cross-shard sagas completed successfully"),
		)
		return err
	})
	register("business_refunds", func() error {
		businessRefundsTotal, err = meter.Int64Counter(
			MetricBusinessRefunds,
			metric.WithDescription("Total number of refunds processed"),
		)
		return err
	})
	register("business_disputes", func() error {
		businessDisputesTotal, err = meter.Int64Counter(
			MetricBusinessDisputes,
			metric.WithDescription("Total number of disputes by status (opened/resolved)"),
		)
		return err
	})
	register("business_wallets_created", func() error {
		businessWalletsCreated, err = meter.Int64Counter(
			MetricBusinessWalletsCreated,
			metric.WithDescription("Total number of successfully created customer/merchant wallets"),
		)
		return err
	})
	register("business_deposits", func() error {
		businessDepositsTotal, err = meter.Int64Counter(
			MetricBusinessDeposits,
			metric.WithDescription("Total number of deposit requests processed"),
		)
		return err
	})
	register("business_deposits_amount", func() error {
		businessDepositsAmount, err = meter.Int64Counter(
			MetricBusinessDepositsAmount,
			metric.WithDescription("Total amount deposited from fiat vault into wallets"),
		)
		return err
	})

	return initErr
}

// RecordBusinessGTV records the amount of a successful transfer (Gross Transaction Value).
func RecordBusinessGTV(ctx context.Context, amount int64, currency string) {
	if businessGTVTotal != nil {
		businessGTVTotal.Add(ctx, amount, metric.WithAttributes(
			attribute.String(MetricLabelCurrency, currency),
		))
	}
}

// RecordBusinessTransfer records a transfer outcome for TSR computation.
func RecordBusinessTransfer(ctx context.Context, status, shardType string) {
	if businessTransfersTotal != nil {
		businessTransfersTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelStatus, status),
			attribute.String(MetricLabelShardType, shardType),
		))
	}
}

// RecordBusinessDecline records a declined transfer.
func RecordBusinessDecline(ctx context.Context, reason string) {
	if businessDeclinesTotal != nil {
		businessDeclinesTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelReason, reason),
		))
	}
}

// RecordBusinessSagaDuration records the duration of a cross-shard saga from initiation to completion/failure.
func RecordBusinessSagaDuration(ctx context.Context, duration time.Duration) {
	if businessSagaDuration != nil {
		businessSagaDuration.Record(ctx, duration.Seconds())
	}
}

// RecordBusinessSagaInitiated records a saga initiation.
func RecordBusinessSagaInitiated(ctx context.Context) {
	if businessSagaInitiated != nil {
		businessSagaInitiated.Add(ctx, 1)
	}
}

// RecordBusinessSagaCompleted records a successful saga completion.
func RecordBusinessSagaCompleted(ctx context.Context) {
	if businessSagaCompleted != nil {
		businessSagaCompleted.Add(ctx, 1)
	}
}

// RecordBusinessRefund records a refund processed.
func RecordBusinessRefund(ctx context.Context, currency string) {
	if businessRefundsTotal != nil {
		businessRefundsTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelCurrency, currency),
		))
	}
}

// RecordBusinessDispute records a dispute event.
func RecordBusinessDispute(ctx context.Context, disputeStatus string) {
	if businessDisputesTotal != nil {
		businessDisputesTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelDisputeStatus, disputeStatus),
		))
	}
}

// RecordWalletCreated records a newly created wallet.
func RecordWalletCreated(ctx context.Context, merchantID, currency string) {
	if businessWalletsCreated != nil {
		businessWalletsCreated.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelCurrency, currency),
		))
	}
}

// RecordDepositRequest records a deposit request status and amount.
func RecordDepositRequest(ctx context.Context, merchantID, currency string, amount int64, status string) {
	if businessDepositsTotal != nil {
		businessDepositsTotal.Add(ctx, 1, metric.WithAttributes(
			attribute.String(MetricLabelCurrency, currency),
			attribute.String(MetricLabelStatus, status),
		))
	}
	if status == TransferMetricSuccess && businessDepositsAmount != nil {
		businessDepositsAmount.Add(ctx, amount, metric.WithAttributes(
			attribute.String(MetricLabelCurrency, currency),
		))
	}
}
