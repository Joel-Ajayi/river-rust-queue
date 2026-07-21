package platform

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// Business Metrics
	MetricBusinessGTV                = "rrq_business_gtv_total"
	MetricBusinessTransfers          = "rrq_business_transfers_total"
	MetricBusinessDeclines           = "rrq_business_declines_total"
	MetricBusinessSagaDuration       = "rrq_business_saga_duration_seconds"
	MetricBusinessSagaInitiated      = "rrq_business_saga_initiated_total"
	MetricBusinessSagaCompleted      = "rrq_business_saga_completed_total"
	MetricBusinessRefunds            = "rrq_business_refunds_total"
	MetricBusinessDisputes           = "rrq_business_disputes_total"
	MetricBusinessWalletsCreated     = "rrq_business_wallets_created_total"
	MetricBusinessDeposits           = "rrq_business_deposits_total"
	MetricBusinessDepositsAmount     = "rrq_business_deposits_amount_total"

	// Business Metric Labels
	MetricLabelCurrency      = "currency"
	MetricLabelStatus        = "status"
	MetricLabelShardType     = "shard_type"
	MetricLabelReason        = "reason"
	MetricLabelDisputeStatus = "dispute_status"
)

var (
	businessGTVTotal          metric.Int64Counter
	businessTransfersTotal    metric.Int64Counter
	businessDeclinesTotal     metric.Int64Counter
	businessSagaDuration      metric.Float64Histogram
	businessSagaInitiated     metric.Int64Counter
	businessSagaCompleted     metric.Int64Counter
	businessRefundsTotal      metric.Int64Counter
	businessDisputesTotal     metric.Int64Counter
	businessWalletsCreated    metric.Int64Counter
	businessDepositsTotal     metric.Int64Counter
	businessDepositsAmount    metric.Int64Counter

	businessOnce sync.Once
)

func init() {
	businessOnce.Do(func() {
		var err error

		businessGTVTotal, err = meter.Int64Counter(
			MetricBusinessGTV,
			metric.WithDescription("Gross Transaction Value — total amount of successfully processed transfers"),
		)
		if err != nil {
			panic("failed to initialize business gtv metric: " + err.Error())
		}

		businessTransfersTotal, err = meter.Int64Counter(
			MetricBusinessTransfers,
			metric.WithDescription("Total number of transfers processed by status (success/failed) and shard type (same/cross)"),
		)
		if err != nil {
			panic("failed to initialize business transfers metric: " + err.Error())
		}

		businessDeclinesTotal, err = meter.Int64Counter(
			MetricBusinessDeclines,
			metric.WithDescription("Total number of declined transfers by reason"),
		)
		if err != nil {
			panic("failed to initialize business declines metric: " + err.Error())
		}

		businessSagaDuration, err = meter.Float64Histogram(
			MetricBusinessSagaDuration,
			metric.WithDescription("Duration of cross-shard saga from initiation to completion"),
			metric.WithUnit("s"),
		)
		if err != nil {
			panic("failed to initialize business saga duration metric: " + err.Error())
		}

		businessSagaInitiated, err = meter.Int64Counter(
			MetricBusinessSagaInitiated,
			metric.WithDescription("Total number of cross-shard sagas initiated"),
		)
		if err != nil {
			panic("failed to initialize business saga initiated metric: " + err.Error())
		}

		businessSagaCompleted, err = meter.Int64Counter(
			MetricBusinessSagaCompleted,
			metric.WithDescription("Total number of cross-shard sagas completed successfully"),
		)
		if err != nil {
			panic("failed to initialize business saga completed metric: " + err.Error())
		}

		businessRefundsTotal, err = meter.Int64Counter(
			MetricBusinessRefunds,
			metric.WithDescription("Total number of refunds processed"),
		)
		if err != nil {
			panic("failed to initialize business refunds metric: " + err.Error())
		}

		businessDisputesTotal, err = meter.Int64Counter(
			MetricBusinessDisputes,
			metric.WithDescription("Total number of disputes by status (opened/resolved)"),
		)
		if err != nil {
			panic("failed to initialize business disputes metric: " + err.Error())
		}

		businessWalletsCreated, err = meter.Int64Counter(
			MetricBusinessWalletsCreated,
			metric.WithDescription("Total number of successfully created customer/merchant wallets"),
		)
		if err != nil {
			panic("failed to initialize business wallets created metric: " + err.Error())
		}

		businessDepositsTotal, err = meter.Int64Counter(
			MetricBusinessDeposits,
			metric.WithDescription("Total number of deposit requests processed"),
		)
		if err != nil {
			panic("failed to initialize business deposits metric: " + err.Error())
		}

		businessDepositsAmount, err = meter.Int64Counter(
			MetricBusinessDepositsAmount,
			metric.WithDescription("Total amount deposited from fiat vault into wallets"),
		)
		if err != nil {
			panic("failed to initialize business deposits amount metric: " + err.Error())
		}
	})
}

// RecordBusinessGTV records the amount of a successful transfer (Gross Transaction Value).
func RecordBusinessGTV(ctx context.Context, amount int64, currency string) {
	businessGTVTotal.Add(ctx, amount, metric.WithAttributes(
		attribute.String(MetricLabelCurrency, currency),
	))
}

// RecordBusinessTransfer records a transfer outcome for TSR computation.
func RecordBusinessTransfer(ctx context.Context, status, shardType string) {
	businessTransfersTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelStatus, status),
		attribute.String(MetricLabelShardType, shardType),
	))
}

// RecordBusinessDecline records a declined transfer.
func RecordBusinessDecline(ctx context.Context, reason string) {
	businessDeclinesTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelReason, reason),
	))
}

// RecordBusinessSagaDuration records the duration of a cross-shard saga from initiation to completion/failure.
func RecordBusinessSagaDuration(ctx context.Context, duration time.Duration) {
	businessSagaDuration.Record(ctx, duration.Seconds())
}

// RecordBusinessSagaInitiated records a saga initiation.
func RecordBusinessSagaInitiated(ctx context.Context) {
	businessSagaInitiated.Add(ctx, 1)
}

// RecordBusinessSagaCompleted records a successful saga completion.
func RecordBusinessSagaCompleted(ctx context.Context) {
	businessSagaCompleted.Add(ctx, 1)
}

// RecordBusinessRefund records a refund processed.
func RecordBusinessRefund(ctx context.Context, currency string) {
	businessRefundsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelCurrency, currency),
	))
}

// RecordBusinessDispute records a dispute event.
func RecordBusinessDispute(ctx context.Context, disputeStatus string) {
	businessDisputesTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelDisputeStatus, disputeStatus),
	))
}

// RecordWalletCreated records a newly created wallet.
func RecordWalletCreated(ctx context.Context, merchantID, currency string) {
	businessWalletsCreated.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelMerchantID, merchantID),
		attribute.String(MetricLabelCurrency, currency),
	))
}

// RecordDepositRequest records a deposit request status and amount.
func RecordDepositRequest(ctx context.Context, merchantID, currency string, amount int64, status string) {
	businessDepositsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String(MetricLabelMerchantID, merchantID),
		attribute.String(MetricLabelCurrency, currency),
		attribute.String(MetricLabelStatus, status),
	))
	if status == TransferMetricSuccess {
		businessDepositsAmount.Add(ctx, amount, metric.WithAttributes(
			attribute.String(MetricLabelMerchantID, merchantID),
			attribute.String(MetricLabelCurrency, currency),
		))
	}
}
