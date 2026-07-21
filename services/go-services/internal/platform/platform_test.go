package platform_test

import (
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/stretchr/testify/assert"
)

func TestIDGen_NewTransferID_Unique(t *testing.T) {
	id1 := platform.NewTransferID()
	id2 := platform.NewTransferID()
	assert.NotEqual(t, id1, id2)
	assert.Contains(t, id1, "transfer_")
	assert.Greater(t, len(id1), 26)
}

func TestIDGen_NewMerchantID_Unique(t *testing.T) {
	id1 := platform.NewMerchantID()
	id2 := platform.NewMerchantID()
	assert.NotEqual(t, id1, id2)
	assert.Greater(t, len(id1), 0)
}

func TestIDGen_NewJobID_Unique(t *testing.T) {
	id1 := platform.NewJobID()
	id2 := platform.NewJobID()
	assert.NotEqual(t, id1, id2)
}

func TestIDGen_NewEventID_Unique(t *testing.T) {
	id1 := platform.NewEventID()
	id2 := platform.NewEventID()
	assert.NotEqual(t, id1, id2)
}

func TestJobType_Transfer_IsCorrect(t *testing.T) {
	assert.Equal(t, "transfer", platform.JobTypeTransfer)
}

func TestMerchantStatus_Active_IsCorrect(t *testing.T) {
	assert.Equal(t, "active", platform.MerchantStatusActive)
}

func TestShardStatus_Active_IsCorrect(t *testing.T) {
	assert.Equal(t, "active", platform.ShardStatusActive)
}

func TestDLQStatus_Open_IsCorrect(t *testing.T) {
	assert.Equal(t, "open", platform.DLQStatusOpen)
}

func TestServiceNames_NotEmpty(t *testing.T) {
	assert.NotEmpty(t, platform.ServiceNameCoreAPI)
	assert.NotEmpty(t, platform.ServiceNameLedgerWorker)
	assert.NotEmpty(t, platform.ServiceNameOutboxRelay)
	assert.NotEmpty(t, platform.ServiceNameWebhookWorker)
	assert.NotEmpty(t, platform.ServiceNameFraudWorker)
	assert.NotEmpty(t, platform.ServiceNameReconWorker)
}

func TestShardTypes_NotEqual(t *testing.T) {
	assert.NotEqual(t, platform.ShardTypeSame, platform.ShardTypeCross)
}

func TestTransferMetrics_NotEqual(t *testing.T) {
	assert.NotEqual(t, platform.TransferMetricSuccess, platform.TransferMetricFailed)
	assert.NotEqual(t, platform.TransferMetricFailed, platform.TransferMetricPending)
}

func TestDeclineReasons_NotEmpty(t *testing.T) {
	assert.NotEmpty(t, platform.DeclineReasonInsufficientBalance)
	assert.NotEmpty(t, platform.DeclineReasonWalletFrozen)
	assert.NotEmpty(t, platform.DeclineReasonUnknown)
}
