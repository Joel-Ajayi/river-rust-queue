package rest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/inbound/rest"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
)

type MockAdminUseCase struct {
	mock.Mock
}

func (m *MockAdminUseCase) ReplayDLQ(ctx context.Context, shardID string, source string, limit int) (domain.DLQReplayResult, error) {
	args := m.Called(ctx, shardID, source, limit)
	return args.Get(0).(domain.DLQReplayResult), args.Error(1)
}

func TestHandlerAdmin_DLQReplay_Unauthorized(t *testing.T) {
	t.Parallel()

	srv := rest.NewServer(8080, nil, "default", nil, nil, nil, nil, nil, nil, zap.NewNop())

	req := httptest.NewRequest(http.MethodPost, platform.APIAdminDLQReplayPath, nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestHandlerAdmin_DLQReplay_Success(t *testing.T) {
	t.Parallel()

	mockAdmin := new(MockAdminUseCase)
	expectedRes := domain.DLQReplayResult{
		ReplayedCount: 5,
		ShardID:       "shard-a",
	}
	mockAdmin.On("ReplayDLQ", mock.Anything, "shard-a", "webhook", 50).Return(expectedRes, nil)

	srv := rest.NewServer(8080, nil, "default", nil, nil, nil, nil, mockAdmin, nil, zap.NewNop())

	pbReq := &apiv1.ReplayDLQRequest{
		ShardId: "shard-a",
		Source:  "webhook",
		Limit:   50,
	}
	jsonBytes, err := protojson.Marshal(pbReq)
	assert.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, platform.APIAdminDLQReplayPath, bytes.NewReader(jsonBytes))
	req.Header.Set(rest.HeaderMerchantID, "admin_merchant_1")
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	err = json.Unmarshal(rec.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, float64(5), resp["replayedCount"])
	assert.Equal(t, "shard-a", resp["shardId"])

	mockAdmin.AssertExpectations(t)
}
