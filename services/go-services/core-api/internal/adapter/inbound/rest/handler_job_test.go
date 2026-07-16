package rest_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/inbound/rest"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

func TestGetJobStatus(t *testing.T) {
	handler, shardA, mID := setupEnvironment(t)

	// Seed some jobs directly in the database
	now := time.Now()
	_, err := shardA.Pool.Exec(context.Background(), `
		INSERT INTO jobs (id, merchant_id, idempotency_key, type, request_hash, status, created_at) VALUES 
		('job_01H00000000000000000000001', $2, 'idem_j1', 'transfer', 'hash1', 'pending', $1),
		('job_01H00000000000000000000002', 'merchant_01905335-9781-7000-8000-000000000003', 'idem_j2', 'transfer', 'hash2', 'pending', $1)
	`, now, mID)
	if err != nil {
		t.Fatalf("failed to seed test jobs: %v", err)
	}

	tests := []struct {
		name       string
		jobID      string
		authHeader string
		wantStatus int
	}{
		{
			name:       "Success_ReturnsJob",
			jobID:      "job_01H00000000000000000000001",
			authHeader: mID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "NotFound_MissingJob",
			jobID:      "job_01H00000000000000000000003",
			authHeader: mID,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "NotFound_OtherMerchantJob",
			jobID:      "job_01H00000000000000000000002",
			authHeader: mID,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "Unauthorized_MissingHeader",
			jobID:      "job_01H00000000000000000000001",
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", platform.APIJobPathPrefix+tt.jobID, nil)
			if tt.authHeader != "" {
				req.Header.Set(rest.HeaderKongConsumerCustomID, tt.authHeader)
			}
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Errorf("expected status %v, got %v. Body: %s", tt.wantStatus, rr.Code, rr.Body.String())
			}

			if tt.wantStatus == http.StatusOK {
				var res map[string]interface{}
				if err := json.Unmarshal(rr.Body.Bytes(), &res); err != nil {
					t.Fatalf("failed to unmarshal response: %v", err)
				}
				if res["jobId"] != tt.jobID {
					t.Errorf("expected job id %v, got %v", tt.jobID, res["jobId"])
				}
				if res["status"] != platform.JobStatusPending {
					t.Errorf("expected status %v, got %v", platform.JobStatusPending, res["status"])
				}
			}
		})
	}
}
