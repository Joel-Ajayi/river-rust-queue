package rest_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/adapter/inbound/rest"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

func TestGetJobStatus(t *testing.T) {
	handler, tokenStr, shardA := setupEnvironment(t)

	// Seed some jobs directly in the database
	now := time.Now()
	_, err := shardA.Pool.Exec(context.Background(), `
		INSERT INTO jobs (id, merchant_id, idempotency_key, type, request_hash, status, created_at) VALUES 
		('job_test_1', 'm_123', 'idem_j1', 'transfer', 'hash1', 'pending', $1),
		('job_other_merch', 'm_999', 'idem_j2', 'transfer', 'hash2', 'pending', $1)
	`, now)
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
			jobID:      "job_test_1",
			authHeader: string(rest.HeaderValBearer) + tokenStr,
			wantStatus: http.StatusOK,
		},
		{
			name:       "NotFound_MissingJob",
			jobID:      "job_not_exist",
			authHeader: string(rest.HeaderValBearer) + tokenStr,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "NotFound_OtherMerchantJob",
			jobID:      "job_other_merch",
			authHeader: string(rest.HeaderValBearer) + tokenStr,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "Unauthorized_MissingHeader",
			jobID:      "job_test_1",
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", platform.APIJobPathPrefix+tt.jobID, nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
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
				if res["job_id"] != tt.jobID {
					t.Errorf("expected job id %v, got %v", tt.jobID, res["job_id"])
				}
				if res["status"] != platform.JobStatusPending {
					t.Errorf("expected status %v, got %v", platform.JobStatusPending, res["status"])
				}
			}
		})
	}
}
