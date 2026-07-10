package rest_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/adapter/inbound/rest"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

func TestGetJobStatus(t *testing.T) {
	handler, tokenStr, shardA, mID := setupEnvironment(t)

	// Seed some jobs directly in the database
	now := time.Now()
	_, err := shardA.Pool.Exec(context.Background(), `
		INSERT INTO jobs (id, merchant_id, idempotency_key, type, request_hash, status, created_at) VALUES 
		('11111111-1111-1111-1111-111111111111', $2, 'idem_j1', 'transfer', 'hash1', 'pending', $1),
		('22222222-2222-2222-2222-222222222222', 'm_999', 'idem_j2', 'transfer', 'hash2', 'pending', $1)
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
			jobID:      "11111111-1111-1111-1111-111111111111",
			authHeader: string(rest.HeaderValBearer) + tokenStr,
			wantStatus: http.StatusOK,
		},
		{
			name:       "NotFound_MissingJob",
			jobID:      "33333333-3333-3333-3333-333333333333",
			authHeader: string(rest.HeaderValBearer) + tokenStr,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "NotFound_OtherMerchantJob",
			jobID:      "22222222-2222-2222-2222-222222222222",
			authHeader: string(rest.HeaderValBearer) + tokenStr,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "Unauthorized_MissingHeader",
			jobID:      "11111111-1111-1111-1111-111111111111",
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", platform.APIJobPathPrefix+tt.jobID, nil)
			if tt.authHeader != "" {
				req.Header.Set(string(rest.HeaderAuthorization), tt.authHeader)
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
				if res["status"] != string(domain.JobStatusPending) {
					t.Errorf("expected status %v, got %v", domain.JobStatusPending, res["status"])
				}
			}
		})
	}
}
