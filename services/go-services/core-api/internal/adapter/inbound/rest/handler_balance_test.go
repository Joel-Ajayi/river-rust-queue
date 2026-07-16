package rest_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/inbound/rest"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

func TestGetBalance(t *testing.T) {
	t.Parallel()
	handler, _, mID := setupEnvironment(t)

	walA := ".01905335-9781-7000-8000-000000000001"
	foreignM := "merchant_01905335-9781-7000-8000-000000000003"
	foreignWal := ".01905335-9781-7000-8000-000000000004"

	tests := []struct {
		name       string
		walletID   string
		authHeader string
		wantStatus int
	}{
		{
			name:       "Success_ReturnsBalance",
			walletID:   mID + walA,
			authHeader: mID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MissingWalletID",
			walletID:   "",
			authHeader: mID,
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name:       "ForeignWallet_ReturnsForbidden",
			walletID:   foreignM + foreignWal,
			authHeader: mID,
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name:       "MissingAuthHeader",
			walletID:   mID + walA,
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqURL := platform.APIBalancesPath
			if tt.walletID != "" {
				reqURL += "?wallet_id=" + tt.walletID
			}
			req := httptest.NewRequest(http.MethodGet, reqURL, nil)
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
				// Use "walletId" because of protojson
				if res["walletId"] != tt.walletID {
					t.Errorf("expected wallet id %v, got %v", tt.walletID, res["walletId"])
				}
			}
		})
	}
}
