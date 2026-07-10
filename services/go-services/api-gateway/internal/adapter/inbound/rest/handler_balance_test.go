package rest_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/adapter/inbound/rest"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

func TestGetBalance(t *testing.T) {
	t.Parallel()
	handler, tokenStr, _, mID := setupEnvironment(t)

	tests := []struct {
		name       string
		walletID   string
		authHeader string
		wantStatus int
	}{
		{
			name:       "Success_ReturnsBalance",
			walletID:   mID + ".wal_A",
			authHeader: string(rest.HeaderValBearer) + tokenStr,
			wantStatus: http.StatusOK,
		},
		{
			name:       "MissingWalletID",
			walletID:   "",
			authHeader: string(rest.HeaderValBearer) + tokenStr,
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name:       "ForeignWallet_ReturnsForbidden",
			walletID:   "m_999.wal_foreign",
			authHeader: string(rest.HeaderValBearer) + tokenStr,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "MissingAuthHeader",
			walletID:   mID + ".wal_A",
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
				// Use "walletId" because of protojson
				if res["walletId"] != tt.walletID {
					t.Errorf("expected wallet id %v, got %v", tt.walletID, res["walletId"])
				}
			}
		})
	}
}
