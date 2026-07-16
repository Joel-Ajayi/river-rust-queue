package rest_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/adapter/inbound/rest"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

func TestAuthToken(t *testing.T) {
	t.Parallel()
	handler, _, mID := setupEnvironment(t)

	tests := []struct {
		name       string
		authHeader string
		wantStatus int
	}{
		{
			name:       "Success_ReturnsJWT",
			authHeader: string(rest.HeaderValBearer) + mID + ":secret-123",
			wantStatus: http.StatusOK,
		},
		{
			name:       "InvalidCredentials_BadSecret",
			authHeader: string(rest.HeaderValBearer) + mID + ":wrong-secret",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "InvalidCredentials_UnknownMerchant",
			authHeader: string(rest.HeaderValBearer) + "m_unknown:secret-123",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "MalformedAPIKey",
			authHeader: string(rest.HeaderValBearer) + mID + "-no-colon",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "MissingBearerPrefix",
			authHeader: mID + ":secret-123",
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, platform.APIAuthTokenPath, nil)
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
				// verify JWT is returned
				if _, ok := res["token"]; !ok {
					t.Errorf("expected 'token' in response")
				}
				if _, ok := res["expiresIn"]; !ok {
					t.Errorf("expected 'expiresIn' in response")
				}
			}
		})
	}
}
