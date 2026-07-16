package rest_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

func TestHealth(t *testing.T) {
	t.Parallel()
	handler, _, _ := setupEnvironment(t)

	req := httptest.NewRequest(http.MethodGet, platform.APIHealthPath, nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected status %v, got %v", http.StatusOK, rr.Code)
	}

	var res map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if res["status"] != "ok" {
		t.Errorf("expected status ok, got %v", res["status"])
	}
}

func TestReady(t *testing.T) {
	t.Parallel()
	handler, _, _ := setupEnvironment(t)

	req := httptest.NewRequest(http.MethodGet, platform.APIReadyPath, nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected status %v, got %v", http.StatusOK, rr.Code)
	}

	var res map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if res["status"] != "ready" {
		t.Errorf("expected status ready, got %v", res["status"])
	}
}
