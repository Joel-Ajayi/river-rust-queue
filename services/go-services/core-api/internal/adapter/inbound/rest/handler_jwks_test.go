package rest

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
)

func TestHandleJWKS(t *testing.T) {
	_, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate ed25519 key: %v", err)
	}

	keys := map[string]ed25519.PrivateKey{
		"test-key-id": privKey,
	}

	srv := &Server{
		jwtKeys:        keys,
		jwtActiveKeyID: "test-key-id",
	}

	req := httptest.NewRequest(http.MethodGet, "/.well-known/jwks.json", nil)
	rec := httptest.NewRecorder()

	srv.handleJWKS(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var jwks domain.JWKS
	if err := json.Unmarshal(rec.Body.Bytes(), &jwks); err != nil {
		t.Fatalf("failed to unmarshal jwks response: %v", err)
	}

	if len(jwks.Keys) != 1 {
		t.Fatalf("expected 1 key in jwks, got %d", len(jwks.Keys))
	}

	jwk := jwks.Keys[0]
	if jwk.Kid != "test-key-id" {
		t.Errorf("expected kid 'test-key-id', got '%s'", jwk.Kid)
	}
	if jwk.Kty != "OKP" {
		t.Errorf("expected kty 'OKP', got '%s'", jwk.Kty)
	}
	if jwk.Crv != "Ed25519" {
		t.Errorf("expected crv 'Ed25519', got '%s'", jwk.Crv)
	}
	if jwk.Alg != "EdDSA" {
		t.Errorf("expected alg 'EdDSA', got '%s'", jwk.Alg)
	}
	if jwk.Use != "sig" {
		t.Errorf("expected use 'sig', got '%s'", jwk.Use)
	}
	if jwk.X == "" {
		t.Errorf("expected non-empty X public key coordinate")
	}
}
