package rest

import (
	"crypto/ed25519"
	"encoding/base64"
	"net/http"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
)

// handleJWKS exposes RFC 7517 / RFC 8037 public keys for Envoy Gateway edge authentication.
func (s *Server) handleJWKS(w http.ResponseWriter, r *http.Request) {
	keys := make([]domain.JWK, 0, len(s.jwtKeys))

	for kid, privKey := range s.jwtKeys {
		if len(privKey) == 0 {
			continue
		}
		pubKey := privKey.Public().(ed25519.PublicKey)
		xStr := base64.RawURLEncoding.EncodeToString(pubKey)

		keys = append(keys, domain.JWK{
			Kty: "OKP",
			Crv: "Ed25519",
			Use: "sig",
			Alg: "EdDSA",
			Kid: kid,
			X:   xStr,
		})
	}

	w.Header().Set(ContentType, ApplicationJSON)
	w.Header().Set(HeaderCacheControl, "public, max-age=3600")
	writeJSON(w, http.StatusOK, domain.JWKS{Keys: keys})
}
