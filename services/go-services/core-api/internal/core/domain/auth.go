package domain

// JWK represents a single JSON Web Key per RFC 7517 and RFC 8037 (Ed25519).
type JWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv,omitempty"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	X   string `json:"x,omitempty"`
}

type JWKS struct {
	Keys []JWK `json:"keys"`
}
