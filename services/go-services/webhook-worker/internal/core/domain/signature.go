package domain

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// ComputeHMACSHA256 computes the HMAC-SHA256 signature for a given payload using the provided secret.
func ComputeHMACSHA256(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyHMACSHA256 compares an expected signature against the payload's computed signature securely.
func VerifyHMACSHA256(secret, payload []byte, expectedSignature string) bool {
	expectedBytes, err := hex.DecodeString(expectedSignature)
	if err != nil {
		return false
	}
	
	mac := hmac.New(sha256.New, secret)
	mac.Write(payload)
	actualBytes := mac.Sum(nil)

	return hmac.Equal(actualBytes, expectedBytes)
}
