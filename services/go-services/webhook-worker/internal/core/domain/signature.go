package domain

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// ComputeHMACSHA256 computes the HMAC-SHA256 signature for a given payload and timestamp using the provided secret.
func ComputeHMACSHA256(secret string, timestamp string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "."))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}
