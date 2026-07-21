package pii_test

import (
	"testing"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform/pii"
	"github.com/stretchr/testify/assert"
)

// pan and card_number are in the sensitiveKeys map, so they get fully masked.
func TestMask_PAN_FullyMasked(t *testing.T) {
	out := pii.Mask([]byte(`{"pan": "4111111111111111"}`))
	assert.Contains(t, string(out), "***MASKED***")
	assert.NotContains(t, string(out), "4111111111111111")
}

func TestMask_CardNumber_FullyMasked(t *testing.T) {
	out := pii.Mask([]byte(`{"card_number": "1234567890123456"}`))
	assert.Contains(t, string(out), "***MASKED***")
	assert.NotContains(t, string(out), "1234567890123456")
}

func TestMask_Email_Obfuscates(t *testing.T) {
	out := pii.Mask([]byte(`{"email": "john.doe@example.com"}`))
	assert.NotContains(t, string(out), "john.doe")
	assert.NotContains(t, string(out), "example.com")
	assert.Contains(t, string(out), "***")
}

func TestMask_Phone_Obfuscates(t *testing.T) {
	out := pii.Mask([]byte(`{"phone": "1234567890"}`))
	assert.NotContains(t, string(out), "345678")
}

func TestMask_SensitiveKeys_Replaced(t *testing.T) {
	out := pii.Mask([]byte(`{"password": "hunter2", "api_key": "sk-abc12345"}`))
	assert.NotContains(t, string(out), "hunter2")
	assert.NotContains(t, string(out), "sk-abc12345")
	assert.Contains(t, string(out), "***MASKED***")
}

func TestMask_NestedObject_Masked(t *testing.T) {
	out := pii.Mask([]byte(`{"user": {"email": "test@test.com"}}`))
	assert.NotContains(t, string(out), "test@test.com")
}

func TestMask_NonJSON_ReturnsMasked(t *testing.T) {
	out := pii.Mask([]byte(`not json at all`))
	assert.Contains(t, string(out), "masked")
}

// cvv is in sensitiveKeys, so it is fully masked.
func TestMask_CVV_FullyMasked(t *testing.T) {
	out := pii.Mask([]byte(`{"cvv": "123"}`))
	assert.Contains(t, string(out), "***MASKED***")
}

// security_code is in sensitiveKeys, so it is fully masked.
func TestMask_SecurityCode_FullyMasked(t *testing.T) {
	out := pii.Mask([]byte(`{"security_code": "456"}`))
	assert.Contains(t, string(out), "***MASKED***")
}

func TestMask_Secret_FullyMasked(t *testing.T) {
	out := pii.Mask([]byte(`{"secret": "my-secret-value"}`))
	assert.Contains(t, string(out), "***MASKED***")
}

func TestMask_PrivateKey_FullyMasked(t *testing.T) {
	out := pii.Mask([]byte(`{"private_key": "-----BEGIN RSA PRIVATE KEY-----\\nABC"}`))
	assert.Contains(t, string(out), "***MASKED***")
}
