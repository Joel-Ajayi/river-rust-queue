package pii

import (
	"encoding/json"
	"strings"
)

var (
	sensitiveKeys = map[string]bool{
		"pan":            true,
		"card_number":    true,
		"account_number": true,
		"credit_card":    true,
		"cvv":            true,
		"cvc":            true,
		"security_code":  true,
		"password":       true,
		"secret":         true,
		"api_key":        true,
		"private_key":    true,
		"access_token":   true,
		"refresh_token":  true,
		"webhook_secret": true,
		"hmac_secret":    true,
		"signing_key":    true,
	}
)

func Mask(payload []byte) []byte {
	var obj map[string]interface{}
	if err := json.Unmarshal(payload, &obj); err != nil {
		// Not JSON — mask entire payload as safety
		return []byte(`{"masked": true, "error": "non-json payload"}`)
	}
	maskObject(obj)
	masked, _ := json.Marshal(obj)
	return masked
}

func maskObject(obj map[string]interface{}) {
	for k, v := range obj {
		lowerKey := strings.ToLower(k)
		if sensitiveKeys[lowerKey] {
			obj[k] = "***MASKED***"
			continue
		}

		switch lowerKey {
		case "pan", "card_number", "account_number", "credit_card":
			if s, ok := v.(string); ok {
				obj[k] = maskPAN(s)
			}
		case "cvv", "cvc", "security_code":
			obj[k] = "***"
		case "email":
			if s, ok := v.(string); ok {
				obj[k] = maskEmail(s)
			}
		case "phone", "phone_number", "mobile":
			if s, ok := v.(string); ok {
				obj[k] = maskPhone(s)
			}
		default:
			if nested, ok := v.(map[string]interface{}); ok {
				maskObject(nested)
			} else if arr, ok := v.([]interface{}); ok {
				for _, item := range arr {
					if nested, ok := item.(map[string]interface{}); ok {
						maskObject(nested)
					}
				}
			}
		}
	}
}

func maskPAN(s string) string {
	s = strings.TrimSpace(s)
	if len(s) < 8 {
		return "****"
	}
	return s[:4] + strings.Repeat("*", len(s)-8) + s[len(s)-4:]
}

func maskEmail(s string) string {
	parts := strings.Split(s, "@")
	if len(parts) != 2 {
		return "***@***"
	}
	return maskString(parts[0], 2) + "@" + maskString(parts[1], 2)
}

func maskPhone(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= 4 {
		return strings.Repeat("*", len(s))
	}
	return s[:2] + strings.Repeat("*", len(s)-4) + s[len(s)-2:]
}

func maskString(s string, visible int) string {
	if len(s) <= visible {
		return strings.Repeat("*", len(s))
	}
	return s[:visible] + strings.Repeat("*", len(s)-visible)
}
