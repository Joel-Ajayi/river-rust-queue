package domain

const (
	// Operational Limits
	MaxRequestBytes = 512 * 1024 // 512KB
	// JWT Claims
	ClaimSub  = "sub"
	ClaimIss  = "iss"
	ClaimIat  = "iat"
	ClaimExp  = "exp"
	ClaimTier = "tier"
)
