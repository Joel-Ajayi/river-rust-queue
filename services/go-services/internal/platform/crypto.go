package platform

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

const (
	ArgonTime    = 1
	ArgonMemory  = 64 * 1024 // 64 MB
	ArgonThreads = 4
	ArgonKeyLen  = 32
	ArgonSaltLen = 16

	ArgonFormat = "$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s"
)

// HashAPIKey Secret hashes an API key secret using OWASP-recommended Argon2id parameters.
func HashAPIKeySecret(secret string) (string, error) {
	salt := make([]byte, ArgonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}

	hash := argon2.IDKey([]byte(secret), salt, ArgonTime, ArgonMemory, ArgonThreads, ArgonKeyLen)

	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Hash := base64.RawStdEncoding.EncodeToString(hash)

	encoded := fmt.Sprintf(ArgonFormat, argon2.Version, ArgonMemory, ArgonTime, ArgonThreads, b64Salt, b64Hash)
	return encoded, nil
}

// CompareAPIKeySecret verifies an API key secret against its Argon2id hash.
func CompareAPIKeySecret(hash, secret string) bool {
	if !strings.HasPrefix(hash, "$argon2id$") {
		return false
	}

	parts := strings.Split(hash, "$")
	if len(parts) != 6 {
		return false
	}

	var version int
	var memory uint32
	var timeCost uint32
	var threads uint8

	_, err := fmt.Sscanf(parts[2], "v=%d", &version)
	if err != nil {
		return false
	}

	_, err = fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &timeCost, &threads)
	if err != nil {
		return false
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false
	}

	decodedHash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false
	}

	comparisonHash := argon2.IDKey([]byte(secret), salt, timeCost, memory, threads, uint32(len(decodedHash)))

	return subtle.ConstantTimeCompare(decodedHash, comparisonHash) == 1
}
