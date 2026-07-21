package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

type MerchantService struct {
	merchantsStore port.MerchantStore
	hashRing       *platform.HashRing
}

func NewMerchantService(mStore port.MerchantStore, ring *platform.HashRing) *MerchantService {
	return &MerchantService{
		merchantsStore: mStore,
		hashRing:       ring,
	}
}

// CreateMerchant registers a new merchant, determines their shard, and generates API keys.
func (s *MerchantService) CreateMerchant(ctx context.Context, name, webhookURL, webhookSecret, tier string) (string, string, string, error) {
	if name == "" {
		return "", "", "", domain.ErrInvalidBody
	}

	merchantID := platform.NewMerchantID()
	shardID := s.hashRing.ShardFor(merchantID)

	// Generate random bytes for secure API key secret.
	secretBytes := make([]byte, platform.APIKeySecretLength)
	if _, err := rand.Read(secretBytes); err != nil {
		return "", "", "", err
	}
	secretHex := hex.EncodeToString(secretBytes)

	apiKeyPlain := fmt.Sprintf(platform.APIKeyFormat, merchantID, secretHex)
	apiKeyHash, err := platform.HashAPIKeySecret(secretHex)
	if err != nil {
		return "", "", "", err
	}

	err = s.merchantsStore.Create(
		ctx,
		merchantID,
		name,
		apiKeyHash,
		platform.MerchantTierStandard,
		platform.MerchantStatusActive,
		shardID,
		webhookURL,
		webhookSecret,
	)
	if err != nil {
		return "", "", "", err
	}

	return merchantID, apiKeyPlain, shardID, nil
}

func (s *MerchantService) Authenticate(ctx context.Context, apiKey string) (domain.Principal, error) {
	return s.merchantsStore.AuthenticateAPIKey(ctx, apiKey)
}
