package app

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/port"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

var _ port.Authenticator = (*AuthService)(nil)

type AuthService struct {
	merchantsDir port.MerchantDirectory
}

func NewAuthService(mDir port.MerchantDirectory) *AuthService {
	return &AuthService{
		merchantsDir: mDir,
	}
}

func (s *AuthService) Authenticate(ctx context.Context, apiKey string) (domain.Principal, error) {
	return s.merchantsDir.AuthenticateAPIKey(ctx, apiKey)
}

func (s *AuthService) GetPrincipal(ctx context.Context, merchantID string) (domain.Principal, error) {
	// Check active status
	_, err := s.merchantsDir.ShardFor(ctx, merchantID)
	if err != nil {
		return domain.Principal{}, err
	}

	return domain.Principal{MerchantID: merchantID, Status: string(platform.MerchantStatusActive)}, nil
}
