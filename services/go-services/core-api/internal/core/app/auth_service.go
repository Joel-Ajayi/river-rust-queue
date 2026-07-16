package app

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/port"
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
