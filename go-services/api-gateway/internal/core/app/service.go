// Package app implements the gateway's use-cases (Clean Architecture's
// interactors). It orchestrates domain objects and driven ports; it contains no
// transport code and no SQL, so its logic is unit-testable without a database.
package app

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/port"
)

// Service implements the inbound ports (TransferSubmitter, Authenticator) by
// composing the outbound ports it is given. Dependencies are injected, never
// constructed here — that is the composition root's job (cmd/main.go).
type Service struct {
	dir   port.MerchantDirectory
	jobs  port.JobStore
	newID func() string
}

// Compile-time proof that Service satisfies the driving ports.
var (
	_ port.TransferSubmitter = (*Service)(nil)
	_ port.Authenticator     = (*Service)(nil)
)

// NewService wires the use-cases to their driven ports. newID mints job ids.
func NewService(dir port.MerchantDirectory, jobs port.JobStore, newID func() string) *Service {
	return &Service{dir: dir, jobs: jobs, newID: newID}
}

// Submit accepts a transfer: validate, route to the owning shard, then claim the
// idempotency key while recording the job and its outbox event in one write.
func (s *Service) Submit(ctx context.Context, t domain.Transfer, idempotencyKey string) (domain.SubmitResult, error) {
	if err := t.Validate(); err != nil {
		return domain.SubmitResult{}, err
	}

	shardID, err := s.dir.ShardFor(ctx, t.MerchantID)
	if err != nil {
		return domain.SubmitResult{}, err
	}

	job := domain.Job{ID: s.newID(), Status: "pending"}
	return s.jobs.ClaimAndRecord(ctx, shardID, job, t, idempotencyKey, t.Hash())
}

// Authenticate exchanges a raw API key for the merchant identity behind it.
func (s *Service) Authenticate(ctx context.Context, apiKey string) (domain.Principal, error) {
	return s.dir.AuthenticateAPIKey(ctx, apiKey)
}
