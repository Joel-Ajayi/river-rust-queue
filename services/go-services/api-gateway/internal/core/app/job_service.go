package app

import (
	"context"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/port"
)

var _ port.JobReader = (*JobService)(nil)

type JobService struct {
	merchantsDir port.MerchantDirectory
	jobs         port.JobStore
}

func NewJobService(mDir port.MerchantDirectory, jobs port.JobStore) *JobService {
	return &JobService{
		merchantsDir: mDir,
		jobs:         jobs,
	}
}

func (s *JobService) GetJobStatus(ctx context.Context, merchantID, jobID string) (domain.Job, error) {
	shard, err := s.merchantsDir.ShardFor(ctx, merchantID)
	if err != nil {
		return domain.Job{}, err
	}

	job, err := s.jobs.GetJob(ctx, shard, jobID)
	if err != nil {
		return domain.Job{}, err
	}

	if job.MerchantID != merchantID {
		return domain.Job{}, domain.ErrJobNotFound
	}

	return job, nil
}
