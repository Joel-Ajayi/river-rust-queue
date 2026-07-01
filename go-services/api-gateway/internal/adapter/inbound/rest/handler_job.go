package rest

import (
	"net/http"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/api-gateway/internal/core/domain"
	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	principal, ok := r.Context().Value(ContextPrincipal).(domain.Principal)
	if !ok {
		writeError(w, platform.ErrUnauthorized(domain.ErrMissingAuthContext))
		return
	}

	jobID := r.PathValue("id")
	if jobID == "" {
		writeError(w, platform.ErrValidation("id", "job id is required"))
		return
	}

	job, err := s.svc.GetJobStatus(r.Context(), principal.MerchantID, jobID)
	if err != nil {
		writeError(w, err)
		return
	}

	res := &apiv1.GetJobResponse{
		JobId:     job.ID,
		Type:      job.Type,
		Status:    job.Status,
		CreatedAt: job.CreatedAt.Format(time.RFC3339),
	}
	
	if job.CompletedAt != nil {
		res.CompletedAt = job.CompletedAt.Format(time.RFC3339)
	}

	if job.FailureReason != nil {
		res.Failure = &apiv1.FailureDetail{
			Reason: *job.FailureReason,
		}
	}

	writeJSON(w, http.StatusOK, res)
}
