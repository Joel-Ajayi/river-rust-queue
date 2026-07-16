package rest

import (
	"net/http"
	"time"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	principal, ok := r.Context().Value(ContextPrincipal).(domain.Principal)
	if !ok {
		writeError(w, platform.ErrUnauthorized(domain.ErrMissingAuthContext))
		return
	}

	jobID := r.PathValue(ParamJobID)
	if !platform.IsValidJobID(jobID) {
		writeError(w, platform.ErrValidation(ParamJobID, domain.ErrMsgInValidJobID))
		return
	}

	// Get job status inside the Layer 1 retry boundary.
	var job domain.Job
	err := retryBoundary(r.Context(), func() error {
		var fnErr error
		job, fnErr = s.jobs.GetJobStatus(r.Context(), principal.MerchantID, jobID)
		return fnErr
	})
	if err != nil {
		writeError(w, mapHTTPError(err))
		return
	}

	res := &apiv1.GetJobResponse{
		JobId:     job.ID,
		Type:      string(job.Type),
		Status:    string(job.Status),
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
