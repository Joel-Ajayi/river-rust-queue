package rest

import (
	"net/http"

	apiv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/api/v1"
	"github.com/Joel-Ajayi/river-rust-queue/go-services/internal/platform"
)

// handleAdminDLQReplay processes operator requests to batch replay DLQ entries.
func (s *Server) handleAdminDLQReplay(w http.ResponseWriter, r *http.Request) {
	var req apiv1.ReplayDLQRequest
	if !decodeProtoBody(w, r, &req) {
		return
	}

	res, err := s.admin.ReplayDLQ(
		r.Context(),
		req.GetShardId(),
		req.GetSource(),
		int(req.GetLimit()),
	)
	if err != nil {
		writeError(w, err)
		return
	}

	// Record metrics for replayed messages
	platform.RecordAdminDLQReplayed(r.Context(), res.ShardID, int64(res.ReplayedCount))

	resp := &apiv1.ReplayDLQResponse{
		ReplayedCount: int32(res.ReplayedCount),
		ShardId:       res.ShardID,
	}

	writeJSON(w, http.StatusOK, resp)
}
