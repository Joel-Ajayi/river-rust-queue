package rest

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/Joel-Ajayi/river-rust-queue/go-services/core-api/internal/core/domain"
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
		req.GetSource(),
		int(req.GetLimit()),
	)
	if err != nil {
		writeError(w, err)
		return
	}

	platform.RecordAdminDLQReplayed(r.Context(), "", int64(res.ReplayedCount))

	writeJSON(w, http.StatusOK, &apiv1.ReplayDLQResponse{
		ReplayedCount: int32(res.ReplayedCount),
	})
}

// listDLQRequest is the GET /admin/dlq query params.
type listDLQRequest struct {
	ShardID string
	Source  string
	Status  string
	Limit   int
	Offset  int
}

func parseListDLQ(r *http.Request) listDLQRequest {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get(ParamLimit))
	offset, _ := strconv.Atoi(q.Get(ParamOffset))
	if limit <= 0 {
		limit = platform.DefaultDLQBatchLimit
	}
	return listDLQRequest{
		ShardID: q.Get(ParamShardID),
		Source:  q.Get(ParamSource),
		Status:  q.Get(ParamStatus),
		Limit:   limit,
		Offset:  offset,
	}
}

// handleAdminDLQList lists DLQ entries for operator review + selective replay.
func (s *Server) handleAdminDLQList(w http.ResponseWriter, r *http.Request) {
	req := parseListDLQ(r)

	entries, err := s.admin.ListDLQEntries(r.Context(), req.Source, req.Status, req.Limit, req.Offset)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, entries)
}

// replayOneRequest is the POST /admin/dlq/replay-one body.
type replayOneRequest struct {
	ShardID string `json:"shard_id"`
	Source  string `json:"source"`
	DLQID   string `json:"dlq_id"`
}

// handleAdminDLQReplayOne republishes a single DLQ entry chosen by the operator.
func (s *Server) handleAdminDLQReplayOne(w http.ResponseWriter, r *http.Request) {
	var req replayOneRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, platform.ErrInvalidBody(err))
		return
	}
	if req.DLQID == "" {
		writeError(w, platform.ErrValidation(ParamDLQID, domain.ErrMissingDLQID))
		return
	}

	res, err := s.admin.ReplayDLQEntry(r.Context(), req.Source, req.DLQID)
	if err != nil {
		writeError(w, err)
		return
	}
	platform.RecordAdminDLQReplayed(r.Context(), "", 1)
	writeJSON(w, http.StatusOK, res)
}
