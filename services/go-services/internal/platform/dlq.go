package platform

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"time"

	eventsv1 "github.com/Joel-Ajayi/river-rust-queue/go-services/internal/gen/proto/rrq/events/v1"
	"github.com/failsafe-go/failsafe-go"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// All consumers write dead-lettered messages via WriteDLQEntry; the core-api
// replayer reads them back. Keeps one marshalling path system-wide.
type DBExecer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// ErrorClassificationText renders a proto ErrorClassification as lowercase text
// for the dlq_entries.error_classification column.
func ErrorClassificationText(c eventsv1.ErrorClassification) string {
	switch c {
	case eventsv1.ErrorClassification_ERROR_CLASSIFICATION_POISON:
		return string(ClassificationPoison)
	case eventsv1.ErrorClassification_ERROR_CLASSIFICATION_TRANSIENT:
		return string(ClassificationTransient)
	case eventsv1.ErrorClassification_ERROR_CLASSIFICATION_TERMINAL:
		return string(ClassificationTerminal)
	case eventsv1.ErrorClassification_ERROR_CLASSIFICATION_INFRASTRUCTURE:
		return string(ClassificationInfrastructure)
	default:
		return string(ClassificationInfrastructure)
	}
}

// classificationFromPlatform maps a platform ErrorClassification to the proto
// enum counterpart.
func classificationFromPlatform(c ErrorClassification) eventsv1.ErrorClassification {
	switch c {
	case ClassificationPoison:
		return eventsv1.ErrorClassification_ERROR_CLASSIFICATION_POISON
	case ClassificationTransient:
		return eventsv1.ErrorClassification_ERROR_CLASSIFICATION_TRANSIENT
	case ClassificationTerminal:
		return eventsv1.ErrorClassification_ERROR_CLASSIFICATION_TERMINAL
	case ClassificationInfrastructure:
		return eventsv1.ErrorClassification_ERROR_CLASSIFICATION_INFRASTRUCTURE
	default:
		return eventsv1.ErrorClassification_ERROR_CLASSIFICATION_UNSPECIFIED
	}
}

// DLQStatusText renders a proto DLQStatus as the text stored in the status column.
func DLQStatusText(s eventsv1.DLQStatus) string {
	switch s {
	case eventsv1.DLQStatus_DLQ_STATUS_OPEN:
		return DLQStatusOpen
	case eventsv1.DLQStatus_DLQ_STATUS_REPLAYED:
		return DLQStatusReplayed
	case eventsv1.DLQStatus_DLQ_STATUS_RESOLVED:
		return DLQStatusResolved
	default:
		return DLQStatusOpen
	}
}

// timestampPtr converts a time.Time to a proto Timestamp, returning nil for the
// zero value so the column stays NULL when the caller has no timestamp.
func timestampPtr(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// hashDLQID produces a compact, deterministic id from message identity. It is
// derived, never random: redelivered messages upsert (ON CONFLICT).
func hashDLQID(s string) string {
	h := sha1.Sum([]byte(s))
	return DLQIDPrefix + hex.EncodeToString(h[:8])
}

// DeterministicDLQID derives a stable DLQ id from message identity. `origin` must
// carry per-message identity (Kafka pos, or envelope event id) — NOT keys only.
func DeterministicDLQID(source, origin string) string {
	return hashDLQID(source + DLQOriginSep + origin)
}

// DLQEntryOrigin derives a stable, per-message origin id from a message payload,
// falling back to "topic|key" when not a recognizable envelope. Prefers EventId.
func DLQEntryOrigin(payload []byte, topic, key string) string {
	if env, err := UnmarshalEnvelope(payload); err == nil && env.GetEventId() != "" {
		return env.GetEventId()
	}
	return topic + DLQOriginSep + key
}

// NewDLQEntry builds an open, un-attempted DLQ entry from a failed message. If
// `origin` is non-empty the deterministic id is assigned at construction.
func NewDLQEntry(source, topic, key string, payload []byte, origin string, errorMsg string, classification ErrorClassification, firstFailedAt, lastFailedAt time.Time, traceID, spanID string) *eventsv1.DLQEntry {
	e := eventsv1.DLQEntry{
		Source:              source,
		SourceTopic:         topic,
		OriginalKey:         key,
		OriginalPayload:     payload,
		ErrorMessage:        errorMsg,
		ErrorClassification: classificationFromPlatform(classification),
		AttemptCount:        0,
		FirstFailedAt:       timestampPtr(firstFailedAt),
		LastFailedAt:        timestampPtr(lastFailedAt),
		Status:              eventsv1.DLQStatus_DLQ_STATUS_OPEN,
		TraceId:             traceID,
		SpanId:              spanID,
	}
	if origin != "" {
		e.Id = DeterministicDLQID(source, origin)
	}
	return &e
}

// WriteDLQEntry persists a DLQ entry, upserting on id and bumping attempt_count
// on conflict. entry.Id MUST be set by the caller; empty is rejected.
func WriteDLQEntry(ctx context.Context, db DBExecer, entry *eventsv1.DLQEntry) error {
	if entry == nil {
		return fmt.Errorf(DLQErrorPrefix + " nil entry")
	}
	if entry.Id == "" {
		return fmt.Errorf(DLQErrorPrefix + " entry Id is empty; caller must set a deterministic id before writing")
	}

	_, err := db.Exec(ctx, `
			INSERT INTO dlq_entries (
				id, source, source_topic, original_key, original_payload,
				error_message, error_classification, attempt_count,
				first_failed_at, last_failed_at, status, trace_id, span_id
			) VALUES (
				$1, $2, NULLIF($3, ''), NULLIF($4, ''), $5,
				$6, $7, $8, $9, $10, $11, NULLIF($12, ''), NULLIF($13, '')
			)
			ON CONFLICT (id) DO UPDATE SET
				error_message        = EXCLUDED.error_message,
				error_classification = EXCLUDED.error_classification,
				attempt_count        = dlq_entries.attempt_count + 1,
				last_failed_at       = EXCLUDED.last_failed_at,
				status               = EXCLUDED.status`,
		entry.Id,
		entry.Source,
		entry.SourceTopic,
		entry.OriginalKey,
		entry.OriginalPayload,
		entry.ErrorMessage,
		ErrorClassificationText(entry.ErrorClassification),
		entry.AttemptCount,
		entry.FirstFailedAt,
		entry.LastFailedAt,
		DLQStatusText(entry.Status),
		entry.TraceId,
		entry.SpanId,
	)
	if err != nil {
		return fmt.Errorf(DLQErrorPrefix+" write entry %s: %w", entry.Id, err)
	}
	return nil
}

// ReadDLQEntries returns open DLQ entries for replay from a DB pool.
func ReadDLQEntries(ctx context.Context, pool *pgxpool.Pool, source string, limit int) ([]*eventsv1.DLQEntry, error) {
	rows, err := pool.Query(ctx, `
			SELECT id, source, source_topic, original_key, original_payload, attempt_count
			FROM dlq_entries
			WHERE status = $1 AND ($2 = '' OR source = $2)
			ORDER BY created_at ASC
			LIMIT $3`,
		DLQStatusOpen, source, limit)
	if err != nil {
		return nil, fmt.Errorf(DLQErrorPrefix+" query entries: %w", err)
	}
	defer rows.Close()

	var out []*eventsv1.DLQEntry
	for rows.Next() {
		e := &eventsv1.DLQEntry{}
		var attempts int
		if err := rows.Scan(&e.Id, &e.Source, &e.SourceTopic, &e.OriginalKey, &e.OriginalPayload, &attempts); err != nil {
			return nil, fmt.Errorf(DLQErrorPrefix+" scan entry: %w", err)
		}
		e.AttemptCount = int32(attempts)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(DLQErrorPrefix+" iterate entries: %w", err)
	}
	return out, nil
}

// WriteDLQEntryWithRetry persists a DLQ entry with the service RetryConfig, centralizing
// DLQ-write failure handling here. Returns on exhaustion (caller MUST not ack → redeliver).
func WriteDLQEntryWithRetry(ctx context.Context, logger *zap.Logger, db DBExecer, entry *eventsv1.DLQEntry, retryCfg RetryConfig, component string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf(DLQErrorPrefix+" write aborted, context already done: %w", err)
	}

	err := ExecuteWithJitter(ctx, retryCfg, func(_ failsafe.Execution[any]) error {
		return WriteDLQEntry(ctx, db, entry)
	})
	if err != nil {
		RecordInfrastructureError(ctx, component)
		LoggerWithTrace(ctx, logger).Warn(LogEventDLQWriteExhausted,
			zap.Error(err),
			zap.String(LogFieldDLQID, entry.GetId()),
			zap.String(MetricLabelComponent, component),
		)
		return err // propagate -> caller must NOT ack the source message (no-data-loss)
	}

	RecordDLQIngestion(ctx, component)
	return nil
}

// DLQEntrySummary is the operator-facing view of a DLQ entry returned by the admin
// list/detail endpoints. original_payload is intentionally omitted (large/PII).
type DLQEntrySummary struct {
	ID            string
	Source        string
	SourceTopic   string
	OriginalKey   string
	ErrorMessage  string
	ErrorClass    string
	AttemptCount  int
	FirstFailedAt time.Time
	LastFailedAt  time.Time
	Status        string
	TraceID       string
	ShardID       string
}

const queryListDLQEntries = `
		SELECT id, source, source_topic, original_key, error_message,
		       error_classification, attempt_count, first_failed_at, last_failed_at,
		       status, trace_id
		FROM dlq_entries
		WHERE ($1 = '' OR source = $1)
		  AND ($2 = '' OR status = $2)
		ORDER BY created_at DESC
		LIMIT $3 OFFSET $4`

// ListDLQEntries returns DLQ entries newest-first for operator review. The caller
// scopes `pool` to the relevant shard/global pool.
func ListDLQEntries(ctx context.Context, pool *pgxpool.Pool, source, status string, limit, offset int) ([]DLQEntrySummary, error) {
	rows, err := pool.Query(ctx, queryListDLQEntries, source, status, limit, offset)
	if err != nil {
		return nil, fmt.Errorf(DLQErrorPrefix+" query list: %w", err)
	}
	defer rows.Close()

	out := make([]DLQEntrySummary, 0, limit)
	for rows.Next() {
		s := DLQEntrySummary{}
		var traceID *string
		var attempts int
		if err := rows.Scan(&s.ID, &s.Source, &s.SourceTopic, &s.OriginalKey, &s.ErrorMessage,
			&s.ErrorClass, &attempts, &s.FirstFailedAt, &s.LastFailedAt, &s.Status, &traceID); err != nil {
			return nil, fmt.Errorf(DLQErrorPrefix+" scan list row: %w", err)
		}
		s.AttemptCount = attempts
		if traceID != nil {
			s.TraceID = *traceID
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(DLQErrorPrefix+" iterate list: %w", err)
	}
	return out, nil
}

// DLQReplayTarget holds the columns needed to republish a DLQ entry. Nullable
// text columns are scanned as *string then dereferenced.
type DLQReplayTarget struct {
	ID              string
	Source          string
	SourceTopic     string
	OriginalKey     string
	OriginalPayload []byte
}

const queryGetDLQReplayTarget = `
		SELECT id, source, source_topic, original_key, original_payload
		FROM dlq_entries
		WHERE id = $1 AND status = $2`

// GetDLQReplayTarget fetches the fields needed to republish one open entry.
func GetDLQReplayTarget(ctx context.Context, pool *pgxpool.Pool, id string) (DLQReplayTarget, error) {
	var t DLQReplayTarget
	var topicNS, keyNS *string
	if err := pool.QueryRow(ctx, queryGetDLQReplayTarget, id, DLQStatusOpen).Scan(
		&t.ID, &t.Source, &topicNS, &keyNS, &t.OriginalPayload,
	); err != nil {
		return DLQReplayTarget{}, fmt.Errorf(DLQErrorPrefix+" get %s: %w", id, err)
	}
	if topicNS != nil {
		t.SourceTopic = *topicNS
	}
	if keyNS != nil {
		t.OriginalKey = *keyNS
	}
	return t, nil
}

const queryMarkDLQReplayedByID = `
		UPDATE dlq_entries SET status = $1, replayed_at = NOW()
		WHERE id = $2 AND status = $3`

// MarkDLQReplayedByID flips an open entry to the replayed status (idempotent on id).
func MarkDLQReplayedByID(ctx context.Context, db DBExecer, id string) error {
	ct, err := db.Exec(ctx, queryMarkDLQReplayedByID, DLQStatusReplayed, id, DLQStatusOpen)
	if err != nil {
		return fmt.Errorf(DLQErrorPrefix+" mark replayed %s: %w", id, err)
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf(DLQErrorPrefix+" no open entry %q to mark replayed", id)
	}
	return nil
}

// ResolveReplayTopic maps source/source_topic to the Kafka topic for republish.
// source_topic is authoritative; the fallback covers every DLQSource constant.
func ResolveReplayTopic(source, sourceTopic string) string {
	if sourceTopic != "" {
		return sourceTopic
	}
	switch source {
	case DLQSourceWebhook:
		return TopicNotify
	case DLQSourceLedger, DLQSourceFraud, DLQSourceOutbox:
		return TopicJobs
	default:
		return TopicJobs
	}
}
