package platform

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// pgxQueryTracer emits an OpenTelemetry client span for every SQL statement run
// through the shared pgx pools.
type pgxQueryTracer struct{}

var _ pgx.QueryTracer = pgxQueryTracer{}

// TraceQueryStart starts a client span covering the statement's execution.
func (pgxQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	op := dbOperation(data.SQL)
	if op == "SELECT" {
		return ctx
	}
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "postgresql"),
			attribute.String("db.operation", op),
			attribute.String("db.statement", data.SQL),
		),
	}
	ctx, _ = otel.Tracer(MetricMeterName+".postgres").Start(ctx, op, opts...)
	return ctx
}

func (pgxQueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	if data.Err != nil {
		span.RecordError(data.Err)
		span.SetStatus(codes.Error, data.Err.Error())
	}
	span.End()
}

// dbOperation extracts a coarse statement verb (SELECT, INSERT, BEGIN, ...) so
// the span name and db.operation dimension stay low-cardinality.
func dbOperation(sql string) string {
	// Skip any leading single-line SQL comments.
	for strings.HasPrefix(strings.TrimSpace(sql), "--") {
		if i := strings.IndexByte(sql, '\n'); i >= 0 {
			sql = sql[i+1:]
		} else {
			break
		}
	}
	sql = strings.TrimSpace(sql)
	if i := strings.IndexAny(sql, " \t\r\n("); i >= 0 {
		sql = sql[:i]
	}
	op := strings.ToUpper(strings.Trim(sql, "();"))
	if op == "" {
		return "query"
	}
	return op
}
