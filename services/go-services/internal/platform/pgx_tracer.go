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
//
// The Go eBPF auto-instrumentor can only observe `database/sql` call sites; it
// cannot see pgx-native (pgxpool) queries. Postgres is therefore traced
// manually here so the collector's span_metrics connector can derive DB RED
// metrics (traces_span_metrics_*) keyed by db.system.name="postgresql".
type pgxQueryTracer struct{}

var _ pgx.QueryTracer = pgxQueryTracer{}

// TraceQueryStart starts a client span covering the statement's execution.
func (pgxQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	op := dbOperation(data.SQL)
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "postgresql"),
			attribute.String("db.operation", op),
			// SQL uses positional ($1, $2) placeholders, so no literal values
			// are embedded and argument values are never recorded here.
			attribute.String("db.statement", data.SQL),
		),
	}
	ctx, _ = otel.Tracer(MetricMeterName + ".postgres").Start(ctx, op, opts...)
	return ctx
}

func (pgxQueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
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
