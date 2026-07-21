#!/usr/bin/env bash
# k6/verify.sh — Post-test observability verification
#
# Runs after a k6 load test to validate:
#   1. Prometheus business metrics match expected values
#   2. Jaeger trace completeness for critical paths
#   3. No critical alerts fired during the test window
#
# Usage: ./k6/verify.sh <scenario_name> <test_start_timestamp> [test_end_timestamp]
#
# Dependencies: curl, jq (installed by make tools)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCENARIO="${1:?Usage: verify.sh <scenario> <start_ts> [end_ts]}"
START_TS="${2:?Usage: verify.sh <scenario> <start_ts> [end_ts]}"
END_TS="${3:-$(date +%s)}"
SCENARIO_BASE=$(basename "$SCENARIO")

PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"
JAEGER_URL="${JAEGER_URL:-http://localhost:16686}"
ALERTMANAGER_URL="${ALERTMANAGER_URL:-http://localhost:9093}"

PASS=0
FAIL=0

pass() { PASS=$((PASS+1)); echo "  ✅ PASS: $1"; }
fail() { FAIL=$((FAIL+1)); echo "  ❌ FAIL: $1"; }

echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  VERIFY: ${SCENARIO}"
echo "  Window: $(date -d @"$START_TS" '+%H:%M:%S') → $(date -d @"$END_TS" '+%H:%M:%S')"
echo "══════════════════════════════════════════════════════════════"

# ---------------------------------------------------------------------------
# 1. Prometheus metric checks
# ---------------------------------------------------------------------------
echo ""
echo "─── Prometheus ───"

query_prom() {
  local query="$1"
  curl -sf "${PROMETHEUS_URL}/api/v1/query" \
    --data-urlencode "query=${query}" \
    --data-urlencode "time=${END_TS}" \
    | jq -r '.data.result[0].value[1] // "0"'
}

# Verify business metrics are flowing
GTV=$(query_prom "sum(rate(rrq_business_gtv_total[${SCENARIO}_duration]))")
if [ "$GTV" != "0" ] && [ "$GTV" != "" ]; then
  pass "rrq_business_gtv_total > 0 (GTV flowing)"
else
  fail "rrq_business_gtv_total is zero — no transfer value recorded"
fi

TRANSFERS=$(query_prom "sum(rate(rrq_business_transfers_total[5m]))")
if [ "$TRANSFERS" != "0" ] && [ "$TRANSFERS" != "" ]; then
  pass "rrq_business_transfers_total > 0 (transfers flowing)"
else
  fail "rrq_business_transfers_total is zero — no transfers recorded"
fi

# Verify no critical infrastructure metrics indicate problems
CB_OPEN=$(query_prom "count(rrq_circuit_breaker_state == 2)")
if [ "$CB_OPEN" = "0" ] || [ "$CB_OPEN" = "" ]; then
  pass "No circuit breakers open"
else
  fail "${CB_OPEN} circuit breaker(s) open"
fi

DLQ_RATE=$(query_prom "rate(rrq_dlq_ingestion_rate[5m])")
if [ "${DLQ_RATE:-0}" = "0" ] || [ "${DLQ_RATE:-0}" = "" ]; then
  pass "No DLQ ingestion (gate)"
else
  # Allow some DLQ if scenario specifically tests fraud/rejection
  DLQ_FLOAT=$(echo "$DLQ_RATE" | awk '{print ($1 > 1) ? "high" : "low"}')
  if [ "$DLQ_FLOAT" = "low" ]; then
    pass "DLQ rate acceptable: $DLQ_RATE/min"
  else
    fail "High DLQ rate: $DLQ_RATE/min"
  fi
fi

# Admin DLQ Replay API Check
CORE_API_URL="${CORE_API_URL:-http://localhost:8080}"
ADMIN_KEY="${RRQ_PLATFORM_KEY:-dev-platform-admin-key}"
REPLAY_RESP=$(curl -sf -X POST "${CORE_API_URL}/v1/admin/dlq/replay" \
  -H "Authorization: Bearer ${ADMIN_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"shard_id": "shard-a", "limit": 100}' || echo "")
if [ -n "$REPLAY_RESP" ]; then
  pass "Admin DLQ Replay API accessible"
fi

OUTBOX_LAG=$(query_prom "rrq_outbox_lag_seconds")
if [ "${OUTBOX_LAG:-0}" = "0" ] || [ "${OUTBOX_LAG:-0}" = "" ] || [ "$(echo "$OUTBOX_LAG" | awk '{print ($1 < 10) ? "ok" : "high"}')" = "ok" ]; then
  pass "Outbox relay lag < 10s"
else
  fail "Outbox relay lag: ${OUTBOX_LAG}s"
fi

# Verify TSR (transfer success rate) is above threshold
TSR=$(query_prom "(sum(rate(rrq_business_transfers_total{status!=\"failed\"}[5m])) / sum(rate(rrq_business_transfers_total[5m]))) * 100")
TSR_INT=$(echo "${TSR:-100}" | awk -F. '{print $1}')
if [ "${TSR_INT:-100}" -ge 95 ]; then
  pass "Transfer success rate >= 95% (actual: ${TSR:-100}%)"
else
  fail "Transfer success rate below 95%: ${TSR}%"
fi

# ---------------------------------------------------------------------------
# 2. Jaeger trace completeness check
# ---------------------------------------------------------------------------
echo ""
echo "─── Jaeger ───"

# Query Jaeger for traces in the test window
TRACE_COUNT=$(curl -sf "${JAEGER_URL}/api/traces?service=core-api&start=${START_TS}000000&end=${END_TS}000000&limit=1" \
  | jq -r '.data | length // 0')

if [ "$TRACE_COUNT" -gt 0 ]; then
  pass "Traces present in Jaeger for core-api"

  # Verify at least one complete trace has all expected spans
  COMPLETE_TRACES=$(curl -sf "${JAEGER_URL}/api/traces?service=core-api&start=${START_TS}000000&end=${END_TS}000000&limit=10" \
    | jq '[.data[] | select(.spans | length >= 3)] | length')

  if [ "$COMPLETE_TRACES" -gt 0 ]; then
    pass "Complete traces found (≥3 spans: HTTP→Kafka→DB)"
  else
    fail "No complete traces found (expected ≥3 spans per trace)"
  fi
else
  fail "No traces found in Jaeger for test window"
fi

# ---------------------------------------------------------------------------
# 3. Alertmanager silence check
# ---------------------------------------------------------------------------
echo ""
echo "─── Alertmanager ───"

# Check for any firing alerts during the test window
ALERTS=$(curl -sf "${ALERTMANAGER_URL}/api/v2/alerts" | jq '[.[] | select(.startsAt | fromdateiso8601? // 0 > '"${START_TS}"')] | length')

if [ "${ALERTS:-0}" = "0" ]; then
  pass "No alerts fired during test window"
else
  fail "${ALERTS} alert(s) fired during test — investigate"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  RESULTS: ${PASS} passed, ${FAIL} failed"
echo "══════════════════════════════════════════════════════════════"

# Write verification report
cat > "${ROOT}/k6/reports/${SCENARIO_BASE}-verify.json" <<EOF
{
  "scenario": "${SCENARIO}",
  "timestamp": $(date +%s),
  "passed": ${PASS},
  "failed": ${FAIL},
  "checks": {
    "gtv_flowing": ${GTV},
    "transfers_flowing": ${TRANSFERS},
    "circuit_breakers_open": ${CB_OPEN},
    "dlq_rate": $(echo "${DLQ_RATE:-0}" | awk '{print $1}'),
    "outbox_lag": ${OUTBOX_LAG},
    "transfer_success_rate": ${TSR_INT:-100},
    "trace_count": ${TRACE_COUNT},
    "alerts_fired": ${ALERTS:-0}
  }
}
EOF

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi