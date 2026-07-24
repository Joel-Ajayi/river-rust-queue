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

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCENARIO="${1:?Usage: verify.sh <scenario> <start_ts> [end_ts]}"
START_TS="${2:?Usage: verify.sh <scenario> <start_ts> [end_ts]}"
END_TS="${3:-$(date +%s)}"
SCENARIO_BASE=$(echo "$SCENARIO" | tr '/' '-')

PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"
JAEGER_URL="${JAEGER_URL:-http://localhost:16686}"
ALERTMANAGER_URL="${ALERTMANAGER_URL:-http://localhost:9093}"

PASS=0
FAIL=0

pass() { PASS=$((PASS+1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL+1)); echo "  FAIL: $1"; }

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
  local result
  result=$(curl -sf "${PROMETHEUS_URL}/api/v1/query" \
    --data-urlencode "query=${query}" \
    --data-urlencode "time=${END_TS}" 2>/dev/null)
  if [ $? -ne 0 ] || [ -z "$result" ]; then
    echo "0"
    return
  fi
  echo "$result" | jq -r '.data.result[0].value[1] // "0"'
}

RATE_WINDOW="60s"

GTV=$(query_prom "sum(rate(rrq_business_gtv_total[${RATE_WINDOW}]))")
if [ "$GTV" != "0" ] && [ "$GTV" != "" ] && [ "$GTV" != "null" ]; then
  pass "rrq_business_gtv_total > 0 (GTV flowing)"
else
  fail "rrq_business_gtv_total is zero — no transfer value recorded"
fi

TRANSFERS=$(query_prom "sum(rate(rrq_business_transfers_total[${RATE_WINDOW}]))")
if [ "$TRANSFERS" != "0" ] && [ "$TRANSFERS" != "" ]; then
  pass "rrq_business_transfers_total > 0 (transfers flowing)"
else
  fail "rrq_business_transfers_total is zero — no transfers recorded"
fi

CB_OPEN=$(query_prom 'count(rrq_circuit_breaker_state{service_name="ledger-worker",circuit_breaker=~"shard-.*"} == 2)')
if [ "$CB_OPEN" = "0" ] || [ "$CB_OPEN" = "" ]; then
  pass "No circuit breakers open"
else
  fail "${CB_OPEN} circuit breaker(s) open"
fi

DLQ_RATE=$(query_prom "rate(rrq_dlq_ingestion_rate{service_name=\"ledger-worker\"}[${RATE_WINDOW}])")
if [ "${DLQ_RATE:-0}" = "0" ] || [ "${DLQ_RATE:-0}" = "" ]; then
  pass "No DLQ ingestion (gate)"
else
  DLQ_FLOAT=$(echo "$DLQ_RATE" | awk '{print ($1 > 1) ? "high" : "low"}')
  if [ "$DLQ_FLOAT" = "low" ]; then
    pass "DLQ rate acceptable: $DLQ_RATE/min"
  else
    fail "High DLQ rate: $DLQ_RATE/min"
  fi
fi

# Admin DLQ Replay API Check
CORE_API_URL="${CORE_API_URL:-https://api.127.0.0.1.nip.io}"
ADMIN_KEY="${RRQ_PLATFORM_KEY:-dev-platform-admin-key}"

ADMIN_JWT=$(curl -sf -X POST "${CORE_API_URL}/v1/auth/token" \
  -H "Authorization: Bearer ${ADMIN_KEY}" 2>/dev/null \
  | jq -r '.token' 2>/dev/null || echo "")

if [ -n "$ADMIN_JWT" ] && [ "$ADMIN_JWT" != "null" ]; then
  REPLAY_RESP=$(curl -sf -X POST "${CORE_API_URL}/v1/admin/dlq/replay" \
    -H "Authorization: Bearer ${ADMIN_JWT}" \
    -H "Content-Type: application/json" \
    -d '{"shardId": "shard-a", "source": "ledger", "limit": 100}' 2>/dev/null || echo "")
  if [ -n "$REPLAY_RESP" ]; then
    pass "Admin DLQ Replay API accessible"
  else
    fail "Admin DLQ Replay API request failed"
  fi
else
  fail "Failed to acquire JWT for Admin DLQ Replay"
fi

OUTBOX_LAG=$(query_prom 'rrq_outbox_lag_seconds{service_name="outbox-relay"}')
if [ "${OUTBOX_LAG:-0}" = "0" ] || [ "${OUTBOX_LAG:-0}" = "" ] || [ "$(echo "$OUTBOX_LAG" | awk '{print ($1 < 10) ? "ok" : "high"}')" = "ok" ]; then
  pass "Outbox relay lag < 10s"
else
  fail "Outbox relay lag: ${OUTBOX_LAG}s"
fi

TSR=$(query_prom "(sum(rate(rrq_business_transfers_total{status=\"success\"}[${RATE_WINDOW}])) / sum(rate(rrq_business_transfers_total[${RATE_WINDOW}]))) * 100")
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

TRACE_COUNT=$(curl -sf "${JAEGER_URL}/api/traces?service=core-api&start=$((START_TS * 1000000))&end=$((END_TS * 1000000))&limit=1" 2>/dev/null \
  | jq -r '.data | length // 0' 2>/dev/null || echo "0")

if [ "$TRACE_COUNT" -gt 0 ]; then
  pass "Traces present in Jaeger for core-api"

  COMPLETE_TRACES=$(curl -sf "${JAEGER_URL}/api/traces?service=core-api&start=$((START_TS * 1000000))&end=$((END_TS * 1000000))&limit=10" 2>/dev/null \
    | jq '[.data[] | select(.spans | length >= 3)] | length' 2>/dev/null || echo "0")

  if [ "$COMPLETE_TRACES" -gt 0 ]; then
    pass "Complete traces found (>=3 spans: HTTP->Kafka->DB)"
  else
    fail "No complete traces found (expected >=3 spans per trace)"
  fi
else
  fail "No traces found in Jaeger for test window"
fi

# ---------------------------------------------------------------------------
# 3. Alertmanager silence check
# ---------------------------------------------------------------------------
echo ""
echo "─── Alertmanager ───"

ALERTS=$(curl -sf "${ALERTMANAGER_URL}/api/v2/alerts" 2>/dev/null \
  | jq --argjson start "$START_TS" '[.[] | select((.startsAt | fromdateiso8601? // 0) > $start)] | length' 2>/dev/null || echo "0")

if [ "${ALERTS:-0}" = "0" ]; then
  pass "No alerts fired during test window"
else
  fail "${ALERTS} alert(s) fired during test -- investigate"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  RESULTS: ${PASS} passed, ${FAIL} failed"
echo "══════════════════════════════════════════════════════════════"

jq -n \
  --arg scenario "$SCENARIO" \
  --argjson timestamp "$(date +%s)" \
  --argjson passed "$PASS" \
  --argjson failed "$FAIL" \
  --arg gtv "$GTV" \
  --arg transfers "$TRANSFERS" \
  --arg cb_open "$CB_OPEN" \
  --arg dlq_rate "$(echo "${DLQ_RATE:-0}" | awk '{print $1}')" \
  --arg outbox_lag "$OUTBOX_LAG" \
  --argjson tsr "${TSR_INT:-100}" \
  --argjson trace_count "${TRACE_COUNT:-0}" \
  --argjson alerts "${ALERTS:-0}" \
  '{
    scenario: $scenario,
    timestamp: $timestamp,
    passed: $passed,
    failed: $failed,
    checks: {
      gtv_flowing: $gtv,
      transfers_flowing: $transfers,
      circuit_breakers_open: $cb_open,
      dlq_rate: $dlq_rate,
      outbox_lag: $outbox_lag,
      transfer_success_rate: $tsr,
      trace_count: $trace_count,
      alerts_fired: $alerts
    }
  }' > "${ROOT}/k6/reports/${SCENARIO_BASE}-verify.json"

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi