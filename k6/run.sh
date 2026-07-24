#!/usr/bin/env bash
# k6/run.sh — Run a k6 scenario with post-test observability verification
#
# Usage: ./k6/run.sh <scenario> [--verify] [--no-verify]
#        ./k6/run.sh seed              # Seed test data (create merchants, wallets, fund)
#
# Environment:
#   BASE_URL   - Target URL (default: https://api.127.0.0.1.nip.io)
#   DURATION   - Test duration override (default: per-scenario)
#   VERIFY     - Run post-test verification (default: true)
#   PROMETHEUS_URL, JAEGER_URL, ALERTMANAGER_URL - Observability endpoints

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ── Check for k6 ──
if [ "$1" != "seed" ]; then
  if ! command -v k6 >/dev/null 2>&1; then
    echo "ERROR: k6 is not installed. Run: make tools-k6"
    exit 1
  fi
fi

# ── Seed command: create merchants, wallets, and pre-fund ──
if [ "$1" = "seed" ]; then
  echo "══════════════════════════════════════════════════════════════"
  echo "  SEED: Creating merchants, wallets, and pre-funding"
  echo "  Target: ${BASE_URL:-https://api.127.0.0.1.nip.io}"
  echo "══════════════════════════════════════════════════════════════"
  node "${ROOT}/k6/seed-test-data.mjs"
  exit 0
fi

SCENARIO="${1:-performance/load-sustained}"
BASE_URL="${BASE_URL:-https://api.127.0.0.1.nip.io}"
DURATION="${DURATION:-}"
VERIFY="${VERIFY:-true}"

# Parse --verify/--no-verify flags
for arg in "$@"; do
  case "$arg" in
    --verify) VERIFY=true ;;
    --no-verify) VERIFY=false ;;
  esac
done

SCRIPT="${ROOT}/k6/scenarios/${SCENARIO}.js"
if [ ! -f "$SCRIPT" ]; then
  echo "Scenario not found: $SCENARIO"
  echo "Available scenarios:"
  find "${ROOT}/k6/scenarios" -name "*.js" | sed "s|${ROOT}/k6/scenarios/||" | sed 's/\.js$//' | sed 's/^/  /'
  exit 1
fi

SCENARIO_BASE=$(echo "$SCENARIO" | tr '/' '-')
mkdir -p "${ROOT}/k6/reports"

# Capture start timestamp (seconds since epoch) for verification window
START_TS=$(date +%s)

echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  RUN: ${SCENARIO}"
echo "  Target: ${BASE_URL}"
echo "  Start:  $(date -d @"$START_TS" '+%Y-%m-%d %H:%M:%S')"
echo "══════════════════════════════════════════════════════════════"
echo ""

K6_FLAGS="--env BASE_URL=$BASE_URL"
if [ -n "$DURATION" ]; then
  K6_FLAGS="$K6_FLAGS --env DURATION=$DURATION"
fi
if [ -n "${RRQ_PLATFORM_KEY:-}" ]; then
  K6_FLAGS="$K6_FLAGS --env RRQ_PLATFORM_KEY=$RRQ_PLATFORM_KEY"
fi

k6 run \
  $K6_FLAGS \
  --summary-export="${ROOT}/k6/reports/${SCENARIO_BASE}-summary.json" \
  "$SCRIPT"

END_TS=$(date +%s)
echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  LOAD TEST COMPLETE: ${SCENARIO}"
echo "  Duration: $((END_TS - START_TS))s"
echo "══════════════════════════════════════════════════════════════"

# Post-test observability verification
if [ "$VERIFY" = "true" ]; then
  echo ""
  echo "Running post-test verification..."
  "${ROOT}/k6/verify.sh" "$SCENARIO" "$START_TS" "$END_TS"
else
  echo ""
  echo "Verification skipped (--no-verify)"
fi
