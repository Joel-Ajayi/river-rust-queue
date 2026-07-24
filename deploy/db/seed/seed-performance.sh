#!/usr/bin/env bash
# seed-performance.sh — One-shot performance test data seeder
# Seeds premium platform merchant and default system/fiat_vault wallets.
#
# Environment:
#   GLOBAL_DB_URL, SHARD_A_DB_URL, SHARD_B_DB_URL — postgres connection strings

set -euo pipefail

SEED_NAME="performance-v1"
SCRIPT_CHECKSUM=$(sha256sum "$0" | cut -d' ' -f1)
SEED_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "══════════════════════════════════════════════════════════════"
echo "  SEED: ${SEED_NAME}"
echo "  Checksum: ${SCRIPT_CHECKSUM}"
echo "══════════════════════════════════════════════════════════════"

apply_seed() {
    local label="$1"
    local db_url="$2"
    local sql_file="$3"

    echo "Applying seed SQL: ${sql_file} ..."
    psql "$db_url" -f "$sql_file"

    echo "✓ Applied and recorded."
}

apply_seed "Global DB"     "$GLOBAL_DB_URL" "${SEED_DIR}/global.sql"

# Dynamically apply seed to all shard databases.
for var in $(env | cut -d= -f1); do
	if [[ "$var" =~ ^SHARD_.*_DB_URL$ ]]; then
		shard_label=$(echo "$var" | sed 's/_DB_URL//g' | sed 's/SHARD_//g')
		apply_seed "Shard ${shard_label}" "${!var}" "${SEED_DIR}/shard.sql"
	fi
done

echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  SEED COMPLETE"
echo "  Merchants:        1"
echo "══════════════════════════════════════════════════════════════"
