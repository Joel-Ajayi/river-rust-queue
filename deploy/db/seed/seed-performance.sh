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

    echo ""
    echo "─── ${label} ───"

    local table_exists
    table_exists=$(psql "$db_url" -tAc \
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'seed_status')")

    if [ "$table_exists" != "t" ]; then
        echo "FATAL: seed_status table does not exist in ${label}."
        echo "Run migrations first (000014_create_seed_status on shards, 000002_create_seed_status on global)."
        exit 1
    fi

    local applied_checksum
    applied_checksum=$(psql "$db_url" -tAc \
        "SELECT checksum FROM seed_status WHERE seed_name='${SEED_NAME}'" | tr -d '[:space:]')

    if [ -n "$applied_checksum" ]; then
        if [ "$applied_checksum" = "$SCRIPT_CHECKSUM" ]; then
            echo "✓ Already applied (checksum match). Skipping."
            return 0
        else
            echo "FATAL: Seed '${SEED_NAME}' was applied to ${label} with a DIFFERENT checksum."
            echo "  Expected: ${SCRIPT_CHECKSUM}"
            echo "  Found:    ${applied_checksum}"
            echo ""
            echo "The seed script has changed since it was last applied."
            echo "To re-apply, manually delete the seed_status row:"
            echo "  psql ${db_url} -c \"DELETE FROM seed_status WHERE seed_name='${SEED_NAME}'\""
            echo "Then delete the seeded data before re-running."
            exit 1
        fi
    fi

    echo "Applying seed SQL: ${sql_file} ..."
    psql "$db_url" -f "$sql_file"

    psql "$db_url" -c \
        "INSERT INTO seed_status (seed_name, checksum) VALUES ('${SEED_NAME}', '${SCRIPT_CHECKSUM}')"

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
