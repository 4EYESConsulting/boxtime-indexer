#!/usr/bin/env bash
# status.sh — Print indexing status from the cointime database.
#
# Usage:
#   ./scripts/status.sh

set -euo pipefail

psql_query() {
    docker compose exec -T db psql -U boxtime -d boxtime -t -A -c "$1"
}

# Check connectivity
row_count=$(psql_query "SELECT count(*) FROM cointime;" 2>/dev/null) || {
    echo "Error: cannot connect to database. Is the db container running?" >&2
    exit 1
}

if [ "$row_count" -eq 0 ]; then
    echo "Database is empty — no heights indexed yet."
    exit 0
fi

# Gather stats
stats=$(psql_query "
SELECT
    count(*),
    min(height),
    max(height),
    (max(height) - min(height) + 1) - count(*),
    round(count(*)::numeric / nullif(max(height) - min(height) + 1, 0) * 100, 2)
FROM cointime;
")
IFS='|' read -r total min_h max_h gaps coverage <<< "$stats"

# Latest row
latest=$(psql_query "
SELECT
    height,
    to_char(to_timestamp(timestamp / 1000), 'YYYY-MM-DD HH24:MI:SS UTC'),
    round(cbc / 1e9, 2),
    round(cbd / 1e9, 2),
    round(cbs / 1e9, 2)
FROM cointime
ORDER BY height DESC
LIMIT 1;
")
IFS='|' read -r l_height l_time l_cbc l_cbd l_cbs <<< "$latest"

echo "boxtime-indexer status"
echo "====================="
echo ""
echo "Indexed heights:  $total"
echo "Height range:     $min_h – $max_h"
echo "Gaps:             $gaps"
echo "Coverage:         $coverage%"
echo ""
echo "Latest block:"
echo "  Height:         $l_height"
echo "  Timestamp:      $l_time"
echo "  CBC:            $l_cbc ERG"
echo "  CBD:            $l_cbd ERG"
echo "  CBS:            $l_cbs ERG"
