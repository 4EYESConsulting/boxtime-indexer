#!/usr/bin/env bash
# status.sh — Print indexing status from the cointime database.
#
# Usage:
#   ./scripts/status.sh

set -euo pipefail

# Load .env if present
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

db_url="${DATABASE_URL:-postgresql://boxtime:boxtime@db:5432/boxtime}"

psql_query() {
    # Try local db container first, fall back to direct psql for remote DB
    if docker compose ps --status running db 2>/dev/null | grep -q db; then
        docker compose exec -T db psql -U boxtime -d boxtime -t -A -c "$1"
    else
        psql "$db_url" -t -A -c "$1"
    fi
}

# Check connectivity
row_count=$(psql_query "SELECT count(*) FROM cointime;" 2>/dev/null) || {
    echo "Error: cannot connect to database." >&2
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

# Chain tip from the Ergo node
# Try NODE_URL directly from host first; if that's a Docker-internal name
# (e.g. http://node:9053), query through the indexer container instead.
node_url="${NODE_URL:-http://node:9053}"
chain_height=""
if full_info=$(curl -sf --max-time 5 "${node_url}/info" 2>/dev/null); then
    chain_height=$(echo "$full_info" | python3 -c "import sys,json; print(json.load(sys.stdin).get('fullHeight',''))" 2>/dev/null)
elif docker compose ps --status running indexer 2>/dev/null | grep -q indexer; then
    full_info=$(docker compose exec -T indexer python3 -c "
import urllib.request, json, os
url = os.environ.get('NODE_URL', 'http://node:9053') + '/info'
try:
    with urllib.request.urlopen(url, timeout=5) as r:
        print(json.loads(r.read()).get('fullHeight', ''))
except Exception:
    print('')
" 2>/dev/null)
    chain_height=$(echo "$full_info" | tr -d '[:space:]')
fi

echo "boxtime-indexer status"
echo "====================="
echo ""
echo "Indexed heights:  $total"
echo "Height range:     $min_h – $max_h"
echo "Gaps:             $gaps"
echo "Coverage:         $coverage%"
echo ""
if [ -n "$chain_height" ] && [ "$chain_height" -gt 0 ] 2>/dev/null; then
    remaining=$((chain_height - max_h))
    progress=$(python3 -c "print(round($max_h / $chain_height * 100, 2))")
    echo "Chain tip:        $chain_height"
    echo "Remaining:        $remaining heights ($progress% synced)"
    echo ""
else
    echo "Chain tip:        unavailable (node not reachable)"
    echo ""
fi
echo "Latest block:"
echo "  Height:         $l_height"
echo "  Timestamp:      $l_time"
echo "  CBC:            $l_cbc ERG"
echo "  CBD:            $l_cbd ERG"
echo "  CBS:            $l_cbs ERG"
