#!/usr/bin/env bash
# snapshot.sh — Produce or restore boxtime-indexer CSV snapshots.
#
# Usage:
#   ./scripts/snapshot.sh produce <height>
#   ./scripts/snapshot.sh restore <tarball>
#
# Note: This script is deprecated. CSV files can be backed up directly.

set -euo pipefail

VOLUME_NAME="boxtime-indexer_pgdata"

produce() {
    local height="$1"
    local outfile="boxtime-pgdata-${height}.tar.gz"

    echo "Stopping containers..."
    docker compose down

    echo "Creating snapshot from volume ${VOLUME_NAME}..."
    docker run --rm \
        -v "${VOLUME_NAME}:/data:ro" \
        alpine tar czf - -C /data . > "${outfile}"

    echo "Snapshot written to ${outfile}"
    echo "To upload: gh release create snapshot-${height} ${outfile} --title 'DB snapshot at height ${height}'"
}

restore() {
    local tarball="$1"

    if [ ! -f "${tarball}" ]; then
        echo "Error: file '${tarball}' not found" >&2
        exit 1
    fi

    echo "Creating volume ${VOLUME_NAME} (if needed)..."
    docker volume create "${VOLUME_NAME}" 2>/dev/null || true

    echo "Restoring snapshot from ${tarball}..."
    docker run --rm \
        -v "${VOLUME_NAME}:/data" \
        -v "$(cd "$(dirname "${tarball}")" && pwd)/$(basename "${tarball}"):/backup.tar.gz:ro" \
        alpine tar xzf /backup.tar.gz -C /data

    echo "Snapshot restored. Run 'docker compose up -d' to start."
}

case "${1:-}" in
    produce)
        if [ -z "${2:-}" ]; then
            echo "Usage: $0 produce <height>" >&2
            exit 1
        fi
        produce "$2"
        ;;
    restore)
        if [ -z "${2:-}" ]; then
            echo "Usage: $0 restore <tarball>" >&2
            exit 1
        fi
        restore "$2"
        ;;
    *)
        echo "Usage: $0 {produce <height>|restore <tarball>}" >&2
        exit 1
        ;;
esac
