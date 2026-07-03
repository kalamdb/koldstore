#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/tests/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" up -d postgres15 postgres16 postgres17 minio

for version in 15 16 17; do
  port="55${version}"
  echo "running pg-koldstore E2E matrix for PostgreSQL ${version} on port ${port}"
  PGHOST=127.0.0.1 PGPORT="$port" PGUSER=postgres PGPASSWORD=postgres PGDATABASE=koldstore \
    cargo test --workspace --test '*' -- --include-ignored
done
