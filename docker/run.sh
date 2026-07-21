#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKER_DIR="${ROOT_DIR}/docker"

PG_MAJOR="${PG_MAJOR:-16}"
PG_PORT="${PG_PORT:-5432}"
MINIO_PORT="${MINIO_PORT:-19000}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-19001}"
WITH_MINIO=1
BUILD=1
DETACH=1

usage() {
  cat <<'EOF'
Build and start a PostgreSQL server with the koldstore extension pre-installed.

Usage:
  docker/run.sh [options]

Options:
  --pg-major N          PostgreSQL major version to build (default: 16)
  --port PORT           Host port for PostgreSQL (default: 5432)
  --no-minio            Do not start MinIO
  --no-build            Skip image build and reuse the existing image
  --foreground          Run in the foreground (docker compose up without -d)
  -h, --help            Show this help text

Environment:
  PG_MAJOR              Same as --pg-major
  PG_PORT               Same as --port
  MINIO_PORT            MinIO API port (default: 19000)
  MINIO_CONSOLE_PORT    MinIO console port (default: 19001)

Examples:
  docker/run.sh
  PG_MAJOR=17 docker/run.sh
  docker/run.sh --no-minio --foreground
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pg-major)
      PG_MAJOR="${2:?missing value for --pg-major}"
      shift 2
      ;;
    --port)
      PG_PORT="${2:?missing value for --port}"
      shift 2
      ;;
    --no-minio) WITH_MINIO=0; shift ;;
    --no-build) BUILD=0; shift ;;
    --foreground) DETACH=0; shift ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker is required" >&2
  exit 1
fi

export PG_MAJOR PG_PORT MINIO_PORT MINIO_CONSOLE_PORT

cd "${DOCKER_DIR}"

services=(postgres)
if [[ "${WITH_MINIO}" -eq 1 ]]; then
  services+=(minio minio-init)
fi

compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  elif command -v docker-compose >/dev/null 2>&1; then
    docker-compose "$@"
  else
    echo "error: docker compose is required" >&2
    exit 1
  fi
}

if [[ "${BUILD}" -eq 1 ]]; then
  echo "==> building pg-koldstore image for PostgreSQL ${PG_MAJOR}"
  compose build postgres
fi

echo "==> starting services: ${services[*]}"
if [[ "${DETACH}" -eq 1 ]]; then
  compose up -d "${services[@]}"
else
  compose up "${services[@]}"
  exit 0
fi

echo "==> waiting for PostgreSQL"
deadline=$((SECONDS + 120))
until compose exec -T postgres pg_isready -U postgres -d koldstoredb >/dev/null 2>&1; do
  if (( SECONDS > deadline )); then
    echo "error: PostgreSQL did not become ready in time" >&2
    exit 1
  fi
  sleep 2
done

until compose exec -T postgres \
  psql -U postgres -d koldstoredb -tAc "SELECT 1 FROM pg_extension WHERE extname = 'koldstore'" \
  | grep -q 1; do
  if (( SECONDS > deadline )); then
    echo "error: koldstore extension was not installed" >&2
    exit 1
  fi
  sleep 2
done

cat <<EOF

pg-koldstore is running.

PostgreSQL:
  host:     127.0.0.1
  port:     ${PG_PORT}
  user:     postgres
  password: postgres
  database: koldstoredb
  url:      postgres://postgres:postgres@127.0.0.1:${PG_PORT}/koldstoredb

Extension:
  CREATE EXTENSION koldstore;  -- already applied on first boot
  SELECT koldstore_version();

Connect:
  psql postgres://postgres:postgres@127.0.0.1:${PG_PORT}/koldstoredb

EOF

if [[ "${WITH_MINIO}" -eq 1 ]]; then
  cat <<EOF
MinIO:
  api:      http://127.0.0.1:${MINIO_PORT}
  console:  http://127.0.0.1:${MINIO_CONSOLE_PORT}
  user:     minioadmin
  password: minioadmin

EOF
fi

cat <<EOF
Stop:
  (cd docker && docker compose down)
EOF
