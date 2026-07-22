#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKER_DIR="${ROOT_DIR}/docker"
EXAMPLE_SQL="${DOCKER_DIR}/sql/example.sql"

PG_MAJOR="${PG_MAJOR:-16}"
PG_PORT="${PG_PORT:-5432}"
MINIO_PORT="${MINIO_PORT:-19000}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-19001}"
BUILD=1

usage() {
  cat <<'EOF'
Start the docker stack, run docker/sql/example.sql, and assert the demo worked.

Usage:
  docker/test.sh [options]

Options:
  --no-build     Reuse the existing pg-koldstore image
  --pg-major N   PostgreSQL major version (default: 16)
  --port PORT    Host port for PostgreSQL (default: 5432)
  -h, --help     Show this help text

Manual follow-up:
  psql "postgres://postgres:postgres@127.0.0.1:5432/koldstore" -f docker/sql/example.sql
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-build) BUILD=0; shift ;;
    --pg-major)
      PG_MAJOR="${2:?missing value for --pg-major}"
      shift 2
      ;;
    --port)
      PG_PORT="${2:?missing value for --port}"
      shift 2
      ;;
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

export PG_MAJOR PG_PORT MINIO_PORT MINIO_CONSOLE_PORT

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

psql_cmd() {
  compose exec -T postgres psql -U postgres -d koldstore -v ON_ERROR_STOP=1 "$@"
}

assert_eq() {
  local label="$1"
  local expected="$2"
  local actual="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "error: ${label}: expected '${expected}', got '${actual}'" >&2
    exit 1
  fi
  echo "ok: ${label} = ${actual}"
}

assert_ge() {
  local label="$1"
  local minimum="$2"
  local actual="$3"
  if ! [[ "${actual}" =~ ^[0-9]+$ ]] || (( actual < minimum )); then
    echo "error: ${label}: expected >= ${minimum}, got '${actual}'" >&2
    exit 1
  fi
  echo "ok: ${label} = ${actual}"
}

minio_cat() {
  local object_path="$1"
  compose run --rm --entrypoint /bin/sh minio-init -c \
    "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && mc cat local/${object_path}"
}

cd "${DOCKER_DIR}"

if [[ ! -f "${EXAMPLE_SQL}" ]]; then
  echo "error: missing ${EXAMPLE_SQL}" >&2
  exit 1
fi

# Fresh database so CREATE EXTENSION picks up the current SQL from the image.
echo "==> resetting stack"
compose down -v >/dev/null 2>&1 || true

run_args=(--pg-major "${PG_MAJOR}" --port "${PG_PORT}")
if [[ "${BUILD}" -eq 0 ]]; then
  run_args+=(--no-build)
fi

echo "==> starting postgres + minio"
"${ROOT_DIR}/docker/run.sh" "${run_args[@]}"

echo "==> ensuring MinIO bucket exists"
compose run --rm minio-init

echo "==> running example.sql"
psql_cmd -f - < "${EXAMPLE_SQL}"

echo "==> running concurrent writers/selects/flushes"
pids=()
for worker in $(seq 1 10); do
  (
    psql_cmd -qAt <<SQL >/dev/null
INSERT INTO demo.messages (conversation_id, author, body)
SELECT
  '00000000-0000-4000-8000-000000000001'::uuid,
  'parallel-writer-${worker}',
  'parallel message ' || g
FROM generate_series(1, 25) AS g;

UPDATE demo.messages
SET body = body || ' / touched by writer ${worker}'
WHERE id IN (
  SELECT id
  FROM demo.messages
  ORDER BY created_at DESC
  LIMIT 5
);
SQL
  ) &
  pids+=("$!")

  (
    for _ in $(seq 1 10); do
      psql_cmd -qAt -c "SELECT count(*) FROM demo.messages m JOIN demo.conversations c ON c.id = m.conversation_id;" >/dev/null
    done
  ) &
  pids+=("$!")

  (
    psql_cmd -qAt -c "SELECT koldstore.flush_table('demo.messages'::regclass, force => true);" >/dev/null
  ) &
  pids+=("$!")
done

for pid in "${pids[@]}"; do
  wait "${pid}"
done

echo "==> asserting demo results"
conversation_count="$(psql_cmd -tAc 'SELECT count(*) FROM demo.conversations;' | tr -d '[:space:]')"
message_count="$(psql_cmd -tAc 'SELECT count(*) FROM demo.messages;' | tr -d '[:space:]')"
managed_tables="$(psql_cmd -tAc "SELECT count(*) FROM koldstore.schemas s JOIN pg_class c ON c.oid = s.table_oid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'demo' AND s.active;" | tr -d '[:space:]')"
completed_flush_jobs="$(psql_cmd -tAc "SELECT count(*) FROM koldstore.jobs j JOIN pg_class c ON c.oid = j.table_oid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'demo' AND j.job_type = 'flush' AND j.status = 'completed';" | tr -d '[:space:]')"
in_sync_manifests="$(psql_cmd -tAc "SELECT count(*) FROM koldstore.manifest m JOIN pg_class c ON c.oid = m.table_oid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'demo' AND m.sync_state = 'in_sync';" | tr -d '[:space:]')"
internal_user_columns="$(psql_cmd -tAc "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'demo' AND table_name IN ('conversations', 'messages') AND column_name IN ('_seq', '_commit_seq', '_deleted', '_user_id');" | tr -d '[:space:]')"
mirror_tables="$(psql_cmd -tAc "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'koldstore' AND c.relname IN ('conversations__cl', 'messages__cl');" | tr -d '[:space:]')"
row_events_exists="$(psql_cmd -tAc "SELECT to_regclass('koldstore.row_events') IS NOT NULL;" | tr -d '[:space:]')"
manifest_paths="$(psql_cmd -tAc "SELECT count(*) FROM koldstore.manifest m JOIN pg_class c ON c.oid = m.table_oid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'demo' AND m.manifest_path LIKE 's3://koldstore-test/demo/%/manifest.json';" | tr -d '[:space:]')"
storage_name="$(psql_cmd -tAc "SELECT name FROM koldstore.storage WHERE name = 'local-minio';" | tr -d '[:space:]')"
sample_join="$(psql_cmd -tAc "SELECT count(*) FROM demo.messages m JOIN demo.conversations c ON c.id = m.conversation_id;" | tr -d '[:space:]')"
cold_segments="$(psql_cmd -tAc "SELECT count(*) FROM koldstore.cold_segments s JOIN pg_class c ON c.oid = s.table_oid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'demo' AND s.status = 'active';" | tr -d '[:space:]')"
message_manifest_json="$(minio_cat "koldstore-test/demo/messages/manifest.json")"
conversation_manifest_json="$(minio_cat "koldstore-test/demo/conversations/manifest.json")"
message_manifest_segments="$(printf '%s' "${message_manifest_json}" | grep -c '"segments"')"
conversation_manifest_segments="$(printf '%s' "${conversation_manifest_json}" | grep -c '"segments"')"

assert_eq "conversations" "100" "${conversation_count}"
assert_ge "messages" "10250" "${message_count}"
assert_eq "managed tables" "2" "${managed_tables}"
assert_ge "completed flush jobs" "12" "${completed_flush_jobs}"
assert_eq "in-sync manifests" "2" "${in_sync_manifests}"
assert_eq "internal user-table columns" "0" "${internal_user_columns}"
assert_eq "change-log mirror tables" "2" "${mirror_tables}"
assert_eq "global row_events table exists" "f" "${row_events_exists}"
assert_eq "storage-backed manifest paths" "2" "${manifest_paths}"
assert_eq "storage registration" "local-minio" "${storage_name}"
assert_ge "joinable messages" "10250" "${sample_join}"
assert_ge "active cold segments" "2" "${cold_segments}"
assert_ge "message manifest object has segments" "1" "${message_manifest_segments}"
assert_ge "conversation manifest object has segments" "1" "${conversation_manifest_segments}"
assert_ge "messages per conversation" "100" \
  "$(psql_cmd -tAc 'SELECT min(cnt) FROM (SELECT count(*) AS cnt FROM demo.messages GROUP BY conversation_id) s;' | tr -d '[:space:]')"

cat <<EOF

docker/test.sh passed.

Re-run the demo SQL manually:
  psql "postgres://postgres:postgres@127.0.0.1:${PG_PORT}/koldstore" -f docker/sql/example.sql

Connect:
  psql "postgres://postgres:postgres@127.0.0.1:${PG_PORT}/koldstore"

MinIO console:
  http://127.0.0.1:${MINIO_CONSOLE_PORT}  (minioadmin / minioadmin)
EOF
