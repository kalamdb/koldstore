#!/usr/bin/env bash
# Smoke-test a published/candidate pg-koldstore release image.
# Verifies PostgreSQL starts, koldstore is created, and pg_cron is packaged
# (but not auto-enabled). Asserts koldstore is shared-preloaded.
#
# Does not require host `psql` — all SQL runs via docker exec / a client
# container so release CI runners without postgresql-client still work.
set -euo pipefail

IMAGE="${1:?usage: docker/test-release-image.sh <image>}"
CONTAINER_NAME="${KOLDSTORE_DOCKER_TEST_NAME:-pg-koldstore-release-smoke}"
HOST_PORT="${KOLDSTORE_DOCKER_TEST_PORT:-55432}"
PASSWORD="${KOLDSTORE_DOCKER_TEST_PASSWORD:-postgres}"
DATABASE="${KOLDSTORE_DOCKER_TEST_DB:-koldstoredb}"

cleanup() {
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Run psql inside the server container (new backend each call).
# `-i` so heredocs and piped SQL reach psql.
psql_exec() {
  docker exec -i -e PGPASSWORD="${PASSWORD}" "${CONTAINER_NAME}" \
    psql -U postgres -d "${DATABASE}" "$@"
}

# Connect over the published host port without requiring host postgresql-client.
# Shares the server container network namespace and overrides entrypoint to psql.
psql_host() {
  docker run --rm \
    --network "container:${CONTAINER_NAME}" \
    --entrypoint psql \
    -e PGPASSWORD="${PASSWORD}" \
    "${IMAGE}" \
    -h 127.0.0.1 -U postgres -d "${DATABASE}" "$@"
}

cleanup

echo "==> starting ${IMAGE}"
docker run -d \
  --name "${CONTAINER_NAME}" \
  -e POSTGRES_PASSWORD="${PASSWORD}" \
  -e POSTGRES_DB="${DATABASE}" \
  -p "${HOST_PORT}:5432" \
  "${IMAGE}" >/dev/null

echo "==> waiting for pg_isready"
deadline=$((SECONDS + 120))
until docker exec "${CONTAINER_NAME}" pg_isready -U postgres -d "${DATABASE}" >/dev/null 2>&1; do
  if (( SECONDS > deadline )); then
    echo "error: PostgreSQL did not become ready" >&2
    docker logs "${CONTAINER_NAME}" >&2 || true
    exit 1
  fi
  if [[ "$(docker inspect -f '{{.State.Status}}' "${CONTAINER_NAME}")" != "running" ]]; then
    echo "error: container exited before becoming ready" >&2
    docker logs "${CONTAINER_NAME}" >&2 || true
    exit 1
  fi
  sleep 2
done

echo "==> waiting for koldstore from initdb"
until psql_exec -tAc \
  "SELECT 1 FROM pg_extension WHERE extname = 'koldstore'" \
  | grep -q '^[[:space:]]*1[[:space:]]*$'; do
  if (( SECONDS > deadline )); then
    echo "error: expected koldstore in pg_extension" >&2
    psql_exec -c "SELECT extname FROM pg_extension ORDER BY 1;" >&2 || true
    docker logs "${CONTAINER_NAME}" >&2 || true
    exit 1
  fi
  sleep 2
done

echo "==> confirming pg_cron is packaged but not preloaded; koldstore is preloaded"
docker exec "${CONTAINER_NAME}" bash -lc '
  set -euo pipefail
  test -f "$(pg_config --sharedir)/extension/pg_cron.control"
  test -f "$(pg_config --pkglibdir)/pg_cron.so"
  test -f "$(pg_config --pkglibdir)/koldstore.so"
'
preload="$(psql_exec -tAc "SHOW shared_preload_libraries" | tr -d '[:space:]')"
case ",${preload}," in
  *,pg_cron,*)
    echo "error: shared_preload_libraries unexpectedly includes pg_cron (got '${preload}')" >&2
    exit 1
    ;;
esac
case ",${preload}," in
  *,koldstore,*)
    ;;
  *)
    echo "error: shared_preload_libraries must include koldstore (got '${preload}')" >&2
    exit 1
    ;;
esac

echo "==> CREATE EXTENSION koldstore is idempotent"
psql_exec -v ON_ERROR_STOP=1 <<'SQL'
CREATE EXTENSION IF NOT EXISTS koldstore;
SELECT koldstore_version();
SELECT extname FROM pg_extension WHERE extname = 'koldstore';
SQL

echo "==> ALTER TABLE management syntax works in the release image"
psql_exec -v ON_ERROR_STOP=1 <<'SQL'
SELECT koldstore.register_storage(
  name         => 'release-smoke',
  storage_type => 'filesystem',
  base_path    => '/tmp/koldstore-release-smoke',
  credentials  => '{}'::jsonb,
  config       => '{}'::jsonb
);
CREATE TABLE release_smoke_messages (
  id bigint PRIMARY KEY,
  body text NOT NULL
);
ALTER TABLE release_smoke_messages SET (
  koldstore_enabled = true,
  koldstore_storage = 'release-smoke',
  koldstore_hot_row_limit = 1000,
  koldstore_min_flush_rows = 1,
  koldstore_max_rows_per_file = 1000
);
DO $$
BEGIN
  IF (
    SELECT options->'flush_policy'->>'type'
    FROM koldstore.schemas
    WHERE table_oid = 'release_smoke_messages'::regclass
  ) IS DISTINCT FROM 'row_limit' THEN
    RAISE EXCEPTION 'ALTER TABLE did not persist a row_limit policy';
  END IF;
END
$$;
SQL

echo "==> virgin session merge-scan GUCs (no prior koldstore SQL beyond SHOW)"
# Fresh client process + fresh backend via TCP to the container's Postgres port.
psql_host -v ON_ERROR_STOP=1 \
  -c "SHOW koldstore.enable_merge_scan;" \
  -c "SELECT koldstore.preload_status();" >/dev/null

echo "==> published host port accepts TCP (listen_addresses=*)"
# Reach the published host port from a second container on the default bridge
# using host.docker.internal (Linux CI needs host-gateway).
docker run --rm \
  --add-host=host.docker.internal:host-gateway \
  --entrypoint psql \
  -e PGPASSWORD="${PASSWORD}" \
  "${IMAGE}" \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d "${DATABASE}" \
  -v ON_ERROR_STOP=1 \
  -c "SELECT koldstore_version();" >/dev/null

echo "ok: ${IMAGE} starts with koldstore preloaded; pg_cron packaged but not enabled"
