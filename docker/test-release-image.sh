#!/usr/bin/env bash
# Smoke-test a published/candidate pg-koldstore release image.
# Verifies PostgreSQL starts, koldstore is created, and pg_cron is packaged
# (but not auto-enabled). Asserts koldstore is shared-preloaded.
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
until docker exec -e PGPASSWORD="${PASSWORD}" "${CONTAINER_NAME}" \
  psql -U postgres -d "${DATABASE}" -tAc \
  "SELECT 1 FROM pg_extension WHERE extname = 'koldstore'" \
  | grep -q '^[[:space:]]*1[[:space:]]*$'; do
  if (( SECONDS > deadline )); then
    echo "error: expected koldstore in pg_extension" >&2
    docker exec -e PGPASSWORD="${PASSWORD}" "${CONTAINER_NAME}" \
      psql -U postgres -d "${DATABASE}" -c "SELECT extname FROM pg_extension ORDER BY 1;" >&2 || true
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
preload="$(docker exec -e PGPASSWORD="${PASSWORD}" "${CONTAINER_NAME}" \
  psql -U postgres -d "${DATABASE}" -tAc "SHOW shared_preload_libraries" | tr -d '[:space:]')"
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
docker exec -e PGPASSWORD="${PASSWORD}" "${CONTAINER_NAME}" \
  psql -U postgres -d "${DATABASE}" -v ON_ERROR_STOP=1 <<'SQL'
CREATE EXTENSION IF NOT EXISTS koldstore;
SELECT koldstore_version();
SELECT extname FROM pg_extension WHERE extname = 'koldstore';
SQL

echo "==> virgin session merge-scan GUCs (no prior koldstore SQL beyond SHOW)"
PGPASSWORD="${PASSWORD}" psql \
  "postgres://postgres:${PASSWORD}@127.0.0.1:${HOST_PORT}/${DATABASE}" \
  -v ON_ERROR_STOP=1 \
  -c "SHOW koldstore.enable_merge_scan;" \
  -c "SELECT koldstore.preload_status();" >/dev/null

echo "==> host TCP connect via published port"
PGPASSWORD="${PASSWORD}" psql \
  "postgres://postgres:${PASSWORD}@127.0.0.1:${HOST_PORT}/${DATABASE}" \
  -v ON_ERROR_STOP=1 \
  -c "SELECT koldstore_version();" >/dev/null

echo "ok: ${IMAGE} starts with koldstore preloaded; pg_cron packaged but not enabled"
