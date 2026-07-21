#!/usr/bin/env bash
# Negative smoke: loading koldstore without shared_preload must ERROR.
#
# Uses a temporary postgresql.conf override on a disposable Docker container
# that starts *without* koldstore in shared_preload_libraries, then attempts
# LOAD / CREATE EXTENSION and asserts the hard fail-closed message.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${1:-${KOLDSTORE_PRELOAD_NEG_IMAGE:-jamals86/pg-koldstore:latest}}"
CONTAINER_NAME="${KOLDSTORE_PRELOAD_NEG_NAME:-pg-koldstore-preload-neg}"
PASSWORD="${KOLDSTORE_PRELOAD_NEG_PASSWORD:-postgres}"
DATABASE="${KOLDSTORE_PRELOAD_NEG_DB:-koldstoredb}"

cleanup() {
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

echo "==> starting ${IMAGE} without koldstore shared_preload"
# Bypass release-entrypoint so we control preload ourselves.
docker run -d \
  --name "${CONTAINER_NAME}" \
  -e POSTGRES_PASSWORD="${PASSWORD}" \
  -e POSTGRES_DB="${DATABASE}" \
  --entrypoint docker-entrypoint.sh \
  "${IMAGE}" \
  postgres \
  -c listen_addresses='*' \
  -c shared_preload_libraries='' >/dev/null

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
  sleep 1
done

echo "==> LOAD 'koldstore' must fail without shared_preload"
if docker exec -e PGPASSWORD="${PASSWORD}" "${CONTAINER_NAME}" \
  psql -U postgres -d "${DATABASE}" -v ON_ERROR_STOP=1 \
  -c "LOAD 'koldstore';" >/tmp/koldstore-preload-neg.out 2>&1; then
  echo "error: LOAD 'koldstore' unexpectedly succeeded without shared_preload" >&2
  cat /tmp/koldstore-preload-neg.out >&2
  exit 1
fi
if ! grep -qi 'shared_preload_libraries' /tmp/koldstore-preload-neg.out; then
  echo "error: expected shared_preload_libraries message, got:" >&2
  cat /tmp/koldstore-preload-neg.out >&2
  exit 1
fi

echo "ok: LOAD without shared_preload fails closed"
echo "note: image used was ${IMAGE}; rebuild release image after packaging changes"
