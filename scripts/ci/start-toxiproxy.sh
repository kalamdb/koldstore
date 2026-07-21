#!/usr/bin/env bash
# Start Toxiproxy in front of MinIO for network fault E2E (external Docker image).
#
# API:    http://127.0.0.1:${TOXIPROXY_API_PORT:-8474}
# Listen: ${TOXIPROXY_LISTEN_PORT:-19000} -> host MinIO on ${MINIO_PORT:-9000}
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MINIO_PORT="${MINIO_PORT:-9000}"
TOXIPROXY_LISTEN_PORT="${TOXIPROXY_LISTEN_PORT:-19000}"
TOXIPROXY_API_PORT="${TOXIPROXY_API_PORT:-8474}"
TOXIPROXY_CONTAINER="${TOXIPROXY_CONTAINER:-koldstore-toxiproxy-e2e}"
TOXIPROXY_IMAGE="${TOXIPROXY_IMAGE:-ghcr.io/shopify/toxiproxy:2.9.0}"
PROXY_NAME="${TOXIPROXY_PROXY_NAME:-minio}"

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker is required to start Toxiproxy" >&2
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "error: curl is required to configure Toxiproxy" >&2
  exit 1
fi

bash "${ROOT_DIR}/scripts/ci/start-minio.sh"

if docker ps -a --format '{{.Names}}' | grep -qx "${TOXIPROXY_CONTAINER}"; then
  docker rm -f "${TOXIPROXY_CONTAINER}" >/dev/null
fi

echo "starting Toxiproxy ${TOXIPROXY_IMAGE}"
docker run -d --name "${TOXIPROXY_CONTAINER}" \
  -p "${TOXIPROXY_API_PORT}:8474" \
  -p "${TOXIPROXY_LISTEN_PORT}:19000" \
  --add-host=host.docker.internal:host-gateway \
  "${TOXIPROXY_IMAGE}" >/dev/null

echo "waiting for Toxiproxy API"
for _ in $(seq 1 60); do
  if curl -sf "http://127.0.0.1:${TOXIPROXY_API_PORT}/version" >/dev/null; then
    break
  fi
  sleep 1
done
if ! curl -sf "http://127.0.0.1:${TOXIPROXY_API_PORT}/version" >/dev/null; then
  echo "error: Toxiproxy API did not become ready" >&2
  docker logs "${TOXIPROXY_CONTAINER}" >&2 || true
  exit 1
fi

# Prefer host.docker.internal; fall back to docker bridge gateway.
upstream="host.docker.internal:${MINIO_PORT}"
if ! curl -sf -X POST "http://127.0.0.1:${TOXIPROXY_API_PORT}/proxies" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"${PROXY_NAME}\",\"listen\":\"0.0.0.0:19000\",\"upstream\":\"${upstream}\",\"enabled\":true}" \
  >/dev/null; then
  upstream="172.17.0.1:${MINIO_PORT}"
  curl -sf -X POST "http://127.0.0.1:${TOXIPROXY_API_PORT}/proxies" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"${PROXY_NAME}\",\"listen\":\"0.0.0.0:19000\",\"upstream\":\"${upstream}\",\"enabled\":true}" \
    >/dev/null
fi

export KOLDSTORE_TOXIPROXY=1
export KOLDSTORE_TOXIPROXY_API="http://127.0.0.1:${TOXIPROXY_API_PORT}"
export KOLDSTORE_TOXIPROXY_PROXY="${PROXY_NAME}"
export KOLDSTORE_MINIO=1
export KOLDSTORE_MINIO_ENDPOINT="http://127.0.0.1:${TOXIPROXY_LISTEN_PORT}"
export KOLDSTORE_MINIO_ACCESS_KEY="${MINIO_ROOT_USER:-minioadmin}"
export KOLDSTORE_MINIO_SECRET_KEY="${MINIO_ROOT_PASSWORD:-minioadmin}"
export KOLDSTORE_MINIO_BUCKET="${MINIO_BUCKET:-koldstore-test}"

if [[ -n "${GITHUB_ENV:-}" ]]; then
  {
    echo "KOLDSTORE_TOXIPROXY=1"
    echo "KOLDSTORE_TOXIPROXY_API=${KOLDSTORE_TOXIPROXY_API}"
    echo "KOLDSTORE_TOXIPROXY_PROXY=${PROXY_NAME}"
    echo "KOLDSTORE_MINIO=1"
    echo "KOLDSTORE_MINIO_ENDPOINT=${KOLDSTORE_MINIO_ENDPOINT}"
    echo "KOLDSTORE_MINIO_ACCESS_KEY=${KOLDSTORE_MINIO_ACCESS_KEY}"
    echo "KOLDSTORE_MINIO_SECRET_KEY=${KOLDSTORE_MINIO_SECRET_KEY}"
    echo "KOLDSTORE_MINIO_BUCKET=${KOLDSTORE_MINIO_BUCKET}"
  } >> "${GITHUB_ENV}"
fi

echo "Toxiproxy ready: API ${KOLDSTORE_TOXIPROXY_API} upstream=${upstream} endpoint=${KOLDSTORE_MINIO_ENDPOINT}"
