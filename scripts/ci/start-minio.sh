#!/usr/bin/env bash
# Start a local MinIO for S3-backed E2E / storage tests and ensure the test bucket exists.
set -euo pipefail

MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9001}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-koldstore-test}"
MINIO_CONTAINER="${MINIO_CONTAINER:-koldstore-minio-e2e}"
MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:latest}"
MC_IMAGE="${MC_IMAGE:-minio/mc:latest}"

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker is required to start MinIO for E2E" >&2
  exit 1
fi

if docker ps --format '{{.Names}}' | grep -qx "${MINIO_CONTAINER}"; then
  echo "MinIO container ${MINIO_CONTAINER} already running"
else
  if docker ps -a --format '{{.Names}}' | grep -qx "${MINIO_CONTAINER}"; then
    docker rm -f "${MINIO_CONTAINER}" >/dev/null
  fi
  echo "starting MinIO on :${MINIO_PORT} (console :${MINIO_CONSOLE_PORT})"
  docker run -d --name "${MINIO_CONTAINER}" \
    -p "${MINIO_PORT}:9000" \
    -p "${MINIO_CONSOLE_PORT}:9001" \
    -e "MINIO_ROOT_USER=${MINIO_ROOT_USER}" \
    -e "MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}" \
    "${MINIO_IMAGE}" server /data --console-address ":9001" >/dev/null
fi

echo "waiting for MinIO health"
for _ in $(seq 1 60); do
  if curl -sf "http://127.0.0.1:${MINIO_PORT}/minio/health/live" >/dev/null; then
    break
  fi
  sleep 1
done
if ! curl -sf "http://127.0.0.1:${MINIO_PORT}/minio/health/live" >/dev/null; then
  echo "error: MinIO did not become healthy on port ${MINIO_PORT}" >&2
  docker logs "${MINIO_CONTAINER}" >&2 || true
  exit 1
fi

echo "ensuring bucket ${MINIO_BUCKET} exists"
docker run --rm --network host --entrypoint /bin/sh "${MC_IMAGE}" \
  -c "
    mc alias set local http://127.0.0.1:${MINIO_PORT} '${MINIO_ROOT_USER}' '${MINIO_ROOT_PASSWORD}' >/dev/null
    mc mb --ignore-existing \"local/${MINIO_BUCKET}\" >/dev/null
  "

export KOLDSTORE_MINIO=1
export KOLDSTORE_MINIO_ENDPOINT="http://127.0.0.1:${MINIO_PORT}"
export KOLDSTORE_MINIO_ACCESS_KEY="${MINIO_ROOT_USER}"
export KOLDSTORE_MINIO_SECRET_KEY="${MINIO_ROOT_PASSWORD}"
export KOLDSTORE_MINIO_BUCKET="${MINIO_BUCKET}"

# Emit GitHub Actions env exports when running under GHA.
if [[ -n "${GITHUB_ENV:-}" ]]; then
  {
    echo "KOLDSTORE_MINIO=1"
    echo "KOLDSTORE_MINIO_ENDPOINT=http://127.0.0.1:${MINIO_PORT}"
    echo "KOLDSTORE_MINIO_ACCESS_KEY=${MINIO_ROOT_USER}"
    echo "KOLDSTORE_MINIO_SECRET_KEY=${MINIO_ROOT_PASSWORD}"
    echo "KOLDSTORE_MINIO_BUCKET=${MINIO_BUCKET}"
  } >> "${GITHUB_ENV}"
fi

echo "MinIO ready at ${KOLDSTORE_MINIO_ENDPOINT} (bucket ${MINIO_BUCKET})"
