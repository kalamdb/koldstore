#!/usr/bin/env bash
# Regenerate the README terminal demo GIF with Charmbracelet VHS.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TAPE="docs/demo/koldstore.tape"
OUTPUT="docs/assets/koldstore-demo.gif"
IMAGE="${KOLDSTORE_DEMO_IMAGE:-jamals86/pg-koldstore:latest}"
PLATFORM="${KOLDSTORE_DEMO_PLATFORM:-linux/amd64}"

usage() {
  cat <<'EOF'
Generate docs/assets/koldstore-demo.gif from docs/demo/koldstore.tape.

Usage:
  scripts/generate-readme-demo.sh

Environment:
  KOLDSTORE_DEMO_IMAGE     Docker image (default: jamals86/pg-koldstore:latest)
  KOLDSTORE_DEMO_PLATFORM  docker pull/run platform (default: linux/amd64)

Requires: vhs, docker, ffmpeg, ttyd
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

for cmd in vhs docker; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "error: $cmd is required" >&2
    exit 1
  fi
done

if [[ ! -f "$TAPE" ]]; then
  echo "error: missing $TAPE" >&2
  exit 1
fi

if [[ ! -f docs/demo/setup.sql ]]; then
  echo "error: missing docs/demo/setup.sql" >&2
  exit 1
fi

if [[ ! -f docs/demo/koldstore-demo.sql ]]; then
  echo "error: missing docs/demo/koldstore-demo.sql" >&2
  exit 1
fi

cleanup() {
  docker rm -f koldstore-readme-demo >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Pulling ${IMAGE} (${PLATFORM})..."
docker pull --platform "$PLATFORM" "$IMAGE"

echo "Recording ${TAPE}..."
vhs "$TAPE"

if [[ ! -f "$OUTPUT" ]]; then
  echo "error: expected output missing: $OUTPUT" >&2
  exit 1
fi

echo "Wrote $OUTPUT ($(wc -c < "$OUTPUT" | tr -d ' ') bytes)"
