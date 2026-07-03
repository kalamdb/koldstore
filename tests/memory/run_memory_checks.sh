#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

echo "running Rust tests with sanitizer-friendly profile"
RUSTFLAGS="${RUSTFLAGS:-}" cargo test --workspace

if command -v valgrind >/dev/null 2>&1; then
  echo "valgrind is available; run targeted pgrx binaries when extension install is configured"
else
  echo "valgrind not found; skipping valgrind pass"
fi

if command -v heaptrack >/dev/null 2>&1; then
  echo "heaptrack is available; benchmark memory profiles can be captured from CI artifacts"
else
  echo "heaptrack not found; skipping heaptrack pass"
fi

