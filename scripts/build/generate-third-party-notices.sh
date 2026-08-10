#!/usr/bin/env bash
# Regenerates the license bundle shipped with KoldStore release artifacts.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

if ! command -v cargo-about >/dev/null 2>&1; then
  echo "error: cargo-about is required; install cargo-about 0.8.4 first" >&2
  exit 1
fi

cargo about -L off generate \
  --locked \
  --fail \
  --manifest-path crates/pg_koldstore/Cargo.toml \
  --no-default-features \
  --features 'pg16 s3' \
  -c about.toml \
  -o THIRD_PARTY_NOTICES.html \
  third_party_licenses.hbs
