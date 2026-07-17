#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNNER="$ROOT_DIR/scripts/run-pg-e2e.sh"

help_output="$($RUNNER --help)"
grep -Fq -- "--mode <strict|async>" <<<"$help_output"

invalid_output_file="$(mktemp)"
trap 'rm -f "$invalid_output_file"' EXIT
if "$RUNNER" --mode unsupported >"$invalid_output_file" 2>&1; then
  echo "expected unsupported E2E mode to fail" >&2
  exit 1
fi
grep -Fq -- "expected strict or async" "$invalid_output_file"

echo "run-pg-e2e argument tests passed"
