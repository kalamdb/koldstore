#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNNER="$ROOT_DIR/scripts/run-pg-e2e.sh"

help_output="$($RUNNER --help)"
grep -Fq -- "WAL-only" <<<"$help_output"
if grep -Eq -- "--mode" <<<"$help_output"; then
  echo "expected run-pg-e2e.sh help to omit --mode" >&2
  exit 1
fi

invalid_output_file="$(mktemp)"
trap 'rm -f "$invalid_output_file"' EXIT
if "$RUNNER" --mode async >"$invalid_output_file" 2>&1; then
  echo "expected unknown --mode argument to fail" >&2
  exit 1
fi
grep -Fq -- "unknown argument" "$invalid_output_file"

echo "run-pg-e2e argument tests passed"
