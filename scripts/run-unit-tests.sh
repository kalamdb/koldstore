#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

usage() {
  cat <<'EOF'
Usage: scripts/run-unit-tests.sh [options]

Runs workspace unit / shell-contract tests via cargo-nextest.

Excludes suites that need a prepared pgrx PostgreSQL (or are covered by
dedicated scripts): e2e, examples, storage-comparison, benchmarks,
memory-tests, and stress.

This matches CI "Unit tests (nextest)" and scripts/run-all-tests.sh unit step.
Does not run in-server #[pg_test] (use scripts/run-all-tests.sh / CI pg_test).

Options:
  --ci                 Use nextest --profile ci (JUnit under target/nextest/ci/)
  -h, --help           Show this help text

Environment:
  KOLDSTORE_UNIT_NEXTEST_PROFILE  Optional nextest profile name (overrides --ci)
EOF
}

PROFILE_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --ci)
      PROFILE_ARGS=(--profile ci)
      shift
      ;;
    -*)
      echo "error: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
    *)
      echo "error: unexpected positional argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -n "${KOLDSTORE_UNIT_NEXTEST_PROFILE:-}" ]]; then
  PROFILE_ARGS=(--profile "${KOLDSTORE_UNIT_NEXTEST_PROFILE}")
fi

if ! cargo nextest --version >/dev/null 2>&1; then
  echo "error: cargo-nextest is required; install with: cargo install cargo-nextest --locked" >&2
  exit 1
fi

# Exclude suites that need a prepared pgrx cluster or are covered later.
UNIT_EXCLUDES=(
  --exclude e2e
  --exclude examples
  --exclude storage-comparison
  --exclude pg-koldstore-benchmarks
  --exclude koldstore-memory-tests
  --exclude stress
)

echo "running workspace unit tests (cargo nextest --workspace --no-default-features)"
cargo nextest run \
  ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"} \
  --workspace \
  --no-default-features \
  "${UNIT_EXCLUDES[@]}"
echo "unit tests passed"
