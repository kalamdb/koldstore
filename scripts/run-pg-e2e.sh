#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${1:-${KOLDSTORE_E2E_PGVERSION:-16}}"
PREPARE_ONLY="${KOLDSTORE_E2E_PREPARE_ONLY:-0}"
PG_FEATURE="pg${PG_VERSION}"
PG_PORT="${KOLDSTORE_E2E_PGPORT:-288${PG_VERSION}}"
PG_HOST="${KOLDSTORE_E2E_PGHOST:-127.0.0.1}"
PG_USER="${KOLDSTORE_E2E_PGUSER:-$(whoami)}"
PG_DATABASE="${KOLDSTORE_E2E_PGDATABASE:-koldstore_pgrx_e2e}"
PG_PASSWORD="${KOLDSTORE_E2E_PGPASSWORD:-}"
PG_CONFIG="${PGRX_PG_CONFIG:-$(cargo pgrx info pg-config "$PG_VERSION")}"
PSQL="$(dirname "$PG_CONFIG")/psql"

echo "starting pgrx-managed PostgreSQL ${PG_VERSION}"
cargo pgrx start "$PG_FEATURE"

echo "installing koldstore into pgrx PostgreSQL ${PG_VERSION}"
INSTALL_ARGS=(
  -p pg_koldstore
  --no-default-features
  --features "$PG_FEATURE"
  --pg-config "$PG_CONFIG"
)
if [[ "${KOLDSTORE_PGRX_INSTALL_RELEASE:-}" == "1" || "${KOLDSTORE_PGRX_INSTALL_RELEASE:-}" == "true" ]]; then
  # release-pg: optimized + panic=unwind (plain --release uses panic=abort and
  # aborts on PostgreSQL ereport/longjmp from extension hooks).
  INSTALL_ARGS+=(--profile release-pg)
fi
if [[ "${KOLDSTORE_PGRX_INSTALL_SUDO:-}" == "1" || "${KOLDSTORE_PGRX_INSTALL_SUDO:-}" == "true" ]]; then
  INSTALL_ARGS+=(--sudo)
fi
cargo pgrx install "${INSTALL_ARGS[@]}"

echo "restarting pgrx-managed PostgreSQL ${PG_VERSION} to load extension"
cargo pgrx stop "$PG_FEATURE"
cargo pgrx start "$PG_FEATURE"

echo "recreating local E2E database ${PG_DATABASE} on ${PG_HOST}:${PG_PORT}"
# WITH (FORCE) terminates leftover sessions (e.g. pg_cron scheduler) that would
# otherwise block DROP DATABASE after a prior cron smoke or interrupted run.
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d postgres -v ON_ERROR_STOP=1 \
  -c "DROP DATABASE IF EXISTS ${PG_DATABASE} WITH (FORCE)" \
  -c "CREATE DATABASE ${PG_DATABASE}"
"$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -v ON_ERROR_STOP=1 \
  -c "CREATE EXTENSION IF NOT EXISTS koldstore;"

server_version="$("$PSQL" -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DATABASE" -tAc "SHOW server_version")"
if [[ ! "${server_version}" =~ ^${PG_VERSION} ]]; then
  echo "error: expected PostgreSQL ${PG_VERSION} on ${PG_HOST}:${PG_PORT}, got '${server_version}'" >&2
  exit 1
fi
echo "verified pgrx PostgreSQL ${PG_VERSION} on ${PG_HOST}:${PG_PORT} (${server_version})"

export KOLDSTORE_E2E_PGVERSION="$PG_VERSION"
export KOLDSTORE_E2E_PGHOST="$PG_HOST"
export KOLDSTORE_E2E_PGPORT="$PG_PORT"
export KOLDSTORE_E2E_PGUSER="$PG_USER"
export KOLDSTORE_E2E_PGPASSWORD="$PG_PASSWORD"
export KOLDSTORE_E2E_PGDATABASE="$PG_DATABASE"
export KOLDSTORE_E2E_WAIT_FOR_STARTUP=1

if [[ "${PREPARE_ONLY}" == "1" || "${PREPARE_ONLY}" == "true" ]]; then
  echo "E2E PostgreSQL ${PG_VERSION} is ready (prepare-only; skipping cargo nextest)"
  exit 0
fi

echo "running pg-koldstore E2E tests against pgrx PostgreSQL ${PG_VERSION} on ${PG_HOST}:${PG_PORT}"
if [[ "${KOLDSTORE_MINIO:-}" == "1" || -n "${KOLDSTORE_MINIO_ENDPOINT:-}" ]]; then
  echo "MinIO-backed E2E enabled (KOLDSTORE_MINIO / KOLDSTORE_MINIO_ENDPOINT)"
else
  echo "MinIO-backed E2E skipped (set KOLDSTORE_MINIO=1 to enable flush_minio)"
fi

# Enumerate integration harnesses declared in tests/e2e/Cargo.toml.
E2E_HARNESSES=()
while IFS= read -r harness; do
  [[ -n "${harness}" ]] || continue
  E2E_HARNESSES+=("${harness}")
done < <(
  awk '/^\[\[test\]\]/{p=1;next} p&&/^name =/{gsub(/"/,"",$3); print $3; p=0}' \
    "${ROOT_DIR}/tests/e2e/Cargo.toml"
)
if [[ ${#E2E_HARNESSES[@]} -eq 0 ]]; then
  echo "error: no [[test]] harnesses found in tests/e2e/Cargo.toml" >&2
  exit 1
fi

FILTER="${KOLDSTORE_E2E_NEXTEST_FILTER:-}"
RUNNER="${KOLDSTORE_E2E_RUNNER:-}"
if [[ -z "${RUNNER}" ]]; then
  # macOS: nextest lists every harness with --list in parallel; those processes
  # often freeze in dyld_start (silent hang right after "Finished test profile").
  # Serial cargo test avoids that. Override with KOLDSTORE_E2E_RUNNER=nextest.
  if [[ "$(uname -s)" == "Darwin" ]]; then
    RUNNER=cargo
  else
    RUNNER=nextest
  fi
fi

NOCAPTURE_ARGS=()
if [[ "${KOLDSTORE_E2E_VERBOSE:-}" == "1" || "${KOLDSTORE_E2E_VERBOSE:-}" == "true" ]]; then
  echo "E2E verbose logging enabled (KOLDSTORE_E2E_VERBOSE); showing live test output"
  NOCAPTURE_ARGS+=(--nocapture)
fi

select_harnesses() {
  local filter="$1"
  local harness
  SELECTED_HARNESSES=()
  if [[ -z "${filter}" ]]; then
    SELECTED_HARNESSES=("${E2E_HARNESSES[@]}")
    return 0
  fi
  if [[ "${filter}" =~ ^binary\((.+)\)$ ]]; then
    local want="${BASH_REMATCH[1]}"
    for harness in "${E2E_HARNESSES[@]}"; do
      if [[ "${harness}" == "${want}" ]]; then
        SELECTED_HARNESSES+=("${harness}")
        return 0
      fi
    done
    echo "error: no e2e harness named '${want}' (KOLDSTORE_E2E_NEXTEST_FILTER=${filter})" >&2
    exit 1
  fi
  # test(name) / free-form: run all harnesses; cargo/nextest apply the name filter.
  SELECTED_HARNESSES=("${E2E_HARNESSES[@]}")
}

cargo_name_filter() {
  local filter="$1"
  if [[ -z "${filter}" ]]; then
    echo ""
    return 0
  fi
  if [[ "${filter}" =~ ^binary\((.+)\)$ ]]; then
    echo ""
    return 0
  fi
  if [[ "${filter}" =~ ^test\((.+)\)$ ]]; then
    echo "${BASH_REMATCH[1]}"
    return 0
  fi
  echo "${filter}"
}

if [[ "${RUNNER}" == "nextest" ]]; then
  if ! cargo nextest --version >/dev/null 2>&1; then
    echo "error: cargo-nextest is required; install with: cargo install cargo-nextest --locked" >&2
    echo "hint: on macOS set KOLDSTORE_E2E_RUNNER=cargo to avoid nextest, or install nextest" >&2
    exit 1
  fi
  NEXT_ARGS=(-p e2e --test-threads 1)
  if [[ -n "${FILTER}" ]]; then
    echo "E2E nextest filter: ${FILTER}"
    NEXT_ARGS+=(-E "${FILTER}")
  fi
  if [[ ${#NOCAPTURE_ARGS[@]} -gt 0 ]]; then
    NEXT_ARGS+=(--no-capture)
  fi
  echo "E2E runner: nextest (set KOLDSTORE_E2E_RUNNER=cargo if discovery hangs after compile)"
  cargo nextest run "${NEXT_ARGS[@]}"
  exit 0
fi

if [[ "${RUNNER}" != "cargo" ]]; then
  echo "error: unknown KOLDSTORE_E2E_RUNNER='${RUNNER}' (expected cargo or nextest)" >&2
  exit 1
fi

select_harnesses "${FILTER}"
NAME_FILTER="$(cargo_name_filter "${FILTER}")"
echo "E2E runner: serial cargo test (${#SELECTED_HARNESSES[@]} harnesses; avoids macOS nextest --list dyld hang)"
if [[ -n "${FILTER}" ]]; then
  echo "E2E filter: ${FILTER}"
fi

# Build once so per-harness runs do not each wait on a cold compile with no output.
echo "building e2e test binaries..."
cargo test -p e2e --no-run -j1

failed=0
for harness in "${SELECTED_HARNESSES[@]}"; do
  echo "=== e2e harness: ${harness} ==="
  cmd=(cargo test -p e2e --test "${harness}" -j1)
  if [[ -n "${NAME_FILTER}" ]]; then
    cmd+=("${NAME_FILTER}")
  fi
  cmd+=(-- --test-threads 1)
  if [[ ${#NOCAPTURE_ARGS[@]} -gt 0 ]]; then
    cmd+=("${NOCAPTURE_ARGS[@]}")
  fi
  if ! "${cmd[@]}"; then
    echo "error: harness ${harness} failed" >&2
    failed=1
    if [[ "${KOLDSTORE_E2E_FAIL_FAST:-1}" == "1" || "${KOLDSTORE_E2E_FAIL_FAST:-}" == "true" ]]; then
      exit 1
    fi
  fi
done

if [[ "${failed}" -ne 0 ]]; then
  echo "error: one or more e2e harnesses failed" >&2
  exit 1
fi
echo "E2E complete: ${#SELECTED_HARNESSES[@]} harnesses passed"
