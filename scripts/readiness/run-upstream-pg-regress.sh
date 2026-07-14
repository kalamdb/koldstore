#!/usr/bin/env bash
# Optional: run upstream PostgreSQL installcheck against a cluster with koldstore loaded.
#
# This is an EXTERNAL confidence signal only. Passing does not claim full PostgreSQL
# behavioral compatibility. Skip gracefully when upstream sources are unavailable.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${1:-${KOLDSTORE_E2E_PGVERSION:-16}}"
PG_SRC="${KOLDSTORE_PG_SRC:-}"
PG_CONFIG="${PGRX_PG_CONFIG:-$(cargo pgrx info pg-config "$PG_VERSION" 2>/dev/null || true)}"

if [[ -z "${PG_SRC}" ]]; then
  # Common local layout hints; none are required.
  for candidate in \
    "${HOME}/src/postgresql-${PG_VERSION}" \
    "${HOME}/postgresql-${PG_VERSION}" \
    "/usr/src/postgresql-${PG_VERSION}"; do
    if [[ -d "${candidate}/src/test/regress" ]]; then
      PG_SRC="${candidate}"
      break
    fi
  done
fi

if [[ -z "${PG_SRC}" || ! -d "${PG_SRC}/src/test/regress" ]]; then
  echo "upstream PostgreSQL sources not found; skipping installcheck"
  echo "set KOLDSTORE_PG_SRC=/path/to/postgres to enable"
  exit 0
fi

if [[ -z "${PG_CONFIG}" || ! -x "${PG_CONFIG}" ]]; then
  echo "pg_config unavailable for PostgreSQL ${PG_VERSION}; skipping" >&2
  exit 0
fi

echo "NOTE: upstream installcheck is an external confidence signal only."
echo "It does not certify KoldStore as a drop-in replacement for all PG behavior."

export KOLDSTORE_E2E_PREPARE_ONLY=1
bash scripts/run-pg-e2e.sh "$PG_VERSION"

echo "running make installcheck in ${PG_SRC} (with koldstore available on the pgrx cluster)"
(
  cd "${PG_SRC}/src/test/regress"
  # Operators may need extra REGRESS_OPTS / EXTRA_INSTALL to load koldstore;
  # keep the default path skippable and documented.
  if [[ "${KOLDSTORE_UPSTREAM_REGRESS_EXECUTE:-0}" != "1" ]]; then
    echo "KOLDSTORE_UPSTREAM_REGRESS_EXECUTE is not 1; dry-run only (sources present)."
    echo "Set KOLDSTORE_UPSTREAM_REGRESS_EXECUTE=1 to invoke make installcheck."
    exit 0
  fi
  make installcheck
)
