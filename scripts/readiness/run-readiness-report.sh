#!/usr/bin/env bash
# Emit a machine-readable JSON readiness report (+ optional Markdown from template).
#
# Never claims "production safe". Approved gate wording is embedded in the JSON
# summary when all recorded gates passed.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PG_VERSION="${1:-${KOLDSTORE_E2E_PGVERSION:-16}}"
OUT_DIR="${KOLDSTORE_READINESS_OUT:-${ROOT_DIR}/target/readiness}"
mkdir -p "$OUT_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
JSON_OUT="${OUT_DIR}/readiness-${PG_VERSION}-${STAMP}.json"
MD_OUT="${OUT_DIR}/readiness-${PG_VERSION}-${STAMP}.md"
TEMPLATE="${ROOT_DIR}/docs/templates/production-readiness-report.md"

TEST_CATEGORY="${KOLDSTORE_REPORT_CATEGORY:-nightly-readiness}"
PASSED="${KOLDSTORE_REPORT_PASSED:-true}"
DURATION="${KOLDSTORE_REPORT_DURATION:-unknown}"
SEED="${KOLDSTORE_REPORT_SEED:-}"
CRASH_RESTART_COUNT="${KOLDSTORE_REPORT_CRASH_COUNT:-0}"
ROWS_COMPARED="${KOLDSTORE_REPORT_ROWS_COMPARED:-0}"
SEGMENTS_CHECKED="${KOLDSTORE_REPORT_SEGMENTS_CHECKED:-0}"
AMCHECK_RESULT="${KOLDSTORE_REPORT_AMCHECK:-skipped}"
ARTIFACT_PATHS="${KOLDSTORE_REPORT_ARTIFACTS:-${OUT_DIR}}"
KNOWN_EXCLUSIONS="${KOLDSTORE_REPORT_EXCLUSIONS:-HammerDB skipped unless installed; SQLsmith skipped unless installed; upstream PG regress is external signal only}"
REMAINING_RISKS="${KOLDSTORE_REPORT_RISKS:-Full PostgreSQL behavioral compatibility; SERIALIZABLE anomalies across hot+cold; all object-store vendor failure modes; long-running memory leaks; backup/PITR + pg_upgrade edge cases; uninstrumented code paths}"

if [[ "${PASSED}" == "true" || "${PASSED}" == "1" ]]; then
  SUMMARY="All currently implemented production-readiness gates passed for PostgreSQL ${PG_VERSION} under the documented test configurations."
else
  SUMMARY="One or more implemented production-readiness gates failed for PostgreSQL ${PG_VERSION}. See gate results and artifacts."
fi

python3 - <<PY
import json
from pathlib import Path

passed = $([[ "${PASSED}" == "true" || "${PASSED}" == "1" ]] && echo True || echo False)
report = {
    "test_category": "${TEST_CATEGORY}",
    "postgresql_version": "${PG_VERSION}",
    "passed": passed,
    "duration": "${DURATION}",
    "seed": "${SEED}",
    "crash_restart_count": int("${CRASH_RESTART_COUNT}" or "0"),
    "rows_compared": int("${ROWS_COMPARED}" or "0"),
    "segments_checked": int("${SEGMENTS_CHECKED}" or "0"),
    "pg_amcheck_result": "${AMCHECK_RESULT}",
    "artifact_log_locations": "${ARTIFACT_PATHS}",
    "known_exclusions": "${KNOWN_EXCLUSIONS}",
    "remaining_risks": "${REMAINING_RISKS}",
    "summary": """${SUMMARY}""",
    "wording_note": "Never claim production safe merely because gates passed.",
}
Path("""${JSON_OUT}""").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
print(f"wrote {Path('''${JSON_OUT}''')}")
PY

if [[ -f "$TEMPLATE" ]]; then
  sed \
    -e "s/{{TEST_CATEGORY}}/${TEST_CATEGORY}/g" \
    -e "s/{{PG_VERSION}}/${PG_VERSION}/g" \
    -e "s/{{PG_VERSIONS}}/${PG_VERSION}/g" \
    -e "s/{{PASSED}}/${PASSED}/g" \
    -e "s/{{DURATION}}/${DURATION}/g" \
    -e "s/{{SEED}}/${SEED}/g" \
    -e "s/{{CRASH_RESTART_COUNT}}/${CRASH_RESTART_COUNT}/g" \
    -e "s/{{ROWS_COMPARED}}/${ROWS_COMPARED}/g" \
    -e "s/{{SEGMENTS_CHECKED}}/${SEGMENTS_CHECKED}/g" \
    -e "s/{{AMCHECK_RESULT}}/${AMCHECK_RESULT}/g" \
    -e "s|{{ARTIFACT_PATHS}}|${ARTIFACT_PATHS}|g" \
    -e "s|{{KNOWN_EXCLUSIONS}}|${KNOWN_EXCLUSIONS}|g" \
    -e "s|{{REMAINING_RISKS}}|${REMAINING_RISKS}|g" \
    -e "s/{{UNIT_RESULT}}/${KOLDSTORE_REPORT_UNIT:-n\/a}/g" \
    -e "s/{{PG_TEST_RESULT}}/${KOLDSTORE_REPORT_PG_TEST:-n\/a}/g" \
    -e "s/{{SQLREG_RESULT}}/${KOLDSTORE_REPORT_SQLREG:-n\/a}/g" \
    -e "s/{{ISOLATION_RESULT}}/${KOLDSTORE_REPORT_ISOLATION:-n\/a}/g" \
    -e "s/{{CRASH_RESULT}}/${KOLDSTORE_REPORT_CRASH:-n\/a}/g" \
    -e "s/{{SQLSMITH_RESULT}}/${KOLDSTORE_REPORT_SQLSMITH:-n\/a}/g" \
    -e "s/{{INTEGRITY_RESULT}}/${KOLDSTORE_REPORT_INTEGRITY:-n\/a}/g" \
    -e "s/{{HAMMER_RESULT}}/${KOLDSTORE_REPORT_HAMMER:-n\/a}/g" \
    "$TEMPLATE" >"$MD_OUT"
  echo "wrote ${MD_OUT}"
fi
