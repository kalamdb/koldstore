#!/usr/bin/env bash
# Convert nextest JUnit output into HTML (+ optional Actions job summary).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROFILE="${1:-ci}"
TITLE="${2:-Test report}"
ARTIFACT_DIR="${3:-${ROOT_DIR}/target/ci-reports}"
JUNIT_PATH="${ROOT_DIR}/target/nextest/${PROFILE}/junit.xml"

mkdir -p "${ARTIFACT_DIR}"
if [[ ! -f "${JUNIT_PATH}" ]]; then
  echo "warning: no JUnit report at ${JUNIT_PATH}; skipping HTML publish" >&2
  exit 0
fi

slug="$(printf '%s' "${TITLE}" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9._-' '-' | sed 's/^-//;s/-$//')"
html_out="${ARTIFACT_DIR}/${slug}.html"
md_out="${ARTIFACT_DIR}/${slug}.md"
cp "${JUNIT_PATH}" "${ARTIFACT_DIR}/${slug}.junit.xml"

python3 "${ROOT_DIR}/scripts/ci/junit-to-html.py" \
  "${JUNIT_PATH}" \
  -o "${html_out}" \
  --title "${TITLE}" \
  --summary-md "${md_out}"

echo "CI_REPORT_HTML=${html_out}"
echo "CI_REPORT_JUNIT=${ARTIFACT_DIR}/${slug}.junit.xml"
