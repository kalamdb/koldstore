#!/usr/bin/env bash
# Upsert a single sticky PR comment for storage-bench results.
# Usage: post-storage-bench-comment.sh <markdown-file>
set -euo pipefail

MARKDOWN_FILE="${1:?markdown file required}"
MARKER="<!-- koldstore-storage-bench -->"

if [[ "${GITHUB_EVENT_NAME:-}" != "pull_request" ]]; then
  echo "skipping PR comment (event=${GITHUB_EVENT_NAME:-none})"
  exit 0
fi

if [[ -z "${GITHUB_TOKEN:-}${GH_TOKEN:-}" ]]; then
  echo "error: GITHUB_TOKEN is required to post PR comments" >&2
  exit 1
fi
export GH_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN}}"

PR_NUMBER="${GITHUB_EVENT_PULL_REQUEST_NUMBER:-${PR_NUMBER:-}}"
if [[ -z "${PR_NUMBER}" && -n "${GITHUB_EVENT_PATH:-}" ]]; then
  PR_NUMBER="$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['pull_request']['number'])" "${GITHUB_EVENT_PATH}")"
fi
if [[ -z "${PR_NUMBER}" ]]; then
  echo "error: could not resolve pull request number" >&2
  exit 1
fi

REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY required}"

if ! command -v gh >/dev/null 2>&1; then
  echo "error: gh CLI is required" >&2
  exit 1
fi

existing_id="$(
  gh api "repos/${REPO}/issues/${PR_NUMBER}/comments" --paginate \
    --jq ".[] | select(.body | contains(\"${MARKER}\")) | .id" \
    | head -n 1 || true
)"

payload="$(python3 -c "import json,sys; print(json.dumps({'body': open(sys.argv[1], encoding='utf-8').read()}))" "${MARKDOWN_FILE}")"

if [[ -n "${existing_id}" ]]; then
  echo "updating storage-bench comment ${existing_id}"
  printf '%s' "${payload}" | gh api "repos/${REPO}/issues/comments/${existing_id}" \
    -X PATCH \
    --input - >/dev/null
else
  echo "creating storage-bench comment on PR #${PR_NUMBER}"
  printf '%s' "${payload}" | gh api "repos/${REPO}/issues/${PR_NUMBER}/comments" \
    --input - >/dev/null
fi
