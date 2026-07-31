#!/usr/bin/env bash
# Run `apt-get` with a wall-clock timeout and a few outer retries.
#
# Usage: scripts/ci/apt-get-retry.sh <apt-get args...>
# Example: scripts/ci/apt-get-retry.sh update -y -qq
#          scripts/ci/apt-get-retry.sh install -y build-essential
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <apt-get args...>" >&2
  exit 2
fi

export DEBIAN_FRONTEND=noninteractive

APT_GET=(apt-get)
if [[ "$(id -u)" -ne 0 ]]; then
  APT_GET=(sudo -E apt-get)
fi

# Per-attempt budget: large enough for normal package sets, short enough that a
# stuck mirror does not burn the whole workflow timeout.
ATTEMPT_TIMEOUT_SECS="${KOLDSTORE_APT_ATTEMPT_TIMEOUT_SECS:-300}"
MAX_ATTEMPTS="${KOLDSTORE_APT_MAX_ATTEMPTS:-3}"

attempt=1
while (( attempt <= MAX_ATTEMPTS )); do
  echo "==> apt-get $* (attempt ${attempt}/${MAX_ATTEMPTS}, timeout ${ATTEMPT_TIMEOUT_SECS}s)"
  if timeout --signal=TERM --kill-after=30s "${ATTEMPT_TIMEOUT_SECS}" \
    "${APT_GET[@]}" -o Dpkg::Use-Pty=0 "$@"; then
    exit 0
  fi
  status=$?
  echo "warning: apt-get failed with status ${status} on attempt ${attempt}" >&2
  if (( attempt == MAX_ATTEMPTS )); then
    break
  fi
  # Refresh indexes between retries; ignore failures so we still retry install.
  timeout --signal=TERM --kill-after=15s 90s \
    "${APT_GET[@]}" -o Dpkg::Use-Pty=0 update -y -qq || true
  sleep $(( attempt * 5 ))
  attempt=$(( attempt + 1 ))
done

echo "error: apt-get $* failed after ${MAX_ATTEMPTS} attempts" >&2
exit 1
