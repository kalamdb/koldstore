#!/usr/bin/env bash
# Install the PGDG apt repository signing key.
#
# Prefers the vendored key in this repo (avoids flaky www.postgresql.org
# connectivity on CI runners), then falls back to a few HTTPS mirrors with
# retries.
#
# Usage:
#   scripts/ci/install-pgdg-key.sh /usr/share/keyrings/postgresql.gpg
#   scripts/ci/install-pgdg-key.sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.gpg
#
# Writes a binary keyring (gpg --dearmor) to the destination path.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VENDORED_KEY="${ROOT_DIR}/scripts/ci/keys/ACCC4CF8.asc"

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <destination-keyring-path>" >&2
  exit 2
fi

DEST="$1"
DEST_DIR="$(dirname "${DEST}")"

run_as_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

dearmor_to_dest() {
  local src="$1"
  run_as_root install -d "${DEST_DIR}"
  # Write via temp + mv so a failed dearmor cannot leave a truncated keyring.
  local tmp
  tmp="$(mktemp)"
  # shellcheck disable=SC2064
  trap 'rm -f "'"${tmp}"'"' RETURN
  if ! gpg --batch --yes --dearmor -o "${tmp}" "${src}"; then
    return 1
  fi
  if [[ ! -s "${tmp}" ]]; then
    echo "error: dearmored PGDG keyring is empty" >&2
    return 1
  fi
  run_as_root install -m 0644 "${tmp}" "${DEST}"
}

fetch_url_to() {
  local url="$1"
  local out="$2"
  curl -fsSL --connect-timeout 10 --max-time 60 --retry 3 --retry-delay 2 \
    --retry-all-errors -o "${out}" "${url}"
}

if [[ -f "${VENDORED_KEY}" ]]; then
  echo "==> installing vendored PGDG key (${VENDORED_KEY})"
  dearmor_to_dest "${VENDORED_KEY}"
  exit 0
fi

echo "warning: vendored PGDG key missing at ${VENDORED_KEY}; fetching over HTTPS" >&2

URLS=(
  "https://www.postgresql.org/media/keys/ACCC4CF8.asc"
  "https://apt.postgresql.org/pub/repos/apt/ACCC4CF8.asc"
)

TMP_ASC="$(mktemp)"
# shellcheck disable=SC2064
trap 'rm -f "'"${TMP_ASC}"'"' EXIT

ok=0
for url in "${URLS[@]}"; do
  echo "==> fetching PGDG key from ${url}"
  if fetch_url_to "${url}" "${TMP_ASC}" && [[ -s "${TMP_ASC}" ]]; then
    if dearmor_to_dest "${TMP_ASC}"; then
      ok=1
      break
    fi
  else
    echo "warning: failed to download ${url}" >&2
  fi
done

if (( ok != 1 )); then
  echo "error: could not install PGDG apt signing key from vendored copy or HTTPS mirrors" >&2
  exit 1
fi
