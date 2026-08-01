#!/usr/bin/env bash
# Harden apt for GitHub-hosted runners: drop broken Microsoft sources, set
# short Acquire timeouts / retries, and prefer IPv4 (IPv6 stalls are common on
# azure.archive.ubuntu.com and apt.postgresql.org).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/ci/disable-broken-runner-apt-sources.sh
bash "${ROOT_DIR}/scripts/ci/disable-broken-runner-apt-sources.sh"

sudo tee /etc/apt/apt.conf.d/99koldstore-ci >/dev/null <<'EOF'
// Fail stalled downloads quickly instead of hanging the job for hours.
Acquire::Retries "3";
Acquire::http::Timeout "30";
Acquire::https::Timeout "30";
Acquire::ftp::Timeout "30";
Acquire::ForceIPv4 "true";
Dpkg::Use-Pty "0";
EOF
