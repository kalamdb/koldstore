#!/usr/bin/env bash
# Wrapper: scripts/hammerdb/run.sh
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec bash "${ROOT_DIR}/scripts/hammerdb/run.sh" "$@"
