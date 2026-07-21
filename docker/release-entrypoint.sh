#!/usr/bin/env bash
# Default POSTGRES_DB for the try-it image when callers omit it.
#
# pg_cron is packaged but not preloaded: built-in auto-flush is the default
# scheduler. Operators who want pg_cron can enable it themselves.
set -euo pipefail

export POSTGRES_DB="${POSTGRES_DB:-koldstore}"
export POSTGRES_USER="${POSTGRES_USER:-postgres}"

exec docker-entrypoint.sh "$@"
