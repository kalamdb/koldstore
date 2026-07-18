#!/usr/bin/env bash
# Shared helpers for stopping/starting cargo-pgrx Postgres clusters.
#
# Async mirror bgworkers (and stuck client backends) often make plain
# `cargo pgrx stop` return before the postmaster is gone. The next
# `cargo pgrx start` then fails with "could not start server" and no log dump.
# Source this file from runners that restart pgrx between suites.

pgrx_home() {
  echo "${PGRX_HOME:-${HOME}/.pgrx}"
}

pgrx_data_dir() {
  local ver="$1"
  echo "$(pgrx_home)/data-${ver}"
}

pgrx_log_file() {
  local ver="$1"
  echo "$(pgrx_home)/${ver}.log"
}

pgrx_bindir() {
  local ver="$1"
  local pg_config="${PGRX_PG_CONFIG:-}"
  if [[ -z "${pg_config}" ]]; then
    pg_config="$(cargo pgrx info pg-config "${ver}")"
  fi
  dirname "${pg_config}"
}

pgrx_dump_log() {
  local ver="$1"
  local log_file
  log_file="$(pgrx_log_file "${ver}")"
  echo "──── pgrx PostgreSQL ${ver} log (${log_file}) ────" >&2
  if [[ -f "${log_file}" ]]; then
    tail -n 200 "${log_file}" >&2 || true
  else
    echo "(log file missing)" >&2
  fi
}

# Best-effort: terminate koldstore bgworkers and leftover clients so stop can finish.
pgrx_quiet_backends() {
  local ver="$1"
  local port="${KOLDSTORE_E2E_PGPORT:-288${ver}}"
  local host="${KOLDSTORE_E2E_PGHOST:-127.0.0.1}"
  local psql
  psql="$(pgrx_bindir "${ver}")/psql"
  if ! "${psql}" -h "${host}" -p "${port}" -d postgres -v ON_ERROR_STOP=1 -c "SELECT 1" >/dev/null 2>&1; then
    return 0
  fi
  "${psql}" -h "${host}" -p "${port}" -d postgres -v ON_ERROR_STOP=0 -c \
    "SELECT pg_terminate_backend(pid)
     FROM pg_stat_activity
     WHERE pid <> pg_backend_pid()
       AND backend_type LIKE 'koldstore%';" >/dev/null 2>&1 || true
  "${psql}" -h "${host}" -p "${port}" -d postgres -v ON_ERROR_STOP=0 -c \
    "SELECT pg_terminate_backend(pid)
     FROM pg_stat_activity
     WHERE pid <> pg_backend_pid()
       AND datname IS NOT NULL
       AND backend_type = 'client backend';" >/dev/null 2>&1 || true
}

pgrx_port_in_use() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -ltn 2>/dev/null | grep -qE ":${port}\\b"
    return $?
  fi
  if command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi
  return 1
}

pgrx_postmaster_alive() {
  local data_dir="$1"
  local pid_file="${data_dir}/postmaster.pid"
  [[ -f "${pid_file}" ]] || return 1
  local pid
  pid="$(head -1 "${pid_file}" 2>/dev/null || true)"
  [[ -n "${pid}" ]] || return 1
  kill -0 "${pid}" 2>/dev/null
}

# Stop a pgrx cluster hard enough that the next start is not racing a dying postmaster.
pgrx_force_stop() {
  local ver="$1"
  local feature="pg${ver}"
  local port="${KOLDSTORE_E2E_PGPORT:-288${ver}}"
  local host="${KOLDSTORE_E2E_PGHOST:-127.0.0.1}"
  local data_dir
  data_dir="$(pgrx_data_dir "${ver}")"
  local pg_ctl
  pg_ctl="$(pgrx_bindir "${ver}")/pg_ctl"
  local home
  home="$(pgrx_home)"

  echo "force-stopping pgrx PostgreSQL ${ver} (port ${port})"
  pgrx_quiet_backends "${ver}"
  cargo pgrx stop "${feature}" >/dev/null 2>&1 || true

  local i
  for ((i = 1; i <= 40; i++)); do
    if ! pgrx_postmaster_alive "${data_dir}" && ! pgrx_port_in_use "${port}"; then
      rm -f "${home}/.s.PGSQL.${port}" "${home}/.s.PGSQL.${port}.lock" 2>/dev/null || true
      return 0
    fi
    if ((i == 5)) && [[ -d "${data_dir}" ]]; then
      "${pg_ctl}" -D "${data_dir}" -m fast stop -w -t 10 >/dev/null 2>&1 || true
    fi
    if ((i == 15)) && [[ -d "${data_dir}" ]]; then
      "${pg_ctl}" -D "${data_dir}" -m immediate stop -w -t 5 >/dev/null 2>&1 || true
    fi
    if ((i == 25)) && pgrx_postmaster_alive "${data_dir}"; then
      local pid
      pid="$(head -1 "${data_dir}/postmaster.pid" 2>/dev/null || true)"
      if [[ -n "${pid}" ]]; then
        echo "warning: killing leftover postmaster pid=${pid} for PostgreSQL ${ver}" >&2
        kill -TERM "${pid}" 2>/dev/null || true
        sleep 1
        kill -KILL "${pid}" 2>/dev/null || true
      fi
      rm -f "${data_dir}/postmaster.pid"
    fi
    sleep 0.25
  done

  if pgrx_postmaster_alive "${data_dir}" || pgrx_port_in_use "${port}"; then
    echo "error: could not fully stop pgrx PostgreSQL ${ver} on port ${port}" >&2
    pgrx_dump_log "${ver}"
    return 1
  fi
  rm -f "${home}/.s.PGSQL.${port}" "${home}/.s.PGSQL.${port}.lock" 2>/dev/null || true
}

# Run `cargo pgrx start …`; on failure dump ~/.pgrx/<ver>.log and exit non-zero.
pgrx_start_or_dump() {
  local ver="$1"
  shift
  if cargo pgrx start "$@"; then
    return 0
  fi
  echo "error: cargo pgrx start failed for PostgreSQL ${ver}" >&2
  pgrx_dump_log "${ver}"
  return 1
}
