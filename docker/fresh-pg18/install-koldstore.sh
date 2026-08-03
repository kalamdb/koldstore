#!/usr/bin/env bash
# Install koldstore from a GitHub Release onto PostgreSQL 18 (Ubuntu/PGDG).
#
# Works in two modes:
#   1. Fresh container / host — download latest release, install files, configure
#   2. Already-running Postgres — install files + conf, then restart + CREATE EXTENSION
#
# Usage:
#   install-koldstore.sh                 # download + install files + write conf
#   install-koldstore.sh --force         # re-download even if already installed
#   install-koldstore.sh --restart       # restart the cluster after install
#   install-koldstore.sh --create-extension
#   install-koldstore.sh --force --restart --create-extension
#
# Env:
#   KOLDSTORE_RELEASE   release tag or "latest" (default: latest)
#   KOLDSTORE_REPO      owner/name (default: kalamdb/koldstore)
#   PG_MAJOR            PostgreSQL major (default: 18)
#   PG_CONFIG           path to pg_config (default: discovered)
set -euo pipefail

FORCE=0
DO_RESTART=0
DO_CREATE_EXT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force) FORCE=1; shift ;;
    --restart) DO_RESTART=1; shift ;;
    --create-extension) DO_CREATE_EXT=1; shift ;;
    -h|--help)
      sed -n '2,20p' "$0"
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

KOLDSTORE_RELEASE="${KOLDSTORE_RELEASE:-latest}"
KOLDSTORE_REPO="${KOLDSTORE_REPO:-kalamdb/koldstore}"
PG_MAJOR="${PG_MAJOR:-18}"

if [[ -z "${PG_CONFIG:-}" ]]; then
  if command -v pg_config >/dev/null 2>&1; then
    PG_CONFIG="$(command -v pg_config)"
  elif [[ -x "/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config" ]]; then
    PG_CONFIG="/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config"
  else
    echo "error: pg_config not found; install postgresql-${PG_MAJOR} first" >&2
    exit 1
  fi
fi

export PATH="$(dirname "${PG_CONFIG}"):${PATH}"

LIBDIR="$("${PG_CONFIG}" --pkglibdir)"
EXTDIR="$("${PG_CONFIG}" --sharedir)/extension"
ARCH="$(dpkg --print-architecture 2>/dev/null || uname -m)"
case "${ARCH}" in
  aarch64) ARCH=arm64 ;;
  x86_64) ARCH=amd64 ;;
esac

already_installed=0
if [[ -f "${LIBDIR}/koldstore.so" && -f "${EXTDIR}/koldstore.control" ]]; then
  already_installed=1
fi

if [[ "${already_installed}" -eq 1 && "${FORCE}" -eq 0 ]]; then
  echo "koldstore files already present under ${LIBDIR} (use --force to reinstall)"
else
  work="$(mktemp -d)"
  trap 'rm -rf "${work}"' EXIT

  api="https://api.github.com/repos/${KOLDSTORE_REPO}/releases"
  if [[ "${KOLDSTORE_RELEASE}" == "latest" ]]; then
    release_url="${api}/latest"
  elif [[ "${KOLDSTORE_RELEASE}" == v* ]]; then
    release_url="${api}/tags/${KOLDSTORE_RELEASE}"
  else
    release_url="${api}/tags/v${KOLDSTORE_RELEASE}"
  fi

  echo "Fetching release metadata from ${release_url}"
  meta_file="${work}/release.json"
  curl -fsSL "${release_url}" -o "${meta_file}"
  tag="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["tag_name"])' "${meta_file}")"
  echo "Using release ${tag}"

  # Prefer Ubuntu PG18 packages; fall back to Rocky tarball (portable .so + install.sh).
  mapfile -t picked < <(python3 - "${meta_file}" "${ARCH}" <<'PY'
import fnmatch
import json
import sys

meta_path, arch = sys.argv[1], sys.argv[2]
assets = json.load(open(meta_path))["assets"]
names = [a["name"] for a in assets]
urls = {a["name"]: a["browser_download_url"] for a in assets}
candidates = [
    f"pg_koldstore-*-pg18-ubuntu24.04-{arch}.deb",
    f"pg_koldstore-*-pg18-ubuntu24.04-{arch}.tar.gz",
    f"pg_koldstore-*-pg18-rocky9-{arch}.tar.gz",
]
for pattern in candidates:
    for name in sorted(names):
        if fnmatch.fnmatch(name, pattern):
            print(name)
            print(urls[name])
            raise SystemExit(0)
print("error: no pg18 asset found for arch", arch, file=sys.stderr)
print("available:", *names, sep="\n  ", file=sys.stderr)
raise SystemExit(1)
PY
)

  asset_name="${picked[0]}"
  asset_url="${picked[1]}"
  echo "Downloading ${asset_name}"
  curl -fsSL -o "${work}/${asset_name}" "${asset_url}"

  if [[ "${asset_name}" == *.deb ]]; then
    dpkg -i "${work}/${asset_name}"
  else
    tar -xzf "${work}/${asset_name}" -C "${work}"
    install_sh="$(find "${work}" -type f -name install.sh | head -n 1)"
    if [[ -z "${install_sh}" ]]; then
      echo "error: install.sh missing from ${asset_name}" >&2
      exit 1
    fi
    bash "${install_sh}" "${PG_CONFIG}"
  fi
fi

# Package-managed Ubuntu/Debian cluster conf (ignored by Docker PGDATA -c flags).
conf_d="/etc/postgresql/${PG_MAJOR}/main/conf.d"
if [[ -d "/etc/postgresql/${PG_MAJOR}/main" ]]; then
  mkdir -p "${conf_d}"
  cat >"${conf_d}/koldstore.conf" <<'EOF'
# Required: planner hooks must load in every backend (reload is not enough).
shared_preload_libraries = 'koldstore'
# Required: async mirror / manage_table capture.
wal_level = logical
EOF
  echo "Wrote ${conf_d}/koldstore.conf"
fi

# Seed defaults for first initdb inside Docker images.
sample="$("${PG_CONFIG}" --sharedir)/postgresql.conf.sample"
if [[ -w "${sample}" ]]; then
  if ! grep -q "shared_preload_libraries = 'koldstore'" "${sample}" 2>/dev/null; then
    {
      echo ""
      echo "# koldstore (docker/fresh-pg18)"
      echo "listen_addresses = '*'"
      echo "shared_preload_libraries = 'koldstore'"
      echo "wal_level = logical"
    } >>"${sample}"
    echo "Updated ${sample}"
  fi
fi

if [[ "${DO_RESTART}" -eq 1 ]]; then
  if command -v pg_ctlcluster >/dev/null 2>&1; then
    pg_ctlcluster "${PG_MAJOR}" main restart || pg_ctlcluster "${PG_MAJOR}" main start
  elif [[ -n "${PGDATA:-}" && -d "${PGDATA}" ]]; then
    if pg_ctl -D "${PGDATA}" status >/dev/null 2>&1; then
      pg_ctl -D "${PGDATA}" -m fast restart
    fi
  else
    echo "warning: --restart requested but no cluster manager found; restart Postgres manually" >&2
  fi
fi

if [[ "${DO_CREATE_EXT}" -eq 1 ]]; then
  db="${POSTGRES_DB:-postgres}"
  user="${POSTGRES_USER:-postgres}"
  created=0
  for _ in $(seq 1 30); do
    if command -v gosu >/dev/null 2>&1; then
      if gosu postgres psql -v ON_ERROR_STOP=1 -d "${db}" -c "CREATE EXTENSION IF NOT EXISTS koldstore;" >/dev/null 2>&1; then
        created=1
        break
      fi
    fi
    if psql -U "${user}" -d "${db}" -v ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS koldstore;" >/dev/null 2>&1; then
      created=1
      break
    fi
    sleep 1
  done
  if [[ "${created}" -eq 1 ]]; then
    echo "CREATE EXTENSION koldstore OK"
  else
    echo "warning: could not CREATE EXTENSION yet; run it manually after Postgres is ready" >&2
  fi
fi

echo "koldstore install complete."
echo "  Confirm: SHOW shared_preload_libraries;  -- must include koldstore"
echo "  Confirm: SELECT koldstore.preload_status();"
