#!/usr/bin/env bash
# Shared helpers for release packaging. Source from platform entry scripts.

if [[ -n "${BUILD_RELEASE_COMMON_SOURCED:-}" ]]; then
  return 0 2>/dev/null || exit 0
fi
BUILD_RELEASE_COMMON_SOURCED=1

set -euo pipefail

: "${PGRX_VERSION:=0.19.1}"
: "${EXTENSION_CRATE:=pg_koldstore}"
: "${EXTENSION_SQL_NAME:=koldstore}"

build_release_root_dir() {
  cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd
}

read_workspace_version() {
  python3 - <<'PY'
from pathlib import Path
import re

text = Path("Cargo.toml").read_text()
match = re.search(
    r'(?m)^\[workspace\.package\]\s*(?:\n[^\[]*)?^version\s*=\s*"([^"]+)"',
    text,
)
if not match:
    raise SystemExit("workspace.package.version not found in Cargo.toml")
print(match.group(1))
PY
}

default_distro_for_pg() {
  case "$1" in
    15) echo "ubuntu22.04" ;;
    16) echo "ubuntu24.04" ;;
    17) echo "debian12" ;;
    18) echo "rocky9" ;;
    *)
      echo "unsupported PostgreSQL major: $1" >&2
      return 1
      ;;
  esac
}

default_formats_for_pg() {
  case "$1" in
    15|16|17) echo "deb,tar.gz" ;;
    18) echo "rpm,tar.gz" ;;
    *)
      echo "unsupported PostgreSQL major: $1" >&2
      return 1
      ;;
  esac
}

artifact_basename() {
  local version="$1"
  local pg="$2"
  local distro="$3"
  local arch="$4"
  local ext="$5"
  printf 'pg_koldstore-v%s-pg%s-%s-%s.%s' "$version" "$pg" "$distro" "$arch" "$ext"
}

pgrx_package_root() {
  local pg="$1"
  printf 'target/release/%s-pg%s' "$EXTENSION_SQL_NAME" "$pg"
}

require_command() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || {
    echo "error: required command not found: $cmd" >&2
    return 1
  }
}

ensure_cargo_pgrx() {
  if command -v cargo-pgrx >/dev/null 2>&1 \
    && cargo pgrx --version 2>/dev/null | grep -q "cargo-pgrx ${PGRX_VERSION}$"; then
    return 0
  fi
  cargo install cargo-pgrx --version "${PGRX_VERSION}" --locked
}

run_cargo_pgrx_package() {
  local pg="$1"
  local pg_config="$2"
  ensure_cargo_pgrx
  cargo pgrx init --pg"${pg}" "${pg_config}"
  cargo pgrx package \
    -p "${EXTENSION_CRATE}" \
    --no-default-features \
    --features "pg${pg}" \
    --pg-config "${pg_config}"
  local root
  root="$(pgrx_package_root "${pg}")"
  if [[ ! -d "${root}" ]]; then
    echo "error: expected pgrx package directory ${root}" >&2
    return 1
  fi
  if ! find "${root}" -type f \( -name 'koldstore.so' -o -name 'koldstore.dylib' -o -name 'koldstore.dll' \) | grep -q .; then
    echo "error: extension library not found under ${root}" >&2
    return 1
  fi
}

write_install_sh() {
  local dest="$1"
  cat >"${dest}/install.sh" <<'INSTALL'
#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PG_CONFIG="${1:-pg_config}"
LIBDIR="$("$PG_CONFIG" --pkglibdir)"
SHAREDIR="$("$PG_CONFIG" --sharedir)/extension"
LIB_SRC="$(find "$SCRIPT_DIR" -type f \( -name 'koldstore.so' -o -name 'koldstore.dylib' \) | head -n 1)"
test -n "$LIB_SRC"
install -m 0755 "$LIB_SRC" "$LIBDIR/$(basename "$LIB_SRC")"
install -m 0644 "$SCRIPT_DIR"/usr/share/postgresql/*/extension/koldstore.control "$SHAREDIR/"
install -m 0644 "$SCRIPT_DIR"/usr/share/postgresql/*/extension/koldstore--*.sql "$SHAREDIR/"
echo "Installed koldstore. Run: CREATE EXTENSION koldstore;"
INSTALL
  chmod +x "${dest}/install.sh"
}

stage_release_tree() {
  local pg="$1"
  local stage_dir="$2"
  local root
  root="$(pgrx_package_root "${pg}")"
  rm -rf "${stage_dir}"
  mkdir -p "${stage_dir}"
  cp -a "${root}/." "${stage_dir}/"
  write_install_sh "${stage_dir}"
}

create_tarball_package() {
  local version="$1"
  local pg="$2"
  local distro="$3"
  local arch="$4"
  local stage_dir="$5"
  local dist_dir="dist/${version}"
  local base
  base="$(artifact_basename "${version}" "${pg}" "${distro}" "${arch}" "tar.gz")"
  local inner="${base%.tar.gz}"
  mkdir -p "${dist_dir}"
  rm -f "${dist_dir}/${base}"
  tar -C "${stage_dir%/*}" -czf "${dist_dir}/${base}" "$(basename "${stage_dir}")"
  echo "created ${dist_dir}/${base}"
}

create_deb_package() {
  local version="$1"
  local pg="$2"
  local distro="$3"
  local arch="$4"
  local stage_dir="$5"
  require_command dpkg-deb
  local dist_dir="dist/${version}"
  local out
  out="$(artifact_basename "${version}" "${pg}" "${distro}" "${arch}" "deb")"
  local deb_root
  deb_root="$(mktemp -d)"
  cp -a "${stage_dir}/usr" "${deb_root}/"
  mkdir -p "${deb_root}/DEBIAN"
  cat >"${deb_root}/DEBIAN/control" <<EOF
Package: postgresql-${pg}-koldstore
Version: ${version}
Architecture: ${arch}
Maintainer: pg-koldstore <https://github.com/pg-koldstore/pg-kalam>
Depends: postgresql-${pg}
Section: database
Priority: optional
Description: pg-koldstore hot/cold storage extension for PostgreSQL ${pg}
 PostgreSQL extension providing hot/cold table storage for PostgreSQL ${pg}.
EOF
  mkdir -p "${dist_dir}"
  rm -f "${dist_dir}/${out}"
  dpkg-deb --build --root-owner-group "${deb_root}" "${dist_dir}/${out}"
  rm -rf "${deb_root}"
  echo "created ${dist_dir}/${out}"
}

rpm_arch_name() {
  case "$1" in
    amd64) echo "x86_64" ;;
    arm64) echo "aarch64" ;;
    *)
      echo "unsupported rpm arch: $1" >&2
      return 1
      ;;
  esac
}

create_rpm_package() {
  local version="$1"
  local pg="$2"
  local distro="$3"
  local arch="$4"
  local stage_dir="$5"
  require_command rpmbuild
  local rpm_arch
  rpm_arch="$(rpm_arch_name "${arch}")"
  local dist_dir="dist/${version}"
  local out
  out="$(artifact_basename "${version}" "${pg}" "${distro}" "${arch}" "rpm")"
  local topdir
  topdir="$(mktemp -d)"
  mkdir -p "${topdir}"/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
  local spec="${topdir}/SPECS/postgresql${pg}-koldstore.spec"
  cat >"${spec}" <<EOF
Name:           postgresql${pg}-koldstore
Version:        ${version}
Release:        1%{?dist}
Summary:        pg-koldstore hot/cold storage extension for PostgreSQL ${pg}
License:        Apache-2.0
URL:            https://github.com/pg-koldstore/pg-kalam
Requires:       postgresql${pg}-server
BuildArch:      ${rpm_arch}

%description
PostgreSQL extension providing hot/cold table storage for PostgreSQL ${pg}.

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}
cp -a ${stage_dir}/usr %{buildroot}/

%files
%defattr(-,root,root,-)
/usr/lib/postgresql/${pg}/lib/koldstore.so
/usr/share/postgresql/${pg}/extension/koldstore.control
/usr/share/postgresql/${pg}/extension/koldstore--*.sql

%changelog
* $(date -u '+%a %b %d %Y') pg-koldstore <https://github.com/pg-koldstore/pg-kalam> - ${version}-1
- Release ${version} for PostgreSQL ${pg}
EOF
  rpmbuild -bb \
    --define "_topdir ${topdir}" \
    --define "_buildrootdir ${topdir}/BUILDROOT" \
    "${spec}"
  mkdir -p "${dist_dir}"
  rm -f "${dist_dir}/${out}"
  cp "${topdir}/RPMS/${rpm_arch}/"postgresql"${pg}"-koldstore-"${version}"-*.rpm "${dist_dir}/${out}"
  rm -rf "${topdir}"
  echo "created ${dist_dir}/${out}"
}

create_zip_package() {
  local version="$1"
  local pg="$2"
  local distro="$3"
  local arch="$4"
  local stage_dir="$5"
  require_command zip
  local dist_dir="dist/${version}"
  local out
  out="$(artifact_basename "${version}" "${pg}" "${distro}" "${arch}" "zip")"
  mkdir -p "${dist_dir}"
  rm -f "${dist_dir}/${out}"
  (
    cd "${stage_dir%/*}"
    zip -qr "${dist_dir}/${out}" "$(basename "${stage_dir}")"
  )
  echo "created ${dist_dir}/${out}"
}

build_release_artifacts() {
  local version="$1"
  local pg="$2"
  local distro="$3"
  local arch="$4"
  local formats_csv="$5"
  local stage_parent
  stage_parent="$(mktemp -d)"
  local stage_dir="${stage_parent}/$(artifact_basename "${version}" "${pg}" "${distro}" "${arch}" "tree")"
  stage_release_tree "${pg}" "${stage_dir}"

  IFS=',' read -r -a formats <<<"${formats_csv}"
  for format in "${formats[@]}"; do
    format="$(echo "${format}" | xargs)"
    case "${format}" in
      tar.gz) create_tarball_package "${version}" "${pg}" "${distro}" "${arch}" "${stage_dir}" ;;
      deb) create_deb_package "${version}" "${pg}" "${distro}" "${arch}" "${stage_dir}" ;;
      rpm) create_rpm_package "${version}" "${pg}" "${distro}" "${arch}" "${stage_dir}" ;;
      zip) create_zip_package "${version}" "${pg}" "${distro}" "${arch}" "${stage_dir}" ;;
      *)
        echo "error: unsupported package format: ${format}" >&2
        return 1
        ;;
    esac
  done
  rm -rf "${stage_parent}"
}
