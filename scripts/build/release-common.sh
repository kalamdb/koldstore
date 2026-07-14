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
: "${CARGO_PROFILE:=release-pg-dist}"

build_release_root_dir() {
  cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd
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
  printf 'target/%s/%s-pg%s' "${CARGO_PROFILE}" "${EXTENSION_SQL_NAME}" "$pg"
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
  echo "packaging pg_koldstore with cargo profile ${CARGO_PROFILE}"
  cargo pgrx package \
    -p "${EXTENSION_CRATE}" \
    --profile "${CARGO_PROFILE}" \
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
CONTROL_SRC="$(find "$SCRIPT_DIR" -type f -name 'koldstore.control' | head -n 1)"
test -n "$LIB_SRC"
test -n "$CONTROL_SRC"
install -m 0755 "$LIB_SRC" "$LIBDIR/$(basename "$LIB_SRC")"
install -m 0644 "$CONTROL_SRC" "$SHAREDIR/"
while IFS= read -r sql; do
  install -m 0644 "$sql" "$SHAREDIR/"
done < <(find "$SCRIPT_DIR" -type f -name 'koldstore--*.sql' | sort)
echo "Installed koldstore. Run: CREATE EXTENSION koldstore;"
INSTALL
  chmod +x "${dest}/install.sh"
}

# Emit RPM %files entries using the layout produced by pgrx (Debian or PGDG/RHEL).
discover_rpm_file_entries() {
  local stage_dir="$1"
  local lib control sql_dir lib_path control_path sql_glob
  lib="$(find "${stage_dir}" -type f -name 'koldstore.so' | head -n 1)"
  control="$(find "${stage_dir}" -type f -name 'koldstore.control' | head -n 1)"
  if [[ -z "${lib}" || -z "${control}" ]]; then
    echo "error: extension artifacts missing under ${stage_dir}" >&2
    find "${stage_dir}" -type f | sort >&2 || true
    return 1
  fi
  sql_dir="$(dirname "${control}")"
  lib_path="/${lib#"${stage_dir}/"}"
  control_path="/${control#"${stage_dir}/"}"
  sql_glob="/${sql_dir#"${stage_dir}/"}"/koldstore--\*.sql
  printf '%s\n' "${lib_path}" "${control_path}" "${sql_glob}"
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
Maintainer: pg-koldstore <https://github.com/kalamdb/koldstore>
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

# RPM Version allows only [0-9A-Za-z.]. Semver pre-releases (e.g. 0.1.2-beta.1)
# must be split: Version=0.1.2, Release=beta.1%{?dist}.
parse_rpm_version_fields() {
  local full="$1"
  local version_no_build="${full%%+*}"

  if [[ "${version_no_build}" =~ ^([0-9]+\.[0-9]+\.[0-9]+)-(.+)$ ]]; then
    RPM_EPOCH_VERSION="${BASH_REMATCH[1]}"
    RPM_EPOCH_RELEASE="${BASH_REMATCH[2]}"
  elif [[ "${version_no_build}" =~ ^([0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
    RPM_EPOCH_VERSION="${BASH_REMATCH[1]}"
    RPM_EPOCH_RELEASE="1"
  else
    echo "error: unsupported version for rpm packaging: ${full}" >&2
    return 1
  fi

  if [[ "${full}" == *"+"* ]]; then
    local build_meta="${full#*+}"
    build_meta="${build_meta//-/.}"
    RPM_EPOCH_RELEASE="${RPM_EPOCH_RELEASE}.${build_meta}"
  fi
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
  local rpm_version rpm_release
  parse_rpm_version_fields "${version}" || return 1
  rpm_version="${RPM_EPOCH_VERSION}"
  rpm_release="${RPM_EPOCH_RELEASE}"
  local dist_dir="dist/${version}"
  local out
  out="$(artifact_basename "${version}" "${pg}" "${distro}" "${arch}" "rpm")"
  local topdir
  topdir="$(mktemp -d)"
  mkdir -p "${topdir}"/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
  local spec="${topdir}/SPECS/postgresql${pg}-koldstore.spec"
  local rpm_files
  rpm_files="$(discover_rpm_file_entries "${stage_dir}")"
  cat >"${spec}" <<EOF
Name:           postgresql${pg}-koldstore
Version:        ${rpm_version}
Release:        ${rpm_release}%{?dist}
Summary:        pg-koldstore hot/cold storage extension for PostgreSQL ${pg}
License:        Apache-2.0
URL:            https://github.com/kalamdb/koldstore
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
$(printf '%s\n' "${rpm_files}")

%changelog
* $(date -u '+%a %b %d %Y') pg-koldstore <https://github.com/kalamdb/koldstore> - ${rpm_version}-${rpm_release}
- Release ${version} for PostgreSQL ${pg}
EOF
  rpmbuild -bb \
    --define "_topdir ${topdir}" \
    --define "_buildrootdir ${topdir}/BUILDROOT" \
    "${spec}"
  mkdir -p "${dist_dir}"
  rm -f "${dist_dir}/${out}"
  cp "${topdir}/RPMS/${rpm_arch}/"postgresql"${pg}"-koldstore-"${rpm_version}"-"${rpm_release}"*.rpm "${dist_dir}/${out}"
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
