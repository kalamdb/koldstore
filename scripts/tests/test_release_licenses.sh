#!/usr/bin/env bash
# Verifies release staging carries the legal notices required by every artifact.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"
# shellcheck source=scripts/build/release-common.sh
source "${ROOT_DIR}/scripts/build/release-common.sh"

test_root="$(mktemp -d)"
stage_dir="${test_root}/stage"
trap 'rm -rf "${test_root}"' EXIT

CARGO_PROFILE="test-release-license"
package_root="$(pgrx_package_root 16)"
mkdir -p "${package_root}/usr/lib/postgresql/16/lib" \
  "${package_root}/usr/share/postgresql/16/extension"
touch "${package_root}/usr/lib/postgresql/16/lib/koldstore.so" \
  "${package_root}/usr/share/postgresql/16/extension/koldstore.control" \
  "${package_root}/usr/share/postgresql/16/extension/koldstore--0.1.0.sql"
trap 'rm -rf "${test_root}" "${package_root}"' EXIT

stage_release_tree 16 "${stage_dir}"

test -f "${stage_dir}/LICENSE"
test -f "${stage_dir}/NOTICE"
test -f "${stage_dir}/THIRD_PARTY_NOTICES.html"
test -x "${stage_dir}/install.sh"

create_tarball_package 0.0.0-license-test 16 test amd64 "${stage_dir}"
archive="dist/0.0.0-license-test/pg_koldstore-v0.0.0-license-test-pg16-test-amd64.tar.gz"
trap 'rm -rf "${test_root}" "${package_root}" "dist/0.0.0-license-test"' EXIT
tar -tzf "${archive}" | rg '/LICENSE$'
tar -tzf "${archive}" | rg '/NOTICE$'
tar -tzf "${archive}" | rg '/THIRD_PARTY_NOTICES.html$'
