#!/usr/bin/env bash
# Build sqlsmith from source into target/tools/sqlsmith (CI / local optional).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

REF="${KOLDSTORE_SQLSMITH_REF:-}"
PREFIX="${KOLDSTORE_SQLSMITH_PREFIX:-${ROOT_DIR}/target/tools/sqlsmith}"
SRC="${KOLDSTORE_SQLSMITH_SRC:-${ROOT_DIR}/target/sqlsmith-src}"
BIN="${PREFIX}/bin/sqlsmith"

if [[ -x "$BIN" ]]; then
  echo "sqlsmith already installed at ${BIN}"
  echo "${BIN}"
  exit 0
fi

echo "installing sqlsmith into ${PREFIX}"
# autoconf-archive supplies AX_CXX_COMPILE_STDCXX_* / AX_BOOST_* / AX_LIB_POSTGRESQL
# used by sqlsmith's configure.ac; without it, autoreconf leaves raw macros in ./configure.
sudo apt-get install -y -qq build-essential autoconf automake autoconf-archive libtool \
  libpq-dev libpqxx-dev libboost-regex-dev pkg-config >/dev/null

rm -rf "$SRC"
if [[ -n "$REF" ]]; then
  git clone --depth 1 --branch "$REF" https://github.com/anse1/sqlsmith.git "$SRC"
else
  git clone --depth 1 https://github.com/anse1/sqlsmith.git "$SRC"
fi
(
  cd "$SRC"
  autoreconf -fi
  ./configure --prefix="$PREFIX"
  make -j"$(nproc)"
  make install
)

if [[ ! -x "$BIN" ]]; then
  echo "error: sqlsmith binary missing after install (${BIN})" >&2
  exit 1
fi

echo "installed ${BIN}"
echo "${BIN}"
