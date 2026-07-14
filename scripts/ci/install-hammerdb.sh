#!/usr/bin/env bash
# Download HammerDB CLI into target/tools/HammerDB (CI / local optional).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

VERSION="${KOLDSTORE_HAMMERDB_VERSION:-4.12}"
PREFIX="${KOLDSTORE_HAMMERDB_PREFIX:-${ROOT_DIR}/target/tools}"
ARCHIVE_NAME="HammerDB-${VERSION}-Linux.tar.gz"
URL="${KOLDSTORE_HAMMERDB_URL:-https://github.com/TPC-Council/HammerDB/releases/download/v${VERSION}/${ARCHIVE_NAME}}"
ARCHIVE="${PREFIX}/${ARCHIVE_NAME}"

mkdir -p "$PREFIX"

existing="$(find "$PREFIX" -maxdepth 2 -type f -name hammerdbcli 2>/dev/null | head -n 1 || true)"
if [[ -n "$existing" && -x "$existing" ]]; then
  echo "HammerDB already installed at ${existing}"
  echo "${existing}"
  exit 0
fi

echo "downloading HammerDB ${VERSION} from ${URL}"
curl -fsSL "$URL" -o "$ARCHIVE"
tar -xzf "$ARCHIVE" -C "$PREFIX"

BIN="$(find "$PREFIX" -maxdepth 2 -type f -name hammerdbcli | head -n 1 || true)"
if [[ -z "$BIN" || ! -x "$BIN" ]]; then
  echo "error: hammerdbcli missing after extract under ${PREFIX}" >&2
  exit 1
fi

echo "installed ${BIN}"
echo "${BIN}"
