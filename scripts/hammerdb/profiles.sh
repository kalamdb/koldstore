#!/usr/bin/env bash
# Resolve HammerDB scale profiles for deep (and reusable) runs.
#
# Usage:
#   source scripts/hammerdb/profiles.sh
#   resolve_hammerdb_profile standard
#   # exports: PROFILE WAREHOUSES VIRTUAL_USERS DURATION RAMPUP READ_ITERS BUILD_VU
#
# Env overrides always win when set before resolve (or after for custom):
#   KOLDSTORE_HAMMERDB_WAREHOUSES / VU / MINUTES / RAMPUP / READ_ITERS
set -euo pipefail

resolve_hammerdb_profile() {
  local profile="${1:-standard}"
  PROFILE="$profile"

  case "$profile" in
    smoke)
      WAREHOUSES="${KOLDSTORE_HAMMERDB_WAREHOUSES:-2}"
      VIRTUAL_USERS="${KOLDSTORE_HAMMERDB_VU:-2}"
      DURATION="${KOLDSTORE_HAMMERDB_MINUTES:-${KOLDSTORE_HAMMERDB_SECONDS:-2}}"
      RAMPUP="${KOLDSTORE_HAMMERDB_RAMPUP:-0}"
      READ_ITERS="${KOLDSTORE_HAMMERDB_READ_ITERS:-50}"
      ;;
    standard)
      WAREHOUSES="${KOLDSTORE_HAMMERDB_WAREHOUSES:-10}"
      VIRTUAL_USERS="${KOLDSTORE_HAMMERDB_VU:-8}"
      DURATION="${KOLDSTORE_HAMMERDB_MINUTES:-${KOLDSTORE_HAMMERDB_SECONDS:-10}}"
      RAMPUP="${KOLDSTORE_HAMMERDB_RAMPUP:-1}"
      READ_ITERS="${KOLDSTORE_HAMMERDB_READ_ITERS:-200}"
      ;;
    heavy)
      WAREHOUSES="${KOLDSTORE_HAMMERDB_WAREHOUSES:-50}"
      VIRTUAL_USERS="${KOLDSTORE_HAMMERDB_VU:-32}"
      DURATION="${KOLDSTORE_HAMMERDB_MINUTES:-${KOLDSTORE_HAMMERDB_SECONDS:-30}}"
      RAMPUP="${KOLDSTORE_HAMMERDB_RAMPUP:-2}"
      READ_ITERS="${KOLDSTORE_HAMMERDB_READ_ITERS:-200}"
      ;;
    custom)
      WAREHOUSES="${KOLDSTORE_HAMMERDB_WAREHOUSES:?custom profile requires KOLDSTORE_HAMMERDB_WAREHOUSES}"
      VIRTUAL_USERS="${KOLDSTORE_HAMMERDB_VU:?custom profile requires KOLDSTORE_HAMMERDB_VU}"
      DURATION="${KOLDSTORE_HAMMERDB_MINUTES:-${KOLDSTORE_HAMMERDB_SECONDS:?custom profile requires KOLDSTORE_HAMMERDB_MINUTES}}"
      RAMPUP="${KOLDSTORE_HAMMERDB_RAMPUP:-0}"
      READ_ITERS="${KOLDSTORE_HAMMERDB_READ_ITERS:-200}"
      ;;
    *)
      echo "error: unknown profile '${profile}' (smoke|standard|heavy|custom)" >&2
      return 1
      ;;
  esac

  # Schema build VU must be <= warehouse count.
  BUILD_VU="$VIRTUAL_USERS"
  if (( BUILD_VU > WAREHOUSES )); then
    BUILD_VU="$WAREHOUSES"
  fi

  export PROFILE WAREHOUSES VIRTUAL_USERS DURATION RAMPUP READ_ITERS BUILD_VU
}

print_hammerdb_profile() {
  echo "profile=${PROFILE} warehouses=${WAREHOUSES} vu=${VIRTUAL_USERS} duration=${DURATION}m rampup=${RAMPUP}m read_iters=${READ_ITERS} build_vu=${BUILD_VU}"
}
