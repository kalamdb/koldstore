#!/usr/bin/env bash
# GitHub-hosted Ubuntu images ship Microsoft apt sources that can break `apt-get update`.
set -euo pipefail

shopt -s nullglob

disable_source() {
  local source="$1"
  if [[ -f "$source" && ! "$source" =~ \.disabled$ ]]; then
    sudo mv "$source" "${source}.disabled"
  fi
}

for source in /etc/apt/sources.list.d/*; do
  if grep -Eq 'packages\.microsoft\.com|repos\.azure\.com' "$source" 2>/dev/null; then
    disable_source "$source"
  fi
done
