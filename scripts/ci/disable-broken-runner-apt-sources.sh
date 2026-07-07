#!/usr/bin/env bash
# GitHub-hosted Ubuntu images ship Microsoft apt sources that can break `apt-get update`.
set -euo pipefail

for repo in /etc/apt/sources.list.d/microsoft*.list /etc/apt/sources.list.d/azure*.list; do
  if [[ -e "$repo" ]]; then
    sudo mv "$repo" "${repo}.disabled"
  fi
done
