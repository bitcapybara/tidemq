#!/usr/bin/env bash
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
inventory="$repo/deploy/lab/inventory.json"
image=$(jq -er .docker.runtimeBaseImage "$inventory")
expected=$(jq -er .docker.runtimeBaseImageID "$inventory")
actual=$(docker image inspect "$image" --format '{{.Id}}' 2>/dev/null || true)
if [[ -z "$actual" ]]; then
  source_image=$(jq -er .docker.runtimeBaseSource "$inventory")
  docker pull --platform linux/amd64 "$source_image"
  docker tag "$source_image" "$image"
  actual=$(docker image inspect "$image" --format '{{.Id}}')
fi
[[ "$actual" == "$expected" ]] || { printf 'Runtime image identity differs from inventory\n' >&2; exit 1; }
