#!/usr/bin/env bash
set -euo pipefail

valid_image() {
  [[ $# == 1 && "$1" =~ ^localhost/tidemq-nextest:[A-Za-z0-9][A-Za-z0-9_.-]*$ ]]
}

if [[ ${1:-} == --selfcheck ]]; then
  valid_image localhost/tidemq-nextest:ubuntu2404-rust1.100.0
  for invalid in ubuntu:latest localhost/tidemq-nextest: localhost/other:test 'localhost/tidemq-nextest:bad name'; do
    if valid_image "$invalid"; then
      printf 'Invalid nextest image name was accepted\n' >&2
      exit 1
    fi
  done
  printf 'Nextest image name boundary checks passed\n'
  exit
fi
image=${1:-}
valid_image "$image" || {
  printf 'The nextest image must use the local immutable executor name\n' >&2; exit 2;
}

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
mkdir -p -- "$repo/target/ci"
exec 9>"$repo/target/ci/nextest-image.lock"
flock 9
docker=(docker --host unix:///var/run/docker.sock)
if [[ $("${docker[@]}" image inspect "$image" \
  --format '{{index .Config.Labels "tidemq.lab/nextest"}}' 2>/dev/null) == true ]]; then
  exit
fi

inventory="$repo/deploy/lab/inventory.json"
base=$(jq -er .docker.runtimeBaseSource "$inventory")
runtime_image=$(jq -er .docker.runtimeBaseImage "$inventory")
expected=$(jq -er .docker.runtimeBaseImageID "$inventory")
[[ "$base" =~ ^[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}$ &&
   "$runtime_image" =~ ^[a-zA-Z0-9./:_-]+$ &&
   "$expected" =~ ^sha256:[a-f0-9]{64}$ ]] || {
  printf 'The pinned runtime base inventory is invalid\n' >&2; exit 2;
}
actual=$("${docker[@]}" image inspect "$runtime_image" --format '{{.Id}}' 2>/dev/null || true)
if [[ -z "$actual" ]]; then
  "${docker[@]}" pull --platform linux/amd64 "$base"
  "${docker[@]}" tag "$base" "$runtime_image"
  actual=$("${docker[@]}" image inspect "$runtime_image" --format '{{.Id}}')
fi
[[ "$actual" == "$expected" ]] || {
  printf 'The installed runtime base image differs from inventory\n' >&2; exit 1;
}

context=$(mktemp -d "$repo/target/ci/nextest-image.XXXXXX")
trap 'rm -rf -- "$context"' EXIT
cp "$repo/deploy/lab/nextest/Dockerfile" "$context/Dockerfile"
cp /etc/ssl/certs/ca-certificates.crt "$context/ca-certificates.crt"
DOCKER_BUILDKIT=1 "${docker[@]}" build --pull=false --build-arg "UBUNTU_IMAGE=$base" \
  --label tidemq.lab/nextest=true --tag "$image" "$context"
"${docker[@]}" image inspect "$image" --format '{{.Id}}'
