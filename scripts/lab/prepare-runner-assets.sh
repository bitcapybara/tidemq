#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
pinned_image() { [[ "$1" =~ ^[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}$ ]]; }
if [[ ${1:-} == --selfcheck ]]; then
  pinned_image "docker.io/library/ubuntu@sha256:$(printf 'a%.0s' {1..64})"
  for image in ubuntu:latest 'ubuntu@sha256:abc' 'ubuntu;id'; do
    if pinned_image "$image"; then printf 'Unpinned image was accepted\n' >&2; exit 1; fi
  done
  printf 'Runner asset source boundary checks passed\n'
  exit
fi
[[ $EUID == 0 && $# == 0 ]] || { printf 'Usage: sudo prepare-runner-assets.sh\n' >&2; exit 2; }
runtime_source=$(jq -er .docker.runtimeBaseSource "$lab_inventory")
runtime_image=$(jq -er .docker.runtimeBaseImage "$lab_inventory")
runtime_id=$(jq -er .docker.runtimeBaseImageID "$lab_inventory")
builder_source=$(jq -er .docker.builderSource "$lab_inventory")
pinned_image "$runtime_source" && pinned_image "$builder_source" || { printf 'Image sources must use full digests\n' >&2; exit 2; }
bundle="$lab_state/private/runner-bundle"
install -d -m 0700 "$bundle"
install -d -m 0755 "$lab_state/images" "$lab_state/logs"
docker pull "$runtime_source" > "$lab_state/logs/runtime-base-pull.log" 2>&1
docker tag "$runtime_source" "$runtime_image"
[[ $(docker image inspect "$runtime_image" --format '{{.Id}}') == "$runtime_id" ]] || { printf 'Runtime image identity differs from inventory\n' >&2; exit 1; }
docker save -o "$lab_state/images/runtime-base.tar.next" "$runtime_image"
mv -f "$lab_state/images/runtime-base.tar.next" "$lab_state/images/runtime-base.tar"
sha256sum "$lab_state/images/runtime-base.tar" > "$lab_state/images/runtime-base.sha256"
builder=localhost/tidemq-runner-tools:rust-1.99.1
docker build -f "$lab_repo/deploy/docker/Dockerfile.builder" --build-arg "RUST_IMAGE=$builder_source" -t "$builder" "$lab_repo/deploy/docker" > "$lab_state/logs/runner-tools-build.log" 2>&1
docker run --rm --network none --read-only "$builder" tar -czf - -C /usr/local --exclude=cargo/registry --exclude=cargo/git cargo rustup > "$bundle/rust-toolchain.tar.gz.next"
gzip -t "$bundle/rust-toolchain.tar.gz.next"
mv -f "$bundle/rust-toolchain.tar.gz.next" "$bundle/rust-toolchain.tar.gz"
sha256sum "$bundle/rust-toolchain.tar.gz" > "$bundle/rust-toolchain.sha256"
docker image inspect "$builder" --format '{{.Id}}' > "$bundle/builder-image-id.txt"
printf 'Prepared pinned runtime image and runner toolchain assets.\n'
