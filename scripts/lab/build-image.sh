#!/usr/bin/env bash
set -euo pipefail
umask 077
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
cd "$repo"
[[ $# == 1 && ! -e "$1" ]] || { printf 'Usage: build-image.sh new-private-output-directory\n' >&2; exit 2; }
lab_home=${TIDEMQ_LAB_HOME:?Set TIDEMQ_LAB_HOME}
tools=${TIDEMQ_LAB_TOOLS:-/srv/tidemq/tools}
base=$(jq -er .docker.runtimeBaseImage deploy/lab/inventory.json)
expected=$(jq -er .docker.runtimeBaseImageID deploy/lab/inventory.json)
[[ $(docker image inspect "$base" --format '{{.Id}}') == "$expected" ]] || { printf 'The runtime base image identity differs from inventory\n' >&2; exit 1; }
mkdir -m 0700 -p "$1/bin"
output=$(cd "$1" && pwd -P)
target=${CARGO_TARGET_DIR:-$repo/target}
cargo build --release --locked --bin tidemq --example deploy_validation
cp "$target/release/tidemq" "$output/bin/tidemq"
cp "$target/release/examples/deploy_validation" "$output/bin/deploy_validation"
cp "$tools/collector" "$output/bin/collector"
cp "$lab_home/ca.pem" "$output/ca.pem"
cp deploy/lab/Dockerfile "$output/Dockerfile"
hash=$(cd "$output" && sha256sum bin/tidemq bin/deploy_validation bin/collector ca.pem Dockerfile | sha256sum | cut -d ' ' -f 1)
image="localhost/tidemq-lab:${hash:0:24}"
git rev-parse HEAD > "$output/source-commit.txt"
git diff --binary HEAD -- Cargo.toml Cargo.lock build.rs src crates sdk examples | sha256sum > "$output/source-patch-sha256.txt"
rustc -Vv > "$output/toolchain.txt"
sha256sum "$output/bin/tidemq" "$output/bin/deploy_validation" > "$output/sha256.txt"
docker build --build-arg "RUNTIME_IMAGE=$base" --label "org.opencontainers.image.revision=$(git rev-parse HEAD)" -t "$image" "$output" > "$output/build.log" 2>&1
docker image inspect "$image" --format '{{.Id}}' > "$output/image-id.txt"
docker run --rm --network none "$image" /usr/local/bin/tidemq --version
docker save -o "$output/image.tar" "$image"
printf '%s\n' "$image" > "$output/image.txt"
sha256sum "$output/image.tar" >> "$output/sha256.txt"
printf 'Built fixed lab image %s\n' "$image"
