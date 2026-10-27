#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ $# != 1 || -e $1 ]]; then
  printf 'Usage: %s new-binary-directory\n' "$0" >&2
  exit 2
fi
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
mkdir -- "$1"
output=$(cd -- "$1" && pwd -P)
cd -- "$repo"
revision=${TIDEMQ_PERF_REVISION:-$(git rev-parse HEAD)}
[[ $revision =~ ^[0-9a-f]{40}$ ]] || { printf 'A full source revision is required\n' >&2; exit 2; }
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-4}
export RUSTFLAGS='-C force-frame-pointers=yes'
export CFLAGS='-fno-omit-frame-pointer'
# Release already retains line tables; preserve the same binary for unprofiled and profiled runs.
rustc --version --verbose > "$output/compiler.txt"
cargo --version > "$output/cargo.txt"
cargo build --locked --release -p tidemq --bin tidemq --example single_node_bench > "$output/build.log" 2>&1
target=${CARGO_TARGET_DIR:-$repo/target}
cp -- "$target/release/tidemq" "$output/tidemq"
cp -- "$target/release/examples/single_node_bench" "$output/single_node_bench"
for binary in tidemq single_node_bench; do
  sha=$(sha256sum "$output/$binary" | cut -d ' ' -f 1)
  allocator=system
  [[ $binary != tidemq ]] || allocator=mimalloc
  jq -n --arg revision "$revision" --arg binary "$binary" --arg sha256 "$sha" \
    --arg rustflags "$RUSTFLAGS" --arg cflags "$CFLAGS" --arg jobs "$CARGO_BUILD_JOBS" --arg allocator "$allocator" \
    '{revision:$revision,binary:$binary,sha256:$sha256,profile:"release",cargo_flags:"--locked --release -p tidemq --bin tidemq --example single_node_bench",rustflags:$rustflags,cflags:$cflags,build_jobs:$jobs,allocator:$allocator}' \
    > "$output/$binary.build.json"
done
sha256sum examples/single_node_bench.rs examples/single_node_bench/*.rs scripts/perf/*.sh > "$output/tooling-sha256.txt"
printf 'Preserved standalone binaries in %s\n' "$output"
