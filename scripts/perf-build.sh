#!/usr/bin/env bash
set -euo pipefail
umask 077

output=${1:?Usage: perf-build.sh new-output-directory [system|mimalloc|jemalloc|all]}
selection=${2:-all}
root=$(cd "$(dirname "$0")/.." && pwd -P)
cd "$root"
if [[ -e "$output" ]]; then printf 'Build output already exists\n' >&2; exit 1; fi
if ! git diff --quiet || ! git diff --cached --quiet; then
  printf 'Commit tracked changes before recording a release build identity\n' >&2; exit 1
fi
case "$selection" in
  all) allocators='system mimalloc jemalloc' ;;
  system|mimalloc|jemalloc) allocators=$selection ;;
  *) printf 'Unknown allocator selection\n' >&2; exit 2 ;;
esac
mkdir -p "$output"
output=$(cd "$output" && pwd -P)
sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d ' ' -f 1
  else shasum -a 256 "$1" | cut -d ' ' -f 1; fi
}
json_string() {
  local value=$1
  value=${value//\\/\\\\}; value=${value//\"/\\\"}; value=${value//$'\n'/\\n}
  printf '"%s"' "$value"
}
source_commit=$(git rev-parse HEAD)
lock_hash=$(sha256 Cargo.lock)
rustc_version=$(rustc --version)
host=$(uname -sm)
build_target="$root/target/perf-build"
rustc -Vv > "$output/toolchain.txt"
for allocator in $allocators; do
  feature=allocator-$allocator
  directory="$output/$allocator"
  mkdir "$directory"
  # One shared Cargo target avoids recompiling unchanged dependencies for each allocator.
  CARGO_TARGET_DIR="$build_target" cargo build --release --locked -p tidemq --bin tidemq --example perf_load \
    --no-default-features --features "$feature" -j2
  cp "$build_target/release/tidemq" "$directory/tidemq"
  cp "$build_target/release/examples/perf_load" "$directory/perf_load"
  binary_hash=$(sha256 "$directory/tidemq")
  if [[ $(git rev-parse HEAD) != "$source_commit" ]] || ! git diff --quiet || ! git diff --cached --quiet; then
    printf 'Tracked source changed during the release build; discard this build output\n' >&2; exit 1
  fi
  {
    printf '{"source_commit":"%s","cargo_lock_sha256":"%s","service_binary_sha256":"%s",' "$source_commit" "$lock_hash" "$binary_hash"
    printf '"allocator_feature":"%s","rustc":' "$feature"; json_string "$rustc_version"
    printf ',"host":'; json_string "$host"
    printf ',"profile":"release","tracked_source_clean":true}\n'
  } > "$directory/identity.json"
done
printf 'Created fixed release candidates in %s\n' "$output"
