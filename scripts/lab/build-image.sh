#!/usr/bin/env bash
set -euo pipefail
umask 077
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
cd "$repo"
[[ $# == 1 && ! -e "$1" ]] || { printf 'Usage: build-image.sh new-private-output-directory\n' >&2; exit 2; }
lab_home=${TIDEMQ_LAB_HOME:?Set TIDEMQ_LAB_HOME}
tools=${TIDEMQ_LAB_TOOLS:-/srv/tidemq/tools}
base=$(jq -er .docker.runtimeBaseImage deploy/lab/inventory.json)
profile=release
flags=(--release)
if [[ ${TIDEMQ_API_COVERAGE:-0} == 1 ]]; then
  # The pinned llvm-cov uses its rustc wrapper instead of global RUSTFLAGS.
  [[ ${CARGO_LLVM_COV_SHOW_ENV:-0} == 1 && ${CARGO_LLVM_COV:-0} == 1 && "${RUSTC_WRAPPER:-}" == */cargo-llvm-cov ]] || {
    printf 'API coverage requires the existing llvm-cov instrumentation environment\n' >&2; exit 1;
  }
  profile=debug
  flags=(--features tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen)
fi
bash scripts/lab/ensure-runtime-base.sh
mkdir -m 0700 -p "$1/bin"
output=$(cd "$1" && pwd -P)
target=${CARGO_TARGET_DIR:-$repo/target}
source_manifest() {
  git ls-files --cached --others --exclude-standard -z -- Cargo.toml Cargo.lock build.rs src crates sdk examples tests config console/web \
    scripts/build-console.sh scripts/lab/build-image.sh deploy/docker/Dockerfile deploy/lab/Dockerfile \
    | while IFS= read -r -d '' file; do
      case "$file" in
        */node_modules/*|*/dist/*|*/target/*|*/.git/*|*/private/*|*/.artifacts/*|*/coverage/*|*/.DS_Store|*.tsbuildinfo|*.log|*.pem|*.key|*.token|*/.env|*/.env.*|*/.npmrc) continue ;;
      esac
      [[ -f "$file" ]] || continue
      [[ ! -L "$file" ]] || { printf 'Source identity does not permit symlinks: %s\n' "$file" >&2; exit 1; }
      printf '%s\0' "$file"
    done | LC_ALL=C sort -zu | xargs -0 sha256sum
}
source_manifest > "$output/source-sha256.txt"
cargo build "${flags[@]}" --locked --bin tidemq --example deploy_validation
bash scripts/build-console.sh "$output/console"
source_manifest > "$output/source-after-sha256.txt"
cmp -s "$output/source-sha256.txt" "$output/source-after-sha256.txt" || {
  printf 'Source changed during image compilation; preserve evidence and retry from a frozen snapshot\n' >&2; exit 1;
}
source_hash=$(sha256sum "$output/source-sha256.txt" | cut -d ' ' -f 1)
printf '%s\n' "$source_hash" > "$output/source-tree-sha256.txt"
cp "$target/$profile/tidemq" "$output/bin/tidemq"
cp "$target/$profile/examples/deploy_validation" "$output/bin/deploy_validation"
printf '%s\n' "$profile" > "$output/profile.txt"
cp "$tools/collector" "$output/bin/collector"
cp "$lab_home/ca.pem" "$output/ca.pem"
cp deploy/lab/Dockerfile "$output/Dockerfile"
(cd "$output" && find bin console ca.pem Dockerfile source-tree-sha256.txt -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum) > "$output/runtime-sha256.txt"
hash=$(sha256sum "$output/runtime-sha256.txt" | cut -d ' ' -f 1)
image="localhost/tidemq-lab:${hash:0:24}"
git rev-parse HEAD > "$output/source-commit.txt"
{
  git diff --binary HEAD -- Cargo.toml Cargo.lock build.rs src crates sdk examples tests console scripts/build-console.sh deploy/docker/Dockerfile deploy/lab/Dockerfile scripts/lab/build-image.sh
  cat "$output/source-tree-sha256.txt"
  cat "$output/console/.tidemq-build.json"
} | sha256sum > "$output/source-patch-sha256.txt"
rustc -Vv > "$output/toolchain.txt"
sha256sum "$output/bin/tidemq" "$output/bin/deploy_validation" > "$output/sha256.txt"
find "$output/console" -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum >> "$output/sha256.txt"
sha256sum "$output/runtime-sha256.txt" >> "$output/sha256.txt"
docker build --build-arg "RUNTIME_IMAGE=$base" --label "org.opencontainers.image.revision=$(git rev-parse HEAD)" \
  --label "io.tidemq.source.tree.sha256=$source_hash" -t "$image" "$output" > "$output/build.log" 2>&1
docker image inspect "$image" --format '{{.Id}}' > "$output/image-id.txt"
docker run --rm --network none -e LLVM_PROFILE_FILE=/tmp/version-%p.profraw "$image" /usr/local/bin/tidemq --version
docker save -o "$output/image.tar" "$image"
printf '%s\n' "$image" > "$output/image.txt"
sha256sum "$output/image.tar" >> "$output/sha256.txt"
printf 'Built fixed lab image %s\n' "$image"
