#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
features=tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
mkdir -p target/ci
export TMPDIR=$(mktemp -d "${TMPDIR:-/tmp}/tidemq-ci-integration.XXXXXX")
cleanup() {
  local result=$?
  trap - EXIT
  if (( result != 0 )); then
    node scripts/ci/failure-evidence.mjs "$TMPDIR" target/ci/failures || true
    [[ ! -f target/nextest/ci/junit.xml ]] || cp target/nextest/ci/junit.xml target/ci/failed-junit.xml
  fi
  rm -rf -- "$TMPDIR"
  exit "$result"
}
trap cleanup EXIT
target_dir=${CARGO_TARGET_DIR:-$PWD/target}
node --test tests/lab/*.test.mjs
cargo nextest run --workspace --all-targets --locked --features "$features" --profile ci -E 'kind(test)'
cp target/nextest/ci/junit.xml target/ci/integration-junit.xml
cargo nextest run -p tidemq --test runtime_queue --test runtime_consumer_reads --test runtime_reclamation \
  --locked --features "$features,test-hooks" --profile ci
cp target/nextest/ci/junit.xml target/ci/runtime-faults-junit.xml
if [[ ${TIDEMQ_REQUIRE_EXTERNAL:-0} == 1 ]]; then
  [[ -x ${TIDEMQ_TEST_RUSTFS_BIN:-} && -x ${TIDEMQ_OTELCOL_BIN:-} ]] || { printf 'Verified external dependency binaries are required\n' >&2; exit 1; }
  "$TIDEMQ_TEST_RUSTFS_BIN" --version > target/ci/rustfs-version.txt
  "$TIDEMQ_OTELCOL_BIN" --version > target/ci/collector-version.txt
  sha256sum "$TIDEMQ_TEST_RUSTFS_BIN" "$TIDEMQ_OTELCOL_BIN" > target/ci/external-binary-sha256.txt
  cargo build --locked --example deploy_validation
  export TIDEMQ_TEST_DEPLOY_VALIDATION="$target_dir/debug/examples/deploy_validation"
  # These tests own independent dependency processes and data directories under tests/.
  cargo nextest run -p tidemq --all-targets --locked --features "$features,test-hooks" \
    --profile ci --run-ignored only -E 'kind(test)'
  cp target/nextest/ci/junit.xml target/ci/external-services-junit.xml
fi
