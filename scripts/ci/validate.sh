#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
features=tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen
export CARGO_TERM_COLOR=always
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-3}
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
mkdir -p target/ci
git rev-parse HEAD > target/ci/source-commit.txt
rustc -Vv > target/ci/rustc.txt
cargo fmt --all -- --check
bash deploy/kubernetes/selfcheck.sh
bash scripts/lab/host-control.sh --selfcheck
bash scripts/lab/prepare-runner-assets.sh --selfcheck
node --test scripts/coverage/*.test.mjs scripts/lab/*.test.mjs scripts/ci/*.test.mjs
cargo clippy --workspace --all-targets --no-deps --locked --features "$features" -- -D warnings
cargo clippy --workspace --lib --bins --no-deps --locked --features "$features" -- \
  -D warnings -D clippy::unwrap_used -D clippy::expect_used -D clippy::panic -D clippy::unimplemented \
  -D clippy::todo -D clippy::unreachable -D clippy::indexing_slicing
# Unit targets cannot depend on an installed service, credentials, or a running cluster.
env -u TIDEMQ_TEST_RUSTFS_BIN -u TIDEMQ_OTELCOL_BIN \
  bash scripts/ci/nextest.sh --workspace --lib --bins --examples --locked --features "$features" --profile ci
# Nextest stores reports under the workspace even when Cargo artifacts live elsewhere.
cp target/nextest/ci/junit.xml target/ci/unit-junit.xml
cargo test --workspace --doc --locked --features "$features"
cargo clippy -p tidemq-sdk --all-targets --no-deps --locked -- -D warnings
bash scripts/ci/nextest.sh -p tidemq-sdk --lib --locked --profile ci
cp target/nextest/ci/junit.xml target/ci/sdk-default-unit-junit.xml
cargo build -p tidemq-protocol --features codegen --locked
cargo fmt --all -- --check
git diff --exit-code -- crates/tidemq-protocol/src/generated
for config in config/*.toml; do
  cargo run --locked -- validate-config --config "$config"
done
target_dir=${CARGO_TARGET_DIR:-$PWD/target}
TIDEMQ_BIN="$target_dir/debug/tidemq" bash deploy/kubernetes/selfcheck.sh
TIDEMQ_BIN="$target_dir/debug/tidemq" node --test deploy/console/prepare.test.mjs
