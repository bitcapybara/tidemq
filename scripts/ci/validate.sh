#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
features=tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen
export CARGO_TERM_COLOR=always
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-3}
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
mkdir -p target/ci
target_dir=${CARGO_TARGET_DIR:-$PWD/target}
git rev-parse HEAD > target/ci/source-commit.txt
rustc -Vv > target/ci/rustc.txt
cargo fmt --all -- --check
bash deploy/kubernetes/selfcheck.sh
bash scripts/lab/host-control.sh --selfcheck
node --test scripts/coverage/*.test.mjs scripts/performance/*.test.mjs scripts/lab/*.test.mjs
cargo clippy --workspace --all-targets --no-deps --locked --features "$features" -- -D warnings
cargo clippy --workspace --lib --bins --no-deps --locked --features "$features" -- \
  -D warnings -D clippy::unwrap_used -D clippy::expect_used -D clippy::panic -D clippy::unimplemented \
  -D clippy::todo -D clippy::unreachable -D clippy::indexing_slicing
# Unit targets cannot depend on an installed service, credentials, or a running cluster.
env -u TIDEMQ_TEST_RUSTFS_BIN -u TIDEMQ_OTELCOL_BIN \
  cargo nextest run --workspace --lib --bins --locked --features "$features" --profile ci
cp "$target_dir/nextest/ci/junit.xml" target/ci/unit-junit.xml
cargo test --workspace --doc --locked --features "$features"
cargo clippy -p tidemq-sdk --all-targets --no-deps --locked -- -D warnings
cargo nextest run -p tidemq-sdk --lib --locked --profile ci
cp "$target_dir/nextest/ci/junit.xml" target/ci/sdk-default-unit-junit.xml
cargo build -p tidemq-protocol --features codegen --locked
cargo fmt --all -- --check
git diff --exit-code -- crates/tidemq-protocol/src/generated
cargo run --locked -- validate-config --config config/local.toml
