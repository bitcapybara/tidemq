#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
readonly report_target=/srv/tidemq/cargo-target/report
export CARGO_TARGET_DIR=$report_target
export CARGO_LLVM_COV_TARGET_DIR=$CARGO_TARGET_DIR
[[ ! -L "$report_target" && ! -L /srv/tidemq/cargo-target ]]
# This lane owns only the disposable report directory, not either runner's build cache.
rm -rf -- "$report_target"
trap 'rm -rf -- "$report_target"' EXIT
eval "$(cargo llvm-cov show-env --sh)"
export CARGO_TARGET_DIR="$report_target"
export CARGO_LLVM_COV_TARGET_DIR="$report_target"
node scripts/coverage/bundle.mjs restore
bash scripts/coverage-run.sh target/ci/coverage
cp target/ci/coverage-inputs.json target/ci/coverage/inputs.json
