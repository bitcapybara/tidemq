#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
export CARGO_TARGET_DIR=/srv/tidemq/cargo-target/report
export CARGO_LLVM_COV_TARGET_DIR=$CARGO_TARGET_DIR
[[ ! -L "$CARGO_TARGET_DIR" ]]
# This lane owns only the disposable report directory, not either runner's build cache.
rm -rf -- "$CARGO_TARGET_DIR"
eval "$(cargo llvm-cov show-env --sh)"
node scripts/coverage/bundle.mjs restore coverage-inputs/coverage-inputs-native coverage-inputs/coverage-inputs-lab
bash scripts/coverage-run.sh target/ci/coverage
cp target/ci/coverage-inputs.json target/ci/coverage/inputs.json
