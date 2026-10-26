#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
if [[ ${TIDEMQ_COVERAGE:-0} == 1 ]]; then
  export CARGO_TARGET_DIR=${TIDEMQ_COVERAGE_TARGET_DIR:-$PWD/target/llvm-cov-target}
  export CARGO_LLVM_COV_TARGET_DIR=$CARGO_TARGET_DIR
  # Clean once, then retain profiles from every validation and integration stage.
  cargo llvm-cov clean --workspace
  eval "$(cargo llvm-cov show-env --sh)"
fi
bash scripts/ci/validate.sh
bash scripts/ci/integration.sh
if [[ ${TIDEMQ_COVERAGE:-0} == 1 ]]; then
  export TIDEMQ_API_COVERAGE=1
  # All targets and profiles remain in one instrumented session until the remote API suite finishes.
  bash scripts/ci/lab.sh
  bash scripts/coverage-run.sh target/ci/coverage
fi
