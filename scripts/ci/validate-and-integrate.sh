#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
if [[ ${TIDEMQ_COVERAGE:-0} == 1 ]]; then
  # Discover a cold-image or registry problem before the expensive native suite.
  bash scripts/lab/ensure-runtime-base.sh
  source scripts/ci/coverage-env.sh full
fi
bash scripts/ci/validate.sh
bash scripts/ci/integration.sh
if [[ ${TIDEMQ_COVERAGE:-0} == 1 ]]; then
  export TIDEMQ_API_COVERAGE=1
  # All targets and profiles remain in one instrumented session until the remote API suite finishes.
  bash scripts/ci/lab.sh
  bash scripts/coverage-run.sh target/ci/coverage
fi
