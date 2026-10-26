#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
bash scripts/lab/ensure-runtime-base.sh
source scripts/ci/coverage-env.sh lab
mkdir -p target/ci
git rev-parse HEAD > target/ci/source-commit.txt
rustc --version > target/ci/rustc.txt
bash scripts/ci/integration.sh faults
export TIDEMQ_API_COVERAGE=1
bash scripts/ci/lab.sh
node scripts/coverage/bundle.mjs pack lab target/ci/coverage-inputs
