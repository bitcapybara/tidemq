#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
source scripts/ci/coverage-env.sh native
bash scripts/ci/validate.sh
bash scripts/ci/integration.sh normal
node scripts/coverage/bundle.mjs pack native target/ci/coverage-inputs
