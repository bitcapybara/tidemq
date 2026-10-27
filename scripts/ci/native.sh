#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
source scripts/ci/coverage-env.sh native
bash scripts/ci/validate.sh
bash scripts/ci/integration.sh normal
mkdir -p target/ci/console-server
cp "$CARGO_TARGET_DIR/debug/tidemq" target/ci/console-server/tidemq
cp target/ci/source-commit.txt target/ci/console-server/source-commit.txt
sha256sum target/ci/console-server/tidemq > target/ci/console-server/binary.sha256
node scripts/coverage/bundle.mjs pack native target/ci/coverage-inputs
