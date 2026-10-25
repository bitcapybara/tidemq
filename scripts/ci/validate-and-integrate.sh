#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
bash scripts/ci/validate.sh
bash scripts/ci/integration.sh
