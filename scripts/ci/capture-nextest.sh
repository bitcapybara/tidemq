#!/usr/bin/env bash
set -euo pipefail
runtime=node
if [[ -x /opt/actions-runner/externals/node24/bin/node ]]; then
  runtime=/opt/actions-runner/externals/node24/bin/node
fi
exec "$runtime" "${BASH_SOURCE[0]%/*}/capture-nextest.mjs" "$@"
