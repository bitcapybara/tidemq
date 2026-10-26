#!/usr/bin/env bash
set -euo pipefail
[[ $# -gt 0 ]] || exit 2
# Both runners share downloads; maintenance requires the exclusive side of this lock.
exec 8>/srv/tidemq/maintenance/cargo-cache.lock
flock --shared 8
"$@"
