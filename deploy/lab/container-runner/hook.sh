#!/usr/bin/env bash
set -euo pipefail
phase=$(basename "$0" .sh)
[[ "$phase" =~ ^(before|after|idle)$ ]] || exit 2
source /opt/tidemq-runner/runner-maintenance.sh
source /opt/tidemq-runner/runner-maintenance.env
exec 9>/diag/.cleanup.lock
if [[ "$phase" == idle ]]; then
  flock --nonblock 9 || exit 0
  if pgrep -f '[R]unner.Worker' >/dev/null; then exit 0; fi
else
  flock --wait 600 9
fi
cd /
clean_checkouts /srv/tidemq/work "${GITHUB_WORKSPACE:-/srv/tidemq/work/tidemq/tidemq}"
if [[ "$phase" == after ]]; then bound_cache /srv/tidemq/work/_actions "$ACTION_CACHE_MAX_BYTES" "$CACHE_MAX_DAYS"; fi
[[ ! -d /srv/tidemq/work/_temp ]] || find /srv/tidemq/work/_temp -mindepth 1 -maxdepth 1 -mtime +1 -exec rm -rf --one-file-system -- {} +
bound_cache /srv/tidemq/cargo-target "$TARGET_MAX_BYTES" "$CACHE_MAX_DAYS"
clean_cargo_downloads /srv/tidemq/maintenance/cargo-cache.lock /opt/tidemq-rust/cargo
bound_cache /srv/tidemq/tmp "$TMP_MAX_BYTES" 1
find /diag -type f ! -name .cleanup.lock -mtime +7 -delete
if (( $(du -sx -B1 /diag | cut -f 1) > DIAGNOSTIC_MAX_BYTES )); then
  find /diag -type f ! -name .cleanup.lock -mtime +1 -delete
fi
(( $(free_bytes /srv/tidemq/work) >= DATA_MIN_FREE_BYTES )) || { printf 'Runner data disk is below its free-space budget\n' >&2; exit 1; }
