#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
[[ $EUID == 0 && $# == 0 ]] || { printf 'Usage: sudo deploy-action-cache.sh\n' >&2; exit 2; }
exec 9>/var/lock/tidemq-lab.lock
flock --exclusive --nonblock 9
if lab_ssh tidemq-runner pgrep -u actions -f '[R]unner.Worker' >/dev/null; then
  printf 'Wait for both runner jobs before installing action archives\n' >&2; exit 1
fi
install -d -m 0700 "$lab_state/private"
staging=$(mktemp -d "$lab_state/private/action-cache.XXXXXX")
remote=
cleanup() {
  rm -rf -- "$staging" "$staging.tar.gz"
  [[ -z "$remote" ]] || lab_ssh tidemq-runner rm -rf -- "$remote"
}
trap cleanup EXIT
for name in $(jq -r 'to_entries[] | select(.value.action) | .key' "$lab_repo/deploy/lab/binaries.json"); do
  [[ -f "$lab_state/tools/$name.archive" ]] || bash "$lab_repo/scripts/lab/download-tools.sh" actions
  cp "$lab_state/tools/$name.archive" "$staging/"
done
cp "$lab_repo/deploy/lab/binaries.json" "$lab_repo/scripts/lab/install-action-cache.mjs" "$staging/"
tar -czf "$staging.tar.gz" -C "$staging" .
remote=$(lab_ssh tidemq-runner mktemp -d /tmp/tidemq-action-cache.XXXXXX)
[[ "$remote" =~ ^/tmp/tidemq-action-cache\.[a-zA-Z0-9]+$ ]] || { remote=; exit 1; }
lab_copy tidemq-runner "$staging.tar.gz" "$remote/bundle.tar.gz"
lab_ssh tidemq-runner tar -xzf "$remote/bundle.tar.gz" -C "$remote"
lab_ssh tidemq-runner sudo node "$remote/install-action-cache.mjs" "$remote/binaries.json" "$remote"
lab_ssh tidemq-runner sudo systemctl try-restart actions.runner.bitcapybara-tidemq.tidemq-dev-u126-8.service
printf 'Installed the bounded action archive cache; recreate the container runner to activate its read-only mount.\n'
