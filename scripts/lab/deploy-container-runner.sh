#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
mode=${1:-}
[[ $EUID == 0 && "$mode" =~ ^(build|register|start)$ ]] || { printf 'Usage: sudo deploy-container-runner.sh build|register|start [private-token-file]\n' >&2; exit 2; }
exec 9>/var/lock/tidemq-lab.lock
flock --exclusive --nonblock 9
if lab_ssh tidemq-runner pgrep -u actions -f '[R]unner.Worker' >/dev/null; then
  printf 'A runner is busy; retry installation after the job completes\n' >&2; exit 1
fi
bundle="$lab_state/private/container-runner/source.tar.gz"
install -d -m 0700 "$(dirname "$bundle")"
tar -C "$lab_repo" -czf "$bundle" deploy/lab/container-runner scripts/lab/runner-maintenance.sh \
  deploy/lab/runner-maintenance.env deploy/lab/binaries.json deploy/lab/inventory.json
lab_copy tidemq-runner "$bundle" /tmp/tidemq-container-runner-source.tar.gz
lab_ssh tidemq-runner sudo install -d -m 0755 /srv/tidemq/container-runner/source
lab_ssh tidemq-runner sudo tar -xzf /tmp/tidemq-container-runner-source.tar.gz -C /srv/tidemq/container-runner/source
lab_ssh tidemq-runner sudo rm /tmp/tidemq-container-runner-source.tar.gz
rm "$bundle"
if [[ "$mode" == register ]]; then
  [[ $# == 2 && -f "$2" ]] || exit 2
  trap 'rm -f -- "$2"; lab_ssh tidemq-runner sudo rm -f /srv/tidemq/container-runner/registration.token' EXIT
  lab_copy tidemq-runner "$2" /tmp/tidemq-registration.token
  lab_ssh tidemq-runner sudo mv /tmp/tidemq-registration.token /srv/tidemq/container-runner/registration.token
fi
lab_ssh tidemq-runner sudo bash /srv/tidemq/container-runner/source/deploy/lab/container-runner/setup.sh "$mode"
