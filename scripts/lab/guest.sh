#!/usr/bin/env bash
# Shared transport for the explicitly owned lab virtual machines.
set -euo pipefail
lab_repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
lab_inventory=${TIDEMQ_LAB_INVENTORY:-$lab_repo/deploy/lab/inventory.json}
lab_state=${TIDEMQ_LAB_STATE:-/var/lib/tidemq-lab}
lab_key=${TIDEMQ_LAB_KEY:-$lab_state/private/id_ed25519}
lab_known_hosts=${TIDEMQ_LAB_KNOWN_HOSTS:-$lab_state/private/known_hosts}

lab_address() {
  jq -er --arg name "$1" '.nodes[] | select(.name == $name) | .managementIP' "$lab_inventory"
}

lab_ssh() {
  local name=$1 command
  shift
  printf -v command '%q ' "$@"
  ssh -n -i "$lab_key" -o BatchMode=yes -o ConnectTimeout=5 -o ServerAliveInterval=15 \
    -o StrictHostKeyChecking=accept-new -o "UserKnownHostsFile=$lab_known_hosts" \
    "lab@$(lab_address "$name")" "$command"
}

lab_copy() {
  local name=$1 source=$2 destination=$3
  scp -q -i "$lab_key" -o BatchMode=yes -o ConnectTimeout=5 \
    -o StrictHostKeyChecking=accept-new -o "UserKnownHostsFile=$lab_known_hosts" \
    "$source" "lab@$(lab_address "$name"):$destination"
}

lab_fetch() {
  local name=$1 source=$2 destination=$3
  scp -q -i "$lab_key" -o BatchMode=yes -o ConnectTimeout=5 \
    -o StrictHostKeyChecking=accept-new -o "UserKnownHostsFile=$lab_known_hosts" \
    "lab@$(lab_address "$name"):$source" "$destination"
}

lab_wait() {
  local name=$1
  for ((attempt=0; attempt<180; attempt++)); do
    if lab_ssh "$name" true >/dev/null 2>&1; then
      lab_ssh "$name" sudo timeout 900 cloud-init status --wait --long
      return
    fi
    sleep 5
  done
  printf 'Guest management connection timed out: %s\n' "$name" >&2
  return 1
}
