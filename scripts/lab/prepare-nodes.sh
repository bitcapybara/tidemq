#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
version=$(jq -er .kubernetes.version "$lab_inventory")
pids=()
while IFS=$'\t' read -r name address; do
  printf 'Waiting for managed guest %s\n' "$name"
  lab_wait "$name"
  lab_copy "$name" "$lab_repo/scripts/lab/bootstrap-node.sh" /tmp/tidemq-bootstrap-node.sh
  lab_ssh "$name" sudo bash /tmp/tidemq-bootstrap-node.sh "$version" "$address" > "$lab_state/logs/$name-kubernetes.log" 2>&1 &
  pids+=("$!")
done < <(jq -r '.nodes[] | select(.role == "control-plane" or .role == "worker") | [.name,.dataIP] | @tsv' "$lab_inventory")
result=0
for job in "${pids[@]}"; do wait "$job" || result=1; done
if (( result != 0 )); then printf 'A node bootstrap failed; inspect the per-node logs.\n' >&2; fi
exit "$result"
