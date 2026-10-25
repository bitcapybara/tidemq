#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
pids=()
while IFS=$'\t' read -r name role; do
  lab_wait "$name"
  lab_copy "$name" "$lab_repo/scripts/lab/bootstrap-infra.sh" /tmp/tidemq-bootstrap-infra.sh
  lab_ssh "$name" sudo bash /tmp/tidemq-bootstrap-infra.sh "$role" > "$lab_state/logs/$name-infra.log" 2>&1 &
  pids+=("$!")
done < <(jq -r '.nodes[] | select(.role != "control-plane" and .role != "worker") | [.name,.role] | @tsv' "$lab_inventory")
result=0
for job in "${pids[@]}"; do wait "$job" || result=1; done
if (( result != 0 )); then printf 'Infrastructure bootstrap failed; inspect per-guest logs.\n' >&2; fi
exit "$result"
