#!/usr/bin/env bash
set -euo pipefail
[[ $# -gt 0 ]] || { printf 'Usage: with-lease.sh command [arguments...]\n' >&2; exit 2; }
lab_home=${TIDEMQ_LAB_HOME:?Set TIDEMQ_LAB_HOME to the private runner configuration directory}
coproc LAB_LEASE { ssh -F "$lab_home/ssh_config" lab-host lock; }
lease_pid=$LAB_LEASE_PID
lease_write=${LAB_LEASE[1]}
lease_read=${LAB_LEASE[0]}
cleanup() {
  exec {lease_write}>&-
  wait "$lease_pid" || true
}
trap cleanup EXIT
read -r -t 15 acknowledgement <&"$lease_read"
[[ "$acknowledgement" == LOCKED ]] || { printf 'The lab is already leased or unavailable\n' >&2; exit 1; }
export TIDEMQ_LAB_LEASE_HELD=1
"$@"
