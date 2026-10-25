#!/usr/bin/env bash
set -euo pipefail

allowed_vm() { [[ "$1" =~ ^tidemq-(cp-[abc]|worker-[abc]|fixtures|loadgen)$ ]]; }
valid_request() {
  [[ "$1" == status || "$1" == lock ]] && return 0
  [[ "$1" =~ ^(off|pause|net-cut|restore)\ (tidemq-[a-z-]+)\ ([0-9]{1,3})$ ]] || return 1
  local vm=${BASH_REMATCH[2]} duration=${BASH_REMATCH[3]}
  allowed_vm "$vm" || return 1
  (( 10#$duration >= 5 && 10#$duration <= 300 ))
}
if [[ ${1:-} == --selfcheck ]]; then
  for request in 'off tidemq-worker-a 30' 'net-cut tidemq-cp-c 120' status lock; do valid_request "$request" || exit 1; done
  for request in 'off tidemq-storage 30' 'pause tidemq-observe 60' 'off tidemq-runner 30' 'off tidemq-worker-a 0' 'off tidemq-worker-a 301' 'off tidemq-worker-a 08;id' 'off tidemq-worker-a 30 extra'; do
    if valid_request "$request"; then printf 'Unsafe request was accepted\n' >&2; exit 1; fi
  done
  printf 'Host fault controller boundary checks passed\n'
  exit
fi
[[ $EUID == 0 ]] || exit 1
restore_vm() {
  local vm=$1 state mac
  local -a timers
  allowed_vm "$vm" || exit 2
  state=$(virsh domstate "$vm")
  if [[ "$state" == paused ]]; then virsh resume "$vm"; fi
  if [[ "$state" == 'shut off' ]]; then virsh start "$vm"; fi
  mac=$(jq -er --arg name "$vm" '.nodes[] | select(.name == $name) | .dataMAC' /etc/tidemq-lab/inventory.json)
  virsh domif-setlink "$vm" "$mac" up
  # Retire the watchdog after recovery so it cannot heal a later fault scenario.
  mapfile -t timers < <(systemctl list-units --all --type=timer --no-legend --plain "tidemq-restore-$vm-*.timer" | awk '{print $1}')
  if (( ${#timers[@]} )); then systemctl stop "${timers[@]}"; fi
}
if [[ ${1:-} == restore && $# == 2 && -z ${SSH_ORIGINAL_COMMAND:-} ]]; then restore_vm "$2"; exit; fi
request=${SSH_ORIGINAL_COMMAND:-}
valid_request "$request" || { printf 'Denied lab control request\n' >&2; exit 2; }
if [[ "$request" == lock ]]; then
  exec timeout --foreground 7200 flock --nonblock /var/lock/tidemq-lab.lock sh -c 'printf "LOCKED\n"; cat >/dev/null'
fi
if [[ "$request" == status ]]; then
  for vm in tidemq-cp-{a,b,c} tidemq-worker-{a,b,c} tidemq-fixtures tidemq-loadgen; do
    printf '%s: %s\n' "$vm" "$(virsh domstate "$vm")"
  done
  exit
fi
read -r action vm duration <<< "$request"
if [[ "$action" == restore ]]; then restore_vm "$vm"; exit; fi
unit="tidemq-restore-$vm-$(date +%s)-$RANDOM"
# Arm host-owned recovery before disrupting a guest; job cancellation cannot cancel this timer.
systemd-run --quiet --collect --unit="$unit" --on-active="${duration}s" --timer-property=AccuracySec=1s \
  /usr/local/sbin/tidemq-lab-control restore "$vm"
case "$action" in
  off) virsh destroy "$vm" ;;
  pause) virsh suspend "$vm" ;;
  net-cut)
    mac=$(jq -er --arg name "$vm" '.nodes[] | select(.name == $name) | .dataMAC' /etc/tidemq-lab/inventory.json)
    virsh domif-setlink "$vm" "$mac" down
    ;;
esac
printf 'Fault %s on %s at %s; recovery unit %s is armed for %s seconds\n' "$action" "$vm" "$(date --utc --iso-8601=ns)" "$unit" "$duration"
