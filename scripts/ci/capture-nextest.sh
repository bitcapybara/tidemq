#!/usr/bin/env bash
set -euo pipefail
[[ $# -gt 0 ]] || { printf 'A nextest command is required\n' >&2; exit 2; }
inherited_umask=$(umask)
umask 077
mkdir -p target/ci
capture=$(mktemp -d target/ci/nextest-output.XXXXXX)
: > "$capture/stdout.log"
: > "$capture/stderr.log"
exec 3>&1 4>&2 5<&0

describe_fd() {
  local descriptor=$1 key value
  kind=other
  if [[ -f /dev/fd/$descriptor ]]; then kind=regular_file
  elif [[ -p /dev/fd/$descriptor ]]; then kind=pipe
  elif [[ -t $descriptor ]]; then kind=terminal
  fi
  flags=unavailable
  if [[ -r /proc/self/fdinfo/$descriptor ]]; then
    while read -r key value; do
      if [[ $key == flags: && $value =~ ^[0-7]+$ ]]; then flags=$value; fi
    done < "/proc/self/fdinfo/$descriptor"
  fi
}
describe_fd 3
stdout_kind=$kind stdout_flags=$flags
describe_fd 4
stderr_kind=$kind stderr_flags=$flags

child= pending_signal= cancellation_exit=0 wait_interrupted=0
forward_signal() {
  local running
  wait_interrupted=1
  cancellation_exit=$2
  if [[ -z $child ]]; then pending_signal=$1; return; fi
  for running in $(jobs -pr); do
    if [[ $running == "$child" ]]; then
      kill -s "$1" "$child" 2>> "$capture/cancellation-errors.log"
    fi
  done
}
trap 'forward_signal TERM 143' TERM
trap 'forward_signal INT 130' INT

run_owned() {
  local owned_exit
  "$@" <&5 3>&- 4>&- 5<&- &
  child=$!
  if [[ -n $pending_signal ]]; then
    forward_signal "$pending_signal" "$cancellation_exit"
    pending_signal=
  fi
  while :; do
    wait_interrupted=0
    wait "$child"
    owned_exit=$?
    (( wait_interrupted != 0 )) || break
  done
  child=
  return "$owned_exit"
}

# The actual reporter must never write into the runner's console pipe.
set +e
umask "$inherited_umask"
run_owned "$@" > "$capture/stdout.log" 2> "$capture/stderr.log"
nextest_exit=$?
umask 077
stdout_bytes=$(wc -c < "$capture/stdout.log")
stdout_count_exit=$?
stderr_bytes=$(wc -c < "$capture/stderr.log")
stderr_count_exit=$?
stdout_forward_exit=$cancellation_exit
stderr_forward_exit=$cancellation_exit
if (( cancellation_exit == 0 )); then
  run_owned cat "$capture/stdout.log" >&3 2> "$capture/console-forward-errors.log"
  stdout_forward_exit=$?
fi
if (( cancellation_exit == 0 )); then
  run_owned cat "$capture/stderr.log" >&4 2>> "$capture/console-forward-errors.log"
  stderr_forward_exit=$?
else
  stderr_forward_exit=$cancellation_exit
fi
result=$nextest_exit
if (( result == 0 && cancellation_exit != 0 )); then result=$cancellation_exit; fi
if (( result == 0 && (stdout_forward_exit != 0 || stderr_forward_exit != 0 ||
    stdout_count_exit != 0 || stderr_count_exit != 0) )); then result=1; fi
printf '{"nextest_exit":%d,"effective_exit":%d,"cancellation_exit":%d,"stdout_forward_exit":%d,"stderr_forward_exit":%d,"stdout_bytes":%d,"stderr_bytes":%d,"console_stdout_kind":"%s","console_stdout_flags":"%s","console_stderr_kind":"%s","console_stderr_flags":"%s"}\n' \
  "$nextest_exit" "$result" "$cancellation_exit" "$stdout_forward_exit" "$stderr_forward_exit" \
  "${stdout_bytes:-0}" "${stderr_bytes:-0}" "$stdout_kind" "$stdout_flags" "$stderr_kind" "$stderr_flags" \
  > "$capture/receipt.json"
receipt_exit=$?
if (( result == 0 && receipt_exit != 0 )); then result=1; fi
exit "$result"
