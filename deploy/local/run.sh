#!/usr/bin/env bash
set -euo pipefail
directory=${1:?Usage: run.sh generated-directory [tidemq-binary]}
binary=${2:-target/debug/tidemq}
pids=()
stop() {
  trap - INT TERM EXIT
  for pid in "${pids[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  for pid in "${pids[@]}"; do wait "$pid" || true; done
}
trap stop INT TERM EXIT
for config in "$directory"/node-*.toml; do
  "$binary" serve --config "$config" >"${config%.toml}.log" 2>&1 &
  pids+=("$!")
done
printf 'Started %s runtime processes; press Ctrl-C to drain them\n' "${#pids[@]}"
wait
