#!/usr/bin/env bash
set -euo pipefail
directory=${1:?Usage: run.sh generated-directory [tidemq-binary]}
binary=${2:-target/debug/tidemq}
script_dir=$(cd "$(dirname "$0")" && pwd -P)
if [[ -f $directory/node-1.toml ]] && grep -Eq "^role = ['\"]combined['\"]$" "$directory/node-1.toml"; then
  if [[ ! -s $directory/console/index.html || ! -s $directory/console/.vite/manifest.json ]]; then
    if [[ -e $directory/console ]]; then
      printf 'Console assets are incomplete; preserve or remove %s/console and rebuild with scripts/build-console.sh\n' "$directory" >&2
      exit 1
    fi
    bash "$script_dir/../../scripts/build-console.sh" "$directory/console"
  fi
fi
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
