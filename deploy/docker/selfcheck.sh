#!/usr/bin/env bash
set -euo pipefail
umask 077

script_dir=$(cd "$(dirname "$0")" && pwd -P)
task_dir=$(mktemp -d /tmp/tidemq-docker-selfcheck.XXXXXX)
trap 'rm -rf "$task_dir"' EXIT
export TIDEMQ_IMAGE=${TIDEMQ_IMAGE:-tidemq-validation:static-selfcheck}
export RUSTFS_BINARY=${RUSTFS_BINARY:-/usr/bin/true}

fail() { printf 'Self-check failed: %s\n' "$1" >&2; exit 1; }
compose() {
  if [[ -n ${COMPOSE_BIN:-} ]]; then "$COMPOSE_BIN" "$@"; else docker compose "$@"; fi
}
count_files() { find "$1" -maxdepth 1 -type f -name "$2" | awk 'END {print NR+0}'; }

bash -n "$script_dir/prepare.sh" "$script_dir/init-bucket.sh" "$script_dir/selfcheck.sh"
for mode in rf1 rf3; do
  expected=1
  rustfs_memory=512m
  if [[ $mode == rf3 ]]; then expected=10; rustfs_memory=1g; fi
  state="$task_dir/$mode"
  bash "$script_dir/prepare.sh" "$mode" "$state" "$rustfs_memory" > /dev/null
  state=$(cd "$state" && pwd -P)
  if bash "$script_dir/prepare.sh" "$mode" "$state" > /dev/null 2>&1; then
    fail 'Existing state was accepted'
  fi
  [[ -z $(find "$state" -type d ! -perm 0700 -print -quit) ]] || fail 'Directory permissions differ from 0700'
  [[ -z $(find "$state" -type f ! -perm 0600 -print -quit) ]] || fail 'File permissions differ from 0600'
  [[ $(count_files "$state/client" '*') == 2 ]] || fail 'SDK directory contains additional files'
  cmp "$state/init/ca.pem" "$state/client/ca.pem"
  cmp "$state/init/access.token" "$state/client/access.token"
  [[ $(count_files "$state/rustfs" '*') == 2 ]] || fail 'RustFS directory contains additional files'

  found=0
  for config in "$state"/run/node-*/node.toml; do
    run=$(dirname "$config")
    node=${run##*/node-}
    found=$((found + 1))
    [[ $(count_files "$run" '*.key') == 2 ]] || fail 'Unexpected node private key'
    [[ $(count_files "$run" '*.pem') == 2 ]] || fail 'Unexpected node certificate'
    [[ $(count_files "$run" '*.der') == "$expected" ]] || fail 'Peer certificate set is incomplete'
    cmp "$state/init/node-$node.key" "$run/node-$node.key"
    cmp "$state/init/delivery.key" "$run/delivery.key"
    for peer in "$state"/init/node-*.der; do cmp "$peer" "$run/$(basename "$peer")"; done
    [[ ! -e $run/ca.key && ! -e $run/access.token ]] || fail 'Initialization or SDK secret entered a node directory'
    if grep -Fq "$state/init/" "$config"; then fail 'Host initialization path entered runtime configuration'; fi
    grep -Fq "data_dir = '/var/lib/tidemq'" "$config" || fail 'Runtime data path differs'
    grep -Fq "private_key = '/run/tidemq/node-$node.key'" "$config" || fail 'Node key path differs'
    role=$(awk -F "'" '$1 == "role = " {print $2; exit}' "$config")
    case "$role" in
      combined|storage)
        [[ -f $run/objects.toml ]] || fail 'Storage credentials are missing'
        grep -Fq "credentials_file = '/run/tidemq/objects.toml'" "$config" || fail 'Storage credential path differs'
        ;;
      *) [[ ! -e $run/objects.toml ]] || fail 'Storage credentials entered a non-storage role' ;;
    esac
    if [[ -n ${TIDEMQ_BIN:-} ]]; then "$TIDEMQ_BIN" validate-config --config "$config" > /dev/null; fi
  done
  [[ $found == "$expected" ]] || fail 'Node count differs'
  project=$(awk -F= '$1 == "COMPOSE_PROJECT_NAME" {print $2}' "$state/project.env")
  [[ $project =~ ^tidemq-validation-(rf1|rf3)-[0-9a-f]{16}$ ]] || fail 'Project name differs'
  args=(--project-name "$project" --env-file "$state/project.env" -f "$state/compose.yaml" --profile tools)
  compose "${args[@]}" config --quiet
  compose "${args[@]}" config | awk '
    /^services:/ {services = 1; next}
    services && /^[^[:space:]]/ {exit}
    services {print}
  ' > "$task_dir/$mode.rendered.yaml"
  rendered="$task_dir/$mode.rendered.yaml"
  service_count=$((expected + 4))
  [[ $(compose "${args[@]}" config --services | awk 'END {print NR+0}') == "$service_count" ]] || fail 'Service count differs'
  [[ $(compose "${args[@]}" config --volumes | awk 'END {print NR+0}') == $((expected + 2)) ]] || fail 'Data volume count differs'
  [[ $(grep -c 'network_mode: service:network' "$rendered") == $((service_count - 1)) ]] || fail 'Namespace sharing differs'
  [[ $(grep -Ec 'user: "?10001:10001"?$' "$rendered") == "$service_count" ]] || fail 'Runtime UID differs'
  [[ $(grep -c 'cgroup: private' "$rendered") == "$service_count" ]] || fail 'Cgroup isolation differs'
  [[ $(grep -c 'pids_limit: 128' "$rendered") == "$service_count" ]] || fail 'PID budget differs'
  [[ $(grep -c '/tmp:rw,noexec,nosuid,size=16777216,mode=1777' "$rendered") == "$service_count" ]] || fail 'Tmpfs budget differs'
  [[ $(grep -c '^    read_only: true$' "$rendered") == "$service_count" ]] || fail 'Root filesystem is writable'
  [[ $(grep -c 'stop_grace_period: 6s' "$rendered") == "$service_count" ]] || fail 'Stop budget differs'
  [[ $(grep -c 'no-new-privileges:true' "$rendered") == "$service_count" ]] || fail 'Privilege escalation is enabled'
  [[ $(grep -c '^      - ALL$' "$rendered") == "$service_count" ]] || fail 'Capabilities were retained'
  [[ $(grep -c 'driver: local' "$rendered") == "$service_count" ]] || fail 'Logging driver differs'
  [[ $(grep -c 'max-size: 10m' "$rendered") == "$service_count" ]] || fail 'Log byte budget differs'
  [[ $(grep -c 'max-file: "3"' "$rendered") == "$service_count" ]] || fail 'Log retention differs'
  [[ $(grep -c 'mem_limit: "402653184"' "$rendered") == "$expected" ]] || fail 'Per-node memory budget differs'
  if grep -Eq '^[[:space:]]+(ports|privileged|pid|ipc|devices|extra_hosts):|network_mode: host|docker.sock' "$rendered"; then
    fail 'Unexpected host exposure or namespace sharing'
  fi
  memory=$(awk '$1 == "mem_limit:" {gsub(/"/, "", $2); total += $2} END {printf "%.0f", total}' "$rendered")
  swap=$(awk '$1 == "memswap_limit:" {gsub(/"/, "", $2); total += $2} END {printf "%.0f", total}' "$rendered")
  [[ $memory -le 5368709120 && $memory == "$swap" ]] || fail 'Memory or swap budget differs'
  printf 'Static %s self-check passed: %s nodes, %s services, %s bytes maximum memory\n' "$mode" "$expected" "$service_count" "$memory"
done
for invalid in 'rf2 512m' 'rf1 2g'; do
  read -r mode memory <<< "$invalid"
  if bash "$script_dir/prepare.sh" "$mode" "$task_dir/rejected" "$memory" > /dev/null 2>&1; then
    fail 'Unsupported mode or unbounded memory was accepted'
  fi
done
printf 'No containers were created; image execution, volume ownership and readiness need Docker validation.\n'
