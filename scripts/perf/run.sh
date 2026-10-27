#!/usr/bin/env bash
set -euo pipefail
umask 077

usage() { printf 'Usage: %s service-binary loadgen-binary new-output-directory [traffic|idle|profile]\n' "$0" >&2; exit 2; }
[[ $# -ge 3 && $# -le 4 ]] || usage
[[ $(uname -s) == Linux ]] || { printf 'This harness requires Linux\n' >&2; exit 2; }
service=$(realpath -- "$1")
loadgen=$(realpath -- "$2")
[[ -x $service && -x $loadgen && ! -e $3 ]] || usage
mode=${4:-traffic}
case "$mode" in traffic|idle|profile) ;; *) usage ;; esac
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
messages=${TIDEMQ_PERF_MESSAGES:-2048}
size=${TIDEMQ_PERF_MESSAGE_BYTES:-1024}
concurrency=${TIDEMQ_PERF_CONCURRENCY:-4}
idle_seconds=${TIDEMQ_PERF_IDLE_SECONDS:-10}
port=${TIDEMQ_PERF_PORT:-17410}
service_cpus=${TIDEMQ_PERF_SERVICE_CPUS:-2-5}
client_cpus=${TIDEMQ_PERF_CLIENT_CPUS:-6-7}
for number in "$messages" "$size" "$concurrency" "$idle_seconds" "$port"; do
  [[ $number =~ ^[1-9][0-9]{0,6}$ ]] || usage
done
(( messages <= 200000 && size >= 32 && size <= 65536 && concurrency <= 32 && concurrency <= messages && messages * size <= 536870912 && idle_seconds <= 60 && port >= 1024 && port <= 55500 )) || usage
[[ $service_cpus =~ ^[0-9,-]+$ && $client_cpus =~ ^[0-9,-]+$ ]] || usage
for command in flock curl jq taskset perf perl openssl sha256sum timeout; do command -v "$command" >/dev/null; done
# All shared-host service execution and cleanup stay within the same lab lease.
exec 9>/var/lock/tidemq-lab.lock
flock -n 9 || { printf 'Lab lease is busy; no service was started\n' >&2; exit 75; }
mkdir -- "$3"
output=$(cd -- "$3" && pwd -P)
fixture=$output/.private
service_pid= client_pid= monitor_pid= profile_pid=
cleanup() {
  local status=$? pid
  trap - EXIT INT TERM
  for pid in "$client_pid" "$profile_pid" "$monitor_pid"; do
    if [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null; then kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  done
  if [[ -n $service_pid ]] && kill -0 "$service_pid" 2>/dev/null; then
    kill -TERM "$service_pid" 2>/dev/null || true
    for ((attempt=0; attempt<50; attempt++)); do kill -0 "$service_pid" 2>/dev/null || break; sleep 0.1; done
    if kill -0 "$service_pid" 2>/dev/null; then kill -KILL "$service_pid" 2>/dev/null || true; fi
    wait "$service_pid" 2>/dev/null || true
  fi
  if [[ -f $fixture/access.token ]]; then
    perl -i -pe 'BEGIN { my $path = shift @ARGV; open my $file, "<", $path or die "Cannot open redaction material"; chomp(our $secret = <$file>); } s/\Q$secret\E/[REDACTED]/g' \
      "$fixture/access.token" "$output"/*.log
  fi
  # Only the generator-created private fixture is removed; evidence is retained on failure.
  if [[ -d $fixture ]]; then rm -rf -- "$fixture"; fi
  jq -n --argjson exit_status "$status" --arg service_pid "$service_pid" \
    '{exit_status:$exit_status,owned_service_pid:$service_pid,private_fixture_removed:true,lease_held_through_cleanup:true}' > "$output/cleanup.json"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

ulimit -f 524288
bash "$repo/deploy/local/generate.sh" rf1 "$fixture" > "$output/generate.log" 2>&1
management=$((port + 1000))
internal=$((port + 1))
sed -i -e "s/127.0.0.1:7410/127.0.0.1:$port/g" -e "s/127.0.0.1:7411/127.0.0.1:$internal/g" \
  -e "s/127.0.0.1:8410/127.0.0.1:$management/g" "$fixture/node-1.toml"
cp -- "$fixture/node-1.toml" "$output/service-config.toml"
sha256sum "$service" "$loadgen" "$fixture/node-1.toml" > "$output/binaries-config.sha256"
for binary in "$service" "$loadgen"; do
  [[ -f $binary.build.json ]] || { printf 'Missing build metadata: %s.build.json\n' "$binary" >&2; exit 2; }
  [[ $(jq -er .sha256 "$binary.build.json") == "$(sha256sum "$binary" | cut -d ' ' -f 1)" ]] || { printf 'Binary hash differs from its build metadata\n' >&2; exit 1; }
  cp -- "$binary.build.json" "$output/$(basename -- "$binary").build.json"
done
cp -- "$repo/scripts/perf/run.sh" "$output/harness.sh"
sha256sum "$repo/scripts/perf/run.sh" "$repo/deploy/local/generate.sh" > "$output/harness.sha256"
cp -- "$(dirname -- "$loadgen")/tooling-sha256.txt" "$output/loadgen-tooling.sha256"
uname -a > "$output/kernel.txt"
lscpu > "$output/cpu.txt"
lscpu -e=CPU,CORE,SOCKET,ONLINE > "$output/cpu-topology.txt"
cat /proc/loadavg > "$output/host-load.before.txt"
perf --version > "$output/perf-version.txt"
jq -n --arg mode "$mode" --arg service_cpus "$service_cpus" --arg client_cpus "$client_cpus" \
  --argjson messages "$messages" --argjson payload "$size" --argjson concurrency "$concurrency" --argjson port "$port" \
  '{mode:$mode,profiled:($mode=="profile"),replication_factor:1,service_cpus:$service_cpus,client_cpus:$client_cpus,messages:$messages,payload_bytes:$payload,concurrency:$concurrency,gateway_port:$port,fixture:"deploy/local/generate.sh rf1",profile_event:"cpu-clock",profile_call_graph:"fp",profile_hz:499,profile_max_seconds:15,measurement_timeout_seconds:120}' > "$output/metadata.json"
taskset -c "$service_cpus" "$service" serve --config "$fixture/node-1.toml" > "$output/service.log" 2>&1 &
service_pid=$!
for ((attempt=0; attempt<200; attempt++)); do
  kill -0 "$service_pid" 2>/dev/null || { printf 'Owned service exited during startup\n' >&2; exit 1; }
  if curl --fail --silent --max-time 1 "http://127.0.0.1:$management/readyz" > "$output/ready.json"; then break; fi
  sleep 0.1
done
curl --fail --silent --max-time 2 "http://127.0.0.1:$management/readyz" > "$output/ready.json"
args=(--endpoint "127.0.0.1:$port" --ca "$fixture/ca.pem" --token-file "$fixture/access.token")
taskset -c "$client_cpus" "$loadgen" "${args[@]}" --output-dir "$output/warmup" --messages 32 --concurrency 4 > "$output/warmup.log" 2>&1
extra=()
[[ $mode != profile ]] || extra+=(--profiled)
case_name=traffic
[[ $mode != idle ]] || case_name=idle
taskset -c "$client_cpus" "$loadgen" "${args[@]}" --output-dir "$output/measurement" \
  --case "$case_name" --messages "$messages" --message-bytes "$size" --concurrency "$concurrency" \
  --idle-seconds "$idle_seconds" --wait-for-start "${extra[@]}" > "$output/loadgen.log" 2>&1 &
client_pid=$!
wait_marker() {
  local name=$1
  for ((attempt=0; attempt<1250; attempt++)); do
    [[ ! -f $output/measurement/$name ]] || return 0
    kill -0 "$client_pid" 2>/dev/null || return 1
    sleep 0.1
  done
  printf 'Load generator marker timed out: %s\n' "$name" >&2
  return 1
}
snapshot() {
  local phase=$1
  date -u +%Y-%m-%dT%H:%M:%S.%NZ > "$output/time.$phase.txt"
  cat "/proc/$service_pid/stat" > "$output/service-stat.$phase.txt"
  cat "/proc/$service_pid/status" > "$output/service-status.$phase.txt"
  curl --fail --silent --max-time 5 "http://127.0.0.1:$management/metrics" > "$output/metrics.$phase.prom"
  curl --fail --silent --max-time 5 "http://127.0.0.1:$management/diagnostics" > "$output/diagnostics.$phase.json"
}
wait_marker ready.json
if [[ $mode == profile ]]; then
  pin=41fee1f99f9276008b7cd112fca19dc3ea84ac32
  git init -q "$output/FlameGraph"
  timeout 45 git -C "$output/FlameGraph" fetch -q --depth=1 https://github.com/brendangregg/FlameGraph.git "$pin"
  git -C "$output/FlameGraph" checkout -q --detach FETCH_HEAD
  git -C "$output/FlameGraph" rev-parse HEAD > "$output/flamegraph-commit.txt"
  [[ $(cat "$output/flamegraph-commit.txt") == "$pin" ]]
  perf record -e cpu-clock -F 499 --call-graph fp -p "$service_pid" \
    -o "$output/perf.data" -- sleep 15 > "$output/perf-record.log" 2>&1 &
  profile_pid=$!
  sleep 0.3
  kill -0 "$profile_pid"
fi
snapshot before
(
  while kill -0 "$service_pid" 2>/dev/null; do
    ps -p "$service_pid" -o pid=,pcpu=,rss=,nlwp=,etimes=
    sleep 0.2
  done
) > "$output/service-samples.txt" &
monitor_pid=$!
touch "$output/measurement/start"
measurement_status=0
wait_marker finished.json || measurement_status=1
snapshot after
cat /proc/loadavg > "$output/host-load.after.txt"
if [[ -n $profile_pid ]]; then
  profile_stop_requested=false
  if kill -0 "$profile_pid" 2>/dev/null; then
    kill -INT "$profile_pid" 2>/dev/null || true
    profile_stop_requested=true
  fi
  profile_status=0
  wait "$profile_pid" || profile_status=$?
  profile_pid=
  printf '{"exit_status":%d,"stop_signal":"SIGINT","stop_requested":%s,"maximum_seconds":15}\n' "$profile_status" "$profile_stop_requested" > "$output/profile-status.json"
  # perf forwards termination to its sleep child and can return the child's signal status.
  if [[ $profile_status != 0 && ( $profile_stop_requested != true || ( $profile_status != 130 && $profile_status != 143 ) ) ]]; then measurement_status=1; fi
fi
touch "$output/measurement/collected"
wait "$client_pid" || measurement_status=1
client_pid=
kill -TERM "$monitor_pid" 2>/dev/null || true
wait "$monitor_pid" 2>/dev/null || true
monitor_pid=
ticks=$(getconf CLK_TCK)
awk -v ticks="$ticks" 'NR==1 {user_ticks=$14;system_ticks=$15;next} {printf "{\"user_seconds\":%.6f,\"system_seconds\":%.6f,\"clock_ticks_per_second\":%d}\n",($14-user_ticks)/ticks,($15-system_ticks)/ticks,ticks}' \
  "$output/service-stat.before.txt" "$output/service-stat.after.txt" > "$output/service-cpu.json"
awk '!/^#/ && NF >= 2 {if (NR == FNR) before[$1]=$2; else printf "%s\t%.9g\n",$1,$2-before[$1]}' \
  "$output/metrics.before.prom" "$output/metrics.after.prom" > "$output/metrics.delta.tsv"
if [[ $mode == profile ]]; then
  perf script -i "$output/perf.data" > "$output/perf.script" 2> "$output/perf-script.log"
  awk '/^\t/ {frames=1} END {exit !frames}' "$output/perf.script" || { printf 'No service stack frames were resolved\n' >&2; exit 1; }
  perf report --stdio --no-children -i "$output/perf.data" > "$output/perf-report.txt" 2> "$output/perf-report.log"
  perl "$output/FlameGraph/stackcollapse-perf.pl" "$output/perf.script" > "$output/perf.folded"
  [[ -s $output/perf.folded ]] || { printf 'No service stack samples were collected\n' >&2; exit 1; }
  validation=$(jq -er .status "$output/measurement/report.json")
  perl "$output/FlameGraph/flamegraph.pl" --title "TideMQ service CPU (profiled; SDK validation $validation)" "$output/perf.folded" > "$output/service-flamegraph.svg"
fi
(( measurement_status == 0 ))
jq -e '.status == "passed" and .close_complete == true' "$output/measurement/report.json" >/dev/null
printf 'Completed %s; evidence: %s\n' "$mode" "$output"
