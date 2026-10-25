#!/usr/bin/env bash
set -euo pipefail
umask 077
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
export TIDEMQ_LAB_HOME=${TIDEMQ_LAB_HOME:?Set the private runner lab directory}
export TIDEMQ_LAB_STATE="$TIDEMQ_LAB_HOME"
export TIDEMQ_LAB_KEY="$TIDEMQ_LAB_HOME/ci-guest"
export TIDEMQ_LAB_KNOWN_HOSTS="$TIDEMQ_LAB_HOME/known_hosts"
export KUBECONFIG="$TIDEMQ_LAB_HOME/kubeconfig"
source scripts/lab/guest.sh
run="ci-${GITHUB_RUN_ID:-$(date +%s)}-${GITHUB_RUN_ATTEMPT:-1}"
[[ "$run" =~ ^ci-[0-9]+-[0-9]+$ ]] || exit 2
private="$TIDEMQ_LAB_HOME/runs/$run"
report="$PWD/target/ci/lab-report/$run"
load_root="/srv/tidemq/validation/$run"
validator_pid=
[[ ! -e "$private" ]] || { printf 'Use a new CI run identity\n' >&2; exit 1; }
mkdir -m 0700 -p "$private" "$report"
host_control() { ssh -n -F "$TIDEMQ_LAB_HOME/ssh_config" lab-host "$*"; }
kube() { kubectl --request-timeout=15s --namespace "$run" "$@"; }
wait_runtime() {
  local end=$((SECONDS + 90)) id
  for id in 11 12 13 21 22 23 31 32 41 42; do
    until kube exec "node-$id-0" -c tidemq -- curl --fail --silent --max-time 1 http://127.0.0.1:9419/readyz >> "$report/readiness.log" 2>&1; do
      (( SECONDS < end )) || { printf 'Runtime readiness did not recover within 90 seconds\n' >&2; return 1; }
      sleep 1
    done
  done
}
cleanup() {
  local result=$? cleanup_result=0
  trap - EXIT
  for file in build.log source-commit.txt source-patch-sha256.txt toolchain.txt sha256.txt image-id.txt; do
    [[ ! -f "$private/build/$file" ]] || cp "$private/build/$file" "$report/$file"
  done
  if [[ -n "$validator_pid" ]]; then kill "$validator_pid" 2>/dev/null || true; wait "$validator_pid" 2>/dev/null || true; fi
  lab_ssh tidemq-loadgen pkill -TERM -f "^$load_root/deploy_validation( |$)" >/dev/null 2>&1 || true
  for vm in tidemq-cp-{a,b,c} tidemq-worker-{a,b,c} tidemq-fixtures tidemq-loadgen; do host_control restore "$vm" 5 >> "$report/cleanup.log" 2>&1 || cleanup_result=1; done
  kube get pods -o json > "$report/pods-after.json" 2>/dev/null || true
  kube get events -o json > "$report/events.json" 2>/dev/null || true
  for id in 11 12 13 21 22 23 31 32 41 42; do kube logs "node-$id-0" -c tidemq --tail=200 > "$report/node-$id.log" 2>&1 || true; done
  if kubectl delete namespace "$run" --ignore-not-found --wait=true --timeout=2m >> "$report/cleanup.log" 2>&1; then
    for id in 11 12 13 21 22 23; do kubectl delete pv "$run-node-$id" --ignore-not-found >> "$report/cleanup.log" 2>&1 || cleanup_result=1; done
    for suffix in a b c; do
      lab_ssh "tidemq-worker-$suffix" sudo rm -rf -- "/var/lib/tidemq/storage/$run" "/var/lib/tidemq/metadata/$run" >> "$report/cleanup.log" 2>&1 || cleanup_result=1
    done
  else cleanup_result=1; fi
  lab_ssh tidemq-fixtures sudo /usr/local/sbin/tidemq-fixtures remove "$run" >> "$report/cleanup.log" 2>&1 || cleanup_result=1
  scp -q -r -i "$TIDEMQ_LAB_KEY" -o "UserKnownHostsFile=$TIDEMQ_LAB_KNOWN_HOSTS" "lab@192.168.240.35:$load_root/results/." "$report/" >> "$report/cleanup.log" 2>&1 || true
  lab_ssh tidemq-loadgen sudo rm -rf -- "$load_root" >> "$report/cleanup.log" 2>&1 || cleanup_result=1
  rm -rf -- "$private"
  (( cleanup_result == 0 )) || result=1
  printf '{"exitCode":%s,"cleanupExitCode":%s}\n' "$result" "$cleanup_result" > "$report/result.json"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
host_control status > "$report/vms-before.txt"
kubectl get nodes -o json > "$report/nodes-before.json"
bash scripts/lab/build-image.sh "$private/build"
cp "$private/build/source-commit.txt" "$private/build/source-patch-sha256.txt" "$private/build/toolchain.txt" "$private/build/sha256.txt" "$private/build/image-id.txt" "$report/"
lab_ssh tidemq-fixtures sudo /usr/local/sbin/tidemq-fixtures create "$run" > "$report/fixtures-create.log" 2>&1
lab_fetch tidemq-fixtures "/srv/tidemq/fixtures/$run/a/credentials.toml" "$private/objects.toml"
bash scripts/lab/deploy-workload.sh "$run" "$private/build" https://192.168.241.34:36443 "$private/objects.toml" "$private/workload" > "$report/deploy.log" 2>&1
validator="$private/build/bin/deploy_validation"
material="$private/workload/material"
lab_ssh tidemq-loadgen sudo install -d -m 0700 -o lab -g lab "$load_root" "$load_root/results"
lab_copy tidemq-loadgen "$validator" "$load_root/deploy_validation"
lab_copy tidemq-loadgen "$private/build/bin/tidemq" "$load_root/tidemq"
lab_copy tidemq-loadgen "$material/ca.pem" "$load_root/ca.pem"
lab_copy tidemq-loadgen "$material/access.token" "$load_root/access.token"
connection=(--endpoint 192.168.241.201:7443 --ca "$load_root/ca.pem" --token-file "$load_root/access.token")
common=("${connection[@]}" --messages 64)
tenant=$(lab_ssh tidemq-loadgen "$load_root/tidemq" admin "${connection[@]}" create-tenant validation | jq -er '.result.id')
lab_ssh tidemq-loadgen "$load_root/tidemq" admin "${connection[@]}" create-namespace "$tenant" default > "$report/namespace-created.json"
smoke() {
  local name=$1
  shift
  lab_ssh tidemq-loadgen "$load_root/deploy_validation" "${common[@]}" --output-dir "$load_root/results/$name" "$@" > "$report/$name.log" 2>&1
  mkdir -p "$report/$name"
  lab_fetch tidemq-loadgen "$load_root/results/$name/report.json" "$report/$name/report.json"
  jq -e '.status == "passed" and .client_close_completed == true' "$report/$name/report.json" >/dev/null
}
wait_marker() {
  local file=$1 process=$2
  for ((attempt=0; attempt<120; attempt++)); do
    if lab_ssh tidemq-loadgen test -f "$file"; then return; fi
    kill -0 "$process" 2>/dev/null || { printf 'The SDK validator exited before its fault marker\n' >&2; return 1; }
    sleep 1
  done
  printf 'The SDK validator fault marker timed out\n' >&2
  return 1
}
smoke smoke
smoke replay --replay-settle-ms 5000
staged_fault() {
  local name=$1 action=$2 unavailable=$3 vm
  shift 3
  local flags=(--stage-fault)
  [[ "$unavailable" != 1 ]] || flags+=(--require-unavailable)
  lab_ssh tidemq-loadgen "$load_root/deploy_validation" "${common[@]}" --output-dir "$load_root/results/$name" "${flags[@]}" > "$report/$name.log" 2>&1 &
  validator_pid=$!
  wait_marker "$load_root/results/$name/waiting-fault.json" "$validator_pid"
  for vm in "$@"; do host_control "$action" "$vm" 120 >> "$report/$name-inject.txt"; done
  lab_ssh tidemq-loadgen touch "$load_root/results/$name/fault-injected"
  wait_marker "$load_root/results/$name/fault-observed.json" "$validator_pid"
  for vm in "$@"; do host_control restore "$vm" 5 >> "$report/$name-restore.txt"; done
  kubectl wait node "$@" --for=condition=Ready --timeout=2m
  lab_ssh tidemq-loadgen touch "$load_root/results/$name/recovered"
  wait "$validator_pid"
  validator_pid=
  mkdir -p "$report/$name"
  lab_fetch tidemq-loadgen "$load_root/results/$name/report.json" "$report/$name/report.json"
  jq -e '.status == "passed" and .original_consumer_reused == true and .client_close_completed == true' "$report/$name/report.json" >/dev/null
  wait_runtime
}
staged_fault worker-poweroff off 0 tidemq-worker-c
staged_fault data-isolation net-cut 0 tidemq-worker-b
staged_fault quorum-loss off 1 tidemq-worker-b tidemq-worker-c
host_control off tidemq-cp-a 30 > "$report/control-plane-inject.txt"
kubectl get nodes -o json > "$report/nodes-control-plane-fault.json"
smoke control-plane-fault
host_control restore tidemq-cp-a 5 > "$report/control-plane-restore.txt"
kubectl wait node tidemq-cp-a --for=condition=Ready --timeout=2m
lab_ssh tidemq-fixtures sudo /usr/local/sbin/tidemq-fixtures fault "$run" a stop 20
curl --fail --silent --cacert "$TIDEMQ_LAB_HOME/ca.pem" https://192.168.241.34:36444/health/ready > "$report/fixture-b-during-a-fault.json"
smoke object-store-fault
curl --fail --silent --retry 30 --retry-all-errors --retry-delay 1 --cacert "$TIDEMQ_LAB_HOME/ca.pem" https://192.168.241.34:36443/health/ready > "$report/fixture-a-recovered.json"
host_control pause tidemq-loadgen 10 > "$report/recovery-timer-inject.txt"
host_control status > "$report/recovery-timer-during.txt"
sleep 12
host_control status > "$report/recovery-timer-after.txt"
grep -Fx 'tidemq-loadgen: paused' "$report/recovery-timer-during.txt" >/dev/null
grep -Fx 'tidemq-loadgen: running' "$report/recovery-timer-after.txt" >/dev/null
smoke recovered
bash scripts/ci/observability.sh "$run" "$report/observability"
printf 'Completed RF3 SDK, replay, power loss, network isolation, quorum recovery, control-plane failover, object-store isolation and independent timer recovery.\n'
