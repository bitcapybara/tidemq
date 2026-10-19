#!/usr/bin/env bash
set -euo pipefail
umask 077

build=${1:?Usage: perf-run.sh allocator-build-directory new-trial-directory [small|large|slow|replay] [minimal|recommended|detailed|recommended-otlp] [2|4]}
output=${2:?A new private trial directory is required}
scenario=${3:-small}
observability=${4:-recommended}
request_lanes=${5:-4}
case "$request_lanes" in
  2|4) ;;
  *) printf 'Request lanes must be either 2 or 4\n' >&2; exit 2 ;;
esac
root=$(cd "$(dirname "$0")/.." && pwd -P)
collector_bin=''
if [[ "$observability" == recommended-otlp ]]; then
  [[ "$scenario" == small && "$request_lanes" == 4 ]] || { printf 'OTLP comparison requires small workload and four Request lanes\n' >&2; exit 2; }
  : "${TIDEMQ_PERF_OTELCOL_BIN:?OTLP comparison requires the verified official core 0.162.0 binary}"
  [[ "$TIDEMQ_PERF_OTELCOL_BIN" == /* && -x "$TIDEMQ_PERF_OTELCOL_BIN" ]] || { printf 'Collector binary must be an executable absolute path\n' >&2; exit 2; }
  collector_bin=$TIDEMQ_PERF_OTELCOL_BIN
  [[ -f "$root/deploy/observability/collector.yaml" ]] || { printf 'The approved Collector configuration is missing\n' >&2; exit 1; }
fi
build=$(cd "$build" && pwd -P)
if [[ -e "$output" ]]; then printf 'Trial output already exists\n' >&2; exit 1; fi
mkdir -p "$output"
output=$(cd "$output" && pwd -P)
service_pid=''; rustfs_pid=''; collector_pid=''; collector_capture_errors=0
stop_one() {
  local pid=$1
  local name=${2:-} signal=${3:-TERM} exit_status=0
  [[ -n "$pid" ]] || return 0
  kill -"$signal" "$pid" 2>/dev/null || true
  for _ in {1..100}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      wait "$pid" || exit_status=$?
      if [[ -n "$name" ]]; then
        printf '{"exit_status":%s,"forced":false,"stop_signal":"%s"}\n' "$exit_status" "$signal" > "$output/$name-exit.json"
        [[ "$exit_status" == 0 ]] || return 1
      fi
      return 0
    fi
    sleep 0.1
  done
  printf 'Owned process required forced termination after its drain deadline\n' >&2
  kill -KILL "$pid" 2>/dev/null || true
  wait "$pid" || exit_status=$?
  if [[ -n "$name" ]]; then
    printf '{"exit_status":%s,"forced":true,"stop_signal":"%s"}\n' "$exit_status" "$signal" > "$output/$name-exit.json"
  fi
  return 1
}
stop() {
  trap - EXIT INT TERM
  if [[ -n "$collector_bin" ]]; then stop_one "$service_pid" service || true
  else stop_one "$service_pid" || true; fi
  stop_one "$rustfs_pid" || true
  stop_one "$collector_pid" collector INT || true
}
trap stop EXIT INT TERM
sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d ' ' -f 1
  else shasum -a 256 "$1" | cut -d ' ' -f 1; fi
}
collector_counter() {
  # The finite pilot cannot reach this cap; reject malformed or non-finite JSON values.
  awk -v metric="$2" '
    $0 ~ ("^" metric "(_total)?(\\{| )") {
      value = $NF
      if (value !~ /^[0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?$/ || value + 0 > 1000000000 || value + 0 != int(value + 0)) { invalid = 1; next }
      total += value; found = 1
      if (total > 1000000000) invalid = 1
    }
    END { if (found && !invalid) printf "%.0f\n", total; else print "null" }
  ' "$1"
}
collector_port_available() {
  local port=$1 probe='' probe_status=0
  # Only a direct explicit refusal proves that this loopback endpoint is unused.
  if probe=$(LC_ALL=C curl --disable --proxy '' --noproxy '*' --retry 0 \
    --connect-timeout 1 --max-time 1 --output /dev/null --verbose \
    "http://127.0.0.1:$port/" 2>&1 | head -c 8193); then
    probe_status=0
  else
    probe_status=$?
  fi
  printf '%s' "$probe" > "$output/collector/preflight-$port.txt"
  if [[ "$probe_status" == 7 && $(wc -c < "$output/collector/preflight-$port.txt") -le 8192 ]] \
    && awk -v port="$port" '
      index($0, "connect to 127.0.0.1 port " port " ") && index($0, "failed: Connection refused") { refused = 1 }
      END { exit !refused }
    ' "$output/collector/preflight-$port.txt"; then
    return 0
  fi
  printf 'Collector port %s is occupied or availability is unknown\n' "$port" >&2
  return 1
}
collector_snapshot() {
  local label=$1 path="$output/collector/metrics-$1.txt"
  : > "$path"
  kill -0 "$collector_pid" 2>/dev/null || { printf 'Owned Collector is not running for %s capture\n' "$label" >&2; return 1; }
  if ! curl --fail --silent --show-error --max-time 2 --max-filesize 262144 \
    http://127.0.0.1:18888/metrics | head -c 262145 > "$path"; then
    printf 'Collector %s metrics capture failed\n' "$label" >&2; return 1
  fi
  [[ $(wc -c < "$path") -le 262144 ]] || { printf 'Collector metrics exceeded 256 KiB\n' >&2; return 1; }
  kill -0 "$collector_pid" 2>/dev/null || { printf 'Owned Collector exited during %s capture\n' "$label" >&2; return 1; }
  {
    printf 'source=official_collector_process_self_metrics\nowned_pid=%s\n' "$collector_pid"
    awk '/^otelcol_process_/ { print; found = 1 } END { if (!found) print "process_metrics=unavailable" }' "$path"
  } > "$output/collector/resources-$label.txt"
}
bash "$root/deploy/local/generate.sh" rf1 "$output/cluster"
config="$output/cluster/node-1.toml"
extra=(); load=()
case "$observability" in
  minimal) printf '\n[logging]\nfilter = "warn"\n' >> "$config" ;;
  recommended) ;;
  recommended-otlp)
    cat >> "$config" <<'EOF'

[tracing]
enabled = true
endpoint = 'http://127.0.0.1:4318/v1/traces'
sample_per_mille = 10
spans_per_second = 128
queue_spans = 256
batch_spans = 32
export_interval_ms = 500
export_timeout_ms = 500
EOF
    ;;
  detailed) extra+=(--trace-seconds 300) ;;
  *) printf 'Unknown observability mode\n' >&2; exit 2 ;;
esac
if [[ -n "$collector_bin" ]]; then
  mkdir "$output/collector"
  collector_port_available 4318
  collector_port_available 18888
  cp "$root/deploy/observability/collector.yaml" "$output/collector/config.yaml"
  version=$("$collector_bin" --version | head -c 4097)
  printf '%s\n' "$version" > "$output/collector/version.txt"
  [[ $(wc -c < "$output/collector/version.txt") -le 4096 && "$version" == *'0.162.0'* ]] || { printf 'Unexpected Collector version\n' >&2; exit 1; }
  binary_hash=$(sha256 "$collector_bin")
  config_hash=$(sha256 "$output/collector/config.yaml")
  [[ ${#binary_hash} == 64 && ${#config_hash} == 64 ]] || { printf 'Collector identity hashes are invalid\n' >&2; exit 1; }
  {
    printf 'binary=%s\nbinary_sha256=%s\nconfig_sha256=%s\n' "$collector_bin" "$binary_hash" "$config_hash"
    printf 'GOMAXPROCS=2\nGOMEMLIMIT=192MiB\nmetrics_body_limit_bytes=262144\nstdout_limit_bytes=33554432\n'
  } > "$output/collector/identity.txt"
  env -i PATH=/usr/bin:/bin "$collector_bin" validate --config "$output/collector/config.yaml" \
    > >(head -c 65536 > "$output/collector/config-validation.txt") 2>&1
  env -i PATH=/usr/bin:/bin GOMAXPROCS=2 GOMEMLIMIT=192MiB "$collector_bin" --config "$output/collector/config.yaml" \
    > >(head -c 33554432 > "$output/collector/server.log") 2>&1 &
  collector_pid=$!
  printf '%s\n' "$collector_pid" > "$output/collector/process.pid"
  ready=false
  for _ in {1..150}; do
    kill -0 "$collector_pid" 2>/dev/null || { printf 'Collector exited before readiness\n' >&2; exit 1; }
    if curl --fail --silent --max-time 1 --max-filesize 262144 http://127.0.0.1:18888/metrics >/dev/null; then ready=true; break; fi
    sleep 0.1
  done
  [[ "$ready" == true ]] || { printf 'Collector readiness deadline expired\n' >&2; exit 1; }
  kill -0 "$collector_pid" 2>/dev/null || { printf 'Owned Collector exited after readiness\n' >&2; exit 1; }
fi
case "$scenario" in
  small) load+=(--message-bytes 128 --batch-messages 32 --messages 4096 --warmup-messages 256) ;;
  large) load+=(--message-bytes 262136 --batch-messages 1 --messages 64 --warmup-messages 8) ;;
  slow) load+=(--message-bytes 128 --batch-messages 32 --messages 2048 --warmup-messages 128 --consumer-delay-ms 20) ;;
  replay) load+=(--mode replay --require-cold --replay-settle-ms 5000 --message-bytes 65536 --batch-messages 4 --messages 256 --warmup-messages 16) ;;
  *) printf 'Unknown load scenario\n' >&2; exit 2 ;;
esac
if [[ "$scenario" == replay ]]; then
  : "${TIDEMQ_PERF_RUSTFS_BIN:?Replay requires the verified RustFS 1.0.3 binary path}"
  version=$("$TIDEMQ_PERF_RUSTFS_BIN" --version)
  [[ "$version" == 'rustfs 1.0.3'* ]] || { printf 'Unexpected RustFS version\n' >&2; exit 1; }
  mkdir "$output/rustfs" "$output/rustfs/data" "$output/rustfs/logs"
  access=$(openssl rand -hex 16); secret=$(openssl rand -hex 32)
  printf '%s' "$access" > "$output/rustfs/access"
  printf '%s' "$secret" > "$output/rustfs/secret"
  printf 'access_key = "%s"\nsecret_key = "%s"\n' "$access" "$secret" > "$output/cluster/s3.toml"
  printf 'user = "%s:%s"\naws-sigv4 = "aws:amz:us-east-1:s3"\n' "$access" "$secret" > "$output/rustfs/curl.conf"
  env -i PATH=/usr/bin:/bin RUSTFS_ACCESS_KEY_FILE="$output/rustfs/access" \
    RUSTFS_SECRET_KEY_FILE="$output/rustfs/secret" RUSTFS_DURABILITY_MODE=strict \
    RUSTFS_NEW_BUCKET_DURABILITY_MODE=inherit RUSTFS_CONSOLE_ENABLE=false \
    RUSTFS_OBS_ENDPOINT='' RUSTFS_LOG_LEVEL=warn RUSTFS_LOG_DIRECTORY="$output/rustfs/logs" \
    "$TIDEMQ_PERF_RUSTFS_BIN" server --address 127.0.0.1:19000 --region us-east-1 \
    "$output/rustfs/data" > >(head -c 33554432 > "$output/rustfs/server.log") 2>&1 &
  rustfs_pid=$!
  ready=false
  for _ in {1..150}; do
    kill -0 "$rustfs_pid" 2>/dev/null || { printf 'RustFS exited before readiness\n' >&2; exit 1; }
    if curl --fail --silent --max-time 1 http://127.0.0.1:19000/health/ready >/dev/null; then ready=true; break; fi
    sleep 0.1
  done
  [[ "$ready" == true ]] || { printf 'RustFS readiness deadline expired\n' >&2; exit 1; }
  curl --config "$output/rustfs/curl.conf" --fail --silent --show-error --max-time 5 \
    -X PUT http://127.0.0.1:19000/tidemq-perf > "$output/rustfs/create-bucket.txt"
  cat >> "$config" <<EOF

[runtime.objects]
endpoint = 'http://127.0.0.1:19000'
region = 'us-east-1'
bucket = 'tidemq-perf'
prefix = 'pilot'
credentials_file = '$output/cluster/s3.toml'
allow_http_loopback = true
EOF
fi
"$build/tidemq" validate-config --config "$config" > "$output/config-validation.json"
env -i PATH=/usr/bin:/bin TOKIO_WORKER_THREADS=4 "$build/tidemq" serve --config "$config" ${extra[@]+"${extra[@]}"} > >(head -c 33554432 > "$output/service.log") 2>&1 &
service_pid=$!
printf '%s\n' "$service_pid" > "$output/service.pid"
ready=false
for _ in {1..150}; do
  kill -0 "$service_pid" 2>/dev/null || { printf 'TideMQ exited before readiness\n' >&2; exit 1; }
  if curl --fail --silent --max-time 1 http://127.0.0.1:8410/readyz >/dev/null; then ready=true; break; fi
  sleep 0.1
done
[[ "$ready" == true ]] || { printf 'TideMQ readiness deadline expired\n' >&2; exit 1; }
sleep 1
status=0
if [[ -n "$collector_pid" ]]; then
  if ! collector_snapshot before; then
    printf '{"status":"startup_capture_failed","capture_errors":1}\n' > "$output/collector/evidence.json"
    printf 'Collector validation failed before the SDK workload\n' >&2
    exit 1
  fi
fi
env -i PATH=/usr/bin:/bin "$build/perf_load" --endpoint 127.0.0.1:7410 --server-name node-1.local \
  --ca "$output/cluster/ca.pem" --token-file "$output/cluster/access.token" \
  --management 127.0.0.1:8410 --service-pid "$service_pid" --service-binary "$build/tidemq" \
  --service-config "$config" --build-identity "$build/identity.json" --output "$output/report.json" \
  --workers 4 --request-lanes "$request_lanes" --ack-messages 32 --phase-timeout-secs 120 --cooldown-secs 3 \
  --observability "$observability" "${load[@]}" || status=$?
if [[ -n "$collector_pid" ]]; then
  collector_snapshot after || { status=1; collector_capture_errors=$((collector_capture_errors + 1)); }
fi
curl --fail --silent --max-time 2 http://127.0.0.1:8410/metrics > "$output/metrics-final.txt" || status=1
curl --fail --silent --max-time 2 http://127.0.0.1:8410/diagnostics > "$output/diagnostics-final.json" || status=1
if [[ -n "$collector_pid" ]]; then
  stop_one "$service_pid" service || status=1
else
  stop_one "$service_pid" || status=1
fi
service_pid=''
stop_one "$rustfs_pid" || status=1; rustfs_pid=''
if [[ -n "$collector_pid" ]]; then
  collector_snapshot final || { status=1; collector_capture_errors=$((collector_capture_errors + 1)); }
  accepted_before=$(collector_counter "$output/collector/metrics-before.txt" otelcol_receiver_accepted_spans)
  accepted_after=$(collector_counter "$output/collector/metrics-after.txt" otelcol_receiver_accepted_spans)
  accepted_final=$(collector_counter "$output/collector/metrics-final.txt" otelcol_receiver_accepted_spans)
  refused_before=$(collector_counter "$output/collector/metrics-before.txt" otelcol_receiver_refused_spans)
  refused_after=$(collector_counter "$output/collector/metrics-after.txt" otelcol_receiver_refused_spans)
  refused_final=$(collector_counter "$output/collector/metrics-final.txt" otelcol_receiver_refused_spans)
  failed_before=$(collector_counter "$output/collector/metrics-before.txt" otelcol_receiver_failed_spans)
  failed_after=$(collector_counter "$output/collector/metrics-after.txt" otelcol_receiver_failed_spans)
  failed_final=$(collector_counter "$output/collector/metrics-final.txt" otelcol_receiver_failed_spans)
  evidence=received
  if [[ "$accepted_final" == null || "$accepted_final" == 0 ]]; then
    evidence=no_received_spans; status=1
    printf 'OTLP comparison has no verified Collector span reception; export-cost evidence is insufficient\n' >&2
  fi
  if [[ "$collector_capture_errors" != 0 ]]; then evidence=capture_failed; fi
  printf '{"status":"%s","capture_errors":%s,"accepted_spans_before":%s,"accepted_spans_after":%s,"accepted_spans_final":%s,"refused_spans_before":%s,"refused_spans_after":%s,"refused_spans_final":%s,"failed_spans_before":%s,"failed_spans_after":%s,"failed_spans_final":%s,"scope":"owned_collector_lifetime_including_warmup_and_shutdown","sink":"nop_reception_without_persistence"}\n' \
    "$evidence" "$collector_capture_errors" "$accepted_before" "$accepted_after" "$accepted_final" "$refused_before" "$refused_after" "$refused_final" "$failed_before" "$failed_after" "$failed_final" > "$output/collector/evidence.json"
  stop_one "$collector_pid" collector INT || status=1; collector_pid=''
  if [[ $(wc -c < "$output/collector/server.log") -ge 33554432 ]]; then
    printf 'Collector log reached its exact 32 MiB output limit\n' >&2; status=1
  fi
fi
if [[ $(wc -c < "$output/service.log") -ge 33554432 ]]; then
  printf 'Service log reached its exact 32 MiB output limit\n' >&2; status=1
fi
printf '{"service_tokio_worker_threads":4,"generator_tokio_worker_threads":4,"request_lanes":%s,"service_and_generator_environment":"cleared_except_explicit_PATH_and_service_TOKIO_WORKER_THREADS","server_stdout_limit_bytes":33554432,"exit_status":%s}\n' "$request_lanes" "$status" > "$output/execution.json"
{
  for artifact in report.json metrics-final.txt diagnostics-final.json service.log; do
    if [[ -f "$output/$artifact" ]]; then printf '%s %s\n' "$artifact" "$(wc -c < "$output/$artifact")"; fi
  done
  if [[ -f "$output/rustfs/server.log" ]]; then printf 'rustfs/server.log %s\n' "$(wc -c < "$output/rustfs/server.log")"; fi
  if [[ -d "$output/collector" ]]; then
    for artifact in "$output/collector/"* "$output/service-exit.json" "$output/collector-exit.json"; do
      if [[ -f "$artifact" ]]; then printf '%s %s\n' "${artifact#"$output/"}" "$(wc -c < "$artifact")"; fi
    done
  fi
  printf 'local_data_filesystem_kib '
  du -sk "$output/cluster/data-1" | cut -f 1
} > "$output/artifact-sizes.txt"
printf 'Trial %s finished with status %s; private artifacts: %s\n' "$scenario" "$status" "$output"
exit "$status"
