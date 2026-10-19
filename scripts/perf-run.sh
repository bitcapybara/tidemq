#!/usr/bin/env bash
set -euo pipefail
umask 077

build=${1:?Usage: perf-run.sh allocator-build-directory new-trial-directory [small|large|slow|replay] [minimal|recommended|detailed]}
output=${2:?A new private trial directory is required}
scenario=${3:-small}
observability=${4:-recommended}
root=$(cd "$(dirname "$0")/.." && pwd -P)
build=$(cd "$build" && pwd -P)
if [[ -e "$output" ]]; then printf 'Trial output already exists\n' >&2; exit 1; fi
mkdir -p "$output"
output=$(cd "$output" && pwd -P)
service_pid=''; rustfs_pid=''
stop_one() {
  local pid=$1
  [[ -n "$pid" ]] || return 0
  kill -TERM "$pid" 2>/dev/null || true
  for _ in {1..100}; do
    if ! kill -0 "$pid" 2>/dev/null; then wait "$pid" || true; return 0; fi
    sleep 0.1
  done
  printf 'Owned process required forced termination after its drain deadline\n' >&2
  kill -KILL "$pid" 2>/dev/null || true
  wait "$pid" || true
  return 1
}
stop() {
  trap - EXIT INT TERM
  stop_one "$service_pid" || true
  stop_one "$rustfs_pid" || true
}
trap stop EXIT INT TERM
bash "$root/deploy/local/generate.sh" rf1 "$output/cluster"
config="$output/cluster/node-1.toml"
extra=(); load=()
case "$observability" in
  minimal) printf '\n[logging]\nfilter = "warn"\n' >> "$config" ;;
  recommended) ;;
  detailed) extra+=(--trace-seconds 300) ;;
  *) printf 'Unknown observability mode\n' >&2; exit 2 ;;
esac
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
env -i PATH=/usr/bin:/bin "$build/perf_load" --endpoint 127.0.0.1:7410 --server-name node-1.local \
  --ca "$output/cluster/ca.pem" --token-file "$output/cluster/access.token" \
  --management 127.0.0.1:8410 --service-pid "$service_pid" --service-binary "$build/tidemq" \
  --service-config "$config" --build-identity "$build/identity.json" --output "$output/report.json" \
  --workers 4 --ack-messages 32 --phase-timeout-secs 120 --cooldown-secs 3 \
  --observability "$observability" "${load[@]}" || status=$?
curl --fail --silent --max-time 2 http://127.0.0.1:8410/metrics > "$output/metrics-final.txt" || status=1
curl --fail --silent --max-time 2 http://127.0.0.1:8410/diagnostics > "$output/diagnostics-final.json" || status=1
stop_one "$service_pid" || status=1; service_pid=''
stop_one "$rustfs_pid" || status=1; rustfs_pid=''
if [[ $(wc -c < "$output/service.log") -ge 33554432 ]]; then
  printf 'Service log reached its exact 32 MiB output limit\n' >&2; status=1
fi
printf '{"service_tokio_worker_threads":4,"generator_tokio_worker_threads":4,"service_and_generator_environment":"cleared_except_explicit_PATH_and_service_TOKIO_WORKER_THREADS","server_stdout_limit_bytes":33554432,"exit_status":%s}\n' "$status" > "$output/execution.json"
{
  for artifact in report.json metrics-final.txt diagnostics-final.json service.log; do
    if [[ -f "$output/$artifact" ]]; then printf '%s %s\n' "$artifact" "$(wc -c < "$output/$artifact")"; fi
  done
  if [[ -f "$output/rustfs/server.log" ]]; then printf 'rustfs/server.log %s\n' "$(wc -c < "$output/rustfs/server.log")"; fi
  printf 'local_data_filesystem_kib '
  du -sk "$output/cluster/data-1" | cut -f 1
} > "$output/artifact-sizes.txt"
printf 'Trial %s finished with status %s; private artifacts: %s\n' "$scenario" "$status" "$output"
exit "$status"
