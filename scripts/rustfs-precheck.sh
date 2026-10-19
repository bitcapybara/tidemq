#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
version=1.0.3
archive_digest=9ec6f2ad89df88db9b8889f59e37bb27f6b67c61ee8a97968dd90aaa2ac4dd30
cache_dir="$repo_root/target/tools/rustfs/$version"
run_parent="$repo_root/target/rustfs-precheck"
server_pid=
run_dir=

fail() { printf 'RustFS precheck failed: %s\n' "$*" >&2; exit 1; }

stop_server() {
    if [[ -n "$server_pid" ]]; then
        if kill -0 "$server_pid" 2>/dev/null; then
            kill -TERM "$server_pid"
            for ((attempt = 0; attempt < 50; attempt++)); do
                kill -0 "$server_pid" 2>/dev/null || break
                sleep 0.1
            done
            if kill -0 "$server_pid" 2>/dev/null; then
                kill -KILL "$server_pid"
            fi
        fi
        wait "$server_pid" 2>/dev/null || true
        server_pid=
    fi
}

cleanup() {
    stop_server
    if [[ -n "$run_dir" && "$run_dir" == "$run_parent"/run.* ]]; then
        rm -rf -- "$run_dir"
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

[[ "$(uname -sm)" == 'Darwin arm64' ]] || fail 'this pinned native fixture requires macOS arm64'
for tool in curl unzip shasum file lsof openssl cmp awk; do
    command -v "$tool" >/dev/null || fail "required tool is unavailable: $tool"
done
curl -q --help all | awk '/--aws-sigv4/{found=1} END {exit !found}' || fail 'curl requires AWS SigV4 support'
mkdir -p -- "$cache_dir" "$run_parent"
archive="$cache_dir/rustfs-macos-aarch64-v$version.zip"
if [[ ! -f "$archive" ]]; then
    curl -q --fail --silent --show-error --location --proto '=https' --proto-redir '=https' \
        --connect-timeout 15 --max-time 600 --retry 2 --max-filesize 100000000 \
        --output "$archive.part" \
        "https://github.com/rustfs/rustfs/releases/download/$version/rustfs-macos-aarch64-v$version.zip"
    mv -- "$archive.part" "$archive"
fi
actual_digest=$(shasum -a 256 "$archive" | awk '{print $1}')
[[ "$actual_digest" == "$archive_digest" ]] || fail 'release archive SHA256 does not match the pinned digest'
unzip -oq "$archive" rustfs -d "$cache_dir"
server_bin="$cache_dir/rustfs"
file "$server_bin" | awk '/Mach-O 64-bit executable arm64/{found=1} END {exit !found}' || fail 'release architecture does not match'
"$server_bin" --version | awk '/^rustfs 1.0.3$/{found=1} END {exit !found}' || fail 'release version does not match'
"$server_bin" server --help | awk '/--access-key-file/{found=1} END {exit !found}' || fail 'release CLI does not match the verified startup contract'

run_dir=$(mktemp -d "$run_parent/run.XXXXXXXX")
mkdir -- "$run_dir/data" "$run_dir/logs"
access_key="tmq$(openssl rand -hex 8)"
secret_key=$(openssl rand -hex 24)
printf '%s' "$access_key" > "$run_dir/access-key"
printf '%s' "$secret_key" > "$run_dir/secret-key"
printf 'user = "%s:%s"\n' "$access_key" "$secret_key" > "$run_dir/curl-auth"
unset access_key secret_key

port=$((39000 + RANDOM % 1000))
for ((attempt = 0; attempt < 100; attempt++)); do
    if ! lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
        break
    fi
    port=$((39000 + RANDOM % 1000))
done
! lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1 || fail 'no free fixture port was found'
endpoint="http://127.0.0.1:$port"

start_server() {
    env -i PATH="$PATH" \
        RUSTFS_ACCESS_KEY_FILE="$run_dir/access-key" RUSTFS_SECRET_KEY_FILE="$run_dir/secret-key" \
        RUSTFS_DURABILITY_MODE=strict RUSTFS_NEW_BUCKET_DURABILITY_MODE=inherit \
        RUSTFS_CONSOLE_ENABLE=false RUSTFS_OBS_ENDPOINT='' RUSTFS_LOG_LEVEL=info \
        RUSTFS_LOG_DIRECTORY="$run_dir/logs" \
        "$server_bin" server --address "127.0.0.1:$port" --region us-east-1 "$run_dir/data" \
        > "$run_dir/server.log" 2>&1 &
    server_pid=$!
    for ((attempt = 0; attempt < 150; attempt++)); do
        kill -0 "$server_pid" 2>/dev/null || fail 'isolated server exited during startup'
        if curl -q --noproxy '*' --silent --fail --connect-timeout 1 --max-time 1 \
            "$endpoint/health/ready" > "$run_dir/health.json" 2>/dev/null; then
            lsof -a -nP -p "$server_pid" -iTCP:"$port" -sTCP:LISTEN >/dev/null || fail 'fixture does not own the listening socket'
            return
        fi
        sleep 0.2
    done
    fail 'isolated server readiness timed out'
}

request() {
    local method=$1 path=$2 expected=$3
    shift 3
    local status
    status=$(curl -q --config "$run_dir/curl-auth" --aws-sigv4 'aws:amz:us-east-1:s3' \
        --noproxy '*' --silent --show-error --connect-timeout 2 --max-time 15 \
        --max-filesize 1048576 --header 'x-amz-content-sha256: UNSIGNED-PAYLOAD' \
        --dump-header "$run_dir/headers" --output "$run_dir/response" --write-out '%{http_code}' \
        --request "$method" "$endpoint$path" "$@")
    [[ "$status" == "$expected" ]] || fail "$method $path returned HTTP $status; expected $expected"
}

bucket=tidemq-precheck
admin_path="/rustfs/admin/v3/bucket-durability/$bucket"
start_server
request PUT "/$bucket" 200
request GET "$admin_path" 200
awk '/"mode":null/{found=1} END {exit !found}' "$run_dir/response" || fail 'new bucket does not inherit the explicit strict process setting'
request PUT "$admin_path" 200 --header 'Content-Type: application/json' --data-binary '{"mode":"strict"}'
printf 'TideMQ RustFS precheck: stable object identity survives a process restart.\n' > "$run_dir/input"
request PUT "/$bucket/persistent.txt" 200 --upload-file "$run_dir/input"
request GET "/$bucket/persistent.txt" 200
cmp "$run_dir/input" "$run_dir/response" || fail 'GET does not match uploaded bytes'
request GET "/$bucket/persistent.txt" 206 --range 0-6
printf 'TideMQ ' > "$run_dir/range"
cmp "$run_dir/range" "$run_dir/response" || fail 'Range bytes are incorrect'
request HEAD "/$bucket/persistent.txt" 200 --head
expected_length=$(wc -c < "$run_dir/input" | tr -d ' ')
awk -v expected="$expected_length" 'tolower($1)=="content-length:" {gsub("\r", "", $2); if ($2==expected) found=1} END {exit !found}' \
    "$run_dir/headers" || fail 'HEAD Content-Length is incorrect'
request GET "/$bucket/persistent.txt" 416 --range 99999-100000

# Exercise a pre-existing override without writing any objects under relaxed durability.
request PUT "$admin_path" 200 --header 'Content-Type: application/json' --data-binary '{"mode":"relaxed"}'
kill -KILL "$server_pid"
wait "$server_pid" 2>/dev/null || true
server_pid=
start_server
request GET "$admin_path" 200
awk '/"mode":"relaxed"/{found=1} END {exit !found}' "$run_dir/response" || fail 'restart did not preserve the existing bucket override'
request PUT "$admin_path" 200 --header 'Content-Type: application/json' --data-binary '{"mode":"strict"}'
request GET "$admin_path" 200
awk '/"mode":"strict"/{found=1} END {exit !found}' "$run_dir/response" || fail 'existing bucket strict correction did not take effect'
request GET "/$bucket/persistent.txt" 200
cmp "$run_dir/input" "$run_dir/response" || fail 'acknowledged object changed after process kill and restart'
request DELETE "/$bucket/persistent.txt" 204
request GET "/$bucket/persistent.txt" 404
request DELETE "/$bucket" 204
stop_server
printf '{"rustfs_version":"1.0.3","platform":"macos-aarch64","durability":"strict","new_bucket":"inherit","existing_override_checked":true,"object_process_restart":true,"s3_checks":"bucket,put,get,range,head,delete","aws_sdk_checked":false,"power_loss_checked":false}\n' \
    | tee "$run_parent/last-report.json"
