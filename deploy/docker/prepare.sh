#!/usr/bin/env bash
set -euo pipefail
umask 077

mode=${1:-}
output=${2:-}
rustfs_memory=${3:-512m}
case "$mode:$rustfs_memory" in
  rf1:512m|rf1:1g|rf3:512m|rf3:1g) ;;
  *) printf 'Usage: %s [rf1|rf3] /new/absolute/state [512m|1g]\n' "$0" >&2; exit 2 ;;
esac
if [[ $# -gt 3 || ! $output =~ ^/[A-Za-z0-9_./-]+$ || -e $output || -L $output ]]; then
  printf 'State must be a new absolute directory with simple ASCII path components\n' >&2
  exit 2
fi
script_dir=$(cd "$(dirname "$0")" && pwd -P)
mkdir -m 0700 "$output"
output=$(cd "$output" && pwd -P)
bash "$script_dir/../local/generate.sh" "$mode" "$output/init"
mkdir -m 0700 "$output/run" "$output/client" "$output/rustfs" "$output/scripts"
cp "$output/init/ca.pem" "$output/init/access.token" "$output/client/"
cp "$script_dir/init-bucket.sh" "$output/scripts/"
access="tmq$(openssl rand -hex 16)"
secret=$(openssl rand -hex 32)
printf '%s\n' "$access" > "$output/rustfs/access.key"
printf '%s\n' "$secret" > "$output/rustfs/secret.key"
printf 'COMPOSE_PROJECT_NAME=tidemq-validation-%s-%s\n' "$mode" "$(openssl rand -hex 8)" > "$output/project.env"
cat > "$output/compose.yaml" <<'EOF'
x-runtime: &runtime
  image: ${TIDEMQ_IMAGE:?Set the verified local runtime image}
  pull_policy: never
  user: "10001:10001"
  cap_drop: [ALL]
  security_opt: [no-new-privileges:true]
  read_only: true
  cgroup: private
  pids_limit: 128
  cpus: 0.5
  mem_limit: 384m
  memswap_limit: 384m
  tmpfs: ["/tmp:rw,noexec,nosuid,size=16777216,mode=1777"]
  logging:
    driver: local
    options: {max-size: 10m, max-file: "3"}
  restart: "no"
  stop_grace_period: 6s
services:
  network:
    <<: *runtime
    entrypoint: [/bin/sleep, infinity]
    cpus: 0.05
    mem_limit: 16m
    memswap_limit: 16m
    networks: [private]
  rustfs:
    <<: *runtime
    network_mode: service:network
    depends_on: {network: {condition: service_started}}
    cpus: 1
EOF
printf '    mem_limit: %s\n    memswap_limit: %s\n' "$rustfs_memory" "$rustfs_memory" >> "$output/compose.yaml"
cat >> "$output/compose.yaml" <<'EOF'
    entrypoint: [/usr/local/bin/rustfs]
    command: [server, --address, "127.0.0.1:19000", --region, us-east-1, /var/lib/tidemq/objects]
    environment:
      RUSTFS_ACCESS_KEY_FILE: /run/rustfs/access.key
      RUSTFS_SECRET_KEY_FILE: /run/rustfs/secret.key
      RUSTFS_DURABILITY_MODE: strict
      RUSTFS_NEW_BUCKET_DURABILITY_MODE: inherit
      RUSTFS_CONSOLE_ENABLE: "false"
      RUSTFS_OBS_ENDPOINT: ""
      RUSTFS_LOG_LEVEL: warn
      RUSTFS_LOG_DIRECTORY: /tmp/rustfs-logs
    volumes:
      - type: bind
        source: ${RUSTFS_BINARY:?Set the verified RustFS 1.0.3 executable path}
        target: /usr/local/bin/rustfs
        read_only: true
        bind: {create_host_path: false}
      - type: bind
        source: ./rustfs
        target: /run/rustfs
        read_only: true
        bind: {create_host_path: false}
      - rustfs-data:/var/lib/tidemq
    healthcheck:
      test: [CMD, curl, --fail, --silent, --max-time, "1", "http://127.0.0.1:19000/health/ready"]
      interval: 2s
      timeout: 2s
      retries: 30
      start_period: 5s
  bucket:
    <<: *runtime
    network_mode: service:network
    depends_on: {rustfs: {condition: service_healthy}}
    cpus: 0.1
    mem_limit: 32m
    memswap_limit: 32m
    entrypoint: [/bin/bash, /run/scripts/init-bucket.sh]
    volumes:
      - type: bind
        source: ./rustfs
        target: /run/rustfs
        read_only: true
        bind: {create_host_path: false}
      - type: bind
        source: ./scripts/init-bucket.sh
        target: /run/scripts/init-bucket.sh
        read_only: true
        bind: {create_host_path: false}
  validator:
    <<: *runtime
    profiles: [tools]
    network_mode: service:network
    depends_on: {network: {condition: service_started}}
    mem_limit: 192m
    memswap_limit: 192m
    entrypoint: [/usr/local/bin/deploy_validation]
    command: [--help]
    volumes:
      - type: bind
        source: ./client
        target: /run/client
        read_only: true
        bind: {create_host_path: false}
      - validation-data:/var/lib/tidemq
    extra_hosts:
EOF
for source in "$output"/init/node-*.toml; do
  node=$(awk '$1 == "node_id" {print $3; exit}' "$source")
  printf '      - "node-%s.local:127.0.0.1"\n' "$node" >> "$output/compose.yaml"
done
for source in "$output"/init/node-*.toml; do
  node=$(awk '$1 == "node_id" {print $3; exit}' "$source")
  role=$(awk -F "'" '$1 == "role = " {print $2; exit}' "$source")
  run="$output/run/node-$node"
  mkdir -m 0700 "$run"
  cp "$output/init/ca.pem" "$output/init/access.toml" "$output/init/delivery.key" \
    "$output/init/node-$node.pem" "$output/init/node-$node.key" "$run/"
  cp "$output"/init/node-*.der "$run/"
  awk -v root="$output/init/" '
    /^data_dir = / { print "data_dir = '\''/var/lib/tidemq'\''"; next }
    {
      line = $0
      while ((position = index(line, root)) > 0) {
        line = substr(line, 1, position - 1) "/run/tidemq/" substr(line, position + length(root))
      }
      print line
    }
  ' "$source" > "$run/node.toml"
  case "$role" in
    combined|storage)
      printf "access_key = '%s'\nsecret_key = '%s'\n" "$access" "$secret" > "$run/objects.toml"
      cat >> "$run/node.toml" <<'EOF'

[runtime.objects]
endpoint = 'http://127.0.0.1:19000'
region = 'us-east-1'
bucket = 'tidemq-deploy-validation'
prefix = 'docker-validation'
credentials_file = '/run/tidemq/objects.toml'
allow_http_loopback = true
EOF
      ;;
  esac
  management=$((8400 + node * 10))
  cat >> "$output/compose.yaml" <<EOF
  node-$node:
    <<: *runtime
    network_mode: service:network
    depends_on: {bucket: {condition: service_completed_successfully}}
    entrypoint: [/usr/local/bin/tidemq]
    command: [serve, --config, /run/tidemq/node.toml]
    volumes:
      - type: bind
        source: ./run/node-$node
        target: /run/tidemq
        read_only: true
        bind: {create_host_path: false}
      - data-$node:/var/lib/tidemq
    healthcheck:
      test: [CMD, curl, --fail, --silent, --max-time, "1", "http://127.0.0.1:$management/readyz"]
      interval: 2s
      timeout: 2s
      retries: 30
      start_period: 5s
EOF
done
cat >> "$output/compose.yaml" <<'EOF'
networks:
  private:
    driver: bridge
    internal: true
volumes:
  rustfs-data: {}
  validation-data: {}
EOF
for source in "$output"/init/node-*.toml; do
  node=$(awk '$1 == "node_id" {print $3; exit}' "$source")
  printf '  data-%s: {}\n' "$node" >> "$output/compose.yaml"
done
find "$output" -type f -exec chmod 0600 {} +
printf 'Prepared isolated %s Compose files in %s\n' "$mode" "$output"
printf 'Assign run/client/rustfs/scripts ownership to 10001:10001 before container use; keep init private.\n'
