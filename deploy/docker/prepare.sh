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
awk -v memory="$rustfs_memory" '
  { gsub(/\$\{TIDEMQ_RUSTFS_MEMORY:-512m\}/, memory); print }
' "$script_dir/compose.$mode.yaml" > "$output/compose.yaml"
for source in "$output"/init/node-*.toml; do
  node=$(awk '$1 == "node_id" {print $3; exit}' "$source")
  role=$(awk -F "'" '$1 == "role = " {print $2; exit}' "$source")
  run="$output/run/node-$node"
  mkdir -m 0700 "$run"
  cp "$output/init/ca.pem" \
    "$output/init/node-$node.pem" "$output/init/node-$node.key" "$run/"
  case "$role" in
    combined|metadata) cp "$output/init/bootstrap.token" "$run/" ;;
  esac
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

[storage.objects]
endpoint = 'http://127.0.0.1:19000'
region = 'us-east-1'
bucket = 'tidemq-deploy-validation'
prefix = 'docker-validation'
credentials_file = '/run/tidemq/objects.toml'
allow_http_loopback = true
EOF
      ;;
  esac
done
find "$output" -type f -exec chmod 0600 {} +
printf 'Prepared isolated %s Compose files in %s\n' "$mode" "$output"
printf 'Assign run/client/rustfs/scripts ownership to 10001:10001 before container use; keep init private.\n'
