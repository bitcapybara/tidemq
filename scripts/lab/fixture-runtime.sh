#!/usr/bin/env bash
set -euo pipefail
umask 077
[[ $EUID == 0 && $# -ge 2 && $(cat /etc/tidemq-lab-role) == fixtures ]] || exit 2
action=$1 run=$2
[[ "$run" =~ ^[a-z0-9][a-z0-9-]{0,48}$ ]] || exit 2
directory="/srv/tidemq/fixtures/$run"
if [[ "$action" == remove ]]; then
  for instance in a b; do systemctl stop "tidemq-fixture-$run-$instance.service" || true; done
  rm -f "/etc/nginx/conf.d/tidemq-fixture-$run.conf"
  nginx -t
  systemctl reload nginx
  rm -rf -- "$directory"
  exit
fi
if [[ "$action" == fault ]]; then
  [[ $# == 5 && "$3" =~ ^[ab]$ && "$4" =~ ^(stop|pause)$ && "$5" =~ ^[0-9]{1,3}$ ]] || exit 2
  instance=$3 fault=$4 duration=$5
  (( 10#$duration >= 5 && 10#$duration <= 300 )) || exit 2
  unit="tidemq-fixture-$run-$instance.service"
  restore=start
  [[ "$fault" != pause ]] || restore=thaw
  systemd-run --quiet --unit="tidemq-fixture-restore-$run-$instance-$RANDOM" --on-active="${duration}s" \
    --timer-property=AccuracySec=1s /usr/bin/systemctl "$restore" "$unit"
  if [[ "$fault" == pause ]]; then systemctl freeze "$unit"; else systemctl stop "$unit"; fi
  exit
fi
[[ "$action" == create && ! -e "$directory" ]] || { printf 'Fixture directory already exists or action is invalid\n' >&2; exit 2; }
install -d -m 0700 -o lab -g lab "$directory"
nginx_config="/etc/nginx/conf.d/tidemq-fixture-$run.conf"
printf '' > "$nginx_config"
for instance in a b; do
  backend=19000 frontend=36443
  [[ "$instance" != b ]] || { backend=19010; frontend=36444; }
  path="$directory/$instance"
  install -d -m 0700 -o lab -g lab "$path" "$path/data" "$path/logs"
  openssl rand -hex 12 > "$path/access"
  openssl rand -hex 32 > "$path/secret"
  printf "access_key = '%s'\nsecret_key = '%s'\n" "$(cat "$path/access")" "$(cat "$path/secret")" > "$path/credentials.toml"
  printf 'user = "%s:%s"\naws-sigv4 = "aws:amz:us-east-1:s3"\n' "$(cat "$path/access")" "$(cat "$path/secret")" > "$path/curl.conf"
  chown -R lab:lab "$path"
  systemd-run --quiet --unit="tidemq-fixture-$run-$instance" --property=User=lab --property=Group=lab \
    --property=MemoryMax=512M --property=CPUQuota=100% --property=NoNewPrivileges=yes \
    --property=WorkingDirectory="$path" --property=Restart=on-failure --property=RuntimeMaxSec=7200 \
    --setenv="RUSTFS_ACCESS_KEY_FILE=$path/access" --setenv="RUSTFS_SECRET_KEY_FILE=$path/secret" \
    --setenv=RUSTFS_DURABILITY_MODE=strict --setenv=RUSTFS_NEW_BUCKET_DURABILITY_MODE=inherit \
    --setenv=RUSTFS_CONSOLE_ENABLE=false --setenv=RUSTFS_OBS_ENDPOINT= --setenv=RUSTFS_LOG_LEVEL=warn \
    --setenv="RUSTFS_LOG_DIRECTORY=$path/logs" /usr/local/bin/rustfs server --address "127.0.0.1:$backend" --region us-east-1 "$path/data"
  cat >> "$nginx_config" <<EOF
server {
  listen 192.168.241.34:$frontend ssl;
  server_name fixture.lab.tidemq.test;
  ssl_certificate /etc/tidemq-lab/fixture-tls/server.pem;
  ssl_certificate_key /etc/tidemq-lab/fixture-tls/server.key;
  ssl_protocols TLSv1.2 TLSv1.3;
  client_max_body_size 256m;
  location / {
    proxy_set_header Host \$http_host;
    proxy_http_version 1.1;
    proxy_request_buffering off;
    proxy_buffering off;
    proxy_pass http://127.0.0.1:$backend;
  }
}
EOF
  curl --fail --silent --retry 30 --retry-connrefused --retry-delay 1 "http://127.0.0.1:$backend/health/ready" >/dev/null
  curl --config "$path/curl.conf" --fail --silent --show-error --retry 3 -X PUT "http://127.0.0.1:$backend/tidemq-ci" > "$path/bucket-response.txt"
done
nginx -t
systemctl reload nginx
printf 'Created independent RustFS fixtures a and b for %s\n' "$run"
