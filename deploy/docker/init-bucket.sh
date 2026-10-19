#!/usr/bin/env bash
set -euo pipefail
umask 077

access=$(cat /run/rustfs/access.key)
secret=$(cat /run/rustfs/secret.key)
if [[ ! $access =~ ^tmq[0-9a-f]{32}$ || ! $secret =~ ^[0-9a-f]{64}$ ]]; then
  printf 'RustFS credentials have an invalid format\n' >&2
  exit 1
fi
request() {
  # Credentials enter curl through stdin, never through argv, environment or logs.
  local -a options=(--config -)
  if [[ $1 == HEAD ]]; then options+=(--head); fi
  curl "${options[@]}" <<EOF
url = "http://127.0.0.1:19000/tidemq-deploy-validation"
request = "$1"
user = "$access:$secret"
aws-sigv4 = "aws:amz:us-east-1:s3"
header = "x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
noproxy = "*"
connect-timeout = 1
max-time = 5
max-filesize = 65536
silent
show-error
output = "/dev/null"
write-out = "%{http_code}"
EOF
}
status=$(request HEAD)
if [[ $status == 404 ]]; then status=$(request PUT); fi
if [[ $status != 200 ]]; then
  printf 'RustFS bucket initialization failed with HTTP %s\n' "$status" >&2
  exit 1
fi
printf 'Private RustFS validation bucket is ready\n'
