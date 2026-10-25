#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
export MC_CONFIG_DIR="$lab_state/private/mc"
install -d -m 0700 "$MC_CONFIG_DIR/certs/CAs"
cp "$lab_state/private/tls/ca.pem" "$MC_CONFIG_DIR/certs/CAs/tidemq.pem"
mc="$lab_state/tools/mc"
credentials="$lab_state/private/credentials"
"$mc" alias set lab https://192.168.241.31:18443 "$(cat "$credentials/rustfs-access")" "$(cat "$credentials/rustfs-secret")" > "$lab_state/private/mc-setup.log" 2>&1
for name in kubernetes devices; do
  bucket=tidemq-dev
  [[ "$name" != devices ]] || bucket=tidemq-devices
  [[ -f "$credentials/$name-access" ]] || printf 'tidemq-%s-%s\n' "$name" "$(openssl rand -hex 6)" > "$credentials/$name-access"
  [[ -f "$credentials/$name-secret" ]] || openssl rand -hex 32 > "$credentials/$name-secret"
  "$mc" mb --ignore-existing "lab/$bucket"
  policy="$lab_state/private/$name-policy.json"
  jq -n --arg bucket "$bucket" '{Version:"2012-10-17",Statement:[
    {Effect:"Allow",Action:["s3:GetBucketLocation","s3:ListBucket","s3:ListBucketMultipartUploads"],Resource:[("arn:aws:s3:::"+$bucket)]},
    {Effect:"Allow",Action:["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:AbortMultipartUpload","s3:ListMultipartUploadParts"],Resource:[("arn:aws:s3:::"+$bucket+"/*")]}
  ]}' > "$policy"
  "$mc" admin user add lab "$(cat "$credentials/$name-access")" "$(cat "$credentials/$name-secret")" >> "$lab_state/private/mc-setup.log" 2>&1
  "$mc" admin policy create lab "tidemq-$name" "$policy"
  "$mc" admin policy attach lab "tidemq-$name" --user "$(cat "$credentials/$name-access")" >> "$lab_state/private/mc-setup.log" 2>&1
  printf "access_key = '%s'\nsecret_key = '%s'\n" "$(cat "$credentials/$name-access")" "$(cat "$credentials/$name-secret")" > "$credentials/$name.toml"
done
printf 'Created separate bucket-scoped public service accounts; root credentials remain private.\n'
