#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
directory="$lab_state/private/tls"
install -d -m 0700 "$directory"
if [[ ! -f "$directory/ca.pem" ]]; then
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -sha256 -days 3650 \
    -subj '/CN=TideMQ development lab CA' -keyout "$directory/ca.key" -out "$directory/ca.pem" >/dev/null 2>&1
fi
issue() {
  local name=$1 purpose=$2 san=$3
  [[ -f "$directory/$name.pem" ]] && return
  openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -sha256 \
    -subj "/CN=$name" -keyout "$directory/$name.key" -out "$directory/$name.csr" >/dev/null 2>&1
  printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=%s\nsubjectAltName=%s\n' "$purpose" "$san" > "$directory/$name.ext"
  openssl x509 -req -sha256 -in "$directory/$name.csr" -CA "$directory/ca.pem" -CAkey "$directory/ca.key" \
    -CAcreateserial -days 365 -extfile "$directory/$name.ext" -out "$directory/$name.pem" >/dev/null 2>&1
}
issue storage serverAuth 'DNS:s3.lab.tidemq.test,DNS:rustfs.lab.tidemq.test,IP:192.168.126.8,IP:192.168.240.31,IP:192.168.241.31'
issue observe serverAuth 'DNS:otlp.lab.tidemq.test,DNS:grafana.lab.tidemq.test,DNS:metrics.lab.tidemq.test,IP:192.168.126.8,IP:192.168.240.32,IP:192.168.241.32'
issue fixtures serverAuth 'DNS:fixture.lab.tidemq.test,IP:192.168.240.34,IP:192.168.241.34'
while read -r name; do issue "$name" clientAuth "DNS:$name"; done < <(jq -r '.nodes[].name' "$lab_inventory")
issue kubernetes clientAuth 'DNS:tidemq-kubernetes'
issue tidemq-host clientAuth 'DNS:tidemq-host'
issue external clientAuth 'DNS:tidemq-external-devices'
printf 'Created private lab certificates in %s; only ca.pem is public.\n' "$directory"
