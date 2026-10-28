#!/usr/bin/env bash
set -euo pipefail
umask 077

mode=${1:-rf1}
output=${2:-/tmp/tidemq-${mode}}
if [[ $# -gt 2 ]]; then
  printf 'Usage: %s [rf1|rf1-separated|rf3] [new-output-directory]\n' "$0" >&2
  exit 2
fi
case "$mode" in
  rf1) nodes='1:combined'; voters='[1]'; storage='[1]'; factor=1 ;;
  rf1-separated) nodes='11:metadata 21:storage 31:broker 41:gateway'; voters='[11]'; storage='[21]'; factor=1 ;;
  rf3) nodes='11:metadata 12:metadata 13:metadata 21:storage 22:storage 23:storage 31:broker 32:broker 41:gateway 42:gateway'; voters='[11,12,13]'; storage='[21,22,23]'; factor=3 ;;
  *) printf 'Usage: %s [rf1|rf1-separated|rf3] [new-output-directory]\n' "$0" >&2; exit 2 ;;
esac
if [[ -e "$output" ]]; then printf 'Output directory already exists: %s\n' "$output" >&2; exit 1; fi
mkdir -p "$output"
output=$(cd "$output" && pwd -P)
if [[ "$output" == *"'"* || "$output" == *$'\n'* || "$output" == *$'\r'* ]]; then printf 'Output path contains unsupported TOML characters\n' >&2; exit 1; fi
hex=$(openssl rand -hex 16)
cluster="${hex:0:8}-${hex:8:4}-4${hex:13:3}-8${hex:17:3}-${hex:20:12}"
token=$(openssl rand -hex 32)
cat > "$output/ca.cnf" <<'EOF'
[req]
distinguished_name=dn
x509_extensions=extensions
prompt=no
[dn]
CN=TideMQ local development CA
[extensions]
basicConstraints=critical,CA:TRUE
keyUsage=critical,keyCertSign,cRLSign
EOF
openssl req -new -x509 -sha256 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -pkeyopt ec_param_enc:named_curve -nodes -days 7 \
  -config "$output/ca.cnf" -keyout "$output/ca.key" -out "$output/ca.pem" >/dev/null 2>&1
printf '%s\n' "$token" > "$output/bootstrap.token"
printf '%s\n' "$token" > "$output/access.token"
if [[ $mode == rf1 ]]; then openssl rand 32 > "$output/console-session.key"; fi
for entry in $nodes; do
  node=${entry%:*}
  cat > "$output/node-$node.cnf" <<EOF
[req]
distinguished_name=dn
prompt=no
[dn]
CN=node-$node.local
[extensions]
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:node-$node.local,DNS:localhost,IP:127.0.0.1
EOF
  openssl req -new -sha256 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -pkeyopt ec_param_enc:named_curve -nodes \
    -config "$output/node-$node.cnf" -keyout "$output/node-$node.key" -out "$output/node-$node.csr" >/dev/null 2>&1
  openssl x509 -req -sha256 -in "$output/node-$node.csr" -CA "$output/ca.pem" -CAkey "$output/ca.key" \
    -set_serial "$node" -days 7 -extfile "$output/node-$node.cnf" -extensions extensions \
    -out "$output/node-$node.pem" >/dev/null 2>&1
  openssl x509 -in "$output/node-$node.pem" -outform DER -out "$output/node-$node.der"
done
for entry in $nodes; do
  node=${entry%:*}; role=${entry#*:}
  external=$((7400 + node * 10)); internal=$((external + 1)); management=$((external + 1000))
  cat > "$output/node-$node.toml" <<EOF
node_id = $node
cluster_id = '$cluster'
role = '$role'
management_listen = '127.0.0.1:$management'
data_dir = '$output/data-$node'
EOF
  if [[ $role != combined ]]; then
    printf 'replication_factor = %s\n' "$factor" >> "$output/node-$node.toml"
  fi
  cat >> "$output/node-$node.toml" <<EOF

[memory]
total_bytes = 201326592
control_reserved_bytes = 33554432

[tls]
certificate = '$output/node-$node.pem'
private_key = '$output/node-$node.key'
EOF
  if [[ $role != combined ]]; then
    printf "trust_roots = '%s/ca.pem'\n" "$output" >> "$output/node-$node.toml"
  fi
  printf '\n[authorization]\n' >> "$output/node-$node.toml"
  case "$role" in
    combined|metadata)
      printf "bootstrap_admin_token_file = '%s/bootstrap.token'\n" "$output" >> "$output/node-$node.toml"
      ;;
  esac
  case "$role" in
    combined|gateway)
      cat >> "$output/node-$node.toml" <<EOF

[gateway]
listen = '127.0.0.1:$external'
EOF
      ;;
  esac
  case "$role" in
    combined|storage)
      cat >> "$output/node-$node.toml" <<'EOF'

[storage]
archive_interval_ms = 1000
EOF
      ;;
  esac
  if [[ $role == combined ]]; then
    cat >> "$output/node-$node.toml" <<EOF

[console]
listen = '127.0.0.1:8443'
static_dir = '$output/console'
[console.session]
key_file = '$output/console-session.key'
public_origin = 'https://localhost:8443'

[runtime]
metadata_group = 1
[[runtime.groups]]
group_id = 2
EOF
    continue
  fi
  cat >> "$output/node-$node.toml" <<EOF

[runtime]
internal_listen = '127.0.0.1:$internal'
metadata_group = 1
metadata_voters = $voters
bootstrap = true

[[runtime.groups]]
group_id = 2
voters = $storage
EOF
  for remote in $nodes; do
    peer=${remote%:*}; peer_role=${remote#*:}; port=$((7401 + peer * 10)); zone=$((peer % 3))
    cat >> "$output/node-$node.toml" <<EOF

[[runtime.peers]]
node_id = $peer
role = '$peer_role'
zone = 'zone-$zone'
internal_address = '127.0.0.1:$port'
server_name = 'node-$peer.local'
certificate_der = '$output/node-$peer.der'
EOF
  done
done
printf 'Created %s runtime configuration in %s\n' "$mode" "$output"
printf 'Start each node with: tidemq serve --config %s/node-<id>.toml\n' "$output"
printf 'Private bootstrap administrator token: %s/access.token\n' "$output"
if [[ $mode == rf1 ]]; then
  printf 'Console HTTPS: https://localhost:8443; trust the development CA %s/ca.pem in your browser\n' "$output"
  printf 'Build assets first: bash scripts/build-console.sh %s/console (run.sh also builds missing assets)\n' "$output"
fi
