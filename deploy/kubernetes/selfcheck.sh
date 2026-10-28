#!/usr/bin/env bash
set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd -P)
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT
mkdir "$temporary/bin"
cat > "$temporary/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${KUBE_CALL_LOG:?}"
if [[ " $* " == *' get service '* ]]; then
  if [[ " $* " == *' mq '* ]]; then
    printf '10.96.0.10'
  elif [[ " $* " =~ \ node-([0-9]+)\  ]]; then
    printf '10.96.1.%s' "${BASH_REMATCH[1]}"
  else
    exit 1
  fi
fi
EOF
chmod 700 "$temporary/bin/kubectl"
export KUBE_CALL_LOG="$temporary/calls"
export PATH="$temporary/bin:$PATH"
bash -n "$script_dir/prepare.sh"
bash "$script_dir/prepare.sh" test-context test-mq example/tidemq:source-abcdef standard "$temporary/generated"
nodes='11:metadata 12:metadata 13:metadata 21:storage 22:storage 23:storage 31:broker 32:broker 41:gateway 42:gateway'
for entry in $nodes; do
  node=${entry%:*}; role=${entry#*:}
  config="$temporary/generated/material/node-$node.toml"
  manifest="$temporary/generated/node-$node.yaml"
  test -s "$config"
  openssl verify -CAfile "$temporary/generated/material/ca.pem" "$temporary/generated/material/node-$node.pem" >/dev/null
  openssl x509 -in "$temporary/generated/material/node-$node.pem" -noout -ext subjectAltName | grep -q "DNS:node-$node.local"
  grep -q "role = '$role'" "$config"
  test "$(grep -c '^\[\[runtime.peers\]\]' "$config")" = 10
  grep -q '^replication_factor = 3$' "$config"
  grep -q '^metadata_voters = \[11,12,13\]$' "$config"
  grep -q '^voters = \[21,22,23\]$' "$config"
  ! grep -q "role = 'combined'" "$config"
  grep -q 'whenDeleted: Retain, whenScaled: Retain' "$manifest"
  grep -q 'type: OnDelete' "$manifest"
  grep -q 'runAsNonRoot: true' "$manifest"
  grep -q 'defaultMode: 256' "$manifest"
  grep -q 'http://127.0.0.1:9419/readyz' "$manifest"
  case "$role" in
    metadata|storage) grep -q '^  volumeClaimTemplates:' "$manifest" ;;
    *) ! grep -q '^  volumeClaimTemplates:' "$manifest"; grep -q 'name: data, emptyDir' "$manifest" ;;
  esac
  if [[ $role == metadata ]]; then grep -q bootstrap_admin_token_file "$config"; else ! grep -q bootstrap_admin_token_file "$config"; fi
  if [[ $role == gateway ]]; then
    grep -q '^\[gateway\]' "$config"
    grep -q 'name: quic, containerPort: 7443' "$manifest"
    openssl x509 -in "$temporary/generated/material/node-$node.pem" -noout -ext subjectAltName | grep -q 'IP Address:10.96.0.10'
    openssl x509 -in "$temporary/generated/material/node-$node.pem" -noout -ext subjectAltName | grep -Fq 'DNS:mq.test-mq.svc'
  else
    ! grep -q '^\[gateway\]' "$config"
    ! grep -q 'name: quic, containerPort: 7443' "$manifest"
    if openssl x509 -in "$temporary/generated/material/node-$node.pem" -noout -ext subjectAltName | grep -Eq 'IP Address:10\.96\.0\.10|DNS:mq\.test-mq\.svc'; then
      printf 'A non-Gateway certificate covers the public MQ Service\n' >&2
      exit 1
    fi
  fi
  if [[ $role == storage ]]; then grep -q '^\[storage\]' "$config"; else ! grep -q '^\[storage\]' "$config"; fi
  if [[ -n ${TIDEMQ_BIN:-} ]]; then "$TIDEMQ_BIN" validate-config --config "$config" >/dev/null; fi
done
test "$(grep -c 'publishNotReadyAddresses: true' "$temporary/generated/services.yaml")" = 10
grep -q 'selector: {app: tidemq, tidemq-role: gateway}' "$temporary/generated/services.yaml"
for role in metadata storage; do grep -q 'minAvailable: 2' "$temporary/generated/availability-$role.yaml"; done
for role in broker gateway; do grep -q 'minAvailable: 1' "$temporary/generated/availability-$role.yaml"; done
! grep -q 'ca.key' "$KUBE_CALL_LOG"
if bash "$script_dir/prepare.sh" test-context test-mq example/tidemq:latest standard "$temporary/invalid" >/dev/null 2>&1; then
  printf 'A floating image was accepted\n' >&2
  exit 1
fi
if bash "$script_dir/prepare.sh" test-context test-mq example/tidemq:source-abcdef standard "$temporary/generated" >/dev/null 2>&1; then
  printf 'An existing output directory was accepted\n' >&2
  exit 1
fi
if bash "$script_dir/../local/generate.sh" rf3-combined "$temporary/rejected" >/dev/null 2>&1; then
  printf 'A multi-node Combined deployment was accepted\n' >&2
  exit 1
fi
test ! -e "$temporary/rejected"
printf 'Kubernetes separated RF3 preparation self-check passed: 10 processes, 6 persistent volumes, 4 role disruption budgets\n'
