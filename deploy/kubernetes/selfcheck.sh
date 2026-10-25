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
  case " $* " in
    *' mq '*) printf '10.96.0.10' ;;
    *' node-1 '*) printf '10.96.0.11' ;;
    *' node-2 '*) printf '10.96.0.12' ;;
    *' node-3 '*) printf '10.96.0.13' ;;
    *' node-4 '*) printf '10.96.0.14' ;;
    *) exit 1 ;;
  esac
fi
EOF
chmod 700 "$temporary/bin/kubectl"
export KUBE_CALL_LOG="$temporary/calls"
export PATH="$temporary/bin:$PATH"
bash "$script_dir/prepare.sh" test-context test-mq example/tidemq:source-abcdef standard "$temporary/generated"
for node in 1 2 3 4; do
  config="$temporary/generated/material/node-$node.toml"
  manifest="$temporary/generated/node-$node.yaml"
  test -s "$config"
  openssl verify -CAfile "$temporary/generated/material/ca.pem" "$temporary/generated/material/node-$node.pem" >/dev/null
  openssl x509 -in "$temporary/generated/material/node-$node.pem" -noout -ext subjectAltName | grep -q 'IP Address:10.96.0.10'
  grep -q 'whenDeleted: Retain, whenScaled: Retain' "$manifest"
  grep -q 'type: OnDelete' "$manifest"
  grep -q 'runAsNonRoot: true' "$manifest"
  grep -q 'defaultMode: 256' "$manifest"
done
test "$(grep -c 'publishNotReadyAddresses: true' "$temporary/generated/services.yaml")" = 4
grep -q 'replicas: 0' "$temporary/generated/node-4.yaml"
grep -q 'bootstrap = false' "$temporary/generated/material/node-4.toml"
! grep -q '^node_id = 4$' "$temporary/generated/material/node-1.toml"
! grep -q bootstrap_admin_token_file "$temporary/generated/material/node-4.toml"
! grep -q 'ca.key' "$KUBE_CALL_LOG"
if bash "$script_dir/prepare.sh" test-context test-mq example/tidemq:latest standard "$temporary/invalid" >/dev/null 2>&1; then
  printf 'A floating image was accepted\n' >&2
  exit 1
fi
if bash "$script_dir/prepare.sh" test-context test-mq example/tidemq:source-abcdef standard "$temporary/generated" >/dev/null 2>&1; then
  printf 'An existing output directory was accepted\n' >&2
  exit 1
fi
printf 'Kubernetes preparation self-check passed\n'
