#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
mode=${1:-all}
[[ "$mode" == all || "$mode" == runtime || "$mode" == access ]] || exit 2
if [[ "$mode" != access ]]; then
bundle="$lab_state/private/runner-bundle"
for name in runner.archive rustfs collector collectorTest helm nextest llvmCov; do cp "$lab_state/tools/$name" "$bundle/$name"; done
lab_fetch tidemq-cp-a /usr/bin/kubectl "$bundle/kubectl"
tar -czf "$lab_state/private/runner-bundle.tar.gz" -C "$bundle" .
lab_copy tidemq-runner "$lab_state/private/runner-bundle.tar.gz" /tmp/tidemq-runner.tar.gz
lab_copy tidemq-runner "$lab_repo/scripts/lab/bootstrap-runner.sh" /tmp/tidemq-bootstrap-runner.sh
lab_ssh tidemq-runner bash -c 'install -d -m 0700 /tmp/tidemq-runner; tar -xzf /tmp/tidemq-runner.tar.gz -C /tmp/tidemq-runner'
lab_ssh tidemq-runner sudo bash /tmp/tidemq-bootstrap-runner.sh /tmp/tidemq-runner > "$lab_state/logs/runner-bootstrap.log" 2>&1
lab_ssh tidemq-runner bash -c 'rm -rf /tmp/tidemq-runner /tmp/tidemq-runner.tar.gz'
fi
if [[ "$mode" == runtime ]]; then printf 'Runner runtime is prepared.\n'; exit; fi
private_bundle="$lab_state/private/runner-access"
install -d -m 0700 "$private_bundle/logs" "$private_bundle/telemetry"
cp "$lab_state/private/tls/ca.pem" "$private_bundle/telemetry/ca.pem"
cp "$lab_state/private/tls/kubernetes.pem" "$private_bundle/telemetry/client.pem"
cp "$lab_state/private/tls/kubernetes.key" "$private_bundle/telemetry/client.key"
cp "$lab_state/private/ci-guest" "$lab_state/private/ci-host" "$private_bundle/"
cp "$lab_state/private/admin.conf" "$private_bundle/kubeconfig"
cp "$lab_state/private/tls/ca.pem" "$private_bundle/ca.pem"
cp "$lab_state/private/credentials/metrics-password" "$private_bundle/metrics-password"
cp "$lab_state/private/known_hosts" "$private_bundle/known_hosts"
printf '192.168.240.1 %s\n' "$(cat /etc/ssh/ssh_host_ed25519_key.pub)" >> "$private_bundle/known_hosts"
cat > "$private_bundle/ssh_config" <<'EOF'
Host lab-host
  HostName 192.168.240.1
  User tidemq-ci
  IdentityFile /home/actions/.config/tidemq-lab/ci-host
Host tidemq-*
  User lab
  IdentityFile /home/actions/.config/tidemq-lab/ci-guest
Host *
  BatchMode yes
  StrictHostKeyChecking yes
  UserKnownHostsFile /home/actions/.config/tidemq-lab/known_hosts
  ConnectTimeout 5
  ServerAliveInterval 15
EOF
tar -czf "$lab_state/private/runner-access.tar.gz" -C "$private_bundle" .
lab_copy tidemq-runner "$lab_state/private/runner-access.tar.gz" /tmp/tidemq-access.tar.gz
lab_ssh tidemq-runner sudo tar -xzf /tmp/tidemq-access.tar.gz -C /home/actions/.config/tidemq-lab
lab_ssh tidemq-runner sudo chown -R actions:actions /home/actions/.config/tidemq-lab
lab_ssh tidemq-runner rm /tmp/tidemq-access.tar.gz
lab_ssh tidemq-runner sudo runuser -u actions -- ssh -F /home/actions/.config/tidemq-lab/ssh_config lab-host status
printf 'Runner runtime and restricted lab access are ready; repository registration remains separate.\n'
