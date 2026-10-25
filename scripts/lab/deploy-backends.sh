#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
credentials="$lab_state/private/credentials"
install -d -m 0700 "$credentials"
for name in rustfs-access rustfs-secret grafana-password metrics-password; do
  [[ -f "$credentials/$name" ]] || openssl rand -hex 24 > "$credentials/$name"
done
openssl passwd -apr1 -stdin < "$credentials/metrics-password" | sed 's/^/reader:/' > "$credentials/metrics.htpasswd"
for role in storage observe; do
  bundle="$lab_state/private/$role-bundle"
  install -d -m 0700 "$bundle"
  cp "$lab_state/private/tls/ca.pem" "$lab_state/private/tls/$role.pem" "$lab_state/private/tls/$role.key" "$bundle/"
  if [[ "$role" == storage ]]; then
    cp "$lab_state/tools/rustfs" "$credentials/rustfs-access" "$credentials/rustfs-secret" "$bundle/"
  else
    cp -R "$lab_repo/deploy/lab/observe" "$bundle/"
    cp -R "$lab_repo/deploy/observability/alerts" "$lab_repo/deploy/observability/dashboards" "$bundle/observe/"
    cp "$lab_state/tools/collector" "$credentials/grafana-password" "$credentials/metrics.htpasswd" "$lab_repo/scripts/lab/alert-audit.mjs" "$bundle/"
  fi
  tar -czf "$lab_state/private/$role-bundle.tar.gz" -C "$bundle" .
  name="tidemq-$role"
  lab_copy "$name" "$lab_state/private/$role-bundle.tar.gz" /tmp/tidemq-backends.tar.gz
  lab_copy "$name" "$lab_repo/scripts/lab/install-backends.sh" /tmp/tidemq-install-backends.sh
  lab_ssh "$name" bash -c 'install -d -m 0700 /tmp/tidemq-backends; tar -xzf /tmp/tidemq-backends.tar.gz -C /tmp/tidemq-backends'
  lab_ssh "$name" sudo bash /tmp/tidemq-install-backends.sh "$role" /tmp/tidemq-backends > "$lab_state/logs/$name-backends.log" 2>&1
  lab_ssh "$name" bash -c 'rm -rf /tmp/tidemq-backends /tmp/tidemq-backends.tar.gz'
done
install -m 0755 "$lab_repo/scripts/lab/host-ingress.sh" /usr/local/sbin/tidemq-lab-ingress
cat > /etc/systemd/system/tidemq-lab-ingress.service <<'EOF'
[Unit]
Description=TideMQ lab LAN ingress
After=network-online.target libvirtd.service docker.service
Wants=network-online.target
Requires=libvirtd.service
[Service]
Type=oneshot
ExecStart=/usr/local/sbin/tidemq-lab-ingress
RemainAfterExit=true
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now tidemq-lab-ingress
systemctl restart tidemq-lab-ingress
printf 'Public lab backends and restricted LAN ingress are deployed.\n'
