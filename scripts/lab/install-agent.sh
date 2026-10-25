#!/usr/bin/env bash
set -euo pipefail
[[ $EUID == 0 && $# == 4 ]] || { printf 'Usage: sudo install-agent.sh hostname role management-ip bundle-directory\n' >&2; exit 2; }
name=$1 role=$2 address=$3 bundle=$4
[[ "$name" =~ ^tidemq-[a-z-]+$ && "$role" =~ ^[a-z-]+$ && "$address" =~ ^192\.168\.240\.[0-9]+$ ]] || exit 2
id otelcol >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin --groups adm,systemd-journal otelcol
if [[ -d /var/lib/kubelet ]]; then
  apt-get install -y acl
  [[ -d /var/log/pods ]] || install -d -m 0750 /var/log/pods
  # CRI files are root-owned; default ACLs also cover new pods and rotated log files.
  find /var/log/pods -type d -exec setfacl -m g:otelcol:rx,d:g:otelcol:rx,d:o::--- {} +
  find /var/log/pods -type f -name '*.log' -exec setfacl -m g:otelcol:r {} +
fi
install -d -m 0750 -o otelcol -g otelcol /var/lib/otelcol/storage
install -d -m 0755 /etc/tidemq-lab /etc/systemd/journald.conf.d /var/log/journal
install -d -m 0750 -o root -g otelcol /etc/tidemq-lab/tls
if ! cmp -s "$bundle/collector" /usr/local/bin/otelcol-contrib; then
  install -m 0755 "$bundle/collector" /usr/local/bin/otelcol-contrib.next
  mv -f /usr/local/bin/otelcol-contrib.next /usr/local/bin/otelcol-contrib
fi
install -m 0644 "$bundle/ca.pem" /etc/tidemq-lab/tls/ca.pem
install -m 0640 -o root -g otelcol "$bundle/$name.pem" /etc/tidemq-lab/tls/client.pem
install -m 0640 -o root -g otelcol "$bundle/$name.key" /etc/tidemq-lab/tls/client.key
install -m 0644 "$bundle/ca.pem" /usr/local/share/ca-certificates/tidemq-lab.crt
update-ca-certificates >/dev/null
install -m 0644 "$bundle/agent.yaml" /etc/tidemq-lab/agent.yaml
cat > /etc/tidemq-lab/agent.env <<EOF
LAB_HOSTNAME=$name
LAB_SERVICE_NAME=tidemq-$role
LAB_MANAGEMENT_IP=$address
GOMEMLIMIT=160MiB
GOMAXPROCS=2
EOF
cat > /etc/systemd/journald.conf.d/tidemq-lab.conf <<'EOF'
[Journal]
Storage=persistent
SystemMaxUse=128M
RuntimeMaxUse=32M
MaxRetentionSec=7day
EOF
systemctl restart systemd-journald
cat > /etc/systemd/system/tidemq-agent.service <<'EOF'
[Unit]
Description=TideMQ lab OpenTelemetry agent
After=network-online.target systemd-journald.service
Wants=network-online.target
[Service]
User=otelcol
Group=otelcol
EnvironmentFile=/etc/tidemq-lab/agent.env
ExecStart=/usr/local/bin/otelcol-contrib --config=/etc/tidemq-lab/agent.yaml
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=/var/lib/otelcol
MemoryMax=256M
CPUQuota=50%
LimitNOFILE=8192
[Install]
WantedBy=multi-user.target
EOF
set -a
source /etc/tidemq-lab/agent.env
set +a
cd /
runuser -u otelcol -- /usr/local/bin/otelcol-contrib validate --config=/etc/tidemq-lab/agent.yaml
systemctl daemon-reload
systemctl enable --now tidemq-agent
systemctl restart tidemq-agent
curl --fail --silent --max-time 2 --retry 30 --retry-all-errors --retry-max-time 45 --retry-delay 1 http://127.0.0.1:13133/ >/dev/null
printf 'Installed telemetry agent on %s\n' "$name"
