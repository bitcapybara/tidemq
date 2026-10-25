#!/usr/bin/env bash
set -euo pipefail
[[ $EUID == 0 && $# == 1 ]] || { printf 'Usage: sudo bootstrap-infra.sh managed-role\n' >&2; exit 2; }
role=$1
case "$role" in storage|observe|fixtures|runner|loadgen) ;; *) exit 2 ;; esac
[[ $(cat /etc/tidemq-lab-role) == "$role" ]] || { printf 'Guest role does not match\n' >&2; exit 1; }
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y ca-certificates curl jq openssl nginx apache2-utils unzip git build-essential pkg-config libssl-dev nodejs
install -d -m 0755 /etc/systemd/journald.conf.d /var/log/journal /etc/tidemq-lab
cat > /etc/systemd/journald.conf.d/tidemq-lab.conf <<'EOF'
[Journal]
Storage=persistent
SystemMaxUse=128M
RuntimeMaxUse=32M
MaxRetentionSec=7day
EOF
systemctl restart systemd-journald
rm -f /etc/nginx/sites-enabled/default
if [[ "$role" != loadgen ]]; then
  mountpoint -q /srv/tidemq || { printf 'The managed data disk is not mounted\n' >&2; exit 1; }
  apt-get install -y docker.io docker-compose-v2
  install -d -m 0700 /etc/docker
  cat > /etc/docker/daemon.json <<'EOF'
{"data-root":"/srv/tidemq/docker","log-driver":"journald","live-restore":true,"default-ulimits":{"nofile":{"Name":"nofile","Hard":65536,"Soft":65536}}}
EOF
  systemctl enable --now docker
  systemctl restart docker
fi
printf 'Prepared infrastructure guest %s\n' "$role"
