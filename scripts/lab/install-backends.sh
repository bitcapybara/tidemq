#!/usr/bin/env bash
set -euo pipefail
[[ $EUID == 0 && $# == 2 ]] || { printf 'Usage: sudo install-backends.sh storage|observe bundle-directory\n' >&2; exit 2; }
role=$1 bundle=$2
[[ "$role" == storage || "$role" == observe ]] || exit 2
[[ $(cat /etc/tidemq-lab-role) == "$role" ]] || exit 1
mountpoint -q /srv/tidemq || { printf 'Data disk is not mounted\n' >&2; exit 1; }
id tidemq >/dev/null 2>&1 || useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin tidemq
install -d -m 0755 /opt/tidemq-lab
install -d -m 0700 /etc/tidemq-lab/server-tls
install -m 0600 "$bundle/$role.key" /etc/tidemq-lab/server-tls/server.key
install -m 0644 "$bundle/$role.pem" /etc/tidemq-lab/server-tls/server.pem
install -m 0644 "$bundle/ca.pem" /etc/tidemq-lab/server-tls/ca.pem
if [[ "$role" == storage ]]; then
  install -m 0755 "$bundle/rustfs" /usr/local/bin/rustfs
  install -d -m 0750 -o tidemq -g tidemq /srv/tidemq/rustfs /srv/tidemq/rustfs/data /srv/tidemq/rustfs/logs
  install -m 0600 -o tidemq -g tidemq "$bundle/rustfs-access" /etc/tidemq-lab/rustfs-access
  install -m 0600 -o tidemq -g tidemq "$bundle/rustfs-secret" /etc/tidemq-lab/rustfs-secret
  cat > /etc/tidemq-lab/rustfs.env <<'EOF'
RUSTFS_ACCESS_KEY_FILE=/etc/tidemq-lab/rustfs-access
RUSTFS_SECRET_KEY_FILE=/etc/tidemq-lab/rustfs-secret
RUSTFS_ADDRESS=127.0.0.1:19000
RUSTFS_CONSOLE_ADDRESS=127.0.0.1:19001
RUSTFS_CONSOLE_ENABLE=true
RUSTFS_DURABILITY_MODE=strict
RUSTFS_NEW_BUCKET_DURABILITY_MODE=inherit
RUSTFS_OBS_ENDPOINT=
RUSTFS_LOG_LEVEL=info
RUSTFS_LOG_DIRECTORY=/srv/tidemq/rustfs/logs
EOF
  cat > /etc/systemd/system/tidemq-rustfs.service <<'EOF'
[Unit]
Description=TideMQ public development RustFS
After=network-online.target
Wants=network-online.target
[Service]
User=tidemq
Group=tidemq
EnvironmentFile=/etc/tidemq-lab/rustfs.env
ExecStart=/usr/local/bin/rustfs /srv/tidemq/rustfs/data
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=/srv/tidemq/rustfs
MemoryMax=1G
CPUQuota=150%
LimitNOFILE=65536
[Install]
WantedBy=multi-user.target
EOF
  cat > /etc/nginx/conf.d/tidemq-lab.conf <<'EOF'
server {
  listen 18443 ssl;
  server_name s3.lab.tidemq.test;
  ssl_certificate /etc/tidemq-lab/server-tls/server.pem;
  ssl_certificate_key /etc/tidemq-lab/server-tls/server.key;
  ssl_protocols TLSv1.2 TLSv1.3;
  client_max_body_size 1g;
  location / {
    proxy_set_header Host $http_host;
    proxy_set_header X-Forwarded-Proto https;
    proxy_http_version 1.1;
    proxy_request_buffering off;
    proxy_buffering off;
    proxy_read_timeout 300s;
    proxy_pass http://127.0.0.1:19000;
  }
}
server {
  listen 18445 ssl;
  server_name rustfs.lab.tidemq.test;
  ssl_certificate /etc/tidemq-lab/server-tls/server.pem;
  ssl_certificate_key /etc/tidemq-lab/server-tls/server.key;
  ssl_protocols TLSv1.2 TLSv1.3;
  location / {
    proxy_set_header Host $http_host;
    proxy_set_header X-Forwarded-Proto https;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_pass http://127.0.0.1:19001;
  }
}
EOF
  systemctl daemon-reload
  systemctl enable --now tidemq-rustfs
  systemctl restart tidemq-rustfs
else
  install -d -m 0755 /opt/tidemq-lab/observe
  cp -R "$bundle/observe/." /opt/tidemq-lab/observe/
  chmod -R a+rX /opt/tidemq-lab/observe
  for service in prometheus loki tempo grafana alertmanager alert-audit collector-queue; do
    install -d -m 0750 -o tidemq -g tidemq "/srv/tidemq/$service"
  done
  install -m 0400 -o tidemq -g tidemq "$bundle/grafana-password" /etc/tidemq-lab/grafana-password
  install -m 0640 -o root -g www-data "$bundle/metrics.htpasswd" /etc/tidemq-lab/metrics.htpasswd
  install -m 0644 "$bundle/alert-audit.mjs" /opt/tidemq-lab/alert-audit.mjs
  install -m 0755 "$bundle/collector" /usr/local/bin/otelcol-contrib
  cat > /etc/systemd/system/tidemq-collector.service <<'EOF'
[Unit]
Description=TideMQ central OpenTelemetry gateway
After=network-online.target docker.service
Wants=network-online.target
[Service]
User=tidemq
Group=tidemq
Environment=GOMEMLIMIT=384MiB GOMAXPROCS=2
ExecStart=/usr/local/bin/otelcol-contrib --config=/opt/tidemq-lab/observe/collector.yaml
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=/srv/tidemq/collector-queue
MemoryMax=512M
LimitNOFILE=8192
[Install]
WantedBy=multi-user.target
EOF
  cat > /etc/systemd/system/tidemq-alert-audit.service <<'EOF'
[Unit]
Description=TideMQ lab local alert audit receiver
After=network-online.target
[Service]
User=tidemq
Group=tidemq
ExecStart=/usr/bin/node /opt/tidemq-lab/alert-audit.mjs
Restart=on-failure
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=/srv/tidemq/alert-audit
MemoryMax=128M
[Install]
WantedBy=multi-user.target
EOF
  cat > /etc/nginx/conf.d/tidemq-lab.conf <<'EOF'
server {
  listen 192.168.240.32:4318 ssl;
  listen 192.168.241.32:4318 ssl;
  server_name otlp.lab.tidemq.test;
  ssl_certificate /etc/tidemq-lab/server-tls/server.pem;
  ssl_certificate_key /etc/tidemq-lab/server-tls/server.key;
  ssl_client_certificate /etc/tidemq-lab/server-tls/ca.pem;
  ssl_verify_client on;
  ssl_protocols TLSv1.2 TLSv1.3;
  client_max_body_size 4m;
  location ~ ^/v1/(traces|logs|metrics)$ { proxy_pass http://127.0.0.1:24318; }
  location / { return 404; }
}
server {
  listen 18444 ssl default_server;
  server_name grafana.lab.tidemq.test;
  ssl_certificate /etc/tidemq-lab/server-tls/server.pem;
  ssl_certificate_key /etc/tidemq-lab/server-tls/server.key;
  ssl_protocols TLSv1.2 TLSv1.3;
  location / {
    proxy_set_header Host $http_host;
    proxy_set_header X-Forwarded-Proto https;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_pass http://127.0.0.1:3000;
  }
}
server {
  listen 18444 ssl;
  server_name metrics.lab.tidemq.test;
  ssl_certificate /etc/tidemq-lab/server-tls/server.pem;
  ssl_certificate_key /etc/tidemq-lab/server-tls/server.key;
  ssl_protocols TLSv1.2 TLSv1.3;
  auth_basic "TideMQ lab metrics";
  auth_basic_user_file /etc/tidemq-lab/metrics.htpasswd;
  location /loki/ { limit_except GET { deny all; } proxy_pass http://127.0.0.1:3100; }
  location /tempo/ { limit_except GET { deny all; } proxy_pass http://127.0.0.1:3200/; }
  location /alertmanager/ { limit_except GET { deny all; } proxy_pass http://127.0.0.1:9093/; }
  location / { limit_except GET { deny all; } proxy_set_header Host $http_host; proxy_pass http://127.0.0.1:9090; }
}
EOF
  docker compose -f /opt/tidemq-lab/observe/compose.yaml config --quiet
  docker compose -f /opt/tidemq-lab/observe/compose.yaml up -d --force-recreate
  systemctl daemon-reload
  systemctl enable --now tidemq-collector tidemq-alert-audit
  systemctl restart tidemq-collector tidemq-alert-audit
fi
nginx -t
systemctl enable --now nginx
systemctl reload nginx
printf 'Installed %s backends\n' "$role"
