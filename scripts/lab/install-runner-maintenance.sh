#!/usr/bin/env bash
set -euo pipefail
[[ $EUID == 0 && $# == 1 && $(cat /etc/tidemq-lab-role) == runner ]] || exit 2
bundle=$1
if pgrep -u actions -f '[R]unner.Worker' >/dev/null; then
  printf 'Wait for the runner job before installing maintenance hooks\n' >&2; exit 1
fi
service=actions.runner.bitcapybara-tidemq.tidemq-dev-u126-8.service
restart=0
if systemctl is-active --quiet "$service"; then systemctl stop "$service"; restart=1; fi
trap '(( restart == 0 )) || systemctl start "$service"' EXIT
destination=/usr/local/lib/tidemq-lab
install -d -m 0755 "$destination/scripts/lab" "$destination/deploy/lab"
install -d -m 0700 -o actions -g actions /srv/tidemq/maintenance
for file in runner-maintenance.sh with-lease.sh cleanup-ci.sh guest.sh; do
  install -m 0755 "$bundle/scripts/lab/$file" "$destination/scripts/lab/$file"
done
install -m 0644 "$bundle/deploy/lab/inventory.json" "$bundle/deploy/lab/runner-maintenance.env" "$destination/deploy/lab/"
for phase in before after; do
  printf '#!/usr/bin/env bash\nset -euo pipefail\nexec timeout --signal=TERM --kill-after=30s 12m bash /usr/local/lib/tidemq-lab/scripts/lab/runner-maintenance.sh %s\n' "$phase" > "$destination/$phase.sh"
  chmod 0755 "$destination/$phase.sh"
done
env_file=/srv/tidemq/actions-runner/.env
sed -i '/^ACTIONS_RUNNER_HOOK_JOB_STARTED=/d; /^ACTIONS_RUNNER_HOOK_JOB_COMPLETED=/d' "$env_file"
printf '%s\n' 'ACTIONS_RUNNER_HOOK_JOB_STARTED=/usr/local/lib/tidemq-lab/before.sh' \
  'ACTIONS_RUNNER_HOOK_JOB_COMPLETED=/usr/local/lib/tidemq-lab/after.sh' >> "$env_file"
cat > /etc/systemd/system/tidemq-runner-maintenance.service <<'EOF'
[Unit]
Description=Clean rebuildable TideMQ runner resources when idle
After=network-online.target docker.service
[Service]
Type=oneshot
User=actions
Group=actions
SupplementaryGroups=docker
WorkingDirectory=/
UMask=0077
ExecStart=/usr/bin/bash /usr/local/lib/tidemq-lab/scripts/lab/runner-maintenance.sh idle
TimeoutStartSec=12min
EOF
cat > /etc/systemd/system/tidemq-runner-maintenance.timer <<'EOF'
[Unit]
Description=Bound TideMQ runner disk growth
[Timer]
OnBootSec=10min
OnCalendar=hourly
RandomizedDelaySec=2min
Persistent=true
[Install]
WantedBy=timers.target
EOF
systemctl daemon-reload
systemctl enable --now tidemq-runner-maintenance.timer
printf 'Installed runner hooks and hourly idle cleanup\n'
