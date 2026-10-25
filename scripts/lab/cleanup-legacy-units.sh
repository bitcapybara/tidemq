#!/usr/bin/env bash
set -euo pipefail
units=(explorer-demo-scenarios.service taos-explorer-demo.service taos-explorer.service
  taosadapter.service taosd.service taoskeeper.service taosx.service
  stressctl-cpu.service stressctl-io.service stressctl-memory.service stressctl.target)
if [[ ${1:-} == --selfcheck ]]; then
  for unit in "${units[@]}"; do
    [[ "$unit" =~ ^(explorer-demo-scenarios|taos[-a-z]*|stressctl[-a-z]*)\.(service|target)$ ]] || exit 1
    [[ "$unit" != tidemq-* ]] || exit 1
  done
  printf 'Legacy unit allowlist excludes the current lab and operating system units.\n'
  exit
fi
[[ $EUID == 0 && ${1:-} == --delete-authorized-legacy-data ]] || { printf 'Explicit legacy data deletion authorization is required\n' >&2; exit 2; }
[[ -f /var/lib/tidemq-lab/inventory.json && -d /var/lib/tidemq-lab/images ]] || { printf 'The current lab must exist before legacy cleanup\n' >&2; exit 1; }
for unit in "${units[@]}"; do
  systemctl stop "$unit" 2>/dev/null || true
  systemctl disable "$unit" 2>/dev/null || true
  rm -f -- "/etc/systemd/system/$unit" "/usr/local/lib/systemd/system/$unit"
  rm -rf -- "/etc/systemd/system/$unit.d"
  find /etc/systemd/system -type l -name "$unit" -delete
done
if [[ -f /root/.config/systemd/user/kache.service ]]; then
  XDG_RUNTIME_DIR=/run/user/0 systemctl --user disable --now kache.service 2>/dev/null || true
  find /root/.config/systemd/user -type l -name kache.service -delete
  rm -f /root/.config/systemd/user/kache.service
  XDG_RUNTIME_DIR=/run/user/0 systemctl --user daemon-reload 2>/dev/null || true
fi
rm -rf -- /etc/taos /var/lib/taos /var/log/taos /usr/local/taos /var/tmp/stressctl-io
rm -f /usr/local/sbin/stressctl /etc/default/stressctl /etc/default/taos{d,adapter,x,-explorer} /tmp/taosx-175603-metadata.json
find /usr/bin -maxdepth 1 \( -name 'taos*' -o -name rmtaos \) \( -type f -o -type l \) -delete
find /usr/lib -maxdepth 1 -name 'libtaos*' \( -type f -o -type l \) -delete
systemctl daemon-reload
systemctl reset-failed
ldconfig
for unit in "${units[@]}"; do
  [[ $(systemctl show "$unit" --property=LoadState --value) == not-found ]] || { printf 'Legacy unit still exists: %s\n' "$unit" >&2; exit 1; }
done
systemctl is-active --quiet libvirtd.service ssh.service
printf 'Removed legacy system and user units, enablement links, application binaries, data, configuration and logs.\n'
