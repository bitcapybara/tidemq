#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
[[ $EUID == 0 ]] || exit 1
id tidemq-ci >/dev/null 2>&1 || useradd --create-home --shell /bin/bash tidemq-ci
install -d -m 0755 /etc/tidemq-lab
install -m 0644 "$lab_inventory" /etc/tidemq-lab/inventory.json
install -m 0755 "$lab_repo/scripts/lab/host-control.sh" /usr/local/sbin/tidemq-lab-control
"/usr/local/sbin/tidemq-lab-control" --selfcheck
for name in ci-guest ci-host; do
  if [[ ! -f "$lab_state/private/$name" ]]; then ssh-keygen -q -t ed25519 -N '' -C "tidemq-$name" -f "$lab_state/private/$name"; fi
done
install -d -m 0700 -o tidemq-ci -g tidemq-ci /home/tidemq-ci/.ssh
printf 'restrict,command="sudo -n /usr/local/sbin/tidemq-lab-control" %s\n' "$(cat "$lab_state/private/ci-host.pub")" > /home/tidemq-ci/.ssh/authorized_keys
chmod 0600 /home/tidemq-ci/.ssh/authorized_keys
chown tidemq-ci:tidemq-ci /home/tidemq-ci/.ssh/authorized_keys
cat > /etc/sudoers.d/tidemq-lab-control <<'EOF'
Defaults:tidemq-ci env_keep += "SSH_ORIGINAL_COMMAND"
tidemq-ci ALL=(root) NOPASSWD: /usr/local/sbin/tidemq-lab-control
EOF
chmod 0440 /etc/sudoers.d/tidemq-lab-control
visudo -cf /etc/sudoers.d/tidemq-lab-control
while read -r name; do
  lab_copy "$name" "$lab_state/private/ci-guest.pub" /tmp/tidemq-ci.pub
  lab_ssh "$name" bash -c 'key=$(cat /tmp/tidemq-ci.pub); grep -qF "$key" ~/.ssh/authorized_keys || printf "%s\n" "$key" >> ~/.ssh/authorized_keys; rm /tmp/tidemq-ci.pub'
done < <(jq -r '.nodes[] | select(.role == "control-plane" or .role == "worker" or .role == "fixtures" or .role == "loadgen") | .name' "$lab_inventory")
printf 'Installed restricted host fault control; public infrastructure is excluded.\n'
