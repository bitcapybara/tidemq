#!/usr/bin/env bash
set -euo pipefail
umask 077
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
bundle="$lab_state/private/agent-bundle"
install -d -m 0700 "$bundle"
cp "$lab_state/tools/collector" "$bundle/collector"
cp "$lab_state/private/tls/ca.pem" "$bundle/ca.pem"
cp "$lab_repo/deploy/lab/agent.yaml" "$bundle/agent.yaml"
while IFS=$'\t' read -r name role address; do
  cp "$lab_state/private/tls/$name.pem" "$lab_state/private/tls/$name.key" "$bundle/"
  tar -czf "$lab_state/private/agent-bundle.tar.gz" -C "$bundle" collector ca.pem agent.yaml "$name.pem" "$name.key"
  lab_copy "$name" "$lab_state/private/agent-bundle.tar.gz" /tmp/tidemq-agent.tar.gz
  lab_copy "$name" "$lab_repo/scripts/lab/install-agent.sh" /tmp/tidemq-install-agent.sh
  lab_ssh "$name" bash -c 'install -d -m 0700 /tmp/tidemq-agent; tar -xzf /tmp/tidemq-agent.tar.gz -C /tmp/tidemq-agent'
  lab_ssh "$name" sudo bash /tmp/tidemq-install-agent.sh "$name" "$role" "$address" /tmp/tidemq-agent > "$lab_state/logs/$name-agent.log" 2>&1
  lab_ssh "$name" bash -c 'rm -rf /tmp/tidemq-agent /tmp/tidemq-agent.tar.gz'
  rm "$bundle/$name.pem" "$bundle/$name.key"
done < <(jq -r '.nodes[] | [.name,.role,.managementIP] | @tsv' "$lab_inventory")
