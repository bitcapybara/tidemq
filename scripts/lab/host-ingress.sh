#!/usr/bin/env bash
set -euo pipefail
[[ $EUID == 0 ]] || exit 1
inventory=/etc/tidemq-lab/inventory.json
host=$(jq -er .host "$inventory")
[[ "$host" == 192.168.126.8 ]] || exit 2
for table_chain in 'nat TIDEMQ_LAB_DNAT' 'nat TIDEMQ_LAB_SNAT' 'filter TIDEMQ_LAB_FORWARD'; do
  read -r table chain <<< "$table_chain"
  iptables -w -t "$table" -N "$chain" 2>/dev/null || true
  iptables -w -t "$table" -F "$chain"
done
for entry in 'nat PREROUTING TIDEMQ_LAB_DNAT' 'nat OUTPUT TIDEMQ_LAB_DNAT' 'nat POSTROUTING TIDEMQ_LAB_SNAT' 'filter FORWARD TIDEMQ_LAB_FORWARD'; do
  read -r table source target <<< "$entry"
  iptables -w -t "$table" -C "$source" -j "$target" 2>/dev/null || iptables -w -t "$table" -I "$source" 1 -j "$target"
done
while read -r cidr; do
  [[ "$cidr" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[0-9]+$ ]] || exit 2
  for mapping in 'tcp 18443 192.168.241.31 18443' 'tcp 18445 192.168.241.31 18445' 'tcp 4318 192.168.240.32 4318' 'tcp 18444 192.168.240.32 18444' 'udp 17443 192.168.241.200 7443'; do
    read -r protocol public destination private <<< "$mapping"
    iptables -w -t nat -A TIDEMQ_LAB_DNAT -s "$cidr" -d "$host" -p "$protocol" --dport "$public" -j DNAT --to-destination "$destination:$private"
    iptables -w -A TIDEMQ_LAB_FORWARD -s "$cidr" -d "$destination" -p "$protocol" --dport "$private" -m conntrack --ctstate NEW,ESTABLISHED -j ACCEPT
    iptables -w -A TIDEMQ_LAB_FORWARD -s "$destination" -d "$cidr" -p "$protocol" --sport "$private" -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
    iptables -w -t nat -A TIDEMQ_LAB_SNAT -s "$cidr" -d "$destination" -p "$protocol" --dport "$private" -j MASQUERADE
  done
done < <(jq -r '.allowedClientCIDRs[]' "$inventory")
printf 'Applied LAN ingress to the explicitly listed lab services\n'
