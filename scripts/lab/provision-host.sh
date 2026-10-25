#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)
inventory="$repo_root/deploy/lab/inventory.json"
[[ $EUID == 0 ]] || { printf 'Host provisioning requires root.\n' >&2; exit 1; }
for tool in node jq virsh qemu-img cloud-localds ssh-keygen curl sha256sum fallocate; do
  command -v "$tool" >/dev/null || { printf 'Missing host tool: %s\n' "$tool" >&2; exit 1; }
done
[[ -c /dev/kvm ]] || { printf 'KVM acceleration is unavailable.\n' >&2; exit 1; }
state=$(jq -er .stateDirectory "$inventory")
[[ "$state" == /var/lib/tidemq-lab ]] || exit 1
install -d -m 0755 "$state" "$state/images"
install -d -m 0700 "$state/private" "$state/generated" "$state/logs"
if [[ ! -f "$state/private/id_ed25519" ]]; then
  ssh-keygen -q -t ed25519 -N '' -C tidemq-lab-management -f "$state/private/id_ed25519"
fi
node "$repo_root/scripts/lab/render.mjs" "$inventory" "$state/generated" "$state/private/id_ed25519.pub" > "$state/generated/budget.json"
cp -- "$inventory" "$state/inventory.json"
image="$state/images/ubuntu-noble-20261017.qcow2"
digest=$(jq -er .image.sha256 "$inventory")
if [[ ! -f "$image" ]]; then
  curl -fL --proto '=https' --retry 3 --connect-timeout 15 --max-time 1800 --output "$image.part" "$(jq -er .image.url "$inventory")"
  printf '%s  %s\n' "$digest" "$image.part" | sha256sum --check --status
  mv -- "$image.part" "$image"
fi
printf '%s  %s\n' "$digest" "$image" | sha256sum --check --status
chmod 0444 "$image"
[[ $(qemu-img info --output=json "$image" | jq -r .format) == qcow2 ]] || exit 1

if ! virsh list --all --name | grep -q '^tidemq-'; then
  disk_gib=$(jq -r .diskGiB "$state/generated/budget.json")
  reserve_gib=$(jq -r .minimumHostFreeGiB "$inventory")
  available=$(df --output=avail -B1 "$state" | tail -n 1 | tr -d ' ')
  (( available >= (disk_gib + reserve_gib + 1) * 1024 * 1024 * 1024 )) || { printf 'Insufficient disk capacity for the bounded lab.\n' >&2; exit 1; }
  memory_kib=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
  required_mib=$(jq -r .memoryMiB "$state/generated/budget.json")
  memory_reserve=$(jq -r .minimumHostMemoryReserveMiB "$inventory")
  (( memory_kib >= (required_mib + memory_reserve) * 1024 )) || { printf 'Insufficient memory capacity for the lab.\n' >&2; exit 1; }
fi

for type in management data; do
  network=$(jq -r ".networks.$type.name" "$inventory")
  if ! virsh net-info "$network" >/dev/null 2>&1; then
    virsh net-define "$state/generated/network-$type.xml"
  fi
  virsh net-autostart "$network"
  if ! virsh net-info "$network" | grep -Eq '^Active:.*yes'; then virsh net-start "$network"; fi
done

while IFS= read -r encoded; do
  node=$(printf '%s' "$encoded" | base64 --decode)
  name=$(jq -r .name <<< "$node")
  if virsh dominfo "$name" >/dev/null 2>&1; then
    virsh dumpxml "$name" | grep -Fq '<description>Managed by TideMQ lab tidemq-dev</description>' || { printf 'Refusing an unowned domain: %s\n' "$name" >&2; exit 1; }
  else
    root_gib=$(jq -r .rootGiB <<< "$node")
    qemu-img create -f qcow2 -F qcow2 -b "$image" "$state/images/$name-root.qcow2" "${root_gib}G"
    while IFS=$'\t' read -r disk size; do
      [[ -n "$disk" ]] || continue
      fallocate -l "${size}G" "$state/images/$name-$disk.raw"
    done < <(jq -r '.disks[] | [.name,.sizeGiB] | @tsv' <<< "$node")
    cloud-localds --network-config="$state/generated/$name-network.yaml" "$state/images/$name-seed.iso" "$state/generated/$name-user.yaml" "$state/generated/$name-meta.yaml"
    chown libvirt-qemu:kvm "$state/images/$name-"*
    chmod 0600 "$state/images/$name-"*
    virsh define "$state/generated/$name-domain.xml"
  fi
  virsh autostart "$name"
  if [[ $(virsh domstate "$name" | tr -d '\r\n') != running ]]; then virsh start "$name"; fi
done < <(jq -r '.nodes[] | @base64' "$inventory")

printf 'Created or verified all managed TideMQ virtual machines.\n'
virsh list --all
printf 'Management key and private state: %s/private\n' "$state"
