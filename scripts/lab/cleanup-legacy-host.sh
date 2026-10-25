#!/usr/bin/env bash
set -euo pipefail
umask 077
[[ $EUID == 0 && ${1:-} == --delete-authorized-legacy-data ]] || { printf 'Explicit legacy data deletion authorization is required\n' >&2; exit 2; }
[[ -z $(docker ps -q) ]] || { printf 'Refusing cleanup while a host Docker container is running\n' >&2; exit 1; }
state=/var/lib/tidemq-lab
[[ -d "$state/images" && -f "$state/images/runtime-base.tar" ]] || exit 1
audit="/root/tidemq-lab-cleanup-$(date +%Y%m%d)/authorized-deletion"
install -d -m 0700 "$audit"
df -B1 / > "$audit/disk-before.txt"
mapfile -t containers < <(docker ps -aq)
mapfile -t volumes < <(docker volume ls -q)
if (( ${#containers[@]} )); then
  docker rm --volumes "${containers[@]}" > "$audit/removed-containers.txt"
fi
if (( ${#volumes[@]} )); then
  for volume in "${volumes[@]}"; do
    if docker volume inspect "$volume" >/dev/null 2>&1; then docker volume rm "$volume" >> "$audit/removed-volumes.txt"; fi
  done
fi
base=$(jq -er .docker.runtimeBaseImageID "$state/inventory.json")
while read -r image; do
  [[ "$image" == "$base" ]] || docker image rm --force "$image" >> "$audit/removed-images.txt"
done < <(docker image ls -aq --no-trunc | sort -u)
docker builder prune --all --force > "$audit/builder-prune.txt"
docker network prune --force > "$audit/network-prune.txt"
legacy_directories=(
  /root/explorer-dashboard-1008 /root/taosdata /root/taosx-ab /root/taosx-agent-ha-validation
  /root/taosx-ci733953-20261022 /root/taosx-log-generation-7130692710
  /root/tidemq-codequality-repro-20261020-401c7c9 /root/tidemq-performance
  /root/tidemq-scalable-20261023 /root/tidemq-validation /root/tsdb-docker-stack
  /root/tsdb-nightly /root/yyx-free-license-20261012 /root/stressctl /root/cache
  /root/.apex /root/.kube /opt/explorer-review-2455 /opt/idmp-exam-20261010 /opt/taos-explorer-demo
)
for directory in "${legacy_directories[@]}"; do
  [[ ! -e "$directory" ]] || { printf '%s\n' "$directory" >> "$audit/removed-directories.txt"; rm -rf -- "$directory"; }
done
rm -f "$audit/private-container-inspect.json" "$audit/private-volume-inspect.json" /root/.taos_history
df -B1 / > "$audit/disk-after-docker.txt"
printf 'Deleted authorized legacy services, data, source, configurations and reports; retained current lab resources.\n'
