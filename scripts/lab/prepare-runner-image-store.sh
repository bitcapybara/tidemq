#!/usr/bin/env bash
set -euo pipefail
[[ $EUID == 0 && $(cat /etc/tidemq-lab-role) == runner ]] || exit 2
mountpoint -q /srv/tidemq
if mountpoint -q /var/lib/containerd; then
  [[ $(findmnt -n -o FSROOT /var/lib/containerd) == /containerd ]] || exit 1
  exit
fi
[[ -z $(docker ps -q) ]] || { printf 'Stop runner build containers before moving the image store\n' >&2; exit 1; }
destination=/srv/tidemq/containerd
[[ ! -e "$destination" ]] || { printf 'The destination must be new; inspect an interrupted migration before retrying\n' >&2; exit 1; }
systemctl stop docker.service docker.socket containerd.service
install -d -m 0755 "$destination"
cp -a /var/lib/containerd/. "$destination/"
find /var/lib/containerd -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
mount --bind "$destination" /var/lib/containerd
printf '%s\n' '/srv/tidemq/containerd /var/lib/containerd none bind,x-systemd.requires-mounts-for=/srv/tidemq 0 0' >> /etc/fstab
systemctl start containerd.service docker.service
docker info --format '{{.DockerRootDir}}'
printf 'Runner Docker and containerd image storage use the dedicated data disk.\n'
