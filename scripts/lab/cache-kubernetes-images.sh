#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
cache="$lab_state/images/bootstrap-oci"
install -d -m 0755 "$cache/blobs/sha256"
version=$(jq -er .kubernetes.version "$lab_inventory")
lab_ssh tidemq-cp-a sudo kubeadm config images list --kubernetes-version="$version" > "$cache/references.txt"
accept='application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json'
printf '{"imageLayoutVersion":"1.0.0"}\n' > "$cache/oci-layout"
printf '' > "$cache/descriptors.jsonl"
while read -r image; do
  [[ "$image" =~ ^registry\.k8s\.io/[a-z0-9/.-]+:[a-zA-Z0-9.-]+$ ]] || exit 2
  reference=${image#registry.k8s.io/}
  repository=${reference%:*} tag=${reference##*:}
  base="https://registry.k8s.io/v2/$repository"
  curl -fsSL --retry 3 -H "Accept: $accept" "$base/manifests/$tag" -o "$cache/manifest.json"
  if jq -e '.manifests' "$cache/manifest.json" >/dev/null; then
    digest=$(jq -er '.manifests[] | select(.platform.os == "linux" and .platform.architecture == "amd64") | .digest' "$cache/manifest.json")
    curl -fsSL --retry 3 -H "Accept: $accept" "$base/manifests/$digest" -o "$cache/manifest.json"
    printf '%s  %s\n' "${digest#sha256:}" "$cache/manifest.json" | sha256sum --check --status
  fi
  digest="sha256:$(sha256sum "$cache/manifest.json" | cut -d ' ' -f 1)"
  size=$(stat -c %s "$cache/manifest.json")
  media=$(jq -er .mediaType "$cache/manifest.json")
  cp "$cache/manifest.json" "$cache/blobs/sha256/${digest#sha256:}"
  jq -nc --arg name "$image" --arg digest "$digest" --arg type "$media" --argjson size "$size" \
    '{mediaType:$type,digest:$digest,size:$size,platform:{os:"linux",architecture:"amd64"},annotations:{"io.containerd.image.name":$name,"org.opencontainers.image.ref.name":$name}}' >> "$cache/descriptors.jsonl"
  while read -r blob; do
    [[ "$blob" =~ ^sha256:[a-f0-9]{64}$ ]] || exit 2
    hash=${blob#sha256:}
    if [[ ! -f "$cache/blobs/sha256/$hash" ]]; then
      aria2c --continue=true --split=16 --max-connection-per-server=16 --min-split-size=1M \
        --file-allocation=none --console-log-level=warn --summary-interval=0 --download-result=hide \
        --dir="$cache/blobs/sha256" --out="$hash.part" "$base/blobs/$blob"
      printf '%s  %s\n' "$hash" "$cache/blobs/sha256/$hash.part" | sha256sum --check --status
      mv "$cache/blobs/sha256/$hash.part" "$cache/blobs/sha256/$hash"
    fi
    printf '%s  %s\n' "$hash" "$cache/blobs/sha256/$hash" | sha256sum --check --status
  done < <(jq -r '.config.digest,.layers[].digest' "$cache/manifest.json")
  printf 'Cached official Kubernetes image: %s\n' "$image"
done < "$cache/references.txt"
jq -s '{schemaVersion:2,mediaType:"application/vnd.oci.image.index.v1+json",manifests:.}' "$cache/descriptors.jsonl" > "$cache/index.json"
tar -cf "$lab_state/images/kubernetes-bootstrap.tar" -C "$cache" oci-layout index.json blobs
while read -r name; do
  lab_copy "$name" "$lab_state/images/kubernetes-bootstrap.tar" /tmp/tidemq-bootstrap-images.tar
  lab_ssh "$name" sudo ctr -n k8s.io images import --platform linux/amd64 /tmp/tidemq-bootstrap-images.tar
  lab_ssh "$name" rm /tmp/tidemq-bootstrap-images.tar
done < <(jq -r '.nodes[] | select(.role == "control-plane" or .role == "worker") | .name' "$lab_inventory")
printf 'Imported verified Kubernetes bootstrap images into all six nodes.\n'
