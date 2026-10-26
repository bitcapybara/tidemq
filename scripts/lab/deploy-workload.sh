#!/usr/bin/env bash
set -euo pipefail
[[ $# == 5 ]] || { printf 'Usage: deploy-workload.sh namespace image-build-directory s3-endpoint credentials-file new-private-output\n' >&2; exit 2; }
namespace=$1 build=$2 endpoint=$3 credentials=$4 output=$5
[[ "$namespace" =~ ^(ci-[a-z0-9-]+|tidemq-dev)$ && ${#namespace} -le 50 ]] || exit 2
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
export KUBECONFIG="${TIDEMQ_LAB_HOME:?}/kubeconfig"
context=$(kubectl config current-context)
image=$(cat "$build/image.txt")
sha256sum --check --status "$build/sha256.txt"
index=0
for suffix in a b c; do
  worker="tidemq-worker-$suffix"
  index=$((index + 1))
  for role in metadata storage; do
    id=$((10 + index))
    [[ "$role" != storage ]] || id=$((20 + index))
    lab_ssh "$worker" sudo bash -c 'set -euo pipefail; mountpoint -q "/var/lib/tidemq/$1"; available=$(df --output=avail -B1 "/var/lib/tidemq/$1" | tail -n 1); (( available > 1073741824 )); install -d -m 0700 -o 10001 -g 10001 "/var/lib/tidemq/$1/$2/node-$3"' -- "$role" "$namespace" "$id"
  done
  if [[ ${TIDEMQ_API_COVERAGE:-0} == 1 ]]; then
    [[ "$namespace" =~ ^ci-[0-9]+-[0-9]+$ ]] || exit 2
    ids=$(node --input-type=module -e 'import {roles} from "./scripts/lab/workload.mjs"; console.log(roles.filter(node => node.worker === process.argv[1]).map(node => node.id).join(" "))' "$worker")
    lab_ssh "$worker" sudo bash -c 'set -euo pipefail; for id in $2; do install -d -m 0700 -o 10001 -g 10001 "/var/lib/tidemq/coverage/$1/node-$id"; done' -- "$namespace" "$ids"
  fi
  lab_copy "$worker" "$build/image.tar" /tmp/tidemq-workload-image.tar
  lab_ssh "$worker" sudo ctr -n k8s.io images import --platform linux/amd64 /tmp/tidemq-workload-image.tar > "$lab_state/logs/$namespace-$suffix-image-import.log" 2>&1
  lab_ssh "$worker" rm /tmp/tidemq-workload-image.tar
done
node "$lab_repo/scripts/lab/workload.mjs" "$context" "$namespace" "$image" "$endpoint" "$credentials" "$output"
kubectl --context "$context" -n "$namespace" wait pod --for=condition=Ready --selector=app=tidemq --timeout=5m
kubectl --context "$context" -n "$namespace" get pods -o wide
