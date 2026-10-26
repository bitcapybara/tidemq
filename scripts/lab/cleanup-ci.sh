#!/usr/bin/env bash
set -euo pipefail
[[ $# == 1 && "$1" =~ ^ci-[0-9]+-[0-9]+$ && ${TIDEMQ_LAB_LEASE_HELD:-} == 1 ]] || {
  printf 'Usage: with-lease.sh bash cleanup-ci.sh ci-RUN-ATTEMPT\n' >&2; exit 2;
}
run=$1
export TIDEMQ_LAB_STATE=${TIDEMQ_LAB_HOME:?}
export TIDEMQ_LAB_KEY="$TIDEMQ_LAB_HOME/ci-guest"
export TIDEMQ_LAB_KNOWN_HOSTS="$TIDEMQ_LAB_HOME/known_hosts"
export KUBECONFIG="$TIDEMQ_LAB_HOME/kubeconfig"
source "$(dirname -- "${BASH_SOURCE[0]}")/guest.sh"
for vm in tidemq-cp-{a,b,c} tidemq-worker-{a,b,c} tidemq-fixtures tidemq-loadgen; do
  ssh -n -F "$TIDEMQ_LAB_HOME/ssh_config" lab-host "restore $vm 5"
done
namespace=$(kubectl --request-timeout=15s get namespace "$run" --ignore-not-found -o json)
if [[ -n "$namespace" ]]; then
  jq -e '.metadata.labels["tidemq.lab/ci"] == "true"' <<< "$namespace" >/dev/null || {
    printf 'Refusing to delete a namespace without the CI ownership label\n' >&2; exit 1;
  }
fi
# Validate every volume before deleting the namespace or touching its local data.
kubectl --request-timeout=15s get pv -o json | jq -e --arg run "$run" '
  [.items[] | select(.spec.claimRef.namespace == $run)] | all(.[];
    (.metadata.name | test("^" + $run + "-node-(11|12|13|21|22|23)$")) and
    (.spec.local.path == ("/var/lib/tidemq/" +
      (if (.metadata.name | test("-node-1[123]$")) then "metadata" else "storage" end) +
      "/" + $run + "/node-" + (.metadata.name | split("-node-")[1]))))' >/dev/null
for id in 11 12 13 21 22 23; do
  claim=$(kubectl --request-timeout=15s get pv "$run-node-$id" --ignore-not-found -o json)
  [[ -z "$claim" ]] || jq -e --arg run "$run" '.spec.claimRef.namespace == $run' <<< "$claim" >/dev/null
done
kubectl delete namespace "$run" --ignore-not-found --wait=true --timeout=2m
for id in 11 12 13 21 22 23; do kubectl delete pv "$run-node-$id" --ignore-not-found --timeout=30s; done
for suffix in a b c; do
  lab_ssh "tidemq-worker-$suffix" sudo rm -rf -- "/var/lib/tidemq/storage/$run" "/var/lib/tidemq/metadata/$run" "/var/lib/tidemq/coverage/$run" /tmp/tidemq-workload-image.tar
done
lab_ssh tidemq-fixtures sudo /usr/local/sbin/tidemq-fixtures remove "$run"
lab_ssh tidemq-loadgen pkill -TERM -f "^/srv/tidemq/validation/$run/deploy_validation( |$)" || [[ $? == 1 ]]
lab_ssh tidemq-loadgen sudo rm -rf -- "/srv/tidemq/validation/$run"
rm -rf -- "$TIDEMQ_LAB_HOME/runs/$run"
printf 'Removed disposable CI resources: %s\n' "$run"
