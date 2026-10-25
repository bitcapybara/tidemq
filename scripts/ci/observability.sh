#!/usr/bin/env bash
set -euo pipefail
umask 077
[[ $# == 2 && "$1" =~ ^ci-[0-9]+-[0-9]+$ ]] || { printf 'Usage: observability.sh ci-run-attempt output-directory\n' >&2; exit 2; }
namespace=$1 output=$2
lab_home=${TIDEMQ_LAB_HOME:?}
mkdir -p "$output"
config=$(mktemp "$lab_home/query.XXXXXX")
trap 'rm -f "$config"' EXIT
printf 'user = "reader:%s"\n' "$(cat "$lab_home/metrics-password")" > "$config"
query() {
  curl --fail --silent --show-error --max-time 15 --cacert "$lab_home/ca.pem" --config "$config" --get "https://metrics.lab.tidemq.test:18444/$1" --data-urlencode "$2"
}
for ((attempt=0; attempt<60; attempt++)); do
  query api/v1/query "query=count(up{k8s_namespace_name=\"$namespace\"} == 1)" > "$output/metrics.json"
  query loki/api/v1/query_range "query={service_namespace=\"tidemq-lab\"} | k8s_namespace_name=\"$namespace\" | k8s_container_name=\"tidemq\"" > "$output/logs.json"
  query tempo/api/search "q={ resource.service.name = \"tidemq\" && resource.k8s.namespace.name = \"$namespace\" }" > "$output/traces.json"
  if jq -e '.status == "success" and (.data.result[0].value[1] | tonumber) >= 10' "$output/metrics.json" >/dev/null \
    && jq -e '.status == "success" and (.data.result | length) > 0' "$output/logs.json" >/dev/null \
    && jq -e '(.traces | length) > 0' "$output/traces.json" >/dev/null; then
    printf 'Verified stored application metrics, logs and traces for %s.\n' "$namespace"
    exit
  fi
  sleep 2
done
printf 'Application telemetry did not become queryable within its bounded wait\n' >&2
exit 1
