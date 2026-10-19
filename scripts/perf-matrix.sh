#!/usr/bin/env bash
set -euo pipefail
umask 077

builds=${1:?Usage: perf-matrix.sh fixed-build-directory new-results-directory}
output=${2:?A new results directory is required}
root=$(cd "$(dirname "$0")/.." && pwd -P)
if [[ -e "$output" ]]; then printf 'Matrix output already exists\n' >&2; exit 1; fi
mkdir -p "$output"
status=0
for allocator in system mimalloc jemalloc; do
  for scenario in small large slow replay; do
    bash "$root/scripts/perf-run.sh" "$builds/$allocator" "$output/$allocator-$scenario-recommended" "$scenario" recommended || status=1
  done
done
# Keep observability comparisons on the same System binary and identical small-message workload.
for mode in minimal detailed; do
  bash "$root/scripts/perf-run.sh" "$builds/system" "$output/system-small-$mode" small "$mode" || status=1
done
exit "$status"
