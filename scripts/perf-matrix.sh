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
    bash "$root/scripts/perf-run.sh" "$builds/$allocator" "$output/$allocator-$scenario-recommended-lanes4" "$scenario" recommended 4 || status=1
  done
done
# Keep observability comparisons on the same System binary and identical small-message workload.
for mode in minimal detailed; do
  bash "$root/scripts/perf-run.sh" "$builds/system" "$output/system-small-$mode-lanes4" small "$mode" 4 || status=1
done
# Change only Request lane count; reuse the same System binary and fixed memory limits.
for scenario in small replay; do
  bash "$root/scripts/perf-run.sh" "$builds/system" "$output/system-$scenario-recommended-lanes2" "$scenario" recommended 2 || status=1
done
# Optional exporter cost is isolated from the original three log/metric modes.
bash "$root/scripts/perf-run.sh" "$builds/system" "$output/system-small-recommended-otlp-lanes4" small recommended-otlp 4 || status=1
exit "$status"
