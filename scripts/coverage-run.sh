#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 2 ]]; then
  printf 'Usage: coverage-run.sh candidate-directory output-directory [coverage-gate-options]\n' >&2
  exit 2
fi
tools_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
candidate_dir="$(cd -- "$1" && pwd)"
mkdir -p -- "$2"
report_dir="$(cd -- "$2" && pwd)"
shift 2
features="$(node -e 'console.log(require(process.argv[1]).features.join(","))' "$tools_dir/coverage/policy.json")"
cd -- "$candidate_dir"

# Instrumented dependencies stay reusable; only old workspace profiles are cleared.
export CARGO_LLVM_COV_TARGET_DIR="${CARGO_LLVM_COV_TARGET_DIR:-$candidate_dir/target/llvm-cov-target}"
cargo llvm-cov clean --workspace
cargo llvm-cov nextest --workspace --all-targets --locked --features "$features" \
  --config-file "$tools_dir/.config/nextest.toml" --profile ci --no-report
cargo llvm-cov report --workspace --include-build-script --locked --json --summary-only --output-path "$report_dir/llvm.json"
cargo llvm-cov report --workspace --include-build-script --locked --lcov --output-path "$report_dir/lcov.info"
cargo llvm-cov report --workspace --include-build-script --locked --html --output-dir "$report_dir/html"
node "$tools_dir/scripts/coverage-gate.mjs" compare --tools "$tools_dir" --candidate "$candidate_dir" \
  --report "$report_dir/llvm.json" --output "$report_dir/summary.json" "$@"
