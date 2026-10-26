#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" != 1 || ${CARGO_LLVM_COV_SHOW_ENV:-0} != 1 ]]; then
  printf 'Usage: run validate-and-integrate.sh with TIDEMQ_COVERAGE=1 to collect and report coverage\n' >&2
  exit 2
fi
mkdir -p -- "$1"
report_dir="$(cd -- "$1" && pwd)"
profile_count=$(find "$CARGO_LLVM_COV_TARGET_DIR" -maxdepth 1 -type f -name '*.profraw' | wc -l)
(( profile_count > 0 )) || { printf 'No profiles from the preceding tests\n' >&2; exit 1; }

# Reporting consumes existing binaries and profiles; it never builds or runs tests.
cargo llvm-cov report --workspace --include-build-script --locked --json --summary-only --output-path "$report_dir/llvm.json"
cargo llvm-cov report --workspace --include-build-script --locked --lcov --output-path "$report_dir/lcov.info"
cargo llvm-cov report --workspace --include-build-script --locked --html --output-dir "$report_dir/html"
node scripts/coverage-gate.mjs record --report "$report_dir/llvm.json" \
  --output "$report_dir/measurement.json" --profile-count "$profile_count"
