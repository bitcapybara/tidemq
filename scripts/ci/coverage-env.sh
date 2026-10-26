#!/usr/bin/env bash
# Source this file once before an instrumented test lane.
set -euo pipefail
lane=$1
[[ "$lane" =~ ^(native|lab|full)$ ]] || exit 2
export CARGO_TARGET_DIR=${TIDEMQ_COVERAGE_TARGET_DIR:-$PWD/target/llvm-cov-target}
export CARGO_LLVM_COV_TARGET_DIR=$CARGO_TARGET_DIR
if [[ -f "$CARGO_TARGET_DIR/CACHEDIR.TAG" ]]; then
  cargo llvm-cov clean --workspace
elif [[ -d "$CARGO_TARGET_DIR" && -n $(find "$CARGO_TARGET_DIR" -mindepth 1 -maxdepth 1 -print -quit) ]]; then
  printf 'Refusing an unmarked nonempty coverage target directory\n' >&2; exit 1
fi
if [[ -d "$CARGO_TARGET_DIR" ]]; then
  find "$CARGO_TARGET_DIR" -maxdepth 1 -type f \( -name '*.profraw' -o -name '*.profdata' \) -delete
fi
eval "$(cargo llvm-cov show-env --sh)"
export LLVM_PROFILE_FILE="$CARGO_TARGET_DIR/tidemq-$lane-%p-%4m.profraw"
