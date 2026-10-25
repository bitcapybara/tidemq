#!/usr/bin/env bash
set -euo pipefail
: "${VALIDATED_COMMIT:?}" "${GH_TOKEN:?}" "${GITHUB_RUN_ID:?}"
if [[ "$(git rev-parse HEAD)" != "$VALIDATED_COMMIT" ]]; then
  printf 'Main advanced during measurement; its own run will promote the next baseline\n'
  exit 0
fi
gh run list --workflow rust.yml --branch main --commit "$VALIDATED_COMMIT" --event push --limit 1 --json databaseId,status,conclusion > rust-run.json
rust_run_id="$(node -e 'const [run] = require("./rust-run.json"); if (!Number.isSafeInteger(run?.databaseId)) throw new Error("No Rust push run exists for the measured main commit"); console.log(run.databaseId)')"
gh run watch "$rust_run_id" --exit-status --interval 30
gh run download "$rust_run_id" --name coverage-report --dir covered
git fetch origin main
if [[ "$(git rev-parse origin/main)" != "$VALIDATED_COMMIT" ]]; then
  printf 'Main advanced while waiting for Rust validation; baselines remain unchanged\n'
  exit 0
fi
node scripts/promote-baselines.mjs . measured/summary.json covered/coverage-result/summary.json covered/coverage-result/llvm.json "$rust_run_id" "$GITHUB_RUN_ID"
git config user.name 'github-actions[bot]'
git config user.email '41898282+github-actions[bot]@users.noreply.github.com'
git add performance/baseline.json performance/baseline-report.json coverage/baseline.json
git diff --cached --quiet && exit 0
git commit -m 'chore(ci): record validated performance and coverage baselines'
git fetch origin main
if [[ "$(git rev-parse origin/main)" != "$VALIDATED_COMMIT" ]]; then
  printf 'Main advanced before baseline publication; the local baseline commit will not be pushed\n'
  exit 0
fi
git push origin HEAD:main
