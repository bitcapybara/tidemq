#!/usr/bin/env node
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
import path from 'node:path';
import {validatePromotion as performancePromotion} from './performance-gate.mjs';
import {validatePromotion as coveragePromotion} from './coverage-gate.mjs';
import {validateConfigurationEvidence} from './performance/configuration.mjs';

const [tools, performanceReport, coverageSummary, coverageReport, rustRunId, performanceRunId] = process.argv.slice(2);
assert.ok(tools && performanceReport && coverageSummary && coverageReport,
  'Usage: promote-baselines.mjs tools performance-report coverage-summary llvm-report rust-run-id performance-run-id');
assert.match(rustRunId ?? '', /^[0-9]+$/, 'Missing validated Rust run identity');
assert.match(performanceRunId ?? '', /^[0-9]+$/, 'Missing validated performance run identity');
// Validate both reports against the same clean HEAD before changing either baseline.
const performance = await performancePromotion({'--tools': tools, '--report': performanceReport});
assert.equal(performance.report.environment?.github_run_id, performanceRunId, 'Performance artifact is from another run');
assert.equal(performance.report.trusted_tools_commit, performance.report.candidate.source_commit, 'Promotion tools differ from current main');
const generator = source => ({source_commit: source, script_sha256: createHash('sha256')
  .update(execFileSync('git', ['show', `${source}:deploy/local/generate.sh`], {cwd: performance.tools})).digest('hex')});
validateConfigurationEvidence(performance.report, {baseline: generator(performance.report.baseline.source_commit),
  candidate: generator(performance.report.trusted_tools_commit)}, performance.policy.topologies);
const coverage = await coveragePromotion({'--tools': tools, '--summary': coverageSummary, '--report': coverageReport, '--run-id': rustRunId});
const save = (file, value) => writeFile(file, `${JSON.stringify(value, null, 2)}\n`, {mode: 0o600});
await save(path.join(performance.tools, 'performance/baseline.json'), performance.policy);
await save(path.join(performance.tools, 'performance/baseline-report.json'), performance.report);
await save(coverage.file, coverage.value);
console.log(`Validated performance and coverage baselines for ${coverage.value.source_commit}`);
