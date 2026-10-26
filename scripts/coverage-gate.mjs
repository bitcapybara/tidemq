#!/usr/bin/env node
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import * as fs from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {validateApiCoverage} from './coverage/api.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const METRICS = ['lines', 'functions', 'regions'];
const TEST_STAGES = ['unit', 'sdk-default-unit', 'integration', 'runtime-faults', 'external-services'];
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
const git = (directory, args) => execFileSync('git', args, {cwd: directory, encoding: 'utf8'}).trim();
const save = (file, value) => fs.writeFile(file, `${JSON.stringify(value, null, 2)}\n`, {mode: 0o600});

async function read(file) {
  const stat = await fs.lstat(file);
  assert.ok(stat.isFile() && stat.size > 0 && stat.size <= 128 * 1024 * 1024, 'Expected a bounded regular coverage file');
  const bytes = await fs.readFile(file);
  return {bytes, value: JSON.parse(bytes)};
}

export function validateCounts(counts) {
  for (const metric of METRICS) {
    const {count, covered} = counts?.[metric] ?? {};
    assert.ok(Number.isSafeInteger(count) && count > 0, `Invalid ${metric} count`);
    assert.ok(Number.isSafeInteger(covered) && covered > 0 && covered <= count, `Invalid ${metric} covered count`);
  }
  return Object.fromEntries(METRICS.map(metric => [metric, {count: counts[metric].count, covered: counts[metric].covered}]));
}

export function llvmCounts(report, expectedVersion = '3.1.0') {
  assert.equal(report.type, 'llvm.coverage.json.export', 'Expected an LLVM coverage export');
  assert.equal(report.version, expectedVersion, 'Unsupported LLVM coverage export version');
  assert.equal(report.data?.length, 1, 'Expected exactly one LLVM coverage data set');
  assert.ok(report.data[0].files?.length > 0, 'Missing LLVM file coverage');
  return validateCounts(report.data[0].totals);
}

export function compareCoverage(baseline, candidate) {
  validateCounts(baseline);
  validateCounts(candidate);
  const metrics = Object.fromEntries(METRICS.map(metric => {
    const old = baseline[metric];
    const next = candidate[metric];
    const passed = BigInt(next.covered) * BigInt(old.count) >= BigInt(old.covered) * BigInt(next.count);
    return [metric, {status: passed ? 'passed' : 'regressed', baseline: old, candidate: next}];
  }));
  return {status: Object.values(metrics).every(metric => metric.status === 'passed') ? 'passed' : 'regressed', metrics};
}

async function inputs(tools) {
  const policy = await read(path.join(tools, 'coverage/policy.json'));
  assert.equal(policy.value.schema_version, 1, 'Unsupported coverage policy schema');
  assert.equal(policy.value.platform, 'linux', 'Coverage baseline must use Linux');
  assert.equal(policy.value.arch, 'x64', 'Coverage baseline must use the fixed Linux runner architecture');
  assert.equal(policy.value.include_build_script, true, 'Coverage must include workspace build scripts');
  assert.equal(policy.value.llvm_export_version, '3.1.0', 'Unexpected fixed-toolchain LLVM export schema');
  assert.deepEqual(policy.value.features, ['tidemq-sdk/metrics', 'tidemq-sdk/opentelemetry', 'tidemq-protocol/codegen'],
    'Coverage must include optional SDK instrumentation and protocol code generation');
  assert.deepEqual(policy.value.metrics, METRICS, 'Coverage policy must compare all three metrics');
  const baseline = await read(path.join(tools, 'coverage/baseline.json'));
  assert.equal(baseline.value.schema_version, 1, 'Unsupported coverage baseline schema');
  if (baseline.value.status === 'uninitialized') {
    assert.match(baseline.value.initialization_parent ?? '', /^[0-9a-f]{40}$/, 'Invalid initial coverage parent');
    assert.ok(!('counts' in baseline.value), 'An uninitialized baseline cannot contain fabricated counts');
  } else {
    assert.equal(baseline.value.status, 'accepted', 'Coverage baseline is not accepted');
    assert.match(baseline.value.source_commit ?? '', /^[0-9a-f]{40}$/, 'Invalid accepted coverage source');
    assert.match(baseline.value.llvm_report_sha256 ?? '', /^[0-9a-f]{64}$/, 'Invalid accepted LLVM report digest');
    assert.equal(baseline.value.policy_sha256, digest(policy.bytes), 'Accepted coverage scope differs from the protected policy');
    validateCounts(baseline.value.counts);
  }
  return {policy, baseline};
}

function cleanHead(directory) {
  git(directory, ['diff', '--exit-code']);
  git(directory, ['diff', '--cached', '--exit-code']);
  const head = git(directory, ['rev-parse', 'HEAD']);
  assert.match(head, /^[0-9a-f]{40}$/, 'Invalid coverage source');
  return head;
}

function validateEnvironment(policy, measured) {
  assert.equal(measured?.platform, policy.platform, 'Coverage must be measured on the fixed platform');
  assert.equal(measured.arch, policy.arch, 'Coverage architecture differs from policy');
  assert.ok(measured.rustc?.startsWith(`rustc ${policy.rust_version} `), 'Coverage Rust version differs from policy');
  assert.ok(measured.nextest?.startsWith(`cargo-nextest ${policy.nextest_version} `), 'Coverage nextest version differs from policy');
  assert.equal(measured.llvm_cov, `cargo-llvm-cov ${policy.llvm_cov_version}`, 'Coverage llvm-cov version differs from policy');
  return measured;
}

function environment(policy) {
  const version = args => execFileSync('cargo', args, {encoding: 'utf8'}).trim();
  const rustc = execFileSync('rustc', ['--version'], {encoding: 'utf8'}).trim();
  const nextest = version(['nextest', '--version']);
  const llvmCov = version(['llvm-cov', '--version']);
  return validateEnvironment(policy, {platform: process.platform, arch: process.arch, rustc, nextest, llvm_cov: llvmCov,
    github_run_id: process.env.GITHUB_RUN_ID ?? null, github_job: process.env.GITHUB_JOB ?? null});
}

export function validateMeasurement(measured, policy, sourceCommit, raw, runId) {
  assert.equal(measured.schema_version, 1, 'Unsupported coverage measurement schema');
  assert.equal(measured.source_commit, sourceCommit, 'Coverage profiles belong to another source commit');
  assert.equal(measured.llvm_report_sha256, digest(raw), 'Measured LLVM artifact changed');
  assert.ok(Number.isSafeInteger(measured.profile_count) && measured.profile_count > 0, 'Missing test profiles');
  assert.deepEqual(Object.keys(measured.test_reports ?? {}).sort(), [...TEST_STAGES].sort(), 'Missing preceding test stages');
  for (const hash of Object.values(measured.test_reports)) assert.match(hash, /^[0-9a-f]{64}$/, 'Invalid test report digest');
  assert.match(measured.merge_inputs_sha256 ?? '', /^[0-9a-f]{64}$/, 'Missing LLVM merge input digest');
  const apiCount = validateApiCoverage(measured.api_coverage, sourceCommit, runId);
  assert.ok(measured.profile_count > apiCount, 'Missing native profiles alongside deployed API profiles');
  validateEnvironment(policy, measured.environment);
  if (runId) assert.equal(measured.environment.github_run_id, runId, 'Coverage artifact is from another Rust run');
  return measured.environment;
}

async function record(settings) {
  const {policy} = await inputs(ROOT);
  const raw = await read(path.resolve(settings['--report']));
  llvmCounts(raw.value, policy.value.llvm_export_version);
  const testReports = {};
  for (const stage of TEST_STAGES) {
    const bytes = await fs.readFile(path.join(ROOT, `target/ci/${stage}-junit.xml`));
    assert.ok(bytes.length > 0, `Missing ${stage} test results`);
    testReports[stage] = digest(bytes);
  }
  const api = (await read(path.join(ROOT, 'target/ci/api-coverage.json'))).value;
  const sourceCommit = cleanHead(ROOT);
  validateApiCoverage(api, sourceCommit, process.env.GITHUB_RUN_ID);
  const mergeInputs = await fs.readFile(path.resolve(settings['--merge-inputs']));
  const merged = new Set(mergeInputs.toString('utf8').trim().split('\n'));
  assert.equal(merged.size, Number(settings['--profile-count']), 'LLVM did not merge every collected profile');
  for (const profile of [...api.nodes.flatMap(node => node.profiles), ...api.client_profiles]) {
    const file = path.join(process.env.CARGO_LLVM_COV_TARGET_DIR, profile.file);
    assert.ok(merged.has(file), 'LLVM omitted a deployed API profile');
    const stat = await fs.lstat(file);
    assert.ok(stat.isFile() && stat.size === profile.bytes, 'API profile was lost before final export');
    assert.equal(digest(await fs.readFile(file)), profile.sha256, 'API profile changed before final export');
  }
  const measured = {schema_version: 1, source_commit: sourceCommit, llvm_report_sha256: digest(raw.bytes),
    profile_count: Number(settings['--profile-count']), merge_inputs_sha256: digest(mergeInputs),
    test_reports: testReports, api_coverage: api, environment: environment(policy.value)};
  validateMeasurement(measured, policy.value, measured.source_commit, raw.bytes, process.env.GITHUB_RUN_ID);
  await save(path.resolve(settings['--output']), measured);
  await fs.writeFile(path.join(path.dirname(path.resolve(settings['--output'])), 'profile-inputs.txt'), mergeInputs, {mode: 0o600});
}

async function comparison(settings) {
  const tools = path.resolve(settings['--tools'] ?? ROOT);
  const candidate = path.resolve(settings['--candidate'] ?? tools);
  const {policy, baseline} = await inputs(tools);
  const sourceCommit = cleanHead(candidate);
  if (settings['--require-baseline-unchanged']) {
    const proposed = await fs.readFile(path.join(candidate, 'coverage/baseline.json'));
    assert.deepEqual(proposed, baseline.bytes, 'PRs cannot replace the protected coverage baseline');
  }
  const initializing = baseline.value.status === 'uninitialized';
  if (initializing) {
    assert.ok(settings['--allow-initialize'] && !settings['--require-baseline-unchanged'],
      'Coverage baseline is uninitialized; only an explicit first main cohort may initialize it');
    git(candidate, ['merge-base', '--is-ancestor', baseline.value.initialization_parent, sourceCommit]);
  }
  const raw = await read(path.resolve(settings['--report']));
  const measured = (await read(path.resolve(settings['--measurement']))).value;
  const measuredEnvironment = validateMeasurement(measured, policy.value, sourceCommit, raw.bytes, settings['--run-id']);
  const mergeInputs = await fs.readFile(path.join(path.dirname(path.resolve(settings['--measurement'])), 'profile-inputs.txt'));
  assert.equal(digest(mergeInputs), measured.merge_inputs_sha256, 'LLVM merge inputs changed after export');
  const counts = llvmCounts(raw.value, policy.value.llvm_export_version);
  const result = initializing ? {status: 'passed', metrics: null} : compareCoverage(baseline.value.counts, counts);
  const summary = {schema_version: 1, ...result, initialization: initializing, source_commit: sourceCommit,
    policy_sha256: digest(policy.bytes), previous_baseline_sha256: digest(baseline.bytes),
    llvm_report_sha256: digest(raw.bytes), counts, environment: measuredEnvironment,
    api_profile_count: validateApiCoverage(measured.api_coverage, sourceCommit, settings['--run-id']),
    api_server_count: measured.api_coverage.nodes.length, api_checks: measured.api_coverage.api_checks};
  await save(path.resolve(settings['--output']), summary);
  console.log(`Coverage gate: ${summary.status}${initializing ? ' (initial measurement pending promotion)' : ''}`);
  assert.equal(summary.status, 'passed', 'Coverage decreased; add meaningful tests before accepting this source');
}

export async function validatePromotion(settings) {
  const tools = path.resolve(settings['--tools'] ?? ROOT);
  const {policy, baseline} = await inputs(tools);
  const report = (await read(path.resolve(settings['--summary']))).value;
  const raw = await read(path.resolve(settings['--report']));
  assert.equal(report.schema_version, 1, 'Unsupported coverage summary schema');
  assert.equal(report.status, 'passed', 'Only successful coverage may advance the baseline');
  assert.equal(report.source_commit, cleanHead(tools), 'Coverage source must match the current clean main HEAD');
  assert.equal(report.policy_sha256, digest(policy.bytes), 'Protected coverage scope changed');
  assert.equal(report.previous_baseline_sha256, digest(baseline.bytes), 'Previous coverage baseline changed');
  assert.equal(report.llvm_report_sha256, digest(raw.bytes), 'LLVM coverage artifact changed');
  const counts = llvmCounts(raw.value, policy.value.llvm_export_version);
  assert.deepEqual(report.counts, counts, 'Coverage summary differs from original LLVM counts');
  validateEnvironment(policy.value, report.environment);
  if (settings['--run-id']) assert.equal(report.environment.github_run_id, settings['--run-id'], 'Coverage artifact is from another Rust run');
  assert.equal(report.initialization, baseline.value.status === 'uninitialized', 'Coverage initialization state differs');
  if (!report.initialization) assert.equal(compareCoverage(baseline.value.counts, counts).status, 'passed', 'Recomputed coverage regressed');
  else git(tools, ['merge-base', '--is-ancestor', baseline.value.initialization_parent, report.source_commit]);
  return {file: path.join(tools, 'coverage/baseline.json'), value: {schema_version: 1, status: 'accepted',
    source_commit: report.source_commit, policy_sha256: report.policy_sha256,
    llvm_report_sha256: report.llvm_report_sha256, counts, environment: report.environment}};
}

async function main() {
  const [action, ...args] = process.argv.slice(2);
  const settings = {};
  while (args.length) {
    const key = args.shift();
    assert.match(key, /^--[a-z][a-z0-9-]*$/, 'Invalid coverage gate option');
    assert.ok(!(key in settings), 'Duplicate coverage gate option');
    const value = ['--allow-initialize', '--require-baseline-unchanged'].includes(key) ? true : args.shift();
    assert.ok(value && !String(value).startsWith('--'), `Missing ${key} value`);
    settings[key] = value;
  }
  if (action === 'record') await record(settings);
  else if (action === 'compare') await comparison(settings);
  else if (action === 'promote') {
    const baseline = await validatePromotion(settings);
    await save(baseline.file, baseline.value);
    console.log(`Advanced coverage baseline to ${baseline.value.source_commit}`);
  } else throw new Error('Usage: coverage-gate.mjs record|compare|promote [named paths]');
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => {console.error(error.message); process.exitCode = 1;});
}
