import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync, spawnSync} from 'node:child_process';
import {mkdtemp, mkdir, readFile, rm, symlink, writeFile} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {compareCoverage, llvmCounts, validateCounts, validatePromotion} from '../coverage-gate.mjs';

const ROOT = new URL('../../', import.meta.url);
const policy = JSON.parse(await readFile(new URL('coverage/policy.json', ROOT)));
const perfPolicy = JSON.parse(await readFile(new URL('performance/baseline.json', ROOT)));
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const serialize = value => `${JSON.stringify(value, null, 2)}\n`;
const counts = (covered = 50, count = 100) => Object.fromEntries(['lines', 'functions', 'regions'].map(metric => [metric, {covered, count}]));
const llvm = totals => ({type: 'llvm.coverage.json.export', version: '3.1.0',
  data: [{files: [{filename: '/source/src/lib.rs'}], totals}]});

test('coverage compares exact fractions even below rounded percentages and with large products', () => {
  const old = counts(9_007_199_254_740_990, 9_007_199_254_740_991);
  const next = counts(9_007_199_254_740_989, 9_007_199_254_740_991);
  assert.equal(compareCoverage(old, next).status, 'regressed');
  assert.equal(compareCoverage(counts(1, 3), counts(2, 6)).status, 'passed');
  assert.equal(compareCoverage(counts(4, 6), counts(2, 3)).status, 'passed');
  for (const metric of ['lines', 'functions', 'regions']) {
    const candidate = counts(90);
    candidate[metric] = {covered: 49, count: 100};
    assert.equal(compareCoverage(counts(), candidate).metrics[metric].status, 'regressed');
  }
});

test('missing, empty, fabricated and noninteger LLVM counts fail closed', () => {
  for (const bad of [0, -1, 1.5, '50', Number.MAX_SAFE_INTEGER + 1]) {
    assert.throws(() => validateCounts(counts(bad)), /Invalid/);
  }
  assert.throws(() => validateCounts(counts(101)), /Invalid/);
  assert.throws(() => validateCounts({}), /Invalid/);
  assert.throws(() => llvmCounts({type: 'other'}), /LLVM coverage export/);
  assert.throws(() => llvmCounts({...llvm(counts()), version: '4.0'}), /Unsupported/);
  assert.throws(() => llvmCounts({...llvm(counts()), data: []}), /exactly one/);
  assert.throws(() => llvmCounts({...llvm(counts()), data: [llvm(counts()).data[0], llvm(counts()).data[0]]}), /exactly one/);
  assert.throws(() => llvmCounts({...llvm(counts()), data: [{files: [], totals: counts()}]}), /Missing LLVM file/);
  assert.deepEqual(llvmCounts(llvm(counts())), counts());
});

async function fixture() {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'tidemq-coverage-'));
  const git = (...args) => execFileSync('git', args, {cwd: directory, encoding: 'utf8'}).trim();
  const commit = () => {
    git('add', 'coverage', 'performance', 'deploy');
    git('-c', 'core.hooksPath=/dev/null', '-c', 'user.name=Coverage Test', '-c', 'user.email=coverage@example.invalid',
      'commit', '--no-gpg-sign', '-qm', 'Test source');
    return git('rev-parse', 'HEAD');
  };
  await mkdir(path.join(directory, 'coverage'));
  await mkdir(path.join(directory, 'performance'));
  await mkdir(path.join(directory, 'deploy/local'), {recursive: true});
  const generatorBytes = '#!/usr/bin/env bash\nexit 0\n';
  await writeFile(path.join(directory, 'deploy/local/generate.sh'), generatorBytes);
  const policyBytes = serialize(policy);
  const baseline = {schema_version: 1, status: 'accepted', source_commit: '1'.repeat(40),
    policy_sha256: sha(policyBytes), llvm_report_sha256: '4'.repeat(64), counts: counts(40)};
  const original = serialize(baseline);
  let perfOriginal = serialize(perfPolicy);
  await writeFile(path.join(directory, 'coverage/policy.json'), policyBytes);
  await writeFile(path.join(directory, 'coverage/baseline.json'), original);
  await writeFile(path.join(directory, 'performance/baseline.json'), perfOriginal);
  git('init', '-q', '--initial-branch=main');
  const baselineHead = commit();
  const localPerfPolicy = {...perfPolicy, service_source_commit: baselineHead};
  perfOriginal = serialize(localPerfPolicy);
  await writeFile(path.join(directory, 'performance/baseline.json'), perfOriginal);
  const head = commit();
  const raw = serialize(llvm(counts()));
  const summary = {schema_version: 1, status: 'passed', initialization: false, source_commit: head,
    policy_sha256: sha(policyBytes), previous_baseline_sha256: sha(original), llvm_report_sha256: sha(raw), counts: counts(),
    environment: {platform: 'linux', arch: 'x64', rustc: `rustc ${policy.rust_version} (test)`,
      nextest: `cargo-nextest ${policy.nextest_version} (test)`, llvm_cov: `cargo-llvm-cov ${policy.llvm_cov_version}`,
      github_run_id: '123'}};
  const reportPath = path.join(directory, 'llvm.json');
  const summaryPath = path.join(directory, 'summary.json');
  await writeFile(reportPath, raw);
  await writeFile(summaryPath, serialize(summary));
  const settings = {'--tools': directory, '--report': reportPath, '--summary': summaryPath, '--run-id': '123'};
  return {directory, git, commit, head, baselineHead, localPerfPolicy, generatorBytes, original, perfOriginal, raw, summary, reportPath, summaryPath, settings};
}

test('promotion binds source, policy, previous baseline, raw LLVM counts and successful Rust run', async () => {
  const f = await fixture();
  try {
    const accepted = await validatePromotion(f.settings);
    assert.deepEqual(accepted.value.counts, counts());
    assert.equal(accepted.value.source_commit, f.head);
    assert.equal(await readFile(path.join(f.directory, 'coverage/baseline.json'), 'utf8'), f.original);
    await assert.rejects(validatePromotion({...f.settings, '--run-id': '124'}), /another Rust run/);
    for (const [key, value, message] of [
      ['source_commit', '2'.repeat(40), /current clean main/],
      ['policy_sha256', '0'.repeat(64), /scope changed/],
      ['previous_baseline_sha256', '0'.repeat(64), /baseline changed/],
      ['llvm_report_sha256', '0'.repeat(64), /artifact changed/],
      ['counts', counts(99), /original LLVM counts/],
      ['initialization', true, /initialization state/],
      ['status', 'failed', /successful coverage/],
      ['environment', {...f.summary.environment, platform: 'darwin'}, /fixed platform/],
    ]) {
      await writeFile(f.summaryPath, serialize({...f.summary, [key]: value}));
      await assert.rejects(validatePromotion(f.settings), message);
    }
    await writeFile(f.summaryPath, serialize(f.summary));
    await writeFile(path.join(f.directory, 'coverage/policy.json'), `${serialize(policy)} `);
    await assert.rejects(validatePromotion(f.settings), /scope differs/);
  } finally { await rm(f.directory, {recursive: true, force: true}); }
});

test('a regular original LLVM artifact is required and a claimed pass cannot hide regression', async () => {
  const f = await fixture();
  try {
    await writeFile(path.join(f.directory, 'real.json'), f.raw);
    await rm(f.reportPath);
    await symlink(path.join(f.directory, 'real.json'), f.reportPath);
    await assert.rejects(validatePromotion(f.settings), /bounded regular/);
    await rm(f.reportPath);
    const regressed = serialize(llvm(counts(39)));
    await writeFile(f.reportPath, regressed);
    await writeFile(f.summaryPath, serialize({...f.summary, counts: counts(39), llvm_report_sha256: sha(regressed)}));
    await assert.rejects(validatePromotion(f.settings), /Recomputed coverage regressed/);
  } finally { await rm(f.directory, {recursive: true, force: true}); }
});

test('first initialization requires a real nonzero report and PRs cannot initialize or replace the baseline', async () => {
  const f = await fixture();
  try {
    const marker = serialize({schema_version: 1, status: 'uninitialized', initialization_parent: f.head});
    await writeFile(path.join(f.directory, 'coverage/baseline.json'), marker);
    const current = f.commit();
    await writeFile(f.summaryPath, serialize({...f.summary, initialization: true, source_commit: current,
      previous_baseline_sha256: sha(marker)}));
    assert.deepEqual((await validatePromotion(f.settings)).value.counts, counts());
    const run = extra => spawnSync(process.execPath, [new URL('../coverage-gate.mjs', import.meta.url).pathname,
      'compare', '--tools', f.directory, '--candidate', f.directory, '--report', f.reportPath,
      '--output', path.join(f.directory, 'candidate-summary.json'), ...extra], {encoding: 'utf8'});
    const pr = run(['--require-baseline-unchanged', '--allow-initialize']);
    assert.equal(pr.status, 1);
    assert.match(pr.stderr, /only an explicit first main cohort/);
    const other = await mkdtemp(path.join(os.tmpdir(), 'tidemq-coverage-candidate-'));
    try {
      await mkdir(path.join(other, 'coverage'));
      await writeFile(path.join(other, 'coverage/baseline.json'), '{}');
      execFileSync('git', ['clone', '-q', f.directory, path.join(other, 'clone')]);
      await writeFile(path.join(other, 'clone/coverage/baseline.json'), '{}');
      execFileSync('git', ['add', 'coverage/baseline.json'], {cwd: path.join(other, 'clone')});
      execFileSync('git', ['-c', 'core.hooksPath=/dev/null', '-c', 'user.name=Coverage Test', '-c', 'user.email=coverage@example.invalid',
        'commit', '--no-gpg-sign', '-qm', 'Attempt baseline replacement'], {cwd: path.join(other, 'clone')});
      const replaced = spawnSync(process.execPath, [new URL('../coverage-gate.mjs', import.meta.url).pathname,
        'compare', '--tools', f.directory, '--candidate', path.join(other, 'clone'), '--report', f.reportPath,
        '--output', path.join(other, 'out.json'), '--require-baseline-unchanged'], {encoding: 'utf8'});
      assert.equal(replaced.status, 1);
      assert.match(replaced.stderr, /cannot replace/);
    } finally { await rm(other, {recursive: true, force: true}); }
  } finally { await rm(f.directory, {recursive: true, force: true}); }
});

test('joint promotion leaves both baselines unchanged when either gate fails', async () => {
  const f = await fixture();
  try {
    const sample = {durable_messages_per_second: 1000, produce_mean_ns: 100, produce_p99_upper_ns: 200,
      offered_to_durable_ack_mean_ns: 300, offered_to_durable_ack_p99_upper_ns: 400};
    const pairs = Array.from({length: 8}, (_, pair) => ({pair, order: pair % 2 ? ['candidate', 'baseline'] : ['baseline', 'candidate'],
      baseline: sample, candidate: sample}));
    const perf = {schema_version: 4, status: 'passed', baseline_manifest_sha256: sha(f.perfOriginal), trusted_tools_commit: f.head,
      baseline: {source_commit: f.localPerfPolicy.service_source_commit, service_binary_sha256: '1'.repeat(64)},
      candidate: {source_commit: f.head, service_binary_sha256: '2'.repeat(64)},
      workload_binaries: {baseline: {binary_sha256: '3'.repeat(64)}, candidate: {binary_sha256: '4'.repeat(64)}},
      configuration_generators: {baseline: {source_commit: f.baselineHead, script_sha256: sha(f.generatorBytes)},
        candidate: {source_commit: f.head, script_sha256: sha(f.generatorBytes)}},
      environment: {github_run_id: '456'}, topologies: Object.fromEntries(perfPolicy.topologies.map(name => [name, {pairs: pairs.map(pair => {
        const effective = (name === 'rf1' ? [1] : [1, 2, 3]).map(node => ({node_id: node, configuration_sha256: '5'.repeat(64),
          memory: {total_bytes: 201326592}, protocol: {max_frame_bytes: 1048576}, capacity: {limit: 1}, retention: {limit: 1}, archive_interval_ms: 1000}));
        return {...pair, baseline: {...pair.baseline, workloads: {effective_configurations: effective}},
          candidate: {...pair.candidate, workloads: {effective_configurations: effective}}};
      })}]))};
    const perfReport = path.join(f.directory, 'perf.json');
    await writeFile(perfReport, serialize(perf));
    const run = () => spawnSync(process.execPath, [new URL('../promote-baselines.mjs', import.meta.url).pathname,
      f.directory, perfReport, f.summaryPath, f.reportPath, '123', '456'], {encoding: 'utf8'});
    const bad = serialize(llvm(counts(39)));
    await writeFile(f.reportPath, bad);
    await writeFile(f.summaryPath, serialize({...f.summary, counts: counts(39), llvm_report_sha256: sha(bad)}));
    const rejected = run();
    assert.equal(rejected.status, 1);
    assert.match(rejected.stderr, /Recomputed coverage regressed/);
    assert.equal(await readFile(path.join(f.directory, 'coverage/baseline.json'), 'utf8'), f.original);
    assert.equal(await readFile(path.join(f.directory, 'performance/baseline.json'), 'utf8'), f.perfOriginal);
    await writeFile(f.reportPath, f.raw);
    await writeFile(f.summaryPath, serialize(f.summary));
    const accepted = run();
    assert.equal(accepted.status, 0, accepted.stderr);
    assert.equal(JSON.parse(await readFile(path.join(f.directory, 'coverage/baseline.json'))).source_commit, f.head);
    assert.equal(JSON.parse(await readFile(path.join(f.directory, 'performance/baseline.json'))).service_source_commit, f.head);
    assert.deepEqual(JSON.parse(await readFile(path.join(f.directory, 'performance/baseline-report.json'))), perf);
  } finally { await rm(f.directory, {recursive: true, force: true}); }
});
