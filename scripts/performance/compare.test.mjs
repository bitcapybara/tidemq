import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {mkdtemp, mkdir, readFile, readdir, rm, writeFile} from 'node:fs/promises';
import {execFileSync, spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {comparePairs, summarizeReport, validatePolicy, weightedMeanLatency, workloadFor, HARNESS_FILES} from './compare.mjs';
import {management, canReuseBuilds, reuseBuild} from '../performance-gate.mjs';

const policy = validatePolicy(JSON.parse(readFileSync(new URL('../../performance/baseline.json', import.meta.url))));
const summary = multiplier => ({
  durable_messages_per_second: 1000 / multiplier,
  produce_mean_ns: 500000 * multiplier,
  produce_p99_upper_ns: 1000000 * multiplier,
  offered_to_durable_ack_mean_ns: 1000000 * multiplier,
  offered_to_durable_ack_p99_upper_ns: 2000000 * multiplier,
});
const pairs = values => values.map(value => ({baseline: summary(1), candidate: summary(value)}));

test('a consistent one percent slowdown fails without a five percent allowance', () => {
  assert.equal(comparePairs(pairs(Array(policy.pairs).fill(1.01)), policy).status, 'regressed');
  for (const metric of Object.keys(summary(1))) {
    const isolated = pairs(Array(policy.pairs).fill(1));
    for (const pair of isolated) pair.candidate[metric] *= metric === 'durable_messages_per_second' ? 1 / 1.01 : 1.01;
    const result = comparePairs(isolated, policy);
    assert.equal(result.status, 'regressed', metric);
    assert.equal(result.metrics[metric].status, 'regressed', metric);
  }
});

test('stable improvement and exact equality pass', () => {
  assert.equal(comparePairs(pairs([0.98, 0.981, 0.98, 0.979, 0.98, 0.981, 0.979, 0.98]), policy).status, 'passed');
  assert.equal(comparePairs(pairs(Array(policy.pairs).fill(1)), policy).status, 'passed');
});

test('unstable samples are inconclusive and missing or invalid trials fail', () => {
  assert.equal(comparePairs(pairs([0.7, 1.2, 0.9, 1.3, 1, 0.8, 1.4, 0.9]), policy).status, 'inconclusive');
  assert.throws(() => comparePairs(pairs([1, 1]), policy));
  assert.throws(() => comparePairs(pairs([1, 1, 0, 1, 1, 1, 1, 1]), policy));
  assert.throws(() => comparePairs(pairs(Array(5).fill(1)), policy), /Missing paired trials/);
  assert.throws(() => validatePolicy({...policy, schema_version: 2}), /Unsupported performance policy schema/);
  assert.throws(() => validatePolicy({...policy, pairs: 5}), /requires eight pairs/);
});

test('eight paired log ratios use the Student t critical value with seven degrees of freedom', () => {
  const values = [0.99, 1, 1.01, 1.02, 0.98, 1, 1.01, 0.99];
  const ratios = values.map(Math.log);
  const mean = ratios.reduce((sum, value) => sum + value, 0) / values.length;
  const deviation = Math.sqrt(ratios.reduce((sum, value) => sum + (value - mean) ** 2, 0) / 7);
  const result = comparePairs(pairs(values), policy);
  assert.equal(Object.keys(result.metrics).length, 5);
  assert.ok(Math.abs(result.metrics.produce_mean_ns.log_ci_half_width - 2.3646242515927844 * deviation / Math.sqrt(8)) < 1e-14);
});

test('raw latency means weight unequal partition samples and retain rare long durations', () => {
  assert.equal(weightedMeanLatency([
    {samples: 1, mean_ns: 1000, maximum_ns: 1000},
    {samples: 3, mean_ns: 100, maximum_ns: 100},
  ]), 325);
  assert.equal(weightedMeanLatency([
    {samples: 999, mean_ns: 1000, maximum_ns: 1000},
    {samples: 1, mean_ns: 1000000, maximum_ns: 1000000},
  ]), 1999);
  for (const mean_ns of [null, NaN, Infinity, -1, 1025]) {
    assert.throws(() => weightedMeanLatency([{samples: 1, mean_ns, maximum_ns: 1024}]));
  }
  assert.throws(() => weightedMeanLatency([]));
  assert.throws(() => weightedMeanLatency([{samples: 0, mean_ns: 1, maximum_ns: 1}]));
});

function fixture(scenario = 'throughput') {
  const workload = workloadFor(policy, scenario);
  const measured = workload.messages;
  const warmupCount = workload.warmup_messages;
  const histogram = {samples: measured, mean_ns: 1024, maximum_ns: 1024, histogram_buckets: [[320, measured]]};
  const phase = {
    terminal_error: null, planned_messages: measured, committed_messages: measured,
    acknowledged_messages: measured, durable_acknowledged_before: measured,
    produce_batch: structuredClone(histogram), produce_scheduling_lateness: structuredClone(histogram),
    offered_to_durable_ack: structuredClone(histogram), control_errors: 0,
  };
  const warmup = {...phase, planned_messages: warmupCount, committed_messages: warmupCount, acknowledged_messages: warmupCount,
    durable_acknowledged_before: warmupCount, produce_batch: {...histogram, samples: warmupCount, histogram_buckets: [[320, warmupCount]]},
    offered_to_durable_ack: {...histogram, samples: warmupCount, histogram_buckets: [[320, warmupCount]]},
    produce_scheduling_lateness: {...histogram, samples: warmupCount, histogram_buckets: [[320, warmupCount]]}};
  return {
    schema_version: 3, status: 'passed', configuration: workload,
    identity: {build: {source_commit: policy.service_source_commit, allocator_feature: policy.allocator_feature}},
    warmup: {elapsed_ns: 1000, partitions: Array.from({length: workload.concurrent_partitions}, () => structuredClone(warmup))},
    measured: {elapsed_ns: 2000000000, partitions: Array.from({length: workload.concurrent_partitions}, () => structuredClone(phase))},
  };
}

test('histogram aggregation retains every partition and durability failures cannot look faster', () => {
  const report = fixture();
  const result = summarizeReport(report, policy, policy.service_source_commit);
  assert.equal(result.durable_messages_per_second, 512);
  assert.equal(result.produce_mean_ns, 1024);
  assert.equal(result.produce_p99_upper_ns, 1024);
  report.measured.partitions[3].durable_acknowledged_before = 255;
  assert.throws(() => summarizeReport(report, policy, policy.service_source_commit));
});

test('a rare slow partition changes the merged p99 instead of being averaged away', () => {
  const report = fixture();
  report.measured.partitions[3].produce_batch = {samples: 256, mean_ns: 4096, maximum_ns: 4096, histogram_buckets: [[384, 256]]};
  assert.equal(summarizeReport(report, policy, policy.service_source_commit).produce_p99_upper_ns, 4096);
  report.measured.partitions[3].produce_batch.histogram_buckets = [[384, 255]];
  assert.throws(() => summarizeReport(report, policy, policy.service_source_commit));
});

test('RF3 combined generation creates exactly three full-role quorum members', async () => {
  const temporary = await mkdtemp(path.join(os.tmpdir(), 'tidemq-perf-config-'));
  const cluster = path.join(temporary, 'cluster');
  try {
    execFileSync('bash', [new URL('../../deploy/local/generate.sh', import.meta.url).pathname, 'rf3-combined', cluster], {timeout: 10000});
    const configs = (await readdir(cluster)).filter(name => /^node-\d+\.toml$/.test(name));
    assert.equal(configs.length, 3);
    for (const node of [1, 2, 3]) {
      const text = await readFile(path.join(cluster, `node-${node}.toml`), 'utf8');
      assert.match(text, /role = 'combined'/);
      assert.match(text, /replication_factor = 3/);
      assert.match(text, /metadata_voters = \[1,2,3\]/);
      assert.match(text, /voters = \[1,2,3\]/);
      assert.equal((text.match(/\[\[runtime.peers\]\]/g) ?? []).length, 3);
      assert.equal(new Set([...text.matchAll(/zone = '([^']+)'/g)].map(match => match[1])).size, 3);
    }
  } finally { await rm(temporary, {recursive: true, force: true}); }
});

test('management enforces an absolute deadline when a peer keeps sending data', {timeout: 4000}, async context => {
  const server = http.createServer((request, response) => {
    response.writeHead(200);
    const ticker = setInterval(() => response.write('x'), 50);
    request.once('close', () => clearInterval(ticker));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const get = http.get.bind(http);
  context.mock.method(http, 'get', (_url, settings, callback) =>
    get(`http://127.0.0.1:${server.address().port}/metrics`, settings, callback));
  const started = performance.now();
  try {
    await assert.rejects(management(1, '/metrics'), /Management absolute deadline expired/);
    assert.ok(performance.now() - started < 2500, 'Slow response extended the total request budget');
  } finally { await new Promise(resolve => server.close(resolve)); }
});

test('promotion rejects a stale report or dirty source and accepts only the clean current HEAD', async () => {
  const temporary = await mkdtemp(path.join(os.tmpdir(), 'tidemq-perf-promote-'));
  const git = (...args) => execFileSync('git', args, {cwd: temporary, encoding: 'utf8'}).trim();
  const commit = () => git('-c', 'core.hooksPath=/dev/null', '-c', 'user.name=Performance Test',
    '-c', 'user.email=perf-test@example.invalid', 'commit', '--no-gpg-sign', '-qm', 'Test source');
  try {
    await mkdir(path.join(temporary, 'performance'));
    const original = `${JSON.stringify(policy, null, 2)}\n`;
    await writeFile(path.join(temporary, 'performance/baseline.json'), original);
    git('init', '-q', '--initial-branch=main');
    git('add', 'performance/baseline.json');
    commit();
    const stale = git('rev-parse', 'HEAD');
    await writeFile(path.join(temporary, 'source.txt'), 'current\n');
    git('add', 'source.txt');
    commit();
    const current = git('rev-parse', 'HEAD');
    const report = {schema_version: 3, status: 'passed', baseline_manifest_sha256: createHash('sha256').update(original).digest('hex'),
      baseline: {source_commit: policy.service_source_commit}, candidate: {source_commit: stale},
      topologies: Object.fromEntries(policy.topologies.map(topology => [topology, {pairs: pairs(Array(policy.pairs).fill(0.98))}]))};
    const reportPath = path.join(temporary, 'report.json');
    const promote = () => spawnSync(process.execPath, [new URL('../performance-gate.mjs', import.meta.url).pathname,
      'promote', '--tools', temporary, '--report', reportPath], {encoding: 'utf8', timeout: 5000});
    await writeFile(reportPath, JSON.stringify({...report, schema_version: 2}));
    const previousSchema = promote();
    assert.equal(previousSchema.status, 1);
    assert.match(previousSchema.stderr, /Unsupported performance gate report schema/);
    await writeFile(reportPath, JSON.stringify(report));
    const staleResult = promote();
    assert.equal(staleResult.status, 1);
    assert.match(staleResult.stderr, /Promotion candidate must match the current clean tools HEAD/);
    assert.equal(await readFile(path.join(temporary, 'performance/baseline.json'), 'utf8'), original);
    report.candidate.source_commit = current;
    await writeFile(reportPath, JSON.stringify(report));
    await writeFile(path.join(temporary, 'source.txt'), 'dirty\n');
    assert.equal(promote().status, 1);
    assert.equal(await readFile(path.join(temporary, 'performance/baseline.json'), 'utf8'), original);
    await writeFile(path.join(temporary, 'source.txt'), 'current\n');
    const accepted = promote();
    assert.equal(accepted.status, 0, accepted.stderr);
    assert.equal(JSON.parse(await readFile(path.join(temporary, 'performance/baseline.json'), 'utf8')).service_source_commit, current);
  } finally { await rm(temporary, {recursive: true, force: true}); }
});


test('latency uses the protected fixed schedule and throughput remains unpaced', () => {
  assert.equal(workloadFor(policy, 'throughput').produce_interval_ms, 0);
  assert.equal(workloadFor(policy, 'throughput').concurrent_partitions, 4);
  assert.equal(workloadFor(policy, 'latency').produce_interval_ms, 40);
  assert.equal(workloadFor(policy, 'latency').warmup_messages, 256);
  assert.equal(workloadFor(policy, 'latency').concurrent_partitions, 1);
  assert.throws(() => workloadFor(policy, 'unsupported'));
});

test('A/A calibration binds both binaries and never promotes its source', async () => {
  const temporary = await mkdtemp(path.join(os.tmpdir(), 'tidemq-perf-calibrate-'));
  try {
    const report = fixture('latency');
    report.identity.service_binary_blake3 = '1'.repeat(64);
    report.identity.generator_binary_blake3 = '2'.repeat(64);
    const inputs = [];
    for (let index = 0; index < policy.pairs * 2; index++) {
      const input = path.join(temporary, `${index}.json`);
      await writeFile(input, JSON.stringify(report));
      inputs.push(input);
    }
    const output = path.join(temporary, 'calibration.json');
    const cli = (file) => spawnSync(process.execPath, [new URL('./calibrate.mjs', import.meta.url).pathname,
      file, ...inputs], {encoding: 'utf8', timeout: 5000});
    const accepted = cli(output);
    assert.equal(accepted.status, 0, accepted.stderr);
    const result = JSON.parse(await readFile(output, 'utf8'));
    assert.equal(result.kind, 'same_binary_scheduled_latency_calibration');
    assert.equal(result.schema_version, 2);
    assert.equal(result.status, 'passed');
    assert.ok(!('durable_messages_per_second' in result.metrics));
    assert.equal(Object.keys(result.metrics).length, 4);
    assert.deepEqual(result.pairs.map(pair => pair.order), Array.from({length: policy.pairs}, (_, pair) =>
      pair % 2 === 0 ? ['baseline', 'candidate'] : ['candidate', 'baseline']));
    for (const [label, maximum, bucket] of [['faster', 512, 288], ['slower', 2048, 352]]) {
      const biased = structuredClone(report);
      for (const field of ['produce_batch', 'offered_to_durable_ack']) {
        biased.measured.partitions[0][field] = {
          samples: 1024, mean_ns: maximum, maximum_ns: maximum, histogram_buckets: [[bucket, 1024]],
        };
      }
      for (let pair = 0; pair < policy.pairs; pair++) {
        await writeFile(inputs[pair * 2 + (pair % 2 === 0 ? 1 : 0)], JSON.stringify(biased));
      }
      const biasedOutput = path.join(temporary, `${label}.json`);
      assert.equal(cli(biasedOutput).status, 0);
      assert.equal(JSON.parse(await readFile(biasedOutput, 'utf8')).status, 'temporal_bias_detected');
    }
    report.identity.generator_binary_blake3 = '3'.repeat(64);
    await writeFile(inputs[inputs.length - 1], JSON.stringify(report));
    assert.equal(cli(path.join(temporary, 'invalid.json')).status, 1);
  } finally { await rm(temporary, {recursive: true, force: true}); }
});

test('latency validates its own single-partition counts and all 1024 observations', () => {
  const report = fixture('latency');
  const result = summarizeReport(report, policy, policy.service_source_commit, 'latency');
  assert.equal(result.durable_messages, 1024);
  assert.equal(result.produce_mean_ns, 1024);
  assert.equal(result.produce_p99_upper_ns, 1024);
  assert.throws(() => summarizeReport(report, policy, policy.service_source_commit, 'throughput'));
  report.measured.partitions.push(structuredClone(report.measured.partitions[0]));
  assert.throws(() => summarizeReport(report, policy, policy.service_source_commit, 'latency'));
});

test('latency means use raw message durations without subtracting lateness or using group throughput', () => {
  const report = fixture('latency');
  report.measured.partitions[0].produce_batch.mean_ns = 900.25;
  report.measured.partitions[0].offered_to_durable_ack.mean_ns = 901.75;
  report.measured.partitions[0].produce_scheduling_lateness.mean_ns = 512;
  const result = summarizeReport(report, policy, policy.service_source_commit, 'latency');
  assert.equal(result.produce_mean_ns, 900.25);
  assert.equal(result.offered_to_durable_ack_mean_ns, 901.75);
  assert.notEqual(result.produce_mean_ns, report.measured.elapsed_ns / result.durable_messages);
  report.measured.partitions[0].produce_batch.mean_ns = null;
  assert.throws(() => summarizeReport(report, policy, policy.service_source_commit, 'latency'));
});

test('build reuse requires the same source and every protected harness hash', () => {
  const baseline = {source_commit: '1'.repeat(40),
    harness_files_sha256: Object.fromEntries(HARNESS_FILES.map(file => [file, '2'.repeat(64)]))};
  assert.equal(canReuseBuilds(baseline, structuredClone(baseline)), true);
  assert.equal(canReuseBuilds(baseline, {...baseline, source_commit: '3'.repeat(40)}), false);
  const changed = structuredClone(baseline);
  changed.harness_files_sha256[HARNESS_FILES[0]] = '4'.repeat(64);
  assert.equal(canReuseBuilds(baseline, changed), false);
  assert.equal(canReuseBuilds(baseline, {source_commit: baseline.source_commit}), false);
});

test('same-source reuse copies actual service and load binaries with their original identities', async () => {
  const temporary = await mkdtemp(path.join(os.tmpdir(), 'tidemq-perf-reuse-'));
  try {
    for (const binary of ['tidemq', 'perf_load']) {
      const source = path.join(temporary, `baseline-${binary}`);
      const destination = path.join(temporary, `candidate-${binary}`);
      await mkdir(source);
      const bytes = Buffer.from(`Exact ${binary} fixture binary\n`);
      const hash = createHash('sha256').update(bytes).digest('hex');
      const identity = {source_commit: '1'.repeat(40), [binary === 'tidemq' ? 'service_binary_sha256' : 'binary_sha256']: hash};
      await writeFile(path.join(source, binary), bytes);
      await writeFile(path.join(source, 'identity.json'), JSON.stringify(identity));
      assert.deepEqual(await reuseBuild(source, destination, binary, identity), identity);
      assert.deepEqual(await readFile(path.join(destination, binary)), bytes);
      assert.deepEqual(JSON.parse(await readFile(path.join(destination, 'identity.json'), 'utf8')), identity);
      await writeFile(path.join(source, binary), 'Changed fixture');
      await assert.rejects(reuseBuild(source, path.join(temporary, `invalid-${binary}`), binary, identity), /Reusable build binary changed/);
    }
  } finally { await rm(temporary, {recursive: true, force: true}); }
});
