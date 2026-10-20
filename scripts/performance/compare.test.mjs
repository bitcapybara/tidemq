import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {mkdtemp, mkdir, readFile, readdir, rm, writeFile} from 'node:fs/promises';
import {execFileSync, spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {comparePairs, summarizeReport, validatePolicy, workloadFor} from './compare.mjs';
import {management} from '../performance-gate.mjs';

const policy = validatePolicy(JSON.parse(readFileSync(new URL('../../performance/baseline.json', import.meta.url))));
const summary = multiplier => ({
  durable_messages_per_second: 1000 / multiplier,
  produce_p99_upper_ns: 1000000 * multiplier,
  offered_to_durable_ack_p99_upper_ns: 2000000 * multiplier,
});
const pairs = values => values.map(value => ({baseline: summary(1), candidate: summary(value)}));

test('a consistent one percent slowdown fails without a five percent allowance', () => {
  assert.equal(comparePairs(pairs([1.01, 1.01, 1.01, 1.01, 1.01]), policy).status, 'regressed');
});

test('stable improvement and exact equality pass', () => {
  assert.equal(comparePairs(pairs([0.98, 0.981, 0.98, 0.979, 0.98]), policy).status, 'passed');
  assert.equal(comparePairs(pairs([1, 1, 1, 1, 1]), policy).status, 'passed');
});

test('unstable samples are inconclusive and missing or invalid trials fail', () => {
  assert.equal(comparePairs(pairs([0.7, 1.2, 0.9, 1.3, 1]), policy).status, 'inconclusive');
  assert.throws(() => comparePairs(pairs([1, 1]), policy));
  assert.throws(() => comparePairs(pairs([1, 1, 0, 1, 1]), policy));
});

function fixture() {
  const histogram = {samples: 256, maximum_ns: 1024, histogram_buckets: [[320, 256]]};
  const phase = {
    terminal_error: null, planned_messages: 256, committed_messages: 256,
    acknowledged_messages: 256, durable_acknowledged_before: 256,
    produce_batch: histogram, produce_scheduling_lateness: histogram, offered_to_durable_ack: histogram, control_errors: 0,
  };
  const warmup = {...phase, planned_messages: 32, committed_messages: 32, acknowledged_messages: 32,
    durable_acknowledged_before: 32, produce_batch: {...histogram, samples: 32, histogram_buckets: [[320, 32]]},
    offered_to_durable_ack: {...histogram, samples: 32, histogram_buckets: [[320, 32]]},
    produce_scheduling_lateness: {...histogram, samples: 32, histogram_buckets: [[320, 32]]}};
  return {
    schema_version: 3, status: 'passed', configuration: policy.workload,
    identity: {build: {source_commit: policy.service_source_commit, allocator_feature: policy.allocator_feature}},
    warmup: {elapsed_ns: 1000, partitions: Array.from({length: 4}, () => structuredClone(warmup))},
    measured: {elapsed_ns: 2000000000, partitions: Array.from({length: 4}, () => structuredClone(phase))},
  };
}

test('histogram aggregation retains every partition and durability failures cannot look faster', () => {
  const report = fixture();
  const result = summarizeReport(report, policy, policy.service_source_commit);
  assert.equal(result.durable_messages_per_second, 512);
  assert.equal(result.produce_p99_upper_ns, 1024);
  report.measured.partitions[3].durable_acknowledged_before = 255;
  assert.throws(() => summarizeReport(report, policy, policy.service_source_commit));
});

test('a rare slow partition changes the merged p99 instead of being averaged away', () => {
  const report = fixture();
  report.measured.partitions[3].produce_batch = {samples: 256, maximum_ns: 4096, histogram_buckets: [[384, 256]]};
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
    const report = {status: 'passed', baseline_manifest_sha256: createHash('sha256').update(original).digest('hex'),
      baseline: {source_commit: policy.service_source_commit}, candidate: {source_commit: stale},
      topologies: Object.fromEntries(policy.topologies.map(topology => [topology, {pairs: pairs([0.98, 0.98, 0.98, 0.98, 0.98])}]))};
    const reportPath = path.join(temporary, 'report.json');
    const promote = () => spawnSync(process.execPath, [new URL('../performance-gate.mjs', import.meta.url).pathname,
      'promote', '--tools', temporary, '--report', reportPath], {encoding: 'utf8', timeout: 5000});
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
  assert.equal(workloadFor(policy, 'latency').produce_interval_ms, 160);
  assert.equal(workloadFor(policy, 'latency').warmup_messages, 64);
  assert.throws(() => workloadFor(policy, 'unsupported'));
});
