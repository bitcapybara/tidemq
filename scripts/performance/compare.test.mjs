import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {mkdtemp, readFile, readdir, rm} from 'node:fs/promises';
import {execFileSync} from 'node:child_process';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {comparePairs, summarizeReport, validatePolicy} from './compare.mjs';

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
  const histogram = {samples: 512, maximum_ns: 1024, histogram_buckets: [[320, 512]]};
  const phase = {
    terminal_error: null, planned_messages: 512, committed_messages: 512,
    acknowledged_messages: 512, durable_acknowledged_before: 512,
    produce_batch: histogram, offered_to_durable_ack: histogram, control_errors: 0,
  };
  const warmup = {...phase, planned_messages: 64, committed_messages: 64, acknowledged_messages: 64,
    durable_acknowledged_before: 64, produce_batch: {...histogram, samples: 64, histogram_buckets: [[320, 64]]},
    offered_to_durable_ack: {...histogram, samples: 64, histogram_buckets: [[320, 64]]}};
  return {
    schema_version: 2, status: 'passed', configuration: policy.workload,
    identity: {build: {source_commit: policy.service_source_commit, allocator_feature: policy.allocator_feature}},
    warmup: {elapsed_ns: 1000, partitions: Array.from({length: 4}, () => structuredClone(warmup))},
    measured: {elapsed_ns: 2000000000, partitions: Array.from({length: 4}, () => structuredClone(phase))},
  };
}

test('histogram aggregation retains every partition and durability failures cannot look faster', () => {
  const report = fixture();
  const result = summarizeReport(report, policy, policy.service_source_commit);
  assert.equal(result.durable_messages_per_second, 1024);
  assert.equal(result.produce_p99_upper_ns, 1024);
  report.measured.partitions[3].durable_acknowledged_before = 511;
  assert.throws(() => summarizeReport(report, policy, policy.service_source_commit));
});

test('a rare slow partition changes the merged p99 instead of being averaged away', () => {
  const report = fixture();
  report.measured.partitions[3].produce_batch = {samples: 512, maximum_ns: 4096, histogram_buckets: [[384, 512]]};
  assert.equal(summarizeReport(report, policy, policy.service_source_commit).produce_p99_upper_ns, 4096);
  report.measured.partitions[3].produce_batch.histogram_buckets = [[384, 511]];
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
