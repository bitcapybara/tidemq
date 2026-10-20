import assert from 'node:assert/strict';

const T95_DF4 = 2.7764451051977987;
const METRICS = {
  durable_messages_per_second: -1,
  produce_p99_upper_ns: 1,
  offered_to_durable_ack_p99_upper_ns: 1,
};

export function validatePolicy(policy) {
  assert.equal(policy.schema_version, 1, 'Unsupported performance policy schema');
  assert.match(policy.service_source_commit, /^[0-9a-f]{40}$/, 'Invalid baseline source');
  assert.equal(policy.bootstrap.legacy_service_source_commit, '9fe9c952276d2a2b7b27a41ca1021926aaa3c283',
    'The legacy SDK exemption is limited to the original pre-CancellationToken source');
  assert.match(policy.bootstrap.legacy_harness_source_commit, /^[0-9a-f]{40}$/, 'Invalid immutable bootstrap harness');
  assert.deepEqual(policy.bootstrap.harness_files,
    ['examples/perf_load.rs', 'examples/perf/sample.rs', 'examples/perf/stats.rs', 'examples/perf/workload.rs'],
    'The complete trusted workload source must be overlaid');
  assert.equal(policy.pairs, 5, 'The Student t calibration requires five pairs');
  assert.equal(policy.confidence, 0.95, 'The Student t calibration requires 95% confidence');
  assert.equal(policy.allocator_feature, 'allocator-mimalloc', 'CI uses the default allocator');
  assert.equal(policy.server_worker_threads, 4, 'Server worker budget changed');
  assert.deepEqual(policy.topologies, ['rf1', 'rf3-combined'], 'Both real topologies are required');
  assert.ok(policy.max_log_ci_half_width > 0 && policy.max_log_ci_half_width <= 0.03,
    'Measurement precision must be within three log percentage points');
  const workload = policy.workload;
  assert.equal(workload.concurrent_partitions, 4, 'Four independent partition pairs are required');
  assert.equal(workload.batch_messages, 1, 'Producer p99 requires individual durable requests');
  assert.equal(workload.workers, 1, 'One producer is required per independent partition');
  assert.ok(Number.isSafeInteger(workload.messages) && workload.messages >= 250 && workload.messages <= 2048,
    'Measured message count cannot support bounded p99 observations');
  assert.ok(Number.isSafeInteger(workload.warmup_messages) && workload.warmup_messages >= 32 && workload.warmup_messages <= 256,
    'Warmup budget is invalid');
  assert.equal(workload.message_bytes, 128, 'CI payload shape changed');
  assert.equal(workload.request_lanes, 4, 'CI stream allocation changed');
  assert.equal(workload.ack_messages, 32, 'CI durable ACK batching changed');
  assert.equal(workload.phase_timeout_secs, 60, 'CI phase budget changed');
  assert.equal(workload.request_timeout_ms, 10000, 'CI request budget changed');
  assert.equal(workload.cooldown_secs, 0, 'CI cooldown budget changed');
  return policy;
}

function positive(value, label) {
  assert.ok(Number.isFinite(value) && value > 0, `Invalid ${label}`);
  return value;
}

function p99(distributions) {
  const buckets = new Map();
  let count = 0;
  let maximum = 0;
  for (const distribution of distributions) {
    let observed = 0;
    assert.ok(Array.isArray(distribution.histogram_buckets), 'Missing mergeable histogram');
    for (const [index, samples] of distribution.histogram_buckets) {
      assert.ok(Number.isSafeInteger(index) && index >= 0 && index < 2048, 'Invalid histogram index');
      assert.ok(Number.isSafeInteger(samples) && samples > 0, 'Invalid histogram count');
      assert.ok(samples <= distribution.samples && observed + samples <= distribution.samples,
        'Histogram count exceeds its observations');
      observed += samples;
      buckets.set(index, (buckets.get(index) ?? 0) + samples);
    }
    assert.equal(observed, distribution.samples, 'Histogram observations are incomplete');
    count += observed;
    maximum = Math.max(maximum, positive(distribution.maximum_ns, 'histogram maximum'));
  }
  assert.ok(count >= 1000 && Number.isSafeInteger(count), 'At least 1000 observations are required for p99');
  const rank = Math.ceil(count * 0.99);
  let observed = 0;
  for (const [index, samples] of [...buckets].sort(([left], [right]) => left - right)) {
    observed += samples;
    if (observed >= rank) {
      const exponent = Math.floor(index / 32);
      const base = 2 ** exponent;
      const upper = base + Math.ceil(((index % 32) + 1) * base / 32) - 1;
      return Math.min(maximum, positive(upper, 'p99 bucket upper bound'));
    }
  }
  throw new Error('Histogram p99 rank is unavailable');
}

export function summarizeReport(report, policy, sourceCommit) {
  assert.equal(report.schema_version, 2, 'Load report must support concurrent partition histograms');
  assert.equal(report.status, 'passed', 'The durable workload did not pass');
  assert.equal(report.identity.build.source_commit, sourceCommit, 'Service source identity mismatch');
  assert.equal(report.identity.build.allocator_feature, policy.allocator_feature, 'Allocator mismatch');
  for (const [name, value] of Object.entries(policy.workload)) {
    assert.equal(report.configuration[name], value, `Workload ${name} differs from protected policy`);
  }
  for (const [name, group] of [['warmup', report.warmup], ['measured', report.measured]]) {
    assert.equal(group.partitions.length, policy.workload.concurrent_partitions, 'Missing independent partition');
    const planned = name === 'warmup' ? policy.workload.warmup_messages : policy.workload.messages;
    for (const phase of group.partitions) {
      assert.equal(phase.terminal_error, null, 'Partition validation failed');
      for (const field of ['planned_messages', 'committed_messages', 'acknowledged_messages', 'durable_acknowledged_before']) {
        assert.equal(phase[field], planned, `Incomplete durable ${name}: ${field}`);
      }
      assert.equal(phase.produce_batch.samples, planned, 'Missing durable producer samples');
      assert.equal(phase.offered_to_durable_ack.samples, planned, 'Missing end-to-end samples');
      assert.equal(phase.control_errors, 0, 'Control traffic failed during load');
    }
  }
  const phases = report.measured.partitions;
  const messages = phases.reduce((sum, phase) => sum + phase.acknowledged_messages, 0);
  return {
    durable_messages: messages,
    durable_messages_per_second: messages * 1e9 / positive(report.measured.elapsed_ns, 'group duration'),
    produce_p99_upper_ns: p99(phases.map(phase => phase.produce_batch)),
    offered_to_durable_ack_p99_upper_ns: p99(phases.map(phase => phase.offered_to_durable_ack)),
  };
}

export function comparePairs(pairs, policy) {
  assert.equal(pairs.length, policy.pairs, 'Missing paired trials');
  const metrics = {};
  for (const [metric, direction] of Object.entries(METRICS)) {
    const ratios = pairs.map(pair => direction * Math.log(
      positive(pair.candidate[metric], metric) / positive(pair.baseline[metric], metric)));
    const mean = ratios.reduce((sum, value) => sum + value, 0) / ratios.length;
    const variance = ratios.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (ratios.length - 1);
    const halfWidth = T95_DF4 * Math.sqrt(variance / ratios.length);
    const lower = mean - halfWidth;
    const upper = mean + halfWidth;
    metrics[metric] = {
      candidate_to_baseline: pairs.map(pair => pair.candidate[metric] / pair.baseline[metric]),
      geometric_slowdown_ratio: Math.exp(mean),
      slowdown_ci95: [Math.exp(lower), Math.exp(upper)],
      log_ci_half_width: halfWidth,
      status: lower > 0 ? 'regressed' : halfWidth > policy.max_log_ci_half_width ? 'inconclusive' : 'passed',
    };
  }
  const statuses = Object.values(metrics).map(metric => metric.status);
  return {status: statuses.includes('regressed') ? 'regressed' : statuses.includes('inconclusive') ? 'inconclusive' : 'passed', metrics};
}
