import assert from 'node:assert/strict';

const T95_DF7 = 2.3646242515927844;
export const HARNESS_FILES = Object.freeze([
  'examples/perf_load.rs', 'examples/perf/sample.rs', 'examples/perf/stats.rs', 'examples/perf/workload.rs',
]);
const METRICS = {
  durable_messages_per_second: -1,
  produce_mean_ns: 1,
  produce_p99_upper_ns: 1,
  offered_to_durable_ack_mean_ns: 1,
  offered_to_durable_ack_p99_upper_ns: 1,
};

export function validatePolicy(policy) {
  assert.equal(policy.schema_version, 3, 'Unsupported performance policy schema');
  assert.match(policy.service_source_commit, /^[0-9a-f]{40}$/, 'Invalid baseline source');
  assert.equal(policy.pairs, 8, 'The Student t calibration requires eight pairs');
  assert.equal(policy.confidence, 0.95, 'The Student t calibration requires 95% confidence');
  assert.equal(policy.allocator_feature, 'allocator-mimalloc', 'CI uses the default allocator');
  assert.equal(policy.server_worker_threads, 4, 'Server worker budget changed');
  assert.deepEqual(policy.topologies, ['rf1', 'rf3-combined'], 'Both real topologies are required');
  assert.deepEqual(Object.keys(policy.max_log_ci_half_width).sort(), Object.keys(METRICS).sort(), 'Missing per-metric precision');
  for (const value of Object.values(policy.max_log_ci_half_width)) {
    assert.ok(value > 0 && value <= 0.03, 'Uncalibrated precision must remain within three log percentage points');
  }
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
  assert.equal(workload.messages, 256, 'Finite throughput phase must contain 1024 messages');
  assert.equal(workload.warmup_messages, 32, 'Throughput warmup shape changed');
  assert.equal(workload.produce_interval_ms, 0, 'Throughput phase must remain unpaced');
  assert.deepEqual(policy.latency_workload, {messages: 1024, warmup_messages: 256, concurrent_partitions: 1, produce_interval_ms: 40},
    'The latency phase must retain one partition at its fixed 25 message/s offer schedule');
  return policy;
}

export function workloadFor(policy, scenario) {
  assert.ok(['throughput', 'latency'].includes(scenario), 'Unknown performance scenario');
  return scenario === 'latency' ? {...policy.workload, ...policy.latency_workload} : policy.workload;
}

function positive(value, label) {
  assert.ok(Number.isFinite(value) && value > 0, `Invalid ${label}`);
  return value;
}

export function weightedMeanLatency(distributions) {
  let samples = 0;
  let totalNanoseconds = 0;
  for (const distribution of distributions) {
    assert.ok(Number.isSafeInteger(distribution.samples) && distribution.samples > 0,
      'Invalid latency mean sample count');
    const mean = positive(distribution.mean_ns, 'raw latency mean');
    assert.ok(mean <= positive(distribution.maximum_ns, 'latency maximum'), 'Latency mean exceeds its maximum');
    samples += distribution.samples;
    totalNanoseconds += mean * distribution.samples;
  }
  assert.ok(Number.isSafeInteger(samples) && samples > 0, 'Missing latency mean observations');
  return positive(totalNanoseconds / samples, 'weighted raw latency mean');
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

export function summarizeReport(report, policy, sourceCommit, scenario = 'throughput') {
  const workload = workloadFor(policy, scenario);
  assert.equal(report.schema_version, 3, 'Load report must preserve scheduled offers and lateness');
  assert.equal(report.status, 'passed', 'The durable workload did not pass');
  assert.equal(report.identity.build.source_commit, sourceCommit, 'Service source identity mismatch');
  assert.equal(report.identity.build.allocator_feature, policy.allocator_feature, 'Allocator mismatch');
  for (const [name, value] of Object.entries(workload)) {
    assert.equal(report.configuration[name], value, `Workload ${name} differs from protected policy`);
  }
  for (const [name, group] of [['warmup', report.warmup], ['measured', report.measured]]) {
    assert.equal(group.partitions.length, workload.concurrent_partitions, 'Missing independent partition');
    const planned = name === 'warmup' ? workload.warmup_messages : workload.messages;
    for (const phase of group.partitions) {
      assert.equal(phase.terminal_error, null, 'Partition validation failed');
      for (const field of ['planned_messages', 'committed_messages', 'acknowledged_messages', 'durable_acknowledged_before']) {
        assert.equal(phase[field], planned, `Incomplete durable ${name}: ${field}`);
      }
      assert.equal(phase.produce_batch.samples, planned, 'Missing durable producer samples');
      assert.equal(phase.produce_scheduling_lateness.samples, planned, 'Missing scheduled-offer observations');
      assert.equal(phase.offered_to_durable_ack.samples, planned, 'Missing end-to-end samples');
      assert.equal(phase.control_errors, 0, 'Control traffic failed during load');
    }
  }
  const phases = report.measured.partitions;
  const messages = phases.reduce((sum, phase) => sum + phase.acknowledged_messages, 0);
  return {
    durable_messages: messages,
    durable_messages_per_second: messages * 1e9 / positive(report.measured.elapsed_ns, 'group duration'),
    produce_mean_ns: weightedMeanLatency(phases.map(phase => phase.produce_batch)),
    produce_p99_upper_ns: p99(phases.map(phase => phase.produce_batch)),
    producer_scheduling_lateness_p99_upper_ns: p99(phases.map(phase => phase.produce_scheduling_lateness)),
    offered_to_durable_ack_mean_ns: weightedMeanLatency(phases.map(phase => phase.offered_to_durable_ack)),
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
    const halfWidth = T95_DF7 * Math.sqrt(variance / ratios.length);
    const lower = mean - halfWidth;
    const upper = mean + halfWidth;
    metrics[metric] = {
      candidate_to_baseline: pairs.map(pair => pair.candidate[metric] / pair.baseline[metric]),
      geometric_slowdown_ratio: Math.exp(mean),
      slowdown_ci95: [Math.exp(lower), Math.exp(upper)],
      log_ci_half_width: halfWidth,
      status: lower > 0 ? 'regressed' : halfWidth > policy.max_log_ci_half_width[metric] ? 'inconclusive' : 'passed',
    };
  }
  const statuses = Object.values(metrics).map(metric => metric.status);
  return {status: statuses.includes('regressed') ? 'regressed' : statuses.includes('inconclusive') ? 'inconclusive' : 'passed', metrics};
}
