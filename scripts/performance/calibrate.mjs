#!/usr/bin/env node
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFile, writeFile} from 'node:fs/promises';
import path from 'node:path';
import {comparePairs, summarizeReport, validatePolicy} from './compare.mjs';

async function main() {
  const [output, ...inputs] = process.argv.slice(2);
  assert.ok(output, 'Usage: calibrate.mjs new-output.json ten-ordered-latency-reports.json...');
  const policyBytes = await readFile(new URL('../../performance/baseline.json', import.meta.url));
  const policy = validatePolicy(JSON.parse(policyBytes));
  assert.equal(inputs.length, policy.pairs * 2, 'Calibration requires ten ordered A/A reports');
  const reports = await Promise.all(inputs.map(async input => JSON.parse(await readFile(input, 'utf8'))));
  const first = reports[0].identity;
  assert.match(first.build.source_commit, /^[0-9a-f]{40}$/, 'Calibration requires a service source identity');
  const measurements = reports.map(report => {
    assert.equal(report.identity.build.source_commit, first.build.source_commit, 'Calibration sources differ');
    assert.equal(report.identity.service_binary_blake3, first.service_binary_blake3, 'Calibration service binaries differ');
    assert.equal(report.identity.generator_binary_blake3, first.generator_binary_blake3, 'Calibration workload binaries differ');
    for (const hash of [report.identity.service_binary_blake3, report.identity.generator_binary_blake3]) {
      assert.match(hash, /^[0-9a-f]{64}$/, 'Calibration requires actual binary identities');
    }
    return summarizeReport(report, policy, first.build.source_commit, 'latency');
  });
  const pairs = [];
  for (let index = 0; index < measurements.length; index += 2) {
    pairs.push({pair: index / 2, baseline: measurements[index], candidate: measurements[index + 1]});
  }
  const compared = comparePairs(pairs, policy);
  const metrics = Object.fromEntries(Object.entries(compared.metrics).filter(([metric]) => metric !== 'durable_messages_per_second'));
  const statuses = Object.values(metrics).map(metric => metric.status);
  const biased = Object.values(metrics).some(metric =>
    metric.slowdown_ci95[0] > 1 || metric.slowdown_ci95[1] < 1);
  const result = {
    schema_version: 1, kind: 'same_binary_scheduled_latency_calibration', calibration_only: true, scenario: 'latency',
    status: biased ? 'temporal_bias_detected' : statuses.includes('inconclusive') ? 'inconclusive' : 'passed',
    service_source_commit: first.build.source_commit,
    service_binary_blake3: first.service_binary_blake3,
    workload_binary_blake3: first.generator_binary_blake3,
    sdk_and_harness_source_commit: null,
    protected_policy_sha256: createHash('sha256').update(policyBytes).digest('hex'),
    inputs: inputs.map(input => path.resolve(input)), metrics, pairs,
    limitations: [
      'Identical service and workload binaries: this calibrates temporal noise and does not compare source changes',
      'Raw load reports contain no SDK or harness source identity; preserve the independent build logs for that provenance',
      'The paced throughput is offered-rate limited; its measurements are recorded but excluded from calibration decisions',
      'The artifact does not advance the baseline or relax any precision requirement',
      'Reported confidence is per metric; p99 uses the bounded histogram upper bounds',
    ],
  };
  await writeFile(output, `${JSON.stringify(result, null, 2)}\n`, {flag: 'wx', mode: 0o600});
  console.log(`Scheduled latency calibration: ${result.status}; ${path.resolve(output)}`);
}

main().catch(error => { console.error(error.message); process.exitCode = 1; });
