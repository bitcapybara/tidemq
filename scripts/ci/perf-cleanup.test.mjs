import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

const harness = await fs.readFile(new URL('../perf/run.sh', import.meta.url), 'utf8');
// Execute the harness's actual EXIT handler without starting Linux services or acquiring the lab lease.
const start = harness.indexOf('cleanup() {');
const end = harness.indexOf('trap cleanup EXIT', start);
assert(start >= 0 && end > start, 'The perf EXIT handler must remain available for failure injection');
const handler = harness.slice(start, end);
const token = 'ab'.repeat(32);

for (const scenario of ['success', 'run-failure', 'redaction-failure', 'partial-redaction-failure', 'removal-failure']) {
  test(`perf cleanup: ${scenario}`, async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'tidemq-perf-cleanup-test-'));
    const output = path.join(root, 'evidence');
    const privateDirectory = path.join(root, 'private');
    await fs.mkdir(output);
    await fs.mkdir(path.join(privateDirectory, 'core'), { recursive: true });
    await fs.mkdir(path.join(privateDirectory, 'logs'));
    await fs.writeFile(path.join(privateDirectory, 'core/access.token'), token + '\n');
    await fs.writeFile(path.join(privateDirectory, 'logs/service.log'), `Private token: ${token}\n`);
    if (scenario === 'partial-redaction-failure') {
      await fs.writeFile(path.join(privateDirectory, 'logs/z-failure.log'), token);
      await fs.mkdir(path.join(privateDirectory, 'logs/z-failure.log.sanitized'));
    }
    try {
      const injected = scenario === 'redaction-failure' ? 'perl() { return 1; }'
        : scenario === 'removal-failure' ? 'rm() { return 1; }' : '';
      const result = spawnSync('bash', ['-c', [
        'set -euo pipefail',
        'output=$1; private_directory=$2; fixture=$private_directory/core; raw_logs=$private_directory/logs',
        'service_pid= client_pid= monitor_pid= profile_pid=',
        injected, handler, 'trap cleanup EXIT', scenario === 'run-failure' ? 'exit 17' : ':',
      ].join('\n'), 'perf-cleanup-test', output, privateDirectory], { encoding: 'utf8' });
      assert.equal(result.status, scenario === 'success' ? 0 : scenario === 'run-failure' ? 17 : 1, result.stderr);
      const receipt = JSON.parse(await fs.readFile(path.join(output, 'cleanup.json'), 'utf8'));
      assert.equal(receipt.exit_status, result.status);
      const redactionFailed = scenario.includes('redaction-failure');
      assert.equal(receipt.redaction_complete, !redactionFailed);
      assert.equal(receipt.private_fixture_removed, scenario !== 'removal-failure');
      assert(!JSON.stringify(receipt).includes(token));
      assert(!result.stdout.includes(token) && !result.stderr.includes(token));
      const names = await fs.readdir(output);
      if (redactionFailed) assert.deepEqual(names, ['cleanup.json']);
      else {
        assert.equal(await fs.readFile(path.join(output, 'service.log'), 'utf8'), 'Private token: [REDACTED]\n');
      }
      if (scenario === 'removal-failure') await fs.access(privateDirectory);
      else await assert.rejects(fs.access(privateDirectory));
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
}
