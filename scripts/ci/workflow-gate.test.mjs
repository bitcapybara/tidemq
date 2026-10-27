import assert from 'node:assert/strict';
import fs from 'node:fs';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

const workflow = fs.readFileSync(new URL('../../.github/workflows/rust.yml', import.meta.url), 'utf8');
const gate = workflow.match(/node -e '([^'\n]+)'/)?.[1];
assert(gate, 'The workflow must contain its final executable gate');
const jobs = ['validate', 'faults', 'coverage', 'untrusted', 'console', 'console-browser', 'console-browser-live', 'documentation'];

function evaluate(fork, patch = {}) {
  const results = Object.fromEntries(jobs.map((job) => [job, { result: 'success' }]));
  for (const [job, result] of Object.entries(patch)) results[job] = { result };
  return spawnSync(process.execPath, ['-e', gate], { encoding: 'utf8',
    env: { ...process.env, FORK: String(fork), RESULTS: JSON.stringify(results) } }).status;
}

test('the main gate requires coverage, real backend browsers and every trusted Rust lane', () => {
  assert.equal(evaluate(false, { untrusted: 'skipped' }), 0);
  for (const job of jobs.filter((job) => job !== 'untrusted')) {
    for (const status of ['failure', 'cancelled', 'skipped']) {
      assert.notEqual(evaluate(false, { [job]: status }), 0, `${job}: ${status}`);
    }
  }
});

test('forks require unit, coverage and browser gates without accessing shared services', () => {
  const skipped = { validate: 'skipped', faults: 'skipped', coverage: 'skipped', 'console-browser-live': 'skipped' };
  assert.equal(evaluate(true, skipped), 0);
  for (const job of ['untrusted', 'console', 'console-browser', 'documentation']) {
    assert.notEqual(evaluate(true, { ...skipped, [job]: 'failure' }), 0, job);
  }
});
