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

test('trusted Linux validation shares the existing native executor with fault and coverage jobs', () => {
  const job = (name) => workflow.split(`\n  ${name}:\n`)[1]?.split(/\n  [\w-]+:/u)[0] ?? '';
  const validate = job('validate');
  const linuxRunner = validate.match(/- os: Linux\n\s+runner: (\[[^\n]+\])/u)?.[1];
  assert(linuxRunner, 'Linux validation must retain its runner matrix entry');
  for (const name of ['faults', 'coverage']) {
    const runner = job(name).match(/runs-on: (\[[^\n]+\])/u)?.[1];
    assert.equal(linuxRunner, runner, `${name} must share the native executor with validation`);
  }
  assert.match(validate, /bash scripts\/ci\/with-cargo-cache\.sh bash scripts\/ci\/native\.sh/u);
  const native = fs.readFileSync(new URL('./native.sh', import.meta.url), 'utf8');
  assert.match(native, /bash scripts\/ci\/validate\.sh\nbash scripts\/ci\/integration\.sh normal/u);
  assert.match(native, /bundle\.mjs pack native target\/ci\/coverage-inputs/u);
});

test('every trusted Linux nextest lane enables the same immutable isolated executor', () => {
  const image = 'sha256:b4db0d39a71b7e82611305160c66859b69bb4f8aaf64dec541b82e8aed12672b';
  const settings = [...workflow.matchAll(/TIDEMQ_NEXTEST_IMAGE: ([^\n]+)/gu)].map(match => match[1]);
  assert.deepEqual(settings, [image, image]);
  const scripts = ['validate.sh', 'integration.sh'].map(name => fs.readFileSync(new URL('./' + name, import.meta.url), 'utf8'));
  assert.equal(scripts.flatMap(source => [...source.matchAll(/bash scripts\/ci\/nextest\.sh /gu)]).length, 5);
  assert(!scripts.some(source => /cargo nextest run/u.test(source)));
});
