import assert from 'node:assert/strict';
import fs from 'node:fs';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

const workflow = fs.readFileSync(new URL('../../.github/workflows/rust.yml', import.meta.url), 'utf8');
const gate = workflow.match(/node -e '([^'\n]+)'/)?.[1];
assert(gate, 'The workflow must contain its final executable gate');
const jobs = ['validate', 'faults', 'coverage', 'console', 'console-browser', 'console-browser-live', 'documentation'];
const read = relative => fs.readFileSync(new URL(relative, import.meta.url), 'utf8');
const mockPlaywright = read('../../console/web/playwright.config.ts');
const livePlaywright = read('../../console/web/playwright.live.config.ts');
const consolePackage = JSON.parse(read('../../console/web/package.json'));
const liveRunner = read('./console-browser-live.mjs');

function evaluate(fork, patch = {}) {
  const results = Object.fromEntries(jobs.map((job) => [job, { result: 'success' }]));
  for (const [job, result] of Object.entries(patch)) results[job] = { result };
  return spawnSync(process.execPath, ['-e', gate], { encoding: 'utf8',
    env: { ...process.env, FORK: String(fork), RESULTS: JSON.stringify(results) } }).status;
}

test('the main gate requires coverage, real backend browsers and every trusted Rust lane', () => {
  assert.equal(evaluate(false), 0);
  for (const job of jobs) {
    for (const status of ['failure', 'cancelled', 'skipped']) {
      assert.notEqual(evaluate(false, { [job]: status }), 0, `${job}: ${status}`);
    }
  }
});

test('forks require Console web, mock browsers and documentation without accessing shared services', () => {
  const skipped = { validate: 'skipped', faults: 'skipped', coverage: 'skipped', 'console-browser-live': 'skipped' };
  assert.equal(evaluate(true, skipped), 0);
  for (const job of ['console', 'console-browser', 'documentation']) {
    assert.notEqual(evaluate(true, { ...skipped, [job]: 'failure' }), 0, job);
  }
});

test('both platforms run normal validation and Linux shares the native executor with faults and coverage', () => {
  const job = (name) => workflow.split(`\n  ${name}:\n`)[1]?.split(/\n  [\w-]+:/u)[0] ?? '';
  const validate = job('validate');
  const linuxRunner = validate.match(/- os: Linux\n\s+runner: (\[[^\n]+\])/u)?.[1];
  assert(linuxRunner, 'Linux validation must retain its runner matrix entry');
  for (const name of ['faults', 'coverage']) {
    const runner = job(name).match(/runs-on: (\[[^\n]+\])/u)?.[1];
    assert.equal(linuxRunner, runner, `${name} must share the native executor with validation`);
  }
  assert.match(validate, /bash scripts\/ci\/with-cargo-cache\.sh bash scripts\/ci\/native\.sh/u);
  assert.match(validate, /run: \|\n\s+bash scripts\/ci\/validate\.sh\n\s+bash scripts\/ci\/integration\.sh normal\n\s+if: runner\.os == 'macOS'/u);
  const native = fs.readFileSync(new URL('./native.sh', import.meta.url), 'utf8');
  assert.match(native, /bash scripts\/ci\/validate\.sh\nbash scripts\/ci\/integration\.sh normal/u);
  assert.match(native, /bundle\.mjs pack native\n/u);
  const faults = read('./faults.sh');
  assert.match(faults, /bash scripts\/ci\/integration\.sh faults\n/u);
  assert.match(faults, /bundle\.mjs pack lab\n/u);
  const coverage = job('coverage');
  assert.doesNotMatch(coverage, /download-artifact/u);
  assert.match(coverage, /name: coverage-report[^\n]+\n\s+- name: Remove local coverage inputs\n\s+if: success\(\)\n\s+run: node scripts\/coverage\/bundle\.mjs clean/u);
});

test('every trusted Linux nextest lane enables the same immutable isolated executor', () => {
  const image = 'sha256:c25cdd9dc1718bd719f07067c9454ae81e9980054e6b4b423a7cb1e070c713cc';
  const settings = [...workflow.matchAll(/TIDEMQ_NEXTEST_IMAGE: ([^\n]+)/gu)].map(match => match[1]);
  assert.deepEqual(settings, [image, image]);
  const scripts = ['validate.sh', 'integration.sh'].map(name => fs.readFileSync(new URL('./' + name, import.meta.url), 'utf8'));
  assert.equal(scripts.flatMap(source => [...source.matchAll(/bash scripts\/ci\/nextest\.sh /gu)]).length, 5);
  assert(!scripts.some(source => /cargo nextest run/u.test(source)));
});

test('browser jobs run Chromium only and cache Playwright downloads', () => {
  const job = (name) => workflow.split(`\n  ${name}:\n`)[1]?.split(/\n  [\w-]+:/u)[0] ?? '';
  for (const name of ['console', 'console-browser', 'console-browser-live']) {
    const source = job(name);
    assert.match(source, /actions\/cache@0057852bfaa89a56745cba8c7296529d2fc39830 # v4\.3\.0/u,
      `${name} must use the pinned Playwright cache action`);
    assert.match(source, /path: ~\/\.cache\/ms-playwright/u, `${name} must cache the browser directory`);
    assert.match(source,
      /key: playwright-\$\{\{ runner\.os \}\}-\$\{\{ hashFiles\('console\/web\/package-lock\.json'\) \}\}/u,
      `${name} must invalidate the browser cache when Playwright dependencies change`);
  }
  assert.match(job('console'), /- run: npx --no-install playwright install --with-deps chromium\n/u,
    'console must install Chromium only');
  assert.doesNotMatch(job('console'), /firefox|webkit/u,
    'console must not install Firefox or WebKit');
  assert.match(job('console'),
    /- run: node --test \.\.\/\.\.\/scripts\/ci\/workflow-gate\.test\.mjs\n\s+- run: npx --no-install playwright install --with-deps chromium\n/u,
    'console must enforce the workflow regression test before browser installation');
  assert.match(job('console'), /timeout-minutes: 40/u,
    'console must allow the first browser install to finish');
  for (const name of ['console-browser', 'console-browser-live']) {
    const source = job(name);
    assert.match(source, /timeout-minutes: 40/u, `${name} must allow the first browser install to finish`);
    assert.match(source, /browser: \[chromium\]/u, `${name} must run Chromium only`);
    assert.match(source, /playwright install --with-deps \$\{\{ matrix\.browser \}\}/u,
      `${name} must install the selected matrix browser`);
    assert.equal((source.match(/playwright install --with-deps/gu) ?? []).length, 1,
      `${name} must have one browser installation target`);
  }
  assert.match(job('console-browser'),
    /- run: npm run test:e2e -- --project=\$\{\{ matrix\.browser \}\}/u,
    'console-browser must execute the Chromium mock browser suite');
  assert.match(job('console-browser-live'),
    /- run: npm run test:e2e:live -- --project=\$\{\{ matrix\.browser \}\}/u,
    'console-browser-live must execute the Chromium native browser suite');
  for (const name of ['console', 'console-browser', 'console-browser-live']) {
    const source = job(name);
    const cache = source.indexOf('actions/cache@');
    const npm = source.indexOf('- run: npm ci');
    const browser = source.indexOf('playwright install --with-deps');
    assert(cache >= 0 && cache < npm && npm < browser,
      `${name} must cache before npm install and install the browser last`);
  }
});

test('browser entry points expose Chromium only', () => {
  for (const [name, source] of [['mock Playwright config', mockPlaywright], ['live Playwright config', livePlaywright]]) {
    assert.match(source, /name: "chromium"/u, `${name} must define Chromium`);
    assert.doesNotMatch(source, /firefox|webkit|Desktop Firefox|Desktop Safari/u,
      `${name} must not define another browser`);
  }
  assert.match(consolePackage.scripts['test:e2e'], /--project=chromium/u);
  assert.match(consolePackage.scripts['test:e2e:live'], /--project=chromium/u);
  assert.match(liveRunner, /revocable\.chromium = await credential\('chromium'\)/u);
  assert.doesNotMatch(liveRunner, /\['chromium', 'firefox', 'webkit'\]/u);
});
