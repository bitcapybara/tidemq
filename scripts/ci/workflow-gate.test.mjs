import assert from 'node:assert/strict';
import fs from 'node:fs';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

const workflow = fs.readFileSync(new URL('../../.github/workflows/rust.yml', import.meta.url), 'utf8');
const documentationWorkflow = fs.readFileSync(new URL('../../.github/workflows/docs.yml', import.meta.url), 'utf8');
const gate = workflow.match(/node -e '([^'\n]+)'/)?.[1];
assert(gate, 'The workflow must contain its final executable gate');
const jobs = ['validate-linux', 'faults', 'coverage', 'console', 'console-browser', 'console-browser-live'];
const read = relative => fs.readFileSync(new URL(relative, import.meta.url), 'utf8');
const mockPlaywright = read('../../console/web/playwright.config.ts');
const livePlaywright = read('../../console/web/playwright.live.config.ts');
const consolePackage = JSON.parse(read('../../console/web/package.json'));
const liveRunner = read('./console-browser-live.mjs');
const containerRunnerCompose = read('../../deploy/lab/container-runner/compose.yaml');

function evaluate(patch = {}) {
  const results = Object.fromEntries(jobs.map((job) => [job, { result: 'success' }]));
  for (const [job, result] of Object.entries(patch)) results[job] = { result };
  return spawnSync(process.execPath, ['-e', gate], { encoding: 'utf8',
    env: { ...process.env, RESULTS: JSON.stringify(results) } }).status;
}

test('the main gate requires coverage, real backend browsers and every trusted Rust lane', () => {
  assert.equal(evaluate(), 0);
  for (const job of jobs) {
    for (const status of ['failure', 'cancelled', 'skipped']) {
      assert.notEqual(evaluate({ [job]: status }), 0, `${job}: ${status}`);
    }
  }
});

test('all workflow jobs use Linux self-hosted runners and macOS validation is removed', () => {
  const job = (name) => workflow.split(`\n  ${name}:\n`)[1]?.split(/\n  [\w-]+:/u)[0] ?? '';
  assert.doesNotMatch(workflow, /validate-macos|macos-latest|tests-macOS/u);
  for (const name of jobs) {
    const source = job(name);
    assert.match(source, /runs-on: \[self-hosted, Linux, X64, tidemq-(?:ci-container|dev)\]/u, `${name} must use a self-hosted Linux runner`);
  }
  for (const name of ['validate-linux']) {
    const runner = job(name).match(/runs-on: (\[[^\n]+\])/u)?.[1];
    assert.equal(runner, '[self-hosted, Linux, X64, tidemq-ci-container]');
  }
  for (const name of ['faults', 'coverage', 'console', 'console-browser', 'console-browser-live']) {
    const runner = job(name).match(/runs-on: (\[[^\n]+\])/u)?.[1];
    assert.equal(runner, '[self-hosted, Linux, X64, tidemq-dev]', `${name} must stay on the native executor`);
  }
  assert.equal(job('rust').match(/runs-on: (\[[^\n]+\])/u)?.[1], '[self-hosted, Linux, X64, tidemq-ci-container]');
  const linux = job('validate-linux');
  assert.match(linux, /bash scripts\/ci\/with-cargo-cache\.sh bash scripts\/ci\/native\.sh/u);
  assert.match(job('console-browser-live'), /needs: \[validate-linux\]/u);
  assert.match(job('coverage'), /needs: \[validate-linux, faults\]/u);
  assert.match(job('rust'), /needs: \[validate-linux, faults, coverage, console, console-browser, console-browser-live\]/u);
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

test('the documentation workflow uses self-hosted Linux runners', () => {
  assert.match(documentationWorkflow, /paths:\n\s+- 'docs\/\*\*'\n\s+- 'README\.md'\n\s+- 'README\.zh-CN\.md'\n\s+- '\.github\/workflows\/docs\.yml'/u);
  for (const job of ['build', 'deploy']) {
    const source = documentationWorkflow.split(`\n  ${job}:\n`)[1]?.split(/\n  [\w-]+:/u)[0] ?? '';
    assert.match(source, /runs-on: \[self-hosted, Linux, X64, tidemq-ci-container\]/u, `${job} must use the container runner`);
  }
});

test('only the native Linux fault lane enables the immutable nextest executor', () => {
  const image = 'sha256:c25cdd9dc1718bd719f07067c9454ae81e9980054e6b4b423a7cb1e070c713cc';
  const settings = [...workflow.matchAll(/TIDEMQ_NEXTEST_IMAGE: ([^\n]+)/gu)].map(match => match[1]);
  assert.deepEqual(settings, [image]);
  const validateLinux = workflow.split('\n  validate-linux:\n')[1]?.split(/\n  [\w-]+:/u)[0] ?? '';
  assert.doesNotMatch(validateLinux, /TIDEMQ_NEXTEST_IMAGE:/u);
  assert.match(containerRunnerCompose, /- \/srv\/tidemq\/maintenance\/coverage-handoff:\/srv\/tidemq\/maintenance\/coverage-handoff\n/u);
  const scripts = ['validate.sh', 'integration.sh'].map(name => fs.readFileSync(new URL('./' + name, import.meta.url), 'utf8'));
  assert.equal(scripts.flatMap(source => [...source.matchAll(/bash scripts\/ci\/nextest\.sh /gu)]).length, 5);
  assert(!scripts.some(source => /cargo nextest run/u.test(source)));
});

test('fault evidence survives a failed run until the next CI cleanup', () => {
  const evidence = read('./failure-evidence.mjs');
  const lab = read('./lab.sh');
  const maintenance = fs.readFileSync(new URL('../../scripts/lab/runner-maintenance.sh', import.meta.url), 'utf8');
  assert.match(evidence, /deployment-\[a-z0-9-\]\+\\\.log/u);
  assert.match(lab, /result == 0 && cleanup_result == 0/u);
  assert.match(lab, /cleanupDeferred/u);
  assert.match(lab, /Preserving failed CI resources for the next runner cleanup/u);
  assert.match(maintenance, /if \[\[ "\$phase" == before \]\]; then[\s\S]*\.tidemq-ci[\s\S]*cleanup-ci\.sh/u);
});

test('browser jobs run Chromium only on pre-provisioned runners', () => {
  const job = (name) => workflow.split(`\n  ${name}:\n`)[1]?.split(/\n  [\w-]+:/u)[0] ?? '';
  for (const name of ['console', 'console-browser', 'console-browser-live']) {
    const source = job(name);
    assert.doesNotMatch(source, /actions\/cache@|cache: npm|cache-dependency-path/u,
      `${name} must use runner-local caches`);
  }
  assert.match(job('console'), /- run: npx --no-install playwright install chromium\n/u,
    'console must install Chromium only without sudo');
  assert.doesNotMatch(job('console'), /firefox|webkit/u,
    'console must not install Firefox or WebKit');
  assert.match(job('console'),
    /- run: node --test \.\.\/\.\.\/scripts\/ci\/workflow-gate\.test\.mjs\n\s+- run: npx --no-install playwright install chromium\n/u,
    'console must enforce the workflow regression test before browser installation');
  assert.match(job('console'), /timeout-minutes: 40/u,
    'console must allow the first browser install to finish');
  for (const name of ['console-browser', 'console-browser-live']) {
    const source = job(name);
    assert.match(source, /timeout-minutes: 40/u, `${name} must allow the first browser install to finish`);
    assert.match(source, /playwright install chromium/u, `${name} must install Chromium`);
    assert.equal((source.match(/playwright install chromium/gu) ?? []).length, 1,
      `${name} must have one browser installation target`);
  }
  assert.match(job('console-browser'),
    /- run: npm run test:e2e -- --project=chromium/u,
    'console-browser must execute the Chromium mock browser suite');
  assert.match(job('console-browser-live'),
    /- run: npm run test:e2e:live -- --project=chromium/u,
    'console-browser-live must execute the Chromium native browser suite');
  for (const name of ['console', 'console-browser', 'console-browser-live']) {
    const source = job(name);
    const npm = source.indexOf('- run: npm ci');
    const browser = source.indexOf('playwright install chromium');
    assert(npm >= 0 && npm < browser, `${name} must install the browser after npm dependencies`);
  }
  assert.doesNotMatch(job('console-browser'), /strategy:|matrix:/u);
  assert.doesNotMatch(job('console-browser-live'), /browser: \[chromium\]/u);
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
