import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import {fileURLToPath} from 'node:url';

test('CI rejects browser tests that pass only after a retry', {timeout: 30_000}, t => {
  const web = fileURLToPath(new URL('../../', import.meta.url));
  const directory = fs.mkdtempSync(path.join(web, '.playwright-gate-'));
  t.after(() => fs.rmSync(directory, {recursive: true, force: true}));
  const config = path.join(directory, 'playwright.config.ts');
  const report = path.join(directory, 'report.json');
  fs.writeFileSync(config, `import configuration from '../playwright.config.ts';
export default {
  ...configuration,
  testDir: '.',
  testMatch: 'probe.spec.ts',
  fullyParallel: false,
  workers: 1,
  projects: [{name: 'gate-probe'}],
  webServer: undefined,
  use: {trace: 'off', screenshot: 'off', video: 'off'},
  outputDir: ${JSON.stringify(path.join(directory, 'output'))},
  reporter: [['json', {outputFile: ${JSON.stringify(report)}}]],
};
`);
  fs.writeFileSync(path.join(directory, 'probe.spec.ts'), `import {test, expect} from '@playwright/test';
test('a controlled first attempt fails', ({}, info) => {
  expect(info.retry).toBeGreaterThan(0);
});
`);
  const result = spawnSync(process.execPath, [
    path.join(web, 'node_modules/playwright/cli.js'), 'test', '--config', config,
  ], {cwd: web, env: {...process.env, CI: 'true'}, encoding: 'utf8', timeout: 25_000});
  assert.ifError(result.error);
  assert.equal(result.signal, null, result.stderr);
  assert.ok(fs.existsSync(report), result.stdout + result.stderr);
  assert.equal(result.status, 1, result.stdout + result.stderr);
  const {stats} = JSON.parse(fs.readFileSync(report, 'utf8'));
  assert.equal(stats.expected, 0);
  assert.equal(stats.flaky + stats.unexpected, 1);
  t.diagnostic(`Playwright exit=${result.status}; flaky=${stats.flaky}; unexpected=${stats.unexpected}`);
});
