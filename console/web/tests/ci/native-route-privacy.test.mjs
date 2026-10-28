import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import fs from 'node:fs/promises';
import https from 'node:https';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {fileURLToPath} from 'node:url';
import {cleanup, startChild, stopChild} from '../../../../scripts/ci/console-browser-live.mjs';

test('a real HTTPS forwarding failure never logs synthetic Cookie or CSRF values', {timeout: 30_000}, async t => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'tidemq-route-privacy-'));
  const state = {artifacts: path.join(directory, 'logs'), privateDirectory: path.join(directory, 'private'),
    children: [], secrets: ['probe-bootstrap-token']};
  const cookie = 'probe-only-session-marker-with-no-real-account';
  const csrf = 'probe-only-csrf-marker-with-no-real-account';
  const operationId = '019b6f4c-0000-7000-8000-000000000001';
  let server;
  let receivedCookie = false;
  let receivedCsrf = false;
  try {
    await fs.chmod(directory, 0o700);
    await fs.mkdir(state.artifacts);
    await fs.mkdir(state.privateDirectory, {mode: 0o700});
    const pki = path.join(state.privateDirectory, 'pki');
    const generation = spawnSync('bash', [fileURLToPath(new URL('../../../../deploy/local/generate.sh', import.meta.url)), 'rf1', pki],
      {encoding: 'utf8', timeout: 5_000});
    assert.equal(generation.status, 0, 'Privacy probe certificate generation failed');
    server = https.createServer({cert: await fs.readFile(path.join(pki, 'node-1.pem')), key: await fs.readFile(path.join(pki, 'node-1.key'))},
      (request, response) => {
        if (request.url === '/api/v1/topics') {
          receivedCookie = request.headers.cookie?.includes(cookie) === true;
          receivedCsrf = request.headers['x-csrf-token'] === csrf;
          request.socket.destroy();
          return;
        }
        response.setHeader('set-cookie', `__Host-tidemq_session=${cookie}; HttpOnly; Secure; SameSite=Strict; Path=/`);
        response.setHeader('content-type', 'text/html');
        response.end('<!doctype html><title>Private error-path probe</title>');
      });
    await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
    const address = server.address();
    assert(address && typeof address === 'object', 'Privacy probe requires a bound HTTPS listener');
    const configuration = path.join(state.privateDirectory, 'privacy.config.mjs');
    await fs.writeFile(configuration, `export default {
      testDir:${JSON.stringify(state.privateDirectory)}, testMatch:'failure.spec.ts', workers:1, retries:0,
      timeout:10000, reporter:'line', outputDir:${JSON.stringify(path.join(state.privateDirectory, 'output'))},
      use:{baseURL:${JSON.stringify('https://127.0.0.1:' + address.port)}, ignoreHTTPSErrors:true, trace:'off', video:'off', screenshot:'off'},
      projects:[{name:'chromium', use:{browserName:'chromium'}}]
    };`);
    const testModule = fileURLToPath(new URL('../../node_modules/@playwright/test/index.mjs', import.meta.url));
    const boundary = fileURLToPath(new URL('../../e2e/live/context.ts', import.meta.url));
    await fs.writeFile(path.join(state.privateDirectory, 'failure.spec.ts'), `import {test} from ${JSON.stringify(testModule)};
      import {discardCommittedResponse} from ${JSON.stringify(boundary)};
      test('forwarding fails with only safe diagnostic fields', async ({page}) => {
        await page.route('**/api/v1/topics', async route => { await discardCommittedResponse(route); });
        await page.goto('/');
        await page.evaluate(async ({csrf, operationId}) => {
          await fetch('/api/v1/topics', {method:'POST', headers:{'x-csrf-token':csrf}, body:JSON.stringify({operationId})}).catch(() => {});
        }, {csrf:process.env.PROBE_CSRF, operationId:process.env.PROBE_OPERATION_ID});
      });`);
    const cli = fileURLToPath(new URL('../../node_modules/playwright/cli.js', import.meta.url));
    const child = startChild(state, 'playwright-error', process.execPath, [cli, 'test', '--config', configuration], directory,
      {...process.env, PROBE_CSRF: csrf, PROBE_OPERATION_ID: operationId});
    assert.deepEqual(await child.done, {code: 1, signal: null}, 'The controlled forwarding failure must fail Playwright');
    const log = await fs.readFile(path.join(state.artifacts, 'playwright-error.log'), 'utf8');
    assert.equal(receivedCookie, true, 'The real HTTPS request must carry the synthetic Cookie');
    assert.equal(receivedCsrf, true, 'The real HTTPS request must carry the synthetic CSRF value');
    assert.equal(log.includes(`Native HTTPS forwarding failed: phase=fetch operation_id=${operationId}`), true,
      'The failed reporter must retain its safe stage and operation ID');
    assert.equal(log.includes(cookie), false, 'A failed reporter must not expose the synthetic Cookie');
    assert.equal(log.includes(csrf), false, 'A failed reporter must not expose the synthetic CSRF value');
    assert.equal(log.includes('Call log:'), false, 'The original transport error and cause must not escape');
    await assert.rejects(cleanup(state), /cleanup or a child exit failed/);
    const receipt = JSON.parse(await fs.readFile(path.join(state.artifacts, 'cleanup.json'), 'utf8'));
    assert.equal(receipt.passed, false, 'An expected failing child must not become a successful fixture');
    assert.equal(receipt.private_material_removed, true);
    assert.equal(receipt.credential_logs_clean, true);
    t.diagnostic(JSON.stringify({https_cookie_received: receivedCookie, https_csrf_received: receivedCsrf,
      playwright_exit: 1, safe_phase_and_operation_id_logged: true, cookie_logged: false, csrf_logged: false,
      original_transport_call_log_retained: false, private_material_removed: receipt.private_material_removed}));
  } finally {
    await Promise.all(state.children.map(child => stopChild(child)));
    if (server) {
      server.closeAllConnections();
      await new Promise(resolve => server.close(resolve));
    }
    await fs.rm(directory, {recursive: true, force: true});
  }
});
