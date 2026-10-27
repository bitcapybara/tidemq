import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import test from 'node:test';
import { cleanup, listen, proxyRequest, redactToken, startChild, stopChild } from './console-browser-live.mjs';

const token = 'a1'.repeat(32);

async function redacted(chunks) {
  const output = [];
  for await (const chunk of Readable.from(chunks).pipe(redactToken(token))) output.push(chunk);
  return Buffer.concat(output).toString('utf8');
}

test('redaction removes repeated tokens at every chunk boundary and preserves UTF-8', async () => {
  const text = `Success: ${token}; locator.fill failed: fill("${token}"); tail 🌐`;
  const bytes = Buffer.from(text);
  const expected = text.replaceAll(token, '[REDACTED]');
  for (let cut = 1; cut < bytes.length; cut++) {
    assert.equal(await redacted([bytes.subarray(0, cut), bytes.subarray(cut)]), expected);
  }
  assert.equal(await redacted([...bytes].map((byte) => Buffer.from([byte]))), expected);
  assert.equal(await redacted([Buffer.from('short output')]), 'short output');
});

async function fixture() {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'tidemq-browser-runner-test-'));
  const state = { artifacts: path.join(root, 'public'), privateDirectory: path.join(root, 'private'), token, children: [], proxy: null };
  await fs.mkdir(state.artifacts);
  await fs.mkdir(state.privateDirectory);
  return { root, state };
}

for (const code of [0, 1]) {
  test(`child output is sanitized before logs close, including a failed fill (exit ${code})`, async () => {
    const { root, state } = await fixture();
    try {
      const child = startChild(state, 'playwright', process.execPath, ['-e', `
        const token = process.env.TEST_PRIVATE_TOKEN;
        process.stdout.write('Success: ' + token.slice(0, 17));
        setTimeout(() => {
          process.stdout.write(token.slice(17) + '\\n');
          process.stderr.write('locator.fill: fill("' + token + '") failed\\n');
          process.exitCode = ${code};
        }, 20);
      `], root, { ...process.env, TEST_PRIVATE_TOKEN: token });
      assert.deepEqual(await child.done, { code, signal: null });
      const log = await fs.readFile(path.join(state.artifacts, 'playwright.log'), 'utf8');
      assert(!log.includes(token));
      assert.equal(log.match(/\[REDACTED\]/g)?.length, 2);
      if (code === 0) await cleanup(state);
      else await assert.rejects(cleanup(state), /cleanup or a child exit failed/);
      const receipt = JSON.parse(await fs.readFile(path.join(state.artifacts, 'cleanup.json'), 'utf8'));
      assert.equal(receipt.passed, code === 0);
      assert.equal(receipt.private_material_removed, true);
      assert.equal(receipt.children[0].graceful, code === 0);
      assert(!JSON.stringify(receipt).includes(token));
    } finally {
      await Promise.all(state.children.map((child) => stopChild(child)));
      await fs.rm(root, { recursive: true, force: true });
    }
  });
}

for (const stream of ['stdout', 'stderr']) {
test(`interleaving cannot expose a token split across ${stream} chunks`, async () => {
  const { root, state } = await fixture();
  const other = stream === 'stdout' ? 'stderr' : 'stdout';
  try {
    const child = startChild(state, 'interleaved', process.execPath, ['-e', `
      const token = process.env.TEST_PRIVATE_TOKEN;
      const hold = setInterval(() => {}, 1000);
      process.on('SIGUSR1', () => process.${other}.write('intervening diagnostic\\n'));
      process.on('SIGUSR2', () => { process.${stream}.write(token.slice(17)); clearInterval(hold); });
      process.${stream}.write(token.slice(0, 17));
    `], root, { ...process.env, TEST_PRIVATE_TOKEN: token });
    await new Promise((resolve) => child.child[stream].once('data', resolve));
    const interrupted = new Promise((resolve) => child.child[other].once('data', resolve));
    child.child.kill('SIGUSR1');
    await interrupted;
    child.child.kill('SIGUSR2');
    assert.deepEqual(await child.done, { code: 0, signal: null });
    const log = await fs.readFile(path.join(state.artifacts, 'interleaved.log'), 'utf8');
    assert(log.includes('intervening diagnostic'));
    assert(log.includes('[REDACTED]'));
    assert(!log.includes(token.slice(0, 17)) && !log.includes(token.slice(17)));
    await cleanup(state);
  } finally {
    await Promise.all(state.children.map((child) => stopChild(child)));
    await fs.rm(root, { recursive: true, force: true });
  }
});
}

test('signal exits fail the gate and cleanup continues through remaining children and private files', async () => {
  const { root, state } = await fixture();
  try {
    const survivor = startChild(state, 'combined', process.execPath, ['-e', `
      process.on('SIGTERM', () => process.exit(0));
      process.stdout.write('ready');
      setInterval(() => {}, 1000);
    `], root);
    await new Promise((resolve) => survivor.child.stdout.once('data', resolve));
    const killed = startChild(state, 'console', process.execPath, ['-e', 'setInterval(() => {}, 1000)'], root);
    killed.child.kill('SIGKILL');
    assert.deepEqual(await killed.done, { code: null, signal: 'SIGKILL' });
    await assert.rejects(cleanup(state), /cleanup or a child exit failed/);
    const receipt = JSON.parse(await fs.readFile(path.join(state.artifacts, 'cleanup.json'), 'utf8'));
    assert.equal(receipt.passed, false);
    assert.equal(receipt.private_material_removed, true);
    assert.equal(receipt.children.length, 2);
    assert.equal(receipt.children[0].graceful, false);
    assert.equal(receipt.children[1].graceful, true);
    assert.deepEqual(survivor.result, { code: 0, signal: null });
    await assert.rejects(fs.access(state.privateDirectory));
  } finally {
    await Promise.all(state.children.map((child) => stopChild(child)));
    await fs.rm(root, { recursive: true, force: true });
  }
});

test('the browser fixture proxy preserves origin, cookies, bodies and status without mocking the API', async () => {
  const upstream = http.createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    assert.equal(request.headers.origin, 'https://127.0.0.1:8443');
    assert.equal(request.headers.cookie, '__Host-test=opaque');
    assert.equal(request.headers.host, '127.0.0.1:8443');
    assert.equal(body, '{"operationId":"original-operation"}');
    response.writeHead(409, { 'content-type': 'application/json', 'set-cookie': '__Host-test=new; Secure; HttpOnly; Path=/' });
    response.end('{"error":"operation_conflict"}');
  });
  const upstreamPort = await listen(upstream);
  const proxy = http.createServer((request, response) => proxyRequest(upstreamPort, request, response));
  const proxyPort = await listen(proxy);
  try {
    const result = await new Promise((resolve, reject) => {
      const request = http.request({ hostname: '127.0.0.1', port: proxyPort, path: '/api/v1/operations', method: 'POST',
        headers: { origin: 'https://127.0.0.1:8443', host: '127.0.0.1:8443', cookie: '__Host-test=opaque', 'content-type': 'application/json' } },
      async (response) => {
        let body = '';
        for await (const chunk of response) body += chunk;
        resolve({ status: response.statusCode, cookie: response.headers['set-cookie'], body });
      });
      request.once('error', reject);
      request.end('{"operationId":"original-operation"}');
    });
    assert.deepEqual(result, { status: 409, cookie: ['__Host-test=new; Secure; HttpOnly; Path=/'], body: '{"error":"operation_conflict"}' });
  } finally {
    proxy.closeAllConnections();
    upstream.closeAllConnections();
    await Promise.all([new Promise((resolve) => proxy.close(resolve)), new Promise((resolve) => upstream.close(resolve))]);
  }
});

test('an unavailable Console produces an explicit proxy failure', async () => {
  const stopped = http.createServer();
  const unused = await listen(stopped);
  await new Promise((resolve) => stopped.close(resolve));
  const proxy = http.createServer((request, response) => proxyRequest(unused, request, response));
  const port = await listen(proxy);
  try {
    const response = await fetch(`http://127.0.0.1:${port}/api/v1/session`);
    assert.equal(response.status, 502);
    assert.equal(await response.text(), 'The test Console is unavailable');
  } finally {
    proxy.closeAllConnections();
    await new Promise((resolve) => proxy.close(resolve));
  }
});
