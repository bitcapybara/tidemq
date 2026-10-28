import assert from 'node:assert/strict';
import https from 'node:https';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import test from 'node:test';
import { cleanup, listen, rewriteNodeConfiguration, verifyHttps, redactToken, startChild, stopChild } from './console-browser-live.mjs';

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
  const state = { artifacts: path.join(root, 'public'), privateDirectory: path.join(root, 'private'), token, children: [] };
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

test('native fixtures rewrite every advertised peer and the built-in Console endpoint', () => {
  const ports = new Map([[7410, 41000], [7411, 41001], [8410, 41002], [8443, 41003]]);
  const text = ["role = 'combined'", "listen = '127.0.0.1:7410'", "internal_address = '127.0.0.1:7411'", "management_listen = '127.0.0.1:8410'", "static_dir = '/missing/console'", "public_origin = 'https://localhost:8443'"].join('\n');
  const rewritten = rewriteNodeConfiguration(text, ports, '/published/assets');
  for (const port of [41000, 41001, 41002, 41003]) assert.ok(rewritten.includes(':' + port));
  assert.ok(rewritten.includes('static_dir = "/published/assets"'));
  assert.ok(rewritten.includes("role = 'combined'"));
});

test('native HTTPS fixture verifies its generated CA and rejects an untrusted signer', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'tidemq-native-https-'));
  let server;
  try {
    const generator = fileURLToPath(new URL('../../deploy/local/generate.sh', import.meta.url));
    for (const name of ['trusted', 'other']) {
      const issued = spawnSync('bash', [generator, 'rf1', path.join(root, name)], {encoding: 'utf8'});
      assert.equal(issued.status, 0, issued.stderr);
    }
    server = https.createServer({cert: await fs.readFile(path.join(root, 'trusted/node-1.pem')), key: await fs.readFile(path.join(root, 'trusted/node-1.key'))}, (_request, response) => response.end('Console'));
    const port = await listen(server);
    const url = 'https://127.0.0.1:' + port;
    await verifyHttps(url, path.join(root, 'trusted/ca.pem'));
    await assert.rejects(verifyHttps(url, path.join(root, 'other/ca.pem')));
  } finally {
    if (server) {
      server.closeAllConnections();
      await new Promise(resolve => server.close(resolve));
    }
    await fs.rm(root, {recursive: true, force: true});
  }
});
