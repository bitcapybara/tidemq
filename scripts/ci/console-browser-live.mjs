#!/usr/bin/env node
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import dgram from 'node:dgram';
import { createReadStream, createWriteStream } from 'node:fs';
import fs from 'node:fs/promises';
import https from 'node:https';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { PassThrough, Transform } from 'node:stream';
import { finished, pipeline } from 'node:stream/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { operationId } from '../../console/web/src/features/logic.ts';

const repository = fileURLToPath(new URL('../..', import.meta.url));

export async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert(address && typeof address === 'object', 'A test listener requires an IP address');
  return address.port;
}

async function freePort(udp = false) {
  if (udp) {
    const socket = dgram.createSocket('udp4');
    await new Promise((resolve, reject) => {
      socket.once('error', reject);
      socket.bind(0, '127.0.0.1', resolve);
    });
    const port = socket.address().port;
    await new Promise((resolve) => socket.close(resolve));
    return port;
  }
  const server = net.createServer();
  const port = await listen(server);
  await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  return port;
}

async function uniquePort(used, udp = false) {
  for (let attempt = 0; attempt < 32; attempt++) {
    const port = await freePort(udp);
    if (!used.has(port)) {
      used.add(port);
      return port;
    }
  }
  throw new Error('The fixture could not allocate distinct ports');
}

async function hashFile(file) {
  const digest = createHash('sha256');
  for await (const chunk of createReadStream(file)) digest.update(chunk);
  return digest.digest('hex');
}

export function redactToken(token) {
  if (!token) return new PassThrough();
  const secrets = () => (typeof token === 'function' ? token() : [token]).filter(Boolean).map(value => Buffer.from(value));
  let pending = Buffer.alloc(0);
  return new Transform({
    transform(chunk, _encoding, callback) {
      pending = Buffer.concat([pending, chunk]);
      const values = secrets();
      let match;
      while ((match = values.map(secret => ({secret, at: pending.indexOf(secret)})).filter(value => value.at >= 0).sort((a, b) => a.at - b.at || b.secret.length - a.secret.length)[0])) {
        const {secret, at} = match;
        this.push(pending.subarray(0, at));
        this.push('[REDACTED]');
        pending = pending.subarray(at + secret.length);
      }
      // Retain a possible token prefix until the next chunk of this stream arrives.
      const safe = Math.max(0, pending.length - Math.max(1, ...values.map(value => value.length)) + 1);
      this.push(pending.subarray(0, safe));
      pending = pending.subarray(safe);
      callback();
    },
    flush(callback) {
      this.push(pending);
      callback();
    },
  });
}

export function startChild(state, name, command, args, cwd, env = process.env) {
  const output = createWriteStream(path.join(state.artifacts, `${name}.log`), { flags: 'wx', mode: 0o600 });
  const child = spawn(command, args, { cwd, env, detached: process.platform !== 'win32', stdio: ['ignore', 'pipe', 'pipe'] });
  const managed = { name, child, output, result: null };
  let error;
  const logFailed = () => {
    error = 'Fixture log write failed';
    try { signalChild(managed, 'SIGTERM'); } catch { /* Cleanup retries every owned process. */ }
  };
  // Redact independently so stderr cannot interrupt a token split across stdout chunks.
  const logging = Promise.all([child.stdout, child.stderr].map((source) =>
    pipeline(source, redactToken(() => state.secrets ?? [state.token]), output, { end: false }).catch(logFailed),
  )).then(() => {
    output.end();
    return finished(output);
  }).catch(logFailed);
  managed.done = new Promise((resolve) => {
    child.once('error', () => { error = 'Fixture process could not start'; });
    child.once('close', async (code, signal) => {
      await logging;
      managed.result = { code, signal, ...(error ? { error } : {}) };
      resolve(managed.result);
    });
  });
  state.children.push(managed);
  return managed;
}

async function waitReady(url, managed, signal) {
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    signal.throwIfAborted();
    assert.equal(managed.result, null, `${managed.name} exited before readiness; inspect its log`);
    try {
      const response = await fetch(url, { signal: AbortSignal.any([signal, AbortSignal.timeout(1_000)]) });
      await response.body?.cancel();
      if (response.ok) return;
    } catch (error) {
      if (signal.aborted) throw error;
    }
    await delay(100, undefined, { signal });
  }
  throw new Error(`${managed.name} did not become ready before its deadline`);
}

export function rewriteNodeConfiguration(text, ports, staticDir) {
  return text.replace(/127\.0\.0\.1:(\d+)/g, (address, port) => ports.has(Number(port)) ? '127.0.0.1:' + ports.get(Number(port)) : address)
    .replace(/localhost:8443/g, 'localhost:' + ports.get(8443))
    .replace(/^static_dir = .*$/m, 'static_dir = ' + JSON.stringify(staticDir));
}

export async function verifyHttps(baseURL, caFile, signal) {
  const ca = await fs.readFile(caFile);
  await new Promise((resolve, reject) => {
    const request = https.get(new URL('/', baseURL), {ca, signal}, response => {
      response.resume();
      if (response.statusCode === 200) resolve();
      else reject(new Error('Native HTTPS Console returned ' + response.statusCode));
    });
    request.setTimeout(2_000, () => request.destroy(new Error('Native HTTPS verification timed out')));
    request.once('error', reject);
  });
}

async function startCore(state, binary, web, topology, signal) {
  const generated = path.join(state.privateDirectory, 'core');
  const mode = topology === 'combined' ? 'rf1' : 'rf1-separated';
  const generator = startChild(state, 'generate', 'bash', [
    path.join(repository, 'deploy/local/generate.sh'), mode, generated,
  ], repository);
  assert.deepEqual(await generator.done, {code: 0, signal: null}, 'RF1 configuration generation failed');
  state.token = (await fs.readFile(path.join(generated, 'access.token'), 'utf8')).trim();
  state.secrets = [state.token];
  assert(/^[0-9a-f]{64}$/.test(state.token), 'The fixture requires a generated bootstrap token');
  const nodes = topology === 'combined' ? [1] : [11, 21, 31, 41];
  const ports = new Map();
  for (const node of nodes) {
    for (const [original, udp] of [[7400 + node * 10, true], [7401 + node * 10, true], [8400 + node * 10, false]]) {
      ports.set(original, await uniquePort(state.ports, udp));
    }
  }
  ports.set(8443, await uniquePort(state.ports));
  const processes = [];
  for (const node of nodes) {
    const configuration = path.join(generated, 'node-' + node + '.toml');
    const text = rewriteNodeConfiguration(await fs.readFile(configuration, 'utf8'), ports, path.join(web, 'dist'));
    await fs.writeFile(configuration, text, {mode: 0o600});
    processes.push({node, process: startChild(state, 'node-' + node, binary, ['serve', '--config', configuration], repository, state.serviceEnvironment)});
  }
  await Promise.all(processes.map(item => waitReady('http://127.0.0.1:' + ports.get(8400 + item.node * 10) + '/readyz', item.process, signal)));
  return {generated, ports, tokenFile: path.join(generated, 'access.token'), baseURL: 'https://localhost:' + ports.get(8443)};
}

async function startConsole(state, binary, web, core, signal) {
  const consolePort = await uniquePort(state.ports);
  const managementPort = await uniquePort(state.ports);
  const baseURL = 'https://127.0.0.1:' + consolePort;
  const keyFile = path.join(state.privateDirectory, 'session.key');
  await fs.writeFile(keyFile, randomBytes(32), {mode: 0o600});
  const configuration = path.join(state.privateDirectory, 'console.toml');
  const quote = JSON.stringify;
  const text = [
    'role = "console"',
    'listen = "127.0.0.1:' + consolePort + '"',
    'management_listen = "127.0.0.1:' + managementPort + '"',
    'static_dir = ' + quote(path.join(web, 'dist')),
    '[tls]',
    'certificate = ' + quote(path.join(core.generated, 'node-41.pem')),
    'private_key = ' + quote(path.join(core.generated, 'node-41.key')),
    '[cluster]',
    'address = "127.0.0.1:' + core.ports.get(7810) + '"',
    'server_name = "node-41.local"',
    'trust_roots = ' + quote(path.join(core.generated, 'ca.pem')),
    '[session]',
    'key_file = ' + quote(keyFile),
    'public_origin = ' + quote(baseURL),
    'max_age_seconds = 60',
    '[logging]',
    'filter = "warn,tidemq=info"',
    'json = true',
  ].join('\n');
  await fs.writeFile(configuration, text, {mode: 0o600});
  const consoleService = startChild(state, 'console', binary, ['serve', '--config', configuration], repository, state.serviceEnvironment);
  await waitReady('http://127.0.0.1:' + managementPort + '/health/ready', consoleService, signal);
  return baseURL;
}

function fixtureRequest(baseURL, ca, method, resource, body, headers = {}, signal) {
  return new Promise((resolve, reject) => {
    const payload = body === undefined ? undefined : JSON.stringify(body);
    const request = https.request(new URL(resource, baseURL), {
      method, ca, signal, timeout: 15_000,
      headers: {origin: baseURL, 'content-type': 'application/json', ...headers},
    }, response => {
      let text = '';
      response.on('data', chunk => {
        text += chunk;
        if (text.length > 1_048_576) response.destroy(new Error('Fixture response exceeded its limit'));
      });
      response.on('error', () => reject(new Error('Fixture HTTPS response failed')));
      response.on('end', () => {
        try { resolve({status: response.statusCode, headers: response.headers, value: JSON.parse(text)}); }
        catch { reject(new Error('Fixture HTTPS returned invalid JSON')); }
      });
    });
    request.on('timeout', () => request.destroy());
    request.on('error', () => reject(new Error('Fixture HTTPS request failed')));
    request.end(payload);
  });
}

async function seedContext(state, binary, core, baseURL, topology, signal) {
  const admin = async (name, args) => {
    signal.throwIfAborted();
    const endpoint = '127.0.0.1:' + core.ports.get(topology === 'combined' ? 7410 : 7810);
    const managed = startChild(state, name, binary, ['admin', '--endpoint', endpoint,
      '--ca', path.join(core.generated, 'ca.pem'), '--token-file', core.tokenFile, ...args], repository);
    let stdout = '';
    managed.child.stdout.on('data', chunk => { stdout += chunk; });
    const stop = () => signalChild(managed, 'SIGTERM');
    signal.addEventListener('abort', stop, {once: true});
    try { assert.deepEqual(await managed.done, {code: 0, signal: null}, 'Native seed administration failed'); }
    finally { signal.removeEventListener('abort', stop); }
    return JSON.parse(stdout).result.id;
  };
  const tenantId = await admin('seed-tenant', ['create-tenant', 'live-allowed']);
  const otherTenant = await admin('seed-other-tenant', ['create-tenant', 'live-other']);
  const namespaceId = await admin('seed-namespace', ['create-namespace', tenantId, 'allowed']);
  const deniedNamespace = await admin('seed-denied-namespace', ['create-namespace', tenantId, 'denied']);
  const otherNamespace = await admin('seed-other-namespace', ['create-namespace', otherTenant, 'other']);
  const ca = await fs.readFile(path.join(core.generated, 'ca.pem'));
  const login = await fixtureRequest(baseURL, ca, 'POST', '/api/v1/session', {token: state.token}, {}, signal);
  assert.equal(login.status, 200, 'Native seed session failed');
  const headers = {cookie: login.headers['set-cookie'].map(value => value.split(';')[0]).join('; '),
    'x-csrf-token': login.value.csrfToken};
  const write = async (resource, body) => {
    signal.throwIfAborted();
    const reply = await fixtureRequest(baseURL, ca, 'POST', resource, {...body, operationId: operationId()}, headers, signal);
    assert.equal(reply.status, 200, 'Native seed write HTTP request failed');
    assert.equal(reply.value.status, 'succeeded', 'Native seed write did not commit');
    return reply.value;
  };
  const allowed = await write('/api/v1/topics', {tenantId, namespaceId, name: 'visible'});
  const denied = await write('/api/v1/topics', {tenantId, namespaceId: deniedNamespace, name: 'hidden'});
  const other = await write('/api/v1/topics', {tenantId: otherTenant, namespaceId: otherNamespace, name: 'foreign'});
  const principal = await write('/api/v1/principals', {name: 'live-inspector', tenantId, namespaceId, permissions: ['inspect']});
  const credential = async name => {
    const issued = await write('/api/v1/credentials', {principalId: principal.resourceId, lifetimeMs: '3600000'});
    assert.equal(typeof issued.secret, 'string', 'Native seed credential secret is missing');
    state.secrets.push(issued.secret);
    const tokenFile = path.join(state.privateDirectory, name + '.token');
    await fs.writeFile(tokenFile, issued.secret, {mode: 0o600});
    return {tokenFile, credentialId: issued.resourceId};
  };
  const restricted = await credential('restricted');
  const revocable = {};
  for (const browser of ['chromium', 'firefox', 'webkit']) revocable[browser] = await credential(browser);
  const contextFile = path.join(state.privateDirectory, 'live-context.json');
  await fs.writeFile(contextFile, JSON.stringify({topology, tenantId, namespaceId,
    allowedTopicId: allowed.resourceId, sameTenantDeniedTopicId: denied.resourceId,
    otherTenantTopicId: other.resourceId, restricted, revocable}), {mode: 0o600});
  return contextFile;
}

function signalChild(managed, signal) {
  try {
    if (process.platform !== 'win32' && managed.child.pid) process.kill(-managed.child.pid, signal);
    else managed.child.kill(signal);
  } catch (error) {
    if (error.code !== 'ESRCH') throw error;
  }
}

export async function stopChild(managed) {
  if (!managed.result) signalChild(managed, 'SIGTERM');
  const result = await Promise.race([managed.done, delay(8_000, null, { ref: false })]);
  if (!result) {
    signalChild(managed, 'SIGKILL');
    const killed = await Promise.race([managed.done, delay(5_000, null, { ref: false })]);
    return { name: managed.name, graceful: false, result: killed };
  }
  return { name: managed.name, graceful: !result.error && result.code === 0 && result.signal === null, result };
}

export async function cleanup(state) {
  const failures = [];
  const children = [];
  for (const managed of [...state.children].reverse()) {
    try { children.push(await stopChild(managed)); }
    catch { children.push({ name: managed.name, graceful: false, error: 'Fixture process cleanup failed' }); }
  }
  let credentialLogsClean = true;
  try {
    for (const name of await fs.readdir(state.artifacts)) {
      if (!name.endsWith('.log')) continue;
      const file = path.join(state.artifacts, name);
      const original = await fs.readFile(file, 'utf8');
      let sanitized = original;
      for (const secret of state.secrets ?? [state.token]) {
        if (secret) sanitized = sanitized.replaceAll(secret, '[REDACTED]');
      }
      if (sanitized !== original) {
        credentialLogsClean = false;
        await fs.writeFile(file, sanitized, {mode: 0o600});
      }
    }
  } catch { credentialLogsClean = false; }
  if (!credentialLogsClean) failures.push('Credential redaction failed');
  let privateMaterialRemoved = false;
  try {
    await fs.rm(state.privateDirectory, { recursive: true, force: true });
    privateMaterialRemoved = true;
  } catch { failures.push('Private fixture removal failed'); }
  const passed = privateMaterialRemoved && failures.length === 0 && children.every((child) => child.graceful);
  await fs.writeFile(path.join(state.artifacts, 'cleanup.json'), JSON.stringify({
    passed, private_material_removed: privateMaterialRemoved, credential_logs_clean: credentialLogsClean, children, failures,
  }, null, 2));
  assert(passed, 'Browser fixture cleanup or a child exit failed; inspect cleanup.json');
}

export async function run(args = process.argv.slice(2)) {
  const topology = process.env.TIDEMQ_LIVE_TOPOLOGY ?? 'combined';
  assert(['combined', 'separated-rf1'].includes(topology), 'Unsupported live test topology');
  const binary = path.resolve(process.env.TIDEMQ_BINARY ?? path.join(repository, 'target/debug/tidemq'));
  const web = path.resolve(process.env.TIDEMQ_WEB_DIR ?? path.join(repository, 'console/web'));
  await fs.access(binary);
  await fs.access(path.join(web, 'dist/index.html'));
  const output = path.join(repository, 'target/ci/console-browser-live');
  await fs.mkdir(output, { recursive: true });
  const artifacts = await fs.mkdtemp(path.join(output, 'run-'));
  const privateDirectory = await fs.mkdtemp(path.join(os.tmpdir(), 'tidemq-console-browser-'));
  await fs.chmod(privateDirectory, 0o700);
  const state = { artifacts, privateDirectory, children: [], ports: new Set(),
    serviceEnvironment: { ...process.env, LLVM_PROFILE_FILE: path.join(artifacts, 'service-%p-%m.profraw') } };
  const cancellation = new AbortController();
  const stop = () => cancellation.abort(new Error('The browser fixture was interrupted'));
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
  try {
    await fs.writeFile(path.join(artifacts, 'identity.json'), JSON.stringify({
      binary_sha256: await hashFile(binary), topology: 'native-' + topology,
      execution: topology === 'combined' ? 'local' : 'raft',
      replication_factor: topology === 'combined' ? null : 1,
      mocked_requests: false, browser_arguments: args,
    }, null, 2));
    const core = await startCore(state, binary, web, topology, cancellation.signal);
    const baseURL = topology === 'combined' ? core.baseURL : await startConsole(state, binary, web, core, cancellation.signal);
    await verifyHttps(baseURL, path.join(core.generated, 'ca.pem'), cancellation.signal);
    const contextFile = await seedContext(state, binary, core, baseURL, topology, cancellation.signal);
    cancellation.signal.throwIfAborted();
    console.log(`Running real Console browser tests at ${baseURL}; evidence: ${artifacts}`);
    const browser = startChild(state, 'playwright', 'npx', [
      '--no-install', 'playwright', 'test', '--config', 'playwright.live.config.ts', ...args,
      '--reporter=line', '--trace=off', '--output', path.join(privateDirectory, 'playwright-output'),
    ], web, { ...process.env, TIDEMQ_LIVE_BASE_URL: baseURL, TIDEMQ_LIVE_TOKEN_FILE: core.tokenFile,
      TIDEMQ_LIVE_CONTEXT_FILE: contextFile });
    const abortBrowser = () => signalChild(browser, 'SIGTERM');
    cancellation.signal.addEventListener('abort', abortBrowser, { once: true });
    const result = await browser.done;
    cancellation.signal.removeEventListener('abort', abortBrowser);
    cancellation.signal.throwIfAborted();
    assert.deepEqual(result, { code: 0, signal: null }, `Real Console browser tests failed; inspect ${artifacts}`);
  } finally {
    try {
      await cleanup(state);
    } finally {
      process.removeListener('SIGINT', stop);
      process.removeListener('SIGTERM', stop);
    }
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  run().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
