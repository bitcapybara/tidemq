#!/usr/bin/env node
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import dgram from 'node:dgram';
import { createReadStream, createWriteStream } from 'node:fs';
import fs from 'node:fs/promises';
import http from 'node:http';
import https from 'node:https';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';

const repository = fileURLToPath(new URL('../..', import.meta.url));

export function proxyRequest(port, request, response) {
  const upstream = http.request({
    hostname: '127.0.0.1', port, method: request.method,
    path: request.url, headers: request.headers,
  }, (received) => {
    response.writeHead(received.statusCode ?? 502, received.headers);
    received.pipe(response);
    received.on('error', () => response.destroy());
  });
  upstream.on('error', () => {
    if (!response.headersSent) response.writeHead(502);
    response.end('The test Console is unavailable');
  });
  request.on('aborted', () => upstream.destroy());
  response.on('close', () => upstream.destroy());
  request.pipe(upstream);
}

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

async function hashFile(file) {
  const digest = createHash('sha256');
  for await (const chunk of createReadStream(file)) digest.update(chunk);
  return digest.digest('hex');
}

function startChild(state, name, command, args, cwd, env = process.env) {
  const output = createWriteStream(path.join(state.artifacts, `${name}.log`), { flags: 'wx', mode: 0o600 });
  const child = spawn(command, args, { cwd, env, detached: process.platform !== 'win32', stdio: ['ignore', 'pipe', 'pipe'] });
  child.stdout.pipe(output, { end: false });
  child.stderr.pipe(output, { end: false });
  const managed = { name, child, output, result: null };
  managed.done = new Promise((resolve) => {
    const finish = (result) => {
      if (managed.result) return;
      managed.result = result;
      resolve(result);
    };
    child.once('error', (error) => finish({ error: error.message }));
    child.once('close', (code, signal) => {
      output.end();
      finish({ code, signal });
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

async function startCore(state, binary, signal) {
  const generated = path.join(state.privateDirectory, 'core');
  const generator = startChild(state, 'generate', 'bash', [
    path.join(repository, 'deploy/local/generate.sh'), 'rf1', generated,
  ], repository);
  assert.deepEqual(await generator.done, { code: 0, signal: null }, 'RF1 configuration generation failed');
  const ports = { external: await freePort(true), internal: await freePort(true), management: await freePort() };
  assert.notEqual(ports.external, ports.internal, 'The test QUIC listeners require distinct ports');
  const configuration = path.join(generated, 'node-1.toml');
  const text = (await fs.readFile(configuration, 'utf8'))
    .replaceAll('127.0.0.1:7410', `127.0.0.1:${ports.external}`)
    .replaceAll('127.0.0.1:7411', `127.0.0.1:${ports.internal}`)
    .replaceAll('127.0.0.1:8410', `127.0.0.1:${ports.management}`);
  await fs.writeFile(configuration, text, { mode: 0o600 });
  const core = startChild(state, 'combined', binary, ['serve', '--config', configuration], repository, state.serviceEnvironment);
  await waitReady(`http://127.0.0.1:${ports.management}/readyz`, core, signal);
  return { generated, ports, tokenFile: path.join(generated, 'access.token') };
}

async function startConsole(state, binary, web, core, signal) {
  const consolePort = await freePort();
  const managementPort = await freePort();
  state.proxy = https.createServer({
    key: await fs.readFile(path.join(core.generated, 'node-1.key')),
    cert: await fs.readFile(path.join(core.generated, 'node-1.pem')),
  }, (request, response) => proxyRequest(consolePort, request, response));
  const tlsPort = await listen(state.proxy);
  const baseURL = `https://127.0.0.1:${tlsPort}`;
  const keyFile = path.join(state.privateDirectory, 'session.key');
  await fs.writeFile(keyFile, randomBytes(32), { mode: 0o600 });
  const configuration = path.join(state.privateDirectory, 'console.toml');
  const quote = JSON.stringify;
  await fs.writeFile(configuration, `role = "console"
listen = "127.0.0.1:${consolePort}"
management_listen = "127.0.0.1:${managementPort}"
static_dir = ${quote(path.join(web, 'dist'))}
[cluster]
address = "127.0.0.1:${core.ports.external}"
server_name = "localhost"
trust_roots = ${quote(path.join(core.generated, 'ca.pem'))}
[session]
key_file = ${quote(keyFile)}
public_origin = ${quote(baseURL)}
max_age_seconds = 60
[logging]
filter = "warn,tidemq=info"
json = true
`, { mode: 0o600 });
  const consoleService = startChild(state, 'console', binary, ['serve', '--config', configuration], repository, state.serviceEnvironment);
  await waitReady(`http://127.0.0.1:${managementPort}/health/ready`, consoleService, signal);
  return baseURL;
}

function signalChild(managed, signal) {
  try {
    if (process.platform !== 'win32' && managed.child.pid) process.kill(-managed.child.pid, signal);
    else managed.child.kill(signal);
  } catch (error) {
    if (error.code !== 'ESRCH') throw error;
  }
}

async function stopChild(managed) {
  if (!managed.result) signalChild(managed, 'SIGTERM');
  const result = await Promise.race([managed.done, delay(8_000, null, { ref: false })]);
  if (!result) {
    signalChild(managed, 'SIGKILL');
    const killed = await Promise.race([managed.done, delay(5_000, null, { ref: false })]);
    assert(killed, `${managed.name} did not exit after forced cleanup`);
    return { name: managed.name, graceful: false, result: managed.result };
  }
  return { name: managed.name, graceful: !result.error, result };
}

async function cleanup(state) {
  if (state.proxy) {
    state.proxy.closeAllConnections();
    await new Promise((resolve) => state.proxy.close(resolve));
  }
  const children = [];
  for (const managed of [...state.children].reverse()) children.push(await stopChild(managed));
  await fs.rm(state.privateDirectory, { recursive: true, force: true });
  await fs.writeFile(path.join(state.artifacts, 'cleanup.json'), JSON.stringify({
    private_material_removed: true, children,
  }, null, 2));
  assert(children.every((child) => child.graceful), 'A browser fixture process required forced cleanup');
}

export async function run(args = process.argv.slice(2)) {
  const binary = path.resolve(process.env.TIDEMQ_BINARY ?? path.join(repository, 'target/debug/tidemq'));
  const web = path.resolve(process.env.TIDEMQ_WEB_DIR ?? path.join(repository, 'console/web'));
  await fs.access(binary);
  await fs.access(path.join(web, 'dist/index.html'));
  const output = path.join(repository, 'target/ci/console-browser-live');
  await fs.mkdir(output, { recursive: true });
  const artifacts = await fs.mkdtemp(path.join(output, 'run-'));
  const privateDirectory = await fs.mkdtemp(path.join(os.tmpdir(), 'tidemq-console-browser-'));
  await fs.chmod(privateDirectory, 0o700);
  const state = { artifacts, privateDirectory, children: [], proxy: null,
    serviceEnvironment: { ...process.env, LLVM_PROFILE_FILE: path.join(artifacts, 'service-%p-%m.profraw') } };
  const cancellation = new AbortController();
  const stop = () => cancellation.abort(new Error('The browser fixture was interrupted'));
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
  try {
    await fs.writeFile(path.join(artifacts, 'identity.json'), JSON.stringify({
      binary_sha256: await hashFile(binary), topology: 'independent-combined-console-https-proxy',
      replication_factor: 1, mocked_requests: false, browser_arguments: args,
    }, null, 2));
    const core = await startCore(state, binary, cancellation.signal);
    const baseURL = await startConsole(state, binary, web, core, cancellation.signal);
    console.log(`Running real Console browser tests at ${baseURL}; evidence: ${artifacts}`);
    const browser = startChild(state, 'playwright', 'npx', [
      '--no-install', 'playwright', 'test', '--config', 'playwright.live.config.ts', ...args,
    ], web, { ...process.env, TIDEMQ_LIVE_BASE_URL: baseURL, TIDEMQ_LIVE_TOKEN_FILE: core.tokenFile });
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
