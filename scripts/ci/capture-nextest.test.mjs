import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawn, spawnSync} from 'node:child_process';
import test from 'node:test';

const nextest = new URL('./nextest.sh', import.meta.url).pathname;
const capture = new URL('./capture-nextest.sh', import.meta.url).pathname;

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-nextest-output-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  fs.mkdirSync(path.join(root, 'bin'));
  const source = [
    '#!/usr/bin/env node',
    'const fs = require("node:fs");',
    'const run = () => {',
    '  const files = [fs.fstatSync(1).isFile(), fs.fstatSync(2).isFile()];',
    '  fs.writeSync(1, JSON.stringify({files, args: process.argv.slice(2), umask: process.umask()}) + "\\n");',
    '  fs.writeSync(2, "Complete diagnostic output: \\u03bb\\nSYNTHETIC_FINAL_FAILURE_MARKER\\n");',
    '  if (process.env.GATE) fs.writeFileSync(process.env.GATE + ".done", "done");',
    '  process.exit(files.every(Boolean) ? Number(process.env.TEST_EXIT ?? 0) : 42);',
    '};',
    'if (process.env.GATE) {',
    '  for (const [signal, status] of [["SIGTERM", 143], ["SIGINT", 130]]) process.on(signal, () => { fs.writeSync(2, "Cancellation reached the owned test command\\n"); fs.writeFileSync(process.env.GATE + ".done", "cancelled"); process.exit(Number(process.env.CANCEL_EXIT ?? status)); });',
    '  fs.writeFileSync(process.env.GATE + ".ready", "ready");',
    '  const tick = setInterval(() => { if (fs.existsSync(process.env.GATE)) { clearInterval(tick); run(); } }, 2);',
    '} else run();',
  ].join('\n');
  fs.writeFileSync(path.join(root, 'bin', 'cargo'), source, {mode: 0o755});
  const env = {...process.env, PATH: path.join(root, 'bin') + ':' + process.env.PATH};
  delete env.TIDEMQ_NEXTEST_IMAGE;
  const output = () => {
    const entries = fs.readdirSync(path.join(root, 'target/ci')).filter(name => name.startsWith('nextest-output.'));
    assert.equal(entries.length, 1);
    const dir = path.join(root, 'target/ci', entries[0]);
    return {dir, receipt: JSON.parse(fs.readFileSync(path.join(dir, 'receipt.json'), 'utf8')),
      stdout: fs.readFileSync(path.join(dir, 'stdout.log'), 'utf8'),
      stderr: fs.readFileSync(path.join(dir, 'stderr.log'), 'utf8')};
  };
  return {root, env, output};
}

test('actual native nextest entry uses regular files and forwards both streams without changing arguments', t => {
  const f = fixture(t), args = ['--profile', 'ci', '-E', 'test(a b)', '--locked'];
  const result = spawnSync('bash', [nextest, ...args], {cwd: f.root, env: f.env, encoding: 'utf8'});
  assert.equal(result.status, 0, result.stdout + result.stderr);
  const output = f.output();
  assert.deepEqual(JSON.parse(output.stdout), {files: [true, true], args: ['nextest', 'run', ...args], umask: process.umask()});
  assert.equal(result.stdout, output.stdout);
  assert.equal(result.stderr, output.stderr);
  assert.equal(output.receipt.nextest_exit, 0);
  assert.equal(output.receipt.stdout_bytes, Buffer.byteLength(output.stdout));
  assert.equal(output.receipt.stderr_bytes, Buffer.byteLength(output.stderr));
  assert.equal(fs.statSync(output.dir).mode & 0o777, 0o700);
  for (const name of ['stdout.log', 'stderr.log', 'receipt.json']) assert.equal(fs.statSync(path.join(output.dir, name)).mode & 0o777, 0o600);
});

test('cancellation reaches and joins the owned command before recording its terminal result', {timeout: 10000}, async t => {
  for (const [signal, cancelled, actual] of [['SIGTERM', 143, 143], ['SIGINT', 130, 130], ['SIGTERM', 143, 0], ['SIGTERM', 143, 110]]) {
  const f = fixture(t), gate = path.join(f.root, 'gate');
  const child = spawn('bash', [nextest, '--lib'], {cwd: f.root, env: {...f.env, GATE: gate, CANCEL_EXIT: String(actual)}, stdio: ['ignore', 'pipe', 'pipe']});
  child.stdout.resume(); child.stderr.resume();
  const exited = new Promise((resolve, reject) => { child.once('error', reject); child.once('close', (code, signal) => resolve({code, signal})); });
  const waitFor = async file => {
    const until = Date.now() + 3000;
    while (!fs.existsSync(file)) {
      assert(Date.now() < until, 'The controlled command must reach its lifecycle boundary');
      await new Promise(resolve => setTimeout(resolve, 2));
    }
  };
  try {
    await waitFor(gate + '.ready');
    child.kill(signal);
    const result = await exited;
    assert.deepEqual(result, {code: actual || cancelled, signal: null});
    assert.equal(fs.readFileSync(gate + '.done', 'utf8'), 'cancelled');
    const output = f.output();
    assert.equal(output.receipt.nextest_exit, actual);
    assert.equal(output.receipt.cancellation_exit, cancelled);
    assert(output.stderr.includes('Cancellation reached the owned test command'));
  } finally {
    fs.writeFileSync(gate, 'release');
    await waitFor(gate + '.done');
  }
  }
});

test('test failure and reporter failure remain nonzero with complete raw output', t => {
  for (const status of [100, 110]) {
    const f = fixture(t);
    const result = spawnSync('bash', [nextest, '--lib'], {cwd: f.root, env: {...f.env, TEST_EXIT: String(status)}, encoding: 'utf8'});
    assert.equal(result.status, status, result.stderr);
    const output = f.output();
    assert.equal(output.receipt.nextest_exit, status);
    assert.equal(output.receipt.effective_exit, status);
    assert(output.stderr.includes('SYNTHETIC_FINAL_FAILURE_MARKER'));
    assert.equal(result.stderr, output.stderr);
  }
});

test('closed console readers cannot interrupt execution and cannot make a failure pass', {timeout: 10000}, async t => {
  for (const status of [0, 100, 110]) {
    const f = fixture(t), gate = path.join(f.root, 'gate');
    const child = spawn('bash', [nextest, '--lib'], {cwd: f.root, env: {...f.env, GATE: gate, TEST_EXIT: String(status)}, stdio: ['ignore', 'pipe', 'pipe']});
    const exited = new Promise((resolve, reject) => { child.once('error', reject); child.once('close', (code, signal) => resolve({code, signal})); });
    const until = Date.now() + 3000;
    while (!fs.existsSync(gate + '.ready')) {
      assert(Date.now() < until, 'The controlled test command must enter before the console is closed');
      await new Promise(resolve => setTimeout(resolve, 2));
    }
    child.stdout.destroy(); child.stderr.destroy();
    fs.writeFileSync(gate, 'release');
    const result = await exited;
    assert.equal(result.signal, null);
    assert.equal(result.code, status || 1);
    const output = f.output();
    assert.equal(output.receipt.nextest_exit, status);
    assert.equal(output.receipt.effective_exit, status || 1);
    assert(output.receipt.stdout_forward_exit !== 0 || output.receipt.stderr_forward_exit !== 0);
    assert.deepEqual(JSON.parse(output.stdout).files, [true, true]);
    assert(output.stderr.includes('SYNTHETIC_FINAL_FAILURE_MARKER'));
  }
});

test('capture setup failure refuses execution instead of discarding output', t => {
  const f = fixture(t);
  fs.writeFileSync(path.join(f.root, 'target'), 'not a directory');
  const result = spawnSync('bash', [capture, 'cargo'], {cwd: f.root, env: f.env, encoding: 'utf8'});
  assert.notEqual(result.status, 0);
  assert(!result.stdout.includes('SYNTHETIC_FINAL_FAILURE_MARKER'));
});

test('each invocation keeps its own capture files', t => {
  const f = fixture(t);
  for (let i = 0; i < 2; i++) assert.equal(spawnSync('bash', [nextest, '--lib'], {cwd: f.root, env: f.env, encoding: 'utf8'}).status, 0);
  const entries = fs.readdirSync(path.join(f.root, 'target/ci')).filter(name => name.startsWith('nextest-output.'));
  assert.equal(entries.length, 2);
  for (const entry of entries) assert(fs.readFileSync(path.join(f.root, 'target/ci', entry, 'stderr.log'), 'utf8').includes('SYNTHETIC_FINAL_FAILURE_MARKER'));
});

test('regular-file write errors remain failures instead of being treated as console failures', t => {
  const f = fixture(t), errorFile = path.join(f.root, 'file-error.json');
  const command = 'const fs = require("node:fs"); let written = 0; try { while (written < 16384) written += fs.writeSync(1, Buffer.alloc(16384 - written)); } catch (error) { fs.writeFileSync(process.argv[1], JSON.stringify({code: error.code, written})); fs.writeSync(2, "Controlled regular-file write failed\\n"); process.exit(74); }';
  const result = spawnSync('bash', ['-c', 'trap "" XFSZ; ulimit -f 1; exec bash "$@"', 'file-limit-fixture', capture, process.execPath, '-e', command, errorFile], {cwd: f.root, env: f.env, encoding: 'utf8'});
  assert.equal(result.status, 74, result.stderr);
  assert.equal(JSON.parse(fs.readFileSync(errorFile, 'utf8')).code, 'EFBIG');
  const output = f.output();
  assert.equal(output.receipt.nextest_exit, 74);
  assert.equal(output.receipt.effective_exit, 74);
  assert(output.stderr.includes('Controlled regular-file write failed'));
});

test('cancellation also joins an owned console forwarder and records the skipped stream', {timeout: 10000}, async t => {
  const f = fixture(t), gate = path.join(f.root, 'forward');
  fs.writeFileSync(path.join(f.root, 'bin', 'cat'), [
    '#!/usr/bin/env node', 'const fs = require("node:fs"), gate = process.env.FORWARD_GATE;',
    'process.on("SIGTERM", () => { fs.writeFileSync(gate + ".done", "cancelled"); process.exit(143); });',
    'fs.writeFileSync(gate + ".ready", "ready");',
    'const tick = setInterval(() => { if (fs.existsSync(gate)) { clearInterval(tick); fs.writeFileSync(gate + ".done", "released"); process.exit(0); } }, 2);',
  ].join('\n'), {mode: 0o755});
  const child = spawn('bash', [nextest, '--lib'], {cwd: f.root, env: {...f.env, FORWARD_GATE: gate}, stdio: ['ignore', 'pipe', 'pipe']});
  child.stdout.resume(); child.stderr.resume();
  const exited = new Promise((resolve, reject) => { child.once('error', reject); child.once('close', (code, signal) => resolve({code, signal})); });
  const waitFor = async file => {
    const until = Date.now() + 3000;
    while (!fs.existsSync(file)) { assert(Date.now() < until, 'The owned forwarder must reach its lifecycle boundary'); await new Promise(resolve => setTimeout(resolve, 2)); }
  };
  try {
    await waitFor(gate + '.ready');
    child.kill('SIGTERM');
    assert.deepEqual(await exited, {code: 143, signal: null});
    const output = f.output();
    assert.equal(output.receipt.nextest_exit, 0);
    assert.equal(output.receipt.stdout_forward_exit, 143);
    assert.equal(output.receipt.stderr_forward_exit, 143);
    assert.equal(fs.readFileSync(gate + '.done', 'utf8'), 'cancelled');
  } finally { fs.writeFileSync(gate, 'release'); await waitFor(gate + '.done'); }
});

test('receipt write failure cannot turn a completed command into a successful job', t => {
  const f = fixture(t);
  const command = 'const fs = require("node:fs"), path = require("node:path"); const dir = fs.readdirSync("target/ci").find(name => name.startsWith("nextest-output.")); fs.mkdirSync(path.join("target/ci", dir, "receipt.json"));';
  const result = spawnSync('bash', [capture, process.execPath, '-e', command], {cwd: f.root, env: f.env, encoding: 'utf8'});
  assert.notEqual(result.status, 0);
  assert(result.stderr.includes('receipt.json'));
});
