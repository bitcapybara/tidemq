import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawn, spawnSync} from 'node:child_process';
import test from 'node:test';

const capture = new URL('./capture-nextest.sh', import.meta.url).pathname;
const moduleUrl = new URL('./capture-nextest.mjs', import.meta.url).href;
const waitFor = async condition => {
  const until = Date.now() + 5000;
  while (!condition()) { assert(Date.now() < until, 'The controlled FIFO must reach its requested boundary'); await new Promise(resolve => setTimeout(resolve, 2)); }
};

async function fixture(t, {status = 0, timeout, closeStderrBeforeRelease = false} = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-forward-fifo-'));
  const payloads = [Buffer.alloc(196631), Buffer.alloc(131087)];
  payloads.forEach((buffer, index) => { for (let i = 0; i < buffer.length; i++) buffer[i] = (i * 31 + index * 17) % 256; fs.writeFileSync(path.join(root, `payload-${index}`), buffer); });
  const readers = [], writers = [], chunks = [[], []], drain = [false, false], prefixes = [];
  for (let index = 0; index < 2; index++) {
    const file = path.join(root, `console-${index}`);
    assert.equal(spawnSync('mkfifo', ['-m', '600', file]).status, 0);
    readers.push(fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NONBLOCK));
    writers.push(fs.openSync(file, fs.constants.O_WRONLY));
  }
  const producer = path.join(root, 'producer.mjs');
  fs.writeFileSync(producer, `import fs from 'node:fs';
fs.writeFileSync('producer.ready', 'ready');
const tick = setInterval(() => {
  if (!fs.existsSync('producer.release')) return;
  clearInterval(tick);
  for (let i = 0; i < 2; i++) { const data = fs.readFileSync('payload-' + i); let offset = 0; while (offset < data.length) offset += fs.writeSync(i + 1, data, offset); }
  process.exit(${status});
}, 2);`);
  const args = timeout === undefined ? [capture, process.execPath, producer] : ['--input-type=module', '-e', `import {main} from ${JSON.stringify(moduleUrl)}; process.exit(await main(${JSON.stringify([process.execPath, producer])}, ${timeout}));`];
  const child = spawn(timeout === undefined ? 'bash' : process.execPath, args, {cwd: root, stdio: ['ignore', writers[0], writers[1]]});
  let result;
  const exited = new Promise((resolve, reject) => { child.once('error', reject); child.once('close', (code, signal) => { result = {code, signal}; resolve(result); }); });
  let initializer, initializerExit, directory, cleaned = false;
  const read = index => {
    if (readers[index] === undefined) return;
    const buffer = Buffer.alloc(257);
    while (true) {
      let length;
      try { length = fs.readSync(readers[index], buffer); } catch (error) { if (error.code === 'EAGAIN') return; throw error; }
      if (!length) return;
      chunks[index].push(Buffer.from(buffer.subarray(0, length)));
    }
  };
  const tick = setInterval(() => { for (let i = 0; i < 2; i++) if (drain[i]) read(i); }, 1);
  const cleanup = async () => {
    if (cleaned) return; cleaned = true;
    fs.writeFileSync(path.join(root, 'producer.release'), 'release');
    drain.fill(true);
    fs.writeFileSync(path.join(root, 'initializer.stop'), 'stop');
    if (initializerExit) await initializerExit;
    for (const fd of writers.splice(0)) fs.closeSync(fd);
    await exited; clearInterval(tick);
    for (const fd of readers) if (fd !== undefined) fs.closeSync(fd);
    fs.rmSync(root, {recursive: true, force: true});
  };
  t.after(cleanup);
  await waitFor(() => fs.existsSync(path.join(root, 'producer.ready')) || result);
  assert(!result, 'The owned command must be held before injecting console backpressure');
  directory = path.join(root, 'target/ci', fs.readdirSync(path.join(root, 'target/ci')).find(name => name.startsWith('nextest-output.')));
  // Opening Node standard streams makes these shared pipe descriptors nonblocking on POSIX.
  initializer = spawn(process.execPath, ['-e', 'const fs=require("node:fs");void process.stdout;void process.stderr;fs.writeFileSync("initializer.ready","ready");const tick=setInterval(()=>{if(fs.existsSync("initializer.stop")){clearInterval(tick);process.exit(0)}},2);'], {cwd: root, stdio: ['ignore', writers[0], writers[1]]});
  initializerExit = new Promise((resolve, reject) => { initializer.once('error', reject); initializer.once('close', resolve); });
  await waitFor(() => fs.existsSync(path.join(root, 'initializer.ready')));
  for (let index = 0; index < 2; index++) {
    const fd = fs.openSync(path.join(root, `console-${index}`), fs.constants.O_WRONLY | fs.constants.O_NONBLOCK);
    const fill = Buffer.alloc(4096, 240 + index), blocks = [];
    let saturated = false;
    try {
      for (let i = 0; i < 1024; i++) {
        try { const length = fs.writeSync(fd, fill); blocks.push(Buffer.from(fill.subarray(0, length))); }
        catch (error) { assert.equal(error.code, 'EAGAIN'); saturated = true; break; }
      }
    } finally { fs.closeSync(fd); }
    prefixes.push(Buffer.concat(blocks));
    assert(saturated, 'The real FIFO must report EAGAIN before producer release');
    assert(prefixes[index].length > 0, 'The real pipe must be filled before producer release');
  }
  if (closeStderrBeforeRelease) { fs.closeSync(readers[1]); readers[1] = undefined; }
  fs.writeFileSync(path.join(root, 'producer.release'), 'release');
  return {
    child, exited, directory, root, payloads, prefixes, chunks, drain,
    result: () => result,
    pending: async name => { await waitFor(() => fs.existsSync(path.join(directory, `console-forward-${name}.json`)) || result); },
    receipt: () => JSON.parse(fs.readFileSync(path.join(directory, 'receipt.json'), 'utf8')),
    collected: index => Buffer.concat(chunks[index]),
    finish: async () => { const outcome = await exited; fs.writeFileSync(path.join(root, 'initializer.stop'), 'stop'); await initializerExit; for (const fd of writers.splice(0)) fs.closeSync(fd); for (let i = 0; i < 2; i++) if (drain[i]) read(i); return outcome; },
    closeReader: index => { fs.closeSync(readers[index]); readers[index] = undefined; },
  };
}

test('transient console EAGAIN resumes partial writes without losing or duplicating bytes', {timeout: 15000}, async t => {
  const f = await fixture(t);
  await f.pending('stdout'); f.drain[0] = true;
  await f.pending('stderr'); f.drain[1] = true;
  const outcome = await f.finish();
  const errors = path.join(f.directory, 'console-forward-errors.log');
  assert.deepEqual(outcome, {code: 0, signal: null}, fs.existsSync(errors) ? fs.readFileSync(errors, 'utf8') : 'Transient backpressure must recover');
  for (let i = 0; i < 2; i++) assert.deepEqual(f.collected(i), Buffer.concat([f.prefixes[i], f.payloads[i]]));
  const receipt = f.receipt();
  assert.equal(receipt.stdout_acknowledged_bytes, f.payloads[0].length);
  assert.equal(receipt.stderr_acknowledged_bytes, f.payloads[1].length);
  assert.equal(receipt.stdout_forward_exit, 0); assert.equal(receipt.stderr_forward_exit, 0);
});

test('SIGINT and TERM during stderr forwarding preserve the completed stdout boundary', {timeout: 15000}, async t => {
  for (const [signal, code] of [['SIGINT', 130], ['SIGTERM', 143]]) {
    const f = await fixture(t);
    await f.pending('stdout'); f.drain[0] = true;
    await f.pending('stderr');
    f.child.kill(signal);
    assert.deepEqual(await f.finish(), {code, signal: null});
    const receipt = f.receipt();
    assert.equal(receipt.nextest_exit, 0); assert.equal(receipt.cancellation_exit, code);
    assert.equal(receipt.stdout_forward_exit, 0); assert.equal(receipt.stderr_forward_exit, code);
    assert.equal(receipt.stdout_acknowledged_bytes, f.payloads[0].length);
    assert.deepEqual(fs.readFileSync(path.join(f.directory, 'stderr.log')), f.payloads[1]);
  }
});

test('both console streams share one deadline and permanent backpressure remains a failure', {timeout: 10000}, async t => {
  const f = await fixture(t, {timeout: 2000});
  await f.pending('stdout'); f.drain[0] = true;
  await f.pending('stderr');
  assert.deepEqual(await f.finish(), {code: 1, signal: null});
  const first = JSON.parse(fs.readFileSync(path.join(f.directory, 'console-forward-stdout.json')));
  const second = JSON.parse(fs.readFileSync(path.join(f.directory, 'console-forward-stderr.json')));
  assert.equal(first.deadline, second.deadline);
  assert.equal(f.receipt().stderr_forward_error, 'ETIMEDOUT');
});

test('permanent EPIPE stays nonzero and preserves original test failure codes', {timeout: 15000}, async t => {
  for (const status of [0, 100, 110]) {
    const f = await fixture(t, {status, closeStderrBeforeRelease: true});
    await f.pending('stdout'); f.drain[0] = true;
    assert.deepEqual(await f.finish(), {code: status || 1, signal: null});
    assert.equal(f.receipt().nextest_exit, status);
    assert.equal(f.receipt().stderr_forward_error, 'EPIPE');
    assert.deepEqual(fs.readFileSync(path.join(f.directory, 'stderr.log')), f.payloads[1]);
  }
});
