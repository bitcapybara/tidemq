import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {spawn} from 'node:child_process';
import {setTimeout as delay} from 'node:timers/promises';
import {fileURLToPath} from 'node:url';

function descriptor(fd) {
  const stat = fs.fstatSync(fd);
  const kind = stat.isFile() ? 'regular_file' : stat.isFIFO() ? 'pipe' : stat.isSocket() ? 'socket' : 'other';
  let flags = 'unavailable';
  if (process.platform === 'linux') flags = fs.readFileSync(`/proc/self/fdinfo/${fd}`, 'utf8').match(/^flags:\s+([0-7]+)/m)?.[1] ?? flags;
  return {kind, flags};
}

function errorCode(error) { return error?.code ?? 'EIO'; }

async function* capturedChunks(file, captureDone, signal) {
  signal.throwIfAborted();
  const input = await fs.promises.open(file, 'r');
  const buffer = Buffer.allocUnsafe(65536);
  let position = 0;
  try {
    for (;;) {
      signal.throwIfAborted();
      // A read started before capture ends can see EOF before the last append.
      const finalRead = captureDone();
      const {bytesRead} = await input.read(buffer, 0, buffer.length, position);
      if (bytesRead > 0) {
        position += bytesRead;
        yield buffer.subarray(0, bytesRead);
      } else if (finalRead) {
        break;
      } else if (!captureDone()) {
        await delay(100, undefined, {signal});
      }
    }
  } finally {
    await input.close();
  }
}

async function forwardStream(stream, directory, captureDone, signal, deadline, cancellation) {
  const destination = process[stream.name];
  let pendingError, failChunk;
  destination.on('error', error => { pendingError = error; failChunk?.(error); });
  try {
    const file = path.join(directory, stream.name + '.log');
    for await (const chunk of capturedChunks(file, captureDone, signal)) {
      signal.throwIfAborted();
      if (pendingError) throw pendingError;
      await new Promise((resolve, reject) => {
        let settled = false;
        const finish = error => {
          if (settled) return;
          settled = true;
          signal.removeEventListener('abort', abortChunk);
          failChunk = undefined;
          if (error) reject(error);
          else { stream.acknowledged += chunk.length; resolve(); }
        };
        const abortChunk = () => finish(signal.reason);
        failChunk = finish;
        signal.addEventListener('abort', abortChunk, {once: true});
        try {
          // The callback acknowledges the entire chunk; libuv retains partial pipe writes.
          const ready = destination.write(chunk, finish);
          if (!ready && !stream.blocked) {
            stream.blocked = true;
            fs.writeFileSync(path.join(directory, `console-forward-${stream.name}.json`),
              JSON.stringify({stream: stream.name, chunk_bytes: chunk.length,
                buffered_bytes: destination.writableLength, deadline: deadline()}), {mode: 0o600});
          }
        } catch (error) { finish(error); }
      });
    }
    signal.throwIfAborted();
    stream.exit = 0;
  } catch (error) {
    stream.exit = cancellation() || 1;
    stream.error = signal.reason ?? error;
  }
}

// The capture owner keeps the test command attached to regular files in both modes.
export async function main(command, timeoutMs = 30000, mode = process.env.TIDEMQ_NEXTEST_LOG_MODE ?? 'buffered') {
  if (!command.length) { fs.writeSync(2, 'A nextest command is required\n'); return 2; }
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 30000) throw new Error('Invalid console forwarding deadline');
  if (mode !== 'buffered' && mode !== 'live') throw new Error('Invalid TIDEMQ_NEXTEST_LOG_MODE');
  let directory, child, nextestExit = 0, cancelled = 0, captureDone = false, timer, deadline = null;
  let liveForwarding;
  const abort = new AbortController();
  const streams = ['stdout', 'stderr'].map(name => ({name, exit: null, acknowledged: 0, error: null, blocked: false}));
  const forward = stream => forwardStream(stream, directory, () => captureDone, abort.signal,
    () => deadline, () => cancelled);
  const stop = signal => {
    cancelled = signal === 'SIGINT' ? 130 : 143;
    if (child && child.exitCode === null && child.signalCode === null) child.kill(signal);
    abort.abort(Object.assign(new Error('Console forwarding cancelled'), {code: signal}));
  };
  const interrupt = () => stop('SIGINT'), terminate = () => stop('SIGTERM');
  process.on('SIGINT', interrupt); process.on('SIGTERM', terminate);
  try {
    const stdout = descriptor(1), stderr = descriptor(2);
    fs.mkdirSync('target/ci', {recursive: true});
    directory = fs.mkdtempSync('target/ci/nextest-output.');
    fs.chmodSync(directory, 0o700);
    const output = fs.openSync(path.join(directory, 'stdout.log'), 'wx', 0o600);
    let errors;
    try { errors = fs.openSync(path.join(directory, 'stderr.log'), 'wx', 0o600); }
    catch (error) { fs.closeSync(output); throw error; }
    try {
      if (!cancelled) {
        let spawnError;
        child = spawn(command[0], command.slice(1), {stdio: [0, output, errors]});
        const exited = new Promise(resolve => {
          child.once('error', error => { spawnError = error; });
          child.once('close', (code, signal) => resolve(spawnError ? 127 : code ?? 128 + (os.constants.signals[signal] ?? 1)));
        });
        if (mode === 'live') liveForwarding = Promise.all(streams.map(forward));
        nextestExit = await exited;
        child = undefined;
        if (spawnError) fs.appendFileSync(path.join(directory, 'console-forward-errors.log'), `Command start failed: ${errorCode(spawnError)}\n`, {mode: 0o600});
      }
    } finally { fs.closeSync(output); fs.closeSync(errors); }
    captureDone = true;
    const sizes = streams.map(stream => fs.statSync(path.join(directory, stream.name + '.log')).size);
    deadline = performance.now() + timeoutMs;
    if (!cancelled) {
      timer = setTimeout(() => {
        const error = Object.assign(new Error('Console forwarding exceeded its total deadline'), {code: 'ETIMEDOUT'});
        abort.abort(error);
      }, timeoutMs);
    }
    if (liveForwarding) await liveForwarding;
    else for (const stream of streams) await forward(stream);
    clearTimeout(timer);
    const failed = abort.signal.aborted || streams.some(stream => stream.exit !== 0);
    const result = nextestExit || cancelled || (failed ? 1 : 0);
    for (const stream of streams) if (stream.error) fs.appendFileSync(path.join(directory, 'console-forward-errors.log'), `${stream.name} forwarding failed: ${errorCode(stream.error)}: ${stream.error.message}\n`, {mode: 0o600});
    fs.writeFileSync(path.join(directory, 'receipt.json'), JSON.stringify({
      nextest_exit: nextestExit, effective_exit: result, cancellation_exit: cancelled,
      stdout_forward_exit: streams[0].exit, stderr_forward_exit: streams[1].exit,
      stdout_bytes: sizes[0], stderr_bytes: sizes[1],
      stdout_acknowledged_bytes: streams[0].acknowledged, stderr_acknowledged_bytes: streams[1].acknowledged,
      stdout_forward_error: streams[0].error ? errorCode(streams[0].error) : null,
      stderr_forward_error: streams[1].error ? errorCode(streams[1].error) : null,
      forward_mode: mode, forward_timeout_ms: timeoutMs, forward_deadline: deadline,
      console_stdout_kind: stdout.kind, console_stdout_flags: stdout.flags,
      console_stderr_kind: stderr.kind, console_stderr_flags: stderr.flags,
    }) + '\n', {mode: 0o600});
    return result;
  } catch (error) {
    abort.abort(error);
    await liveForwarding;
    const message = `Nextest capture failed: ${errorCode(error)}: ${error.message}\n`;
    if (directory) { try { fs.appendFileSync(path.join(directory, 'console-forward-errors.log'), message, {mode: 0o600}); } catch {} }
    try { fs.writeSync(2, message); } catch {}
    return nextestExit || cancelled || 1;
  } finally {
    clearTimeout(timer);
    process.off('SIGINT', interrupt); process.off('SIGTERM', terminate);
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  // Errors and cancellation may leave a blocked stdio write; this owner must still terminate.
  process.exit(await main(process.argv.slice(2)));
}
