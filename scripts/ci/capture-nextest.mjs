import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';

function descriptor(fd) {
  const stat = fs.fstatSync(fd);
  const kind = stat.isFile() ? 'regular_file' : stat.isFIFO() ? 'pipe' : stat.isSocket() ? 'socket' : 'other';
  let flags = 'unavailable';
  if (process.platform === 'linux') flags = fs.readFileSync(`/proc/self/fdinfo/${fd}`, 'utf8').match(/^flags:\s+([0-7]+)/m)?.[1] ?? flags;
  return {kind, flags};
}

function errorCode(error) { return error?.code ?? 'EIO'; }

// This process owns capture and forwarding, including signals between the two phases.
export async function main(command, timeoutMs = 30000) {
  if (!command.length) { fs.writeSync(2, 'A nextest command is required\n'); return 2; }
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 30000) throw new Error('Invalid console forwarding deadline');
  let directory, child, nextestExit = 0, cancelled = 0, forwarding = false, timer, rejectWrite;
  const abort = new AbortController();
  const streams = ['stdout', 'stderr'].map(name => ({name, exit: null, acknowledged: 0, error: null, blocked: false}));
  const stop = signal => {
    cancelled = signal === 'SIGINT' ? 130 : 143;
    if (child && child.exitCode === null && child.signalCode === null) child.kill(signal);
    if (forwarding) {
      const error = Object.assign(new Error('Console forwarding cancelled'), {code: signal});
      abort.abort(error); rejectWrite?.(error);
    }
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
        nextestExit = await new Promise(resolve => {
          child.once('error', error => { spawnError = error; });
          child.once('close', (code, signal) => resolve(spawnError ? 127 : code ?? 128 + (os.constants.signals[signal] ?? 1)));
        });
        child = undefined;
        if (spawnError) fs.appendFileSync(path.join(directory, 'console-forward-errors.log'), `Command start failed: ${errorCode(spawnError)}\n`, {mode: 0o600});
      }
    } finally { fs.closeSync(output); fs.closeSync(errors); }
    const sizes = streams.map(stream => fs.statSync(path.join(directory, stream.name + '.log')).size);
    const deadline = performance.now() + timeoutMs;
    forwarding = true;
    if (!cancelled) {
      timer = setTimeout(() => {
        const error = Object.assign(new Error('Console forwarding exceeded its total deadline'), {code: 'ETIMEDOUT'});
        abort.abort(error); rejectWrite?.(error);
      }, timeoutMs);
    }
    for (const stream of streams) {
      if (cancelled || abort.signal.aborted) {
        stream.exit = cancelled || 1;
        stream.error = abort.signal.reason ?? Object.assign(new Error('Console forwarding cancelled'), {code: cancelled === 130 ? 'SIGINT' : 'SIGTERM'});
        continue;
      }
      const destination = process[stream.name];
      let pendingError, failChunk;
      destination.on('error', error => { pendingError = error; failChunk?.(error); });
      try {
        for await (const chunk of fs.createReadStream(path.join(directory, stream.name + '.log'), {highWaterMark: 65536, signal: abort.signal})) {
          if (abort.signal.aborted) throw abort.signal.reason;
          if (pendingError) throw pendingError;
          await new Promise((resolve, reject) => {
            const fail = error => reject(error);
            failChunk = fail; rejectWrite = fail;
            try {
              // The callback acknowledges the entire chunk; libuv retains partial pipe writes.
              const ready = destination.write(chunk, error => {
                if (rejectWrite === fail) rejectWrite = undefined;
                failChunk = undefined;
                if (error) reject(error);
                else { stream.acknowledged += chunk.length; resolve(); }
              });
              if (!ready && !stream.blocked) {
                stream.blocked = true;
                fs.writeFileSync(path.join(directory, `console-forward-${stream.name}.json`), JSON.stringify({stream: stream.name, chunk_bytes: chunk.length, buffered_bytes: destination.writableLength, deadline}), {mode: 0o600});
              }
            } catch (error) { reject(error); }
          });
        }
        if (abort.signal.aborted) throw abort.signal.reason;
        stream.exit = 0;
      } catch (error) {
        stream.exit = cancelled || 1;
        stream.error = abort.signal.reason ?? error;
      }
    }
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
      forward_timeout_ms: timeoutMs, forward_deadline: deadline,
      console_stdout_kind: stdout.kind, console_stdout_flags: stdout.flags,
      console_stderr_kind: stderr.kind, console_stderr_flags: stderr.flags,
    }) + '\n', {mode: 0o600});
    return result;
  } catch (error) {
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
