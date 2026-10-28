import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawn, spawnSync} from 'node:child_process';
import test from 'node:test';

const script = new URL('./nextest.sh', import.meta.url).pathname;

test('runner options and positional or emulated filters keep their original semantics', t => {
  const f = fixture(t);
  const args = ['--lib', '--profile', 'ci', '-E', 'test(a b)', '--run-ignored', 'only',
    '--test-threads', '2', '--retries', '0', '--no-fail-fast', 'substring', '--', '--skip', 'skip-me'];
  const discovery = ['--lib', '--profile', 'ci', '-E', 'test(a b)', '--run-ignored', 'only', 'substring', '--', '--skip', 'skip-me'];
  const execution = ['--profile', 'ci', '-E', 'test(a b)', '--run-ignored', 'only',
    '--test-threads', '2', '--retries', '0', '--no-fail-fast', 'substring', '--', '--skip', 'skip-me'];
  const result = spawnSync('bash', [script, ...args], {cwd: f.root,
    env: {...f.env, TIDEMQ_NEXTEST_IMAGE: 'sha256:' + 'b'.repeat(64)}, encoding: 'utf8'});
  assert.equal(result.status, 0, result.stderr);
  const calls = f.calls(), create = calls.find(a => a.includes('create'));
  assert.deepEqual(calls.find(a => a.includes('--no-run')), ['cargo', 'nextest', 'run', '--no-run', ...args]);
  assert.deepEqual(calls.find(a => a[0] === 'cargo' && a.includes('list')), ['cargo', 'nextest', 'list', '--list-type', 'binaries-only', '--message-format', 'json', ...discovery]);
  assert.deepEqual(create.slice(-execution.length), execution);
});

test('unsupported or incomplete isolated arguments fail before build and Docker', t => {
  for (const args of [['--unsupported'], ['--target', 'other'], ['--features'], ['--filterset'], ['--test-threads']]) {
    const f = fixture(t), result = spawnSync('bash', [script, ...args], {cwd: f.root,
      env: {...f.env, TIDEMQ_NEXTEST_IMAGE: 'sha256:' + 'b'.repeat(64)}, encoding: 'utf8'});
    assert.equal(result.status, 2, result.stderr);
    assert.deepEqual(f.calls(), []);
  }
});


test('symlink mount roots are rejected before any Docker command', t => {
  const f = fixture(t), redirect = path.join(f.root, 'redirect');
  fs.symlinkSync(f.env.TMPDIR, redirect);
  const result = spawnSync('bash', [script, '--lib'], {cwd: f.root,
    env: {...f.env, TIDEMQ_NEXTEST_IMAGE: 'sha256:' + 'b'.repeat(64), TMPDIR: redirect}, encoding: 'utf8'});
  assert.equal(result.status, 2, result.stderr);
  assert.deepEqual(f.calls(), []);
});



test('all five lane selections preserve build and execution arguments', t => {
  const selections = [
    ['--workspace', '--lib', '--bins', '--examples', '--locked', '--features', 'tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen', '--profile', 'ci'],
    ['-p', 'tidemq-sdk', '--lib', '--locked', '--profile', 'ci'],
    ['--workspace', '--all-targets', '--locked', '--features', 'tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen', '--profile', 'ci', '-E', 'kind(test)'],
    ['-p', 'tidemq', '--lib', '--test', 'runtime_queue', '--test', 'runtime_consumer_reads', '--test', 'runtime_consumer_close', '--test', 'runtime_reclamation', '--test', 'runtime_group_watch', '--test', 'sdk_consumer_long_poll', '--locked', '--features', 'tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen,test-hooks', '--profile', 'ci'],
    ['-p', 'tidemq', '--all-targets', '--locked', '--features', 'tidemq-sdk/metrics,tidemq-sdk/opentelemetry,tidemq-protocol/codegen,test-hooks', '--profile', 'ci', '--run-ignored', 'only', '-E', 'kind(test)'],
  ];
  const executions = [
    ['--profile', 'ci'],
    ['--profile', 'ci'],
    ['--profile', 'ci', '-E', 'kind(test)'],
    ['--profile', 'ci'],
    ['--profile', 'ci', '--run-ignored', 'only', '-E', 'kind(test)'],
  ];
  for (const [index, args] of selections.entries()) {
    const f = fixture(t), env = {...f.env, TIDEMQ_NEXTEST_IMAGE: 'sha256:' + 'b'.repeat(64)};
    const result = spawnSync('bash', [script, ...args], {cwd: f.root, env, encoding: 'utf8'});
    assert.equal(result.status, 0, result.stderr);
    const calls = f.calls(), create = calls.find(a => a.includes('create'));
    assert.deepEqual(calls.find(a => a.includes('--no-run')), ['cargo', 'nextest', 'run', '--no-run', ...args]);
    assert.deepEqual(calls.find(a => a[0] === 'cargo' && a.includes('list')), ['cargo', 'nextest', 'list', '--list-type', 'binaries-only', '--message-format', 'json', ...args]);
    const run = create.slice(create.indexOf('/srv/tidemq/tools/cargo-nextest'));
    assert.deepEqual(run, ['/srv/tidemq/tools/cargo-nextest', 'nextest', 'run', '--binaries-metadata', run[4], '--cargo-metadata', run[6], ...executions[index]]);
    assert(calls.slice(calls.indexOf(create) + 1).every(a => a[0] !== 'cargo'), 'The execution phase cannot invoke Cargo');
    for (const directory of [f.root, env.CARGO_TARGET_DIR, env.TMPDIR]) assert(create.includes('type=bind,src=' + directory + ',dst=' + directory));
  }
});

test('compile failure prevents container creation and test failures preserve their exit code', t => {
  for (const [settings, expected, created] of [[{BUILD_FAIL: '1'}, 42, false], [{TEST_EXIT: '100'}, 100, true]]) {
    const f = fixture(t), result = spawnSync('bash', [script, '--lib', '--profile', 'ci'], {cwd: f.root,
      env: {...f.env, TIDEMQ_NEXTEST_IMAGE: 'sha256:' + 'b'.repeat(64), ...settings}, encoding: 'utf8'});
    assert.equal(result.status, expected, result.stderr);
    assert.equal(f.calls().some(a => a.includes('create')), created);
    assert.equal(f.calls().some(a => a.includes('rm')), created);
  }
});

test('mutable images and redirected or private mounts fail before starting Docker', t => {
  for (const settings of [{TIDEMQ_NEXTEST_IMAGE: 'runner:latest'}, {CARGO_TARGET_DIR: '/'}, {TMPDIR: '/srv/tidemq/actions-runner'}]) {
    const f = fixture(t), result = spawnSync('bash', [script, '--lib'], {cwd: f.root,
      env: {...f.env, TIDEMQ_NEXTEST_IMAGE: 'sha256:' + 'b'.repeat(64), ...settings}, encoding: 'utf8'});
    assert.equal(result.status, 2, result.stderr);
    assert.deepEqual(f.calls(), []);
  }
});

test('TERM while attached removes only the created container', {timeout: 10000}, async t => {
  const f = fixture(t), child = spawn('bash', [script, '--lib', '--profile', 'ci'], {cwd: f.root,
    env: {...f.env, TIDEMQ_NEXTEST_IMAGE: 'sha256:' + 'b'.repeat(64), BLOCK_START: '1'}, stdio: 'ignore'});
  t.after(() => child.kill('SIGKILL'));
  const exited = new Promise(resolve => child.once('exit', code => resolve(code)));
  const until = Date.now() + 5000;
  while (!fs.existsSync(f.env.SIGNAL_FILE)) {
    assert(Date.now() < until, 'The controlled attached process must start');
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  const attachedPid = Number(fs.readFileSync(f.env.SIGNAL_FILE, 'utf8'));
  t.after(() => { try { process.kill(attachedPid, 'SIGKILL'); } catch (e) { if (e.code !== 'ESRCH') throw e; } });
  child.kill('SIGTERM');
  assert.equal(await exited, 143);
  assert.deepEqual(f.calls().filter(a => a.includes('rm')), [['docker', '--host', 'unix:///var/run/docker.sock', 'rm', '--force', 'a'.repeat(64)]]);
});

function fixture(t) {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-nextest-')));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  for (const name of ['bin', 'target', 'tmp']) fs.mkdirSync(path.join(root, name));
  const executable = (name, code) => fs.writeFileSync(path.join(root, 'bin', name), code, {mode: 0o755});
  executable('cargo', '#!/usr/bin/env node\nconst fs=require("node:fs");const a=process.argv.slice(2);fs.appendFileSync(process.env.TRACE,JSON.stringify(["cargo",...a])+"\\n");if(a.includes("--no-run")&&process.env.BUILD_FAIL==="1")process.exit(42);if(a.includes("--message-format")||a[0]==="metadata")console.log("{}");\n');
  executable('docker', [
    '#!/usr/bin/env node',
    'const fs=require("node:fs"),a=process.argv.slice(2),file=process.env.SIGNAL_FILE;',
    'fs.appendFileSync(process.env.TRACE,JSON.stringify(["docker",...a])+"\\n");',
    'if(a.includes("start")){const c=fs.readFileSync(process.env.TRACE,"utf8").trim().split("\\n").map(JSON.parse).find(v=>v.includes("create"));const r=c.slice(c.indexOf("/srv/tidemq/tools/cargo-nextest")+3);const build=new Set(["--workspace","--lib","--bins","--examples","--all-targets","--locked","--features","-F","--package","-p","--test"]);if(r.includes("--binaries-metadata")&&r.some(v=>build.has(v.split("=")[0])))process.exit(2);}',
    'if(a.includes("info"))console.log("systemd 2");',
    'else if(a.includes("image"))console.log(process.env.TIDEMQ_NEXTEST_IMAGE);',
    'else if(a.includes("create"))console.log("a".repeat(64));',
    'else if(a.includes("wait"))console.log(process.env.TEST_EXIT??0);',
    'else if(a.includes("start")&&process.env.BLOCK_START==="1"){fs.writeFileSync(file,String(process.pid));setInterval(()=>{},1000);}',
    'else if(a.includes("rm")&&fs.existsSync(file)){try{process.kill(Number(fs.readFileSync(file,"utf8")),"SIGTERM");}catch(e){if(e.code!=="ESRCH")throw e;}}',
  ].join('\n'));
  executable('id', '#!/usr/bin/env bash\nprintf "1001\\n"\n');
  executable('uname', '#!/usr/bin/env bash\nprintf "Linux\\n"\n');
  const env = {...process.env, PATH: path.join(root, 'bin') + ':' + process.env.PATH, TRACE: path.join(root, 'trace'),
    CARGO_TARGET_DIR: path.join(root, 'target'), TMPDIR: path.join(root, 'tmp'), GITHUB_RUN_ID: '123', GITHUB_RUN_ATTEMPT: '1', SIGNAL_FILE: path.join(root, 'started')};
  delete env.TIDEMQ_NEXTEST_IMAGE;
  return {root, env, calls: () => fs.existsSync(env.TRACE) ? fs.readFileSync(env.TRACE, 'utf8').trim().split('\n').map(JSON.parse) : []};
}

test('unconfigured local execution preserves the original nextest argv', t => {
  const f = fixture(t), args = ['--workspace', '--lib', '--profile', 'ci', '-E', 'test(a b)'];
  const result = spawnSync('bash', [script, ...args], {cwd: f.root, env: f.env, encoding: 'utf8'});
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(f.calls(), [['cargo', 'nextest', 'run', ...args]]);
});

test('configured execution builds first and reuses metadata in a fresh bounded container', t => {
  const f = fixture(t), args = ['--workspace', '--all-targets', '--locked', '--features', 'tidemq-sdk/metrics,test-hooks', '--profile', 'ci', '-E', 'kind(test)'];
  const image = 'sha256:' + 'b'.repeat(64);
  const result = spawnSync('bash', [script, ...args], {cwd: f.root, env: {...f.env, TIDEMQ_NEXTEST_IMAGE: image,
    LLVM_PROFILE_FILE: f.env.CARGO_TARGET_DIR + '/native-%p-%m.profraw', GITHUB_TOKEN: 'must-not-leak'}, encoding: 'utf8'});
  assert.equal(result.status, 0, result.stderr);
  const calls = f.calls(), build = calls.findIndex(a => a[0] === 'cargo' && a.includes('--no-run'));
  const create = calls.findIndex(a => a.includes('create'));
  assert(build >= 0 && create > build, 'Compilation must finish before container creation');
  assert.deepEqual(calls[build], ['cargo', 'nextest', 'run', '--no-run', ...args]);
  const command = calls[create];
  for (const value of ['6442450944', '4', '1001:1001', '--read-only', 'system.slice', '--binaries-metadata', '--cargo-metadata', 'LLVM_PROFILE_FILE']) assert(command.includes(value), value);
  assert.deepEqual(command.slice(-4), ['--profile', 'ci', '-E', 'kind(test)']);
  assert(command.includes(image));
  assert(!command.some(a => /must-not-leak|GITHUB_TOKEN|\/state|docker\.sock.*dst|\.credentials/.test(a)));
  assert(!result.stdout.includes('must-not-leak') && !result.stderr.includes('must-not-leak'));
  assert(calls.some(a => a.includes('rm') && a.includes('a'.repeat(64))), 'Only the created container is removed');
});
