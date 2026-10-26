import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

const scripts = path.dirname(fileURLToPath(import.meta.url));
const linux = {skip: process.platform !== 'linux'};
function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-maintenance-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  return root;
}
function shellFunction(name, args) {
  return spawnSync('bash', ['-c', 'source "$1"; shift; "$@"', 'test', path.join(scripts, 'runner-maintenance.sh'), name, ...args], {encoding: 'utf8'});
}
function executable(file, contents) { fs.writeFileSync(file, contents, {mode: 0o755}); }

test('cache bounds preserve warm files, expire old files and purge over-budget data', linux, t => {
  const root = fixture(t), cache = path.join(root, 'cache');
  fs.mkdirSync(cache);
  fs.writeFileSync(path.join(cache, 'warm'), Buffer.alloc(8192));
  fs.writeFileSync(path.join(cache, 'expired'), 'old');
  fs.utimesSync(path.join(cache, 'expired'), 1, 1);
  assert.equal(shellFunction('bound_cache', [cache, '1048576', '7']).status, 0);
  assert.deepEqual(fs.readdirSync(cache), ['warm']);
  assert.equal(shellFunction('bound_cache', [cache, '4096', '7']).status, 0);
  assert.deepEqual(fs.readdirSync(cache), []);
});
test('cleanup refuses redirected roots and never follows child symlinks', linux, t => {
  const root = fixture(t), outside = path.join(root, 'persistent'), cache = path.join(root, 'cache');
  fs.mkdirSync(outside); fs.mkdirSync(cache);
  fs.writeFileSync(path.join(outside, 'identity.key'), 'keep');
  fs.symlinkSync(outside, path.join(root, 'redirect'));
  assert.notEqual(shellFunction('empty_cache', [path.join(root, 'redirect')]).status, 0);
  fs.symlinkSync(outside, path.join(cache, 'child'));
  assert.equal(shellFunction('empty_cache', [cache]).status, 0);
  assert.equal(fs.readFileSync(path.join(outside, 'identity.key'), 'utf8'), 'keep');
  assert.deepEqual(fs.readdirSync(cache), []);
});

function fakeCommands(root) {
  const bin = path.join(root, 'bin'); fs.mkdirSync(bin);
  const env = {...process.env, PATH: `${bin}:${process.env.PATH}`, TEST_ROOT: root};
  executable(path.join(bin, 'ssh'), '#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$TEST_ROOT/ssh.log"\n');
  executable(path.join(bin, 'kubectl'), `#!/usr/bin/env node
const fs = require('node:fs');
const root = process.env.TEST_ROOT, args = process.argv.slice(2);
fs.appendFileSync(root + '/kubectl.log', JSON.stringify(args) + '\\n');
if (args.includes('delete')) process.exit(process.env.FAIL_DELETE === '1' ? 1 : 0);
if (args.includes('namespace')) console.log(JSON.stringify({metadata: {labels: {'tidemq.lab/ci': process.env.OWNED ?? 'true'}}}));
else if (args.includes('pv') && args.includes('ci-123-1-node-11')) {
  if (process.env.CONFLICT === '1') console.log(JSON.stringify({spec: {claimRef: {namespace: 'tidemq-dev'}}}));
} else if (args.includes('pv') && !args.some(arg => arg.startsWith('ci-'))) console.log(JSON.stringify({items: process.env.BAD_PATH === '1' ? [{metadata: {name: 'ci-123-1-node-11'}, spec: {claimRef: {namespace: 'ci-123-1'}, local: {path: '/var/lib/tidemq/metadata/tidemq-dev/node-11'}}}] : []}));
else if (args.includes('pv') && args.some(arg => arg.startsWith('ci-'))) {}
else process.exit(2);
`);
  return env;
}
function cleanupFixture(t) {
  const root = fixture(t), env = fakeCommands(root), home = path.join(root, 'private');
  fs.mkdirSync(path.join(home, 'runs', 'ci-123-1'), {recursive: true});
  fs.writeFileSync(path.join(home, 'runs', 'ci-123-1', '.tidemq-ci'), '');
  return {root, home, env: {...env, TIDEMQ_LAB_HOME: home, TIDEMQ_LAB_LEASE_HELD: '1'}};
}
function cleanup(env, run = 'ci-123-1') {
  return spawnSync('bash', [path.join(scripts, 'cleanup-ci.sh'), run], {env, encoding: 'utf8'});
}
test('CI cleanup rejects public names and calls without the global lease', t => {
  const {root, env} = cleanupFixture(t);
  assert.equal(cleanup(env, 'tidemq-dev').status, 2);
  assert.equal(cleanup({...env, TIDEMQ_LAB_LEASE_HELD: ''}).status, 2);
  assert.equal(fs.existsSync(path.join(root, 'ssh.log')), false);
});
test('an unowned namespace or conflicting public volume stops deletion', t => {
  for (const extra of [{OWNED: 'false'}, {CONFLICT: '1'}, {BAD_PATH: '1'}]) {
    const {root, env, home} = cleanupFixture(t);
    const result = cleanup({...env, ...extra});
    assert.notEqual(result.status, 0, result.stderr);
    assert.ok(fs.existsSync(path.join(home, 'runs', 'ci-123-1')));
    const calls = fs.readFileSync(path.join(root, 'kubectl.log'), 'utf8');
    assert.ok(!calls.includes('"delete"'));
  }
});
test('failed namespace deletion retains the retry marker and data', t => {
  const {root, env, home} = cleanupFixture(t);
  assert.notEqual(cleanup({...env, FAIL_DELETE: '1'}).status, 0);
  assert.ok(fs.existsSync(path.join(home, 'runs', 'ci-123-1', '.tidemq-ci')));
  assert.ok(!fs.readFileSync(path.join(root, 'ssh.log'), 'utf8').includes('rm -rf'));
});
test('successful cleanup deletes only the numbered run and is repeatable', t => {
  const {root, env, home} = cleanupFixture(t);
  const publicDirectory = path.join(home, 'public', 'tidemq-dev');
  fs.mkdirSync(publicDirectory, {recursive: true});
  for (let attempt = 0; attempt < 2; attempt++) {
    const result = cleanup(env);
    assert.equal(result.status, 0, result.stderr);
  }
  assert.ok(!fs.existsSync(path.join(home, 'runs', 'ci-123-1')));
  assert.ok(fs.existsSync(publicDirectory));
  const calls = fs.readFileSync(path.join(root, 'ssh.log'), 'utf8');
  assert.match(calls, /\/var\/lib\/tidemq\/storage\/ci-123-1/);
  assert.ok(!calls.includes('/storage/tidemq-dev'));
});

test('a pruned runtime base is restored from its pinned source; wrong identities fail', t => {
  const root = fixture(t), bin = path.join(root, 'bin'); fs.mkdirSync(bin);
  const inventory = JSON.parse(fs.readFileSync(path.join(scripts, '../../deploy/lab/inventory.json')));
  executable(path.join(bin, 'docker'), `#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "$TEST_ROOT/docker.log"
if [[ "$1 $2" == 'image inspect' ]]; then
  [[ -f "$TEST_ROOT/image" ]] || exit 1
  cat "$TEST_ROOT/image"
elif [[ "$1" == tag ]]; then printf '%s\\n' "$EXPECTED" > "$TEST_ROOT/image"; fi
`);
  const env = {...process.env, PATH: `${bin}:${process.env.PATH}`, TEST_ROOT: root, EXPECTED: inventory.docker.runtimeBaseImageID};
  const run = () => spawnSync('bash', [path.join(scripts, 'ensure-runtime-base.sh')], {env, encoding: 'utf8'});
  assert.equal(run().status, 0);
  const log = fs.readFileSync(path.join(root, 'docker.log'), 'utf8');
  assert.ok(log.includes(`pull --platform linux/amd64 ${inventory.docker.runtimeBaseSource}`));
  fs.writeFileSync(path.join(root, 'docker.log'), '');
  assert.equal(run().status, 0);
  assert.ok(!fs.readFileSync(path.join(root, 'docker.log'), 'utf8').includes('pull'));
  fs.writeFileSync(path.join(root, 'image'), 'sha256:wrong');
  assert.notEqual(run().status, 0);
});
