import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import test from 'node:test';

const script = fileURLToPath(new URL('./build-console.sh', import.meta.url));

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-console-package-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const scripts = path.join(root, 'scripts'), web = path.join(root, 'console/web'), bin = path.join(root, 'bin');
  for (const directory of [scripts, web, bin]) fs.mkdirSync(directory, {recursive: true});
  fs.copyFileSync(script, path.join(scripts, 'build-console.sh'));
  fs.writeFileSync(path.join(web, 'package.json'), '{"name":"fixture","version":"1.0.0","private":true}');
  fs.writeFileSync(path.join(web, 'package-lock.json'), '{"lockfileVersion":3}');
  fs.writeFileSync(path.join(web, 'app.ts'), 'export const value = 1;');
  fs.writeFileSync(path.join(bin, 'npm'), `#!/bin/bash
set -euo pipefail
if [[ "$1" == --version ]]; then printf '11.6.2\\n'; exit; fi
if [[ "$1" == ci ]]; then
  for file in .env .npmrc private.key; do [[ ! -e "$file" ]] || exit 70; done
  [[ ! -e node_modules && ! -e dist && ! -e .git && ! -e .artifacts && ! -e coverage && ! -e .DS_Store ]] || exit 71
  exit
fi
[[ "$FAIL_BUILD" == 0 ]] || exit 42
mkdir -p dist/.vite dist/assets
printf '<html>Console</html>' > dist/index.html
printf 'export {};' > dist/assets/app.js
printf '{"index.html":{"file":"assets/app.js","isEntry":true}}' > dist/.vite/manifest.json
`, {mode: 0o755});
  const env = {...process.env, FAIL_BUILD: '0', PATH: `${bin}:${process.env.PATH}`};
  return {root, web, bin, env, run: (name, patch = {}) => spawnSync('/bin/bash', [path.join(scripts, 'build-console.sh'), path.join(root, name)], {env: {...env, ...patch}, encoding: 'utf8'})};
}

test('packaging builds from isolated sources, excludes private/cache files, and records all source changes', t => {
  const fixtureData = fixture(t), {root, web, run} = fixtureData;
  for (const file of ['.env', '.npmrc', 'private.key']) fs.writeFileSync(path.join(web, file), 'secret');
  for (const directory of ['node_modules', 'dist', '.git', '.artifacts', 'coverage']) fs.mkdirSync(path.join(web, directory));
  const first = run('first'); assert.equal(first.status, 0, first.stderr);
  const read = name => JSON.parse(fs.readFileSync(path.join(root, name, '.tidemq-build.json')));
  assert.match(read('first').sourceSha256, /^[a-f0-9]{64}$/);
  fs.writeFileSync(path.join(web, '.artifacts/browser.json'), 'private browser state');
  fs.writeFileSync(path.join(web, '.DS_Store'), 'desktop metadata');
  fs.writeFileSync(path.join(web, 'tsconfig.tsbuildinfo'), 'incremental compiler cache');
  const same = run('same'); assert.equal(same.status, 0, same.stderr);
  assert.equal(read('first').sourceSha256, read('same').sourceSha256);
  fs.writeFileSync(path.join(web, 'untracked.ts'), 'export const untracked = true;');
  const changed = run('changed'); assert.equal(changed.status, 0, changed.stderr);
  assert.notEqual(read('first').sourceSha256, read('changed').sourceSha256);
  fs.writeFileSync(path.join(web, 'package-lock.json'), '{"lockfileVersion":3,"changed":true}');
  const lock = run('lock'); assert.equal(lock.status, 0, lock.stderr);
  assert.notEqual(read('changed').sourceSha256, read('lock').sourceSha256);
  assert.deepEqual(fs.readdirSync(path.join(root, 'first')).sort(), ['.tidemq-build.json', '.vite', 'assets', 'index.html']);
  assert.notEqual(run('first').status, 0);
});

test('missing lock files and failed builds never publish stale resources', t => {
  const {root, web, run} = fixture(t);
  assert.equal(run('failed', {FAIL_BUILD: '1'}).status, 42);
  assert.equal(fs.existsSync(path.join(root, 'failed')), false);
  fs.unlinkSync(path.join(web, 'package-lock.json'));
  const missing = run('missing'); assert.notEqual(missing.status, 0);
  assert.match(missing.stderr, /lock file is missing/);
  assert.equal(fs.existsSync(path.join(root, 'missing')), false);
});

test('Node preflight follows the frontend dependency engine range', t => {
  const {root, bin, run} = fixture(t);
  fs.writeFileSync(path.join(bin, 'node'), `#!/bin/bash
if [[ "$1" == -e ]]; then
  exec "$REAL_NODE" -e 'Object.defineProperty(process.versions,"node",{value:process.env.TEST_NODE_VERSION}); require("node:vm").runInThisContext(process.argv[1]);' "$2"
fi
exec "$REAL_NODE" "$@"
`, {mode: 0o755});
  for (const version of ['20.19.0', '22.21.0', '23.0.0', '25.0.0', '22.22.0', '24.0.0', '26.0.0']) {
    const output = `node-${version}`;
    const result = run(output, {REAL_NODE: process.execPath, TEST_NODE_VERSION: version});
    const supported = ['22.22.0', '24.0.0', '26.0.0'].includes(version);
    assert.equal(result.status, supported ? 0 : 2, `${version}: ${result.stderr}`);
    assert.equal(fs.existsSync(path.join(root, output)), supported);
  }
});

test('npm bootstrap rejects an invalid archive before extraction or execution', t => {
  const {root, bin, run} = fixture(t);
  fs.unlinkSync(path.join(bin, 'npm'));
  for (const command of ['node', 'tar', 'dirname', 'mktemp', 'mkdir', 'rm', 'cp', 'cat']) {
    const executable = command === 'node' ? process.execPath : spawnSync('/usr/bin/which', [command], {encoding: 'utf8'}).stdout.trim();
    fs.symlinkSync(executable, path.join(bin, command));
  }
  fs.writeFileSync(path.join(bin, 'curl'), '#!/bin/bash\nprintf invalid-archive > "${!#}"\n', {mode: 0o755});
  const result = run('rejected', {PATH: bin});
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /failed SHA512 verification/);
  assert.equal(fs.existsSync(path.join(root, 'rejected')), false);
});
