import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';

const repositories = ['actions/checkout', 'actions/download-artifact', 'actions/upload-artifact'];
const marker = '.tidemq-action-cache.json';

export function installActionCache(manifest, source, destination) {
  assert.ok(path.isAbsolute(destination), 'The action cache path must be absolute');
  assert.equal(path.basename(destination), 'action-archives', 'Unexpected action cache directory');
  const previous = fs.lstatSync(destination, {throwIfNoEntry: false});
  if (previous) {
    assert.ok(previous.isDirectory() && !previous.isSymbolicLink(), 'Refusing a redirected action cache');
    if (fs.readdirSync(destination).length) {
      const owned = fs.lstatSync(path.join(destination, marker), {throwIfNoEntry: false});
      assert.ok(owned?.isFile() && !owned.isSymbolicLink(), 'Refusing an unowned action cache');
      assert.equal(JSON.parse(fs.readFileSync(path.join(destination, marker))).schema_version, 1, 'Unknown cache ownership');
    }
  }
  const entries = Object.entries(manifest).filter(([, value]) => value.action);
  assert.deepEqual(entries.map(([, value]) => value.action).sort(), repositories, 'Expected exactly three pinned actions');
  let total = 0;
  const archives = entries.map(([name, value]) => {
    assert.match(name, /^action[A-Za-z]+$/, 'Invalid action archive name');
    assert.match(value.commit, /^[a-f0-9]{40}$/, 'Invalid action commit');
    assert.match(value.sha256, /^[a-f0-9]{64}$/, 'Invalid action archive digest');
    assert.equal(value.url, `https://codeload.github.com/${value.action}/tar.gz/${value.commit}`, 'Unexpected action source');
    const file = path.join(source, `${name}.archive`);
    const bytes = fs.statSync(file).size;
    assert.ok(bytes > 0 && bytes <= 32 * 1024 * 1024, 'Invalid action archive size');
    total += bytes;
    assert.ok(total <= 64 * 1024 * 1024, 'The action cache exceeds its fixed byte budget');
    const contents = fs.readFileSync(file);
    assert.equal(createHash('sha256').update(contents).digest('hex'), value.sha256, 'Action archive digest differs');
    return {...value, bytes, contents};
  });
  const staging = fs.mkdtempSync(path.join(path.dirname(destination), '.action-archives-'));
  try {
    for (const archive of archives) {
      const directory = path.join(staging, archive.action.replace('/', '_'));
      fs.mkdirSync(directory, {mode: 0o755});
      fs.writeFileSync(path.join(directory, `${archive.commit}.tar.gz`), archive.contents, {mode: 0o444});
    }
    const actions = archives.map(({contents, ...value}) => value);
    fs.writeFileSync(path.join(staging, marker), `${JSON.stringify({schema_version: 1, actions}, null, 2)}\n`, {mode: 0o444});
    fs.chmodSync(staging, 0o755);
    if (previous) fs.rmSync(destination, {recursive: true});
    fs.renameSync(staging, destination);
    return {actions: actions.length, bytes: total};
  } finally {
    fs.rmSync(staging, {recursive: true, force: true});
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  assert.equal(process.getuid(), 0, 'Install the action cache as root');
  assert.equal(fs.readFileSync('/etc/tidemq-lab-role', 'utf8').trim(), 'runner', 'Expected the runner VM');
  const [manifest, source] = process.argv.slice(2);
  assert.ok(manifest && source && process.argv.length === 4, 'Usage: sudo node install-action-cache.mjs manifest source-directory');
  console.log(JSON.stringify(installActionCache(JSON.parse(fs.readFileSync(manifest)), source, '/srv/tidemq/action-archives')));
  const envFile = '/srv/tidemq/actions-runner/.env';
  if (fs.existsSync(envFile)) {
    const current = fs.readFileSync(envFile, 'utf8').replace(/^ACTIONS_RUNNER_ACTION_ARCHIVE_CACHE=.*\n?/gm, '').trimEnd();
    fs.writeFileSync(envFile, `${current}\nACTIONS_RUNNER_ACTION_ARCHIVE_CACHE=/srv/tidemq/action-archives\n`);
  }
}
