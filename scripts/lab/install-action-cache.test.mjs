import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {installActionCache} from './install-action-cache.mjs';

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-actions-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const destination = path.join(root, 'action-archives');
  const manifest = Object.fromEntries(['checkout', 'download-artifact', 'upload-artifact'].map((name, index) => {
    const key = `actionTest${String.fromCharCode(65 + index)}`, commit = String(index + 1).repeat(40);
    const contents = Buffer.from(`official-archive-${name}`);
    fs.writeFileSync(path.join(root, `${key}.archive`), contents);
    return [key, {action: `actions/${name}`, commit, sha256: createHash('sha256').update(contents).digest('hex'),
      url: `https://codeload.github.com/actions/${name}/tar.gz/${commit}`}];
  }));
  return {root, destination, manifest};
}

test('action cache retains only pinned archives and installs readable, immutable files', t => {
  const {root, destination, manifest} = fixture(t);
  assert.equal(installActionCache(manifest, root, destination).actions, 3);
  const old = path.join(destination, 'actions_checkout', `${'a'.repeat(40)}.tar.gz`);
  fs.writeFileSync(old, 'obsolete');
  installActionCache(manifest, root, destination);
  assert.ok(!fs.existsSync(old));
  for (const value of Object.values(manifest)) {
    const file = path.join(destination, value.action.replace('/', '_'), `${value.commit}.tar.gz`);
    assert.equal(fs.statSync(file).mode & 0o777, 0o444);
    assert.equal(createHash('sha256').update(fs.readFileSync(file)).digest('hex'), value.sha256);
  }
});

test('a corrupt download preserves the existing action cache', t => {
  const {root, destination, manifest} = fixture(t);
  installActionCache(manifest, root, destination);
  const marker = fs.readFileSync(path.join(destination, '.tidemq-action-cache.json'));
  fs.writeFileSync(path.join(root, 'actionTestA.archive'), 'corrupt');
  assert.throws(() => installActionCache(manifest, root, destination), /digest differs/);
  assert.deepEqual(fs.readFileSync(path.join(destination, '.tidemq-action-cache.json')), marker);
});

test('action cache refuses foreign directories, redirects and unpinned sources', t => {
  const {root, destination, manifest} = fixture(t);
  const outside = path.join(root, 'persistent');
  fs.mkdirSync(outside); fs.writeFileSync(path.join(outside, 'identity'), 'keep');
  fs.symlinkSync(outside, destination);
  assert.throws(() => installActionCache(manifest, root, destination), /redirected/);
  fs.unlinkSync(destination);
  fs.mkdirSync(destination); fs.writeFileSync(path.join(destination, 'foreign'), 'keep');
  assert.throws(() => installActionCache(manifest, root, destination), /unowned/);
  fs.rmSync(destination, {recursive: true});
  const bad = structuredClone(manifest); bad.actionTestA.url = 'https://example.com/untrusted';
  assert.throws(() => installActionCache(bad, root, destination), /Unexpected action source/);
  assert.equal(fs.readFileSync(path.join(outside, 'identity'), 'utf8'), 'keep');
});

test('workflow pins match the installed action archive identities', () => {
  const manifest = JSON.parse(fs.readFileSync(new URL('../../deploy/lab/binaries.json', import.meta.url)));
  const workflow = fs.readFileSync(new URL('../../.github/workflows/rust.yml', import.meta.url), 'utf8');
  const entries = Object.values(manifest).filter(value => value.action);
  assert.equal(entries.length, 3);
  for (const value of entries) {
    const references = [...workflow.matchAll(new RegExp(`${value.action}@([a-zA-Z0-9._-]+)`, 'g'))];
    assert.ok(references.length > 0);
    assert.ok(references.every(match => match[1] === value.commit), 'Workflow action differs from the installed archive');
  }
});
