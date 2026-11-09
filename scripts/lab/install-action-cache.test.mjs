import test from 'node:test';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {installActionCache} from './install-action-cache.mjs';

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-actions-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const destination = path.join(root, 'action-archives');
  const manifest = Object.fromEntries(['checkout', 'download-artifact', 'upload-artifact', 'download-artifact'].map((name, index) => {
    const key = `actionTest${String.fromCharCode(65 + index)}`, commit = String(index + 1).repeat(40);
    const contents = Buffer.from(`official-archive-${name}-${commit}`);
    fs.writeFileSync(path.join(root, `${key}.archive`), contents);
    return [key, {action: `actions/${name}`, commit, sha256: createHash('sha256').update(contents).digest('hex'),
      url: `https://codeload.github.com/actions/${name}/tar.gz/${commit}`}];
  }));
  return {root, destination, manifest};
}

test('action cache remains readable under a private umask and retains only pinned archives', t => {
  const {root, destination, manifest} = fixture(t);
  const mask = process.umask(0o077);
  try {
    assert.equal(installActionCache(manifest, root, destination).actions, 4);
  } finally {
    process.umask(mask);
  }
  assert.equal(fs.statSync(destination).mode & 0o777, 0o755);
  assert.equal(fs.statSync(path.join(destination, '.tidemq-action-cache.json')).mode & 0o777, 0o444);
  for (const value of Object.values(manifest)) {
    const directory = path.join(destination, value.action.replace('/', '_'));
    assert.equal(fs.statSync(directory).mode & 0o777, 0o755);
    assert.equal(fs.statSync(path.join(directory, `${value.commit}.tar.gz`)).mode & 0o777, 0o444);
  }
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

test('duplicate action revisions preserve the existing action cache', t => {
  const {root, destination, manifest} = fixture(t);
  installActionCache(manifest, root, destination);
  const marker = fs.readFileSync(path.join(destination, '.tidemq-action-cache.json'));
  const duplicate = {...manifest, actionDuplicate: manifest.actionTestB};
  assert.throws(() => installActionCache(duplicate, root, destination), /Duplicate action archive identity/);
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

function workflowArchivePins(workflow) {
  // Keep YAML 1.1 scalars such as the job IDs "yes" and "on" distinct.
  const ruby = `
    class TextScalarScanner < Psych::ScalarScanner
      def tokenize(value)
        value
      end
    end
    loader = Psych::ClassLoader::Restricted.new([], [])
    visitor = Psych::Visitors::ToRuby.new(TextScalarScanner.new(loader), loader)
    print JSON.generate(visitor.accept(Psych.parse(STDIN.read)).fetch("jobs"))
  `;
  const [command, args] = process.platform === 'darwin' || process.env.RUNNER_ENVIRONMENT === 'github-hosted'
    ? ['ruby', ['-ryaml', '-rjson', '-e', ruby]]
    : ['python3', ['-c', 'import json, sys, yaml; print(json.dumps(yaml.load(sys.stdin, Loader=yaml.BaseLoader)["jobs"]))']];
  const jobs = JSON.parse(execFileSync(command, args, {input: workflow, encoding: 'utf8'}));
  const archiveRepositories = new Set(['actions/checkout', 'actions/download-artifact', 'actions/upload-artifact']);
  const pins = new Set();
  for (const job of Object.values(jobs)) {
    for (const step of job.steps ?? []) {
      if (typeof step.uses !== 'string') continue;
      const [actionPath] = step.uses.split('@', 1);
      const repository = actionPath.replaceAll('\\', '/').split('/').filter(Boolean).slice(0, 2).join('/').toLowerCase();
      if (archiveRepositories.has(repository)) pins.add(step.uses);
    }
  }
  return pins;
}

test('workflow pin checks use parsed step values and keep distinct job IDs', () => {
  const workflow = String.raw`jobs:
  validate:
    env:
      EXAMPLE: |
        uses: actions/download-artifact@browser
    steps:
      - uses: actions/download-artifact@coverage#v4
      - name: Checkout
        uses: "actions\u002fcheckout@main" # actions/checkout@archived
      - {uses: actions/upload-artifact@release}
  yes:
    steps:
      - uses: actions//checkout@alternate
  on:
    steps:
      - uses: actions\checkout@backslash
`;
  assert.deepEqual(workflowArchivePins(workflow), new Set([
    'actions/download-artifact@coverage#v4',
    'actions/checkout@main',
    'actions/upload-artifact@release',
    'actions//checkout@alternate',
    'actions\\checkout@backslash',
  ]));
});

test('workflow pins match the installed action archive identities', () => {
  const manifest = JSON.parse(fs.readFileSync(new URL('../../deploy/lab/binaries.json', import.meta.url)));
  const workflow = fs.readFileSync(new URL('../../.github/workflows/rust.yml', import.meta.url), 'utf8');
  const entries = Object.values(manifest).filter(value => value.action);
  const archived = new Set(entries.map(value => `${value.action}@${value.commit}`));
  assert.equal(archived.size, entries.length);
  assert.deepEqual(workflowArchivePins(workflow), archived, 'Workflow action pins differ from the archive manifest');
});
