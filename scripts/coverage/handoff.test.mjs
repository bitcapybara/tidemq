import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {coverageInputs, publishCoverage, removeCoverageInputs} from './handoff.mjs';

const env = {GITHUB_REPOSITORY_ID: '7', GITHUB_RUN_ID: '123', GITHUB_RUN_ATTEMPT: '1', GITHUB_SHA: 'a'.repeat(40)};

function fixture(t) {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'tidemq-handoff-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  return path.join(root, 'store');
}

function pack(lane) {
  return directory => fs.writeFileSync(path.join(directory, 'manifest.json'), lane);
}

test('coverage handoff publishes complete lanes and cleans only the matching attempt', t => {
  const root = fixture(t);
  const retry = {...env, GITHUB_RUN_ATTEMPT: '2'};
  publishCoverage(root, env, 'native', pack('native'));
  assert.throws(() => publishCoverage(root, env, 'lab', directory => {
    pack('partial')(directory);
    assert.throws(() => coverageInputs(root, env), /lane missing: lab/);
    throw new Error('Interrupted pack');
  }), /Interrupted pack/);
  assert.throws(() => coverageInputs(root, env), /lane missing: lab/);
  publishCoverage(root, env, 'lab', pack('lab'));
  const inputs = coverageInputs(root, env);
  assert.deepEqual(inputs.map(input => fs.readFileSync(path.join(input, 'manifest.json'), 'utf8')), ['native', 'lab']);
  assert.deepEqual(fs.readdirSync(path.dirname(inputs[0])).sort(), ['lab', 'native']);
  assert.throws(() => publishCoverage(root, env, 'lab', pack('overwrite')), /already published/);
  assert.throws(() => coverageInputs(root, retry), /inputs missing/);
  publishCoverage(root, retry, 'native', pack('retry'));
  removeCoverageInputs(root, env);
  removeCoverageInputs(root, env);
  assert.equal(fs.readdirSync(root).length, 1);
  assert.throws(() => coverageInputs(root, retry), /lane missing: lab/);
});

test('coverage handoff rejects redirected stores and prunes only expired owned scopes', t => {
  const root = fixture(t);
  const otherRepo = {...env, GITHUB_REPOSITORY_ID: '8'};
  publishCoverage(root, env, 'native', pack('expired'));
  publishCoverage(root, otherRepo, 'native', pack('other repo'));
  const past = new Date(Date.now() - 2 * 24 * 60 * 60 * 1000);
  for (const name of fs.readdirSync(root)) fs.utimesSync(path.join(root, name), past, past);
  const unknown = path.join(root, 'keep');
  fs.mkdirSync(unknown);
  fs.utimesSync(unknown, past, past);
  publishCoverage(root, {...env, GITHUB_RUN_ID: '124'}, 'native', pack('new'));
  assert.deepEqual(fs.readdirSync(root).sort(), ['7-124-1-' + env.GITHUB_SHA, '8-123-1-' + env.GITHUB_SHA, 'keep']);
  const redirected = root + '-link';
  fs.symlinkSync(root, redirected);
  assert.throws(() => removeCoverageInputs(redirected, env), /private and owned/);
  assert.throws(() => removeCoverageInputs(root, {...env, GITHUB_RUN_ID: '../123'}), /Invalid coverage workflow identity/);
  assert.ok(fs.existsSync(unknown));
});
