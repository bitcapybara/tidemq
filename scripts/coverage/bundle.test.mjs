import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {packBundle, restoreBundles} from './bundle.mjs';

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-coverage-bundle-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const identity = {source_commit: 'a'.repeat(40), run_id: '123', workspace: '/work/tidemq', toolchain: {rustc: 'fixed', nextest: 'fixed', llvm_cov: 'fixed'}};
  const bundles = [];
  for (const lane of ['native', 'lab']) {
    const target = path.join(root, lane, 'target'), reports = path.join(root, lane, 'reports'), output = path.join(root, lane, 'bundle');
    fs.mkdirSync(path.join(target, 'debug', 'deps'), {recursive: true});
    fs.mkdirSync(reports);
    fs.writeFileSync(path.join(target, 'debug', 'deps', 'tidemq-' + '1'.repeat(16)), Buffer.concat([Buffer.from([0x7f, 0x45, 0x4c, 0x46]), Buffer.from(lane)]), {mode: 0o755});
    const build = path.join(target, 'debug', 'build', 'tidemq-protocol-' + '1'.repeat(16));
    fs.mkdirSync(build, {recursive: true});
    fs.writeFileSync(path.join(build, 'build-script-build'), Buffer.concat([Buffer.from([0x7f, 0x45, 0x4c, 0x46]), Buffer.from('build-' + lane)]), {mode: 0o755});
    fs.writeFileSync(path.join(target, lane + '.profraw'), lane);
    for (const stage of lane === 'native' ? ['unit', 'sdk-default-unit', 'integration'] : ['runtime-faults', 'external-services']) {
      fs.writeFileSync(path.join(reports, stage + '-junit.xml'), '<testsuites/>');
    }
    if (lane === 'lab') fs.writeFileSync(path.join(reports, 'api-coverage.json'), '{}');
    packBundle(target, reports, output, {...identity, lane});
    bundles.push(output);
  }
  return {root, identity, bundles, target: path.join(root, 'merged'), reports: path.join(root, 'reports')};
}

test('parallel coverage retains both feature objects, all profiles and every test stage', t => {
  const f = fixture(t);
  restoreBundles(f.bundles, f.target, f.reports, f.identity);
  assert.equal(fs.readdirSync(path.join(f.target, 'debug', 'deps')).length, 2);
  for (const file of fs.readdirSync(path.join(f.target, 'debug', 'deps'))) assert.match(file, /^tidemq-[a-f0-9]{16}$/);
  for (const file of fs.readdirSync(path.join(f.target, 'debug', 'build'))) {
    assert.match(file, /^tidemq-protocol-[a-f0-9]{16}$/);
    assert.ok(fs.existsSync(path.join(f.target, 'debug', 'build', file, 'build-script-build')));
  }
  assert.deepEqual(fs.readdirSync(f.target).filter(file => file.endsWith('.profraw')).sort(), ['lab.profraw', 'native.profraw']);
  assert.equal(fs.readdirSync(f.reports).filter(file => file.endsWith('-junit.xml')).length, 5);
  assert.ok(fs.existsSync(path.join(f.reports, 'api-coverage.json')));
});

test('coverage merge rejects other cohorts, missing lanes and modified profiles before restoring files', t => {
  for (const change of ['source_commit', 'run_id', 'workspace', 'toolchain']) {
    const f = fixture(t), expected = {...f.identity, [change]: 'different'};
    assert.throws(() => restoreBundles(f.bundles, f.target, f.reports, expected), /identity differs/);
    assert.ok(!fs.existsSync(f.target));
  }
  const f = fixture(t);
  assert.throws(() => restoreBundles([f.bundles[0], f.bundles[0]], f.target, f.reports, f.identity), /Both coverage lanes/);
  fs.writeFileSync(path.join(f.bundles[0], 'profiles', 'native.profraw'), 'tampered');
  assert.throws(() => restoreBundles(f.bundles, f.target, f.reports, f.identity), /size changed|digest changed/);
  assert.ok(!fs.existsSync(f.target));
});

test('coverage bundles reject symlinks and path traversal', t => {
  const f = fixture(t), manifestFile = path.join(f.bundles[0], 'manifest.json');
  const value = JSON.parse(fs.readFileSync(manifestFile));
  value.entries[0].file = '../outside';
  fs.writeFileSync(manifestFile, JSON.stringify(value));
  assert.throws(() => restoreBundles(f.bundles, f.target, f.reports, f.identity), /escapes/);
  const g = fixture(t), profile = path.join(g.bundles[0], 'profiles', 'native.profraw');
  fs.renameSync(profile, profile + '.original'); fs.symlinkSync(profile + '.original', profile);
  assert.throws(() => restoreBundles(g.bundles, g.target, g.reports, g.identity), /symlinks/);
});
