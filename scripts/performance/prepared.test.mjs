import assert from 'node:assert/strict';
import {execFileSync, spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {chmod, mkdtemp, mkdir, readFile, rm, stat, symlink, writeFile} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {HARNESS_FILES} from './compare.mjs';
import {verifyPreparedBuilds} from '../performance-gate.mjs';

const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const binaries = {baseline: 'tidemq', candidate: 'tidemq', 'baseline-load': 'perf_load', 'candidate-load': 'perf_load'};
const git = (cwd, ...args) => execFileSync('git', args, {cwd, encoding: 'utf8'}).trim();
const commit = cwd => {
  git(cwd, 'add', '.');
  git(cwd, '-c', 'core.hooksPath=/dev/null', '-c', 'user.name=Performance Test',
    '-c', 'user.email=perf-test@example.invalid', 'commit', '--no-gpg-sign', '-qm', 'Test source');
  return git(cwd, 'rev-parse', 'HEAD');
};

async function fixture(sameSource = false) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'tidemq-perf-prepared-'));
  const sourceCommits = {};
  for (const side of ['baseline', 'candidate', 'tools']) {
    const directory = path.join(root, side);
    await mkdir(directory);
    git(directory, 'init', '-q', '--initial-branch=main');
    await writeFile(path.join(directory, 'Cargo.lock'), `Fixture lockfile for ${side}\n`);
    if (side !== 'tools') sourceCommits[side] = commit(directory);
  }
  const policy = JSON.parse(await readFile(new URL('../../performance/baseline.json', import.meta.url)));
  policy.service_source_commit = sourceCommits.baseline;
  const policyBytes = `${JSON.stringify(policy, null, 2)}\n`;
  const tools = path.join(root, 'tools');
  await mkdir(path.join(tools, 'performance'));
  await writeFile(path.join(tools, 'performance/baseline.json'), policyBytes);
  const harness = {};
  for (const file of HARNESS_FILES) {
    await mkdir(path.dirname(path.join(tools, file)), {recursive: true});
    const bytes = `Protected fixture for ${file}\n`;
    await writeFile(path.join(tools, file), bytes);
    harness[file] = hash(bytes);
  }
  const toolsCommit = commit(tools);
  const prepared = path.join(root, 'prepared');
  const builds = {};
  for (const [name, binary] of Object.entries(binaries)) {
    const directory = path.join(prepared, 'builds', name);
    await mkdir(directory, {recursive: true});
    const side = sameSource || name.startsWith('baseline') ? 'baseline' : 'candidate';
    const bytes = `Fixture binary for ${sameSource ? name.replace('candidate', 'baseline') : name}\n`;
    const identity = {source_commit: sourceCommits[side],
      cargo_lock_sha256: hash(await readFile(path.join(root, side, 'Cargo.lock'))),
      ...(binary === 'tidemq' ? {allocator_feature: policy.allocator_feature, profile: 'release',
        tracked_source_clean: true, service_binary_sha256: hash(bytes)} : {
        sdk_source_commit: sourceCommits[side], harness_source_commit: toolsCommit,
        harness_files_sha256: harness, generator_allocator: 'system', binary_sha256: hash(bytes)})};
    await writeFile(path.join(directory, binary), bytes, {mode: 0o644});
    await chmod(path.join(directory, binary), 0o644);
    await writeFile(path.join(directory, 'identity.json'), JSON.stringify(identity));
    await writeFile(path.join(directory, 'build.log'), 'Fixture build log\n');
    builds[name] = identity;
  }
  const manifest = {schema_version: 1, baseline_manifest_sha256: hash(policyBytes), trusted_tools_commit: toolsCommit,
    same_source_reused_builds: sameSource, build_environment: {platform: os.platform(), arch: os.arch()}, builds};
  const bytes = `${JSON.stringify(manifest, null, 2)}\n`;
  await writeFile(path.join(prepared, 'manifest.json'), bytes);
  return {root, prepared, manifest, settings: {'--tools': tools, '--baseline': path.join(root, 'baseline'),
    '--candidate': path.join(root, sameSource ? 'baseline' : 'candidate'), '--prepared': prepared, '--manifest-sha256': hash(bytes)}};
}

test('prepared builds bind both SDKs and services to clean sources before restoring static executable modes', async () => {
  const value = await fixture();
  try {
    const result = await verifyPreparedBuilds(value.settings);
    assert.deepEqual(result.manifest, value.manifest);
    assert.notEqual(result.baselineCommit, result.candidateCommit);
    for (const [name, binary] of Object.entries(binaries)) {
      assert.equal((await stat(path.join(value.prepared, 'builds', name, binary))).mode & 0o777, 0o700);
    }
  } finally { await rm(value.root, {recursive: true, force: true}); }
});

test('corrupt, missing, or symlinked prepared artifacts are rejected before executable modes change', async context => {
  const cases = {
    binary: value => writeFile(path.join(value.prepared, 'builds/candidate-load/perf_load'), 'Corrupt binary\n'),
    identity: value => writeFile(path.join(value.prepared, 'builds/candidate/identity.json'), '{}'),
    manifest: value => writeFile(path.join(value.prepared, 'manifest.json'), '{}'),
    missing: value => rm(path.join(value.prepared, 'builds/candidate-load/identity.json')),
    symlink: async value => {
      const file = path.join(value.prepared, 'builds/candidate-load/perf_load');
      await rm(file);
      await symlink(path.join(value.prepared, 'builds/baseline-load/perf_load'), file);
    },
    source: async value => {
      await writeFile(path.join(value.root, 'candidate/Cargo.lock'), 'Changed candidate lockfile\n');
      commit(path.join(value.root, 'candidate'));
    },
    policy: async value => {
      const file = path.join(value.root, 'tools/performance/baseline.json');
      await writeFile(file, `${await readFile(file, 'utf8')}\n`);
      commit(path.join(value.root, 'tools'));
    },
  };
  for (const [name, corrupt] of Object.entries(cases)) await context.test(name, async () => {
    const value = await fixture();
    try {
      await corrupt(value);
      await assert.rejects(verifyPreparedBuilds(value.settings));
      assert.equal((await stat(path.join(value.prepared, 'builds/baseline/tidemq'))).mode & 0o777, 0o644);
    } finally { await rm(value.root, {recursive: true, force: true}); }
  });
});

test('the measurement command rejects an absent external digest without executing a build or trial', async () => {
  const value = await fixture();
  try {
    const args = Object.entries(value.settings).filter(([key]) => key !== '--manifest-sha256').flat();
    const output = path.join(value.root, 'measurement');
    const result = spawnSync(process.execPath, [new URL('../performance-gate.mjs', import.meta.url).pathname,
      'measure', ...args, '--output', output], {encoding: 'utf8', timeout: 5000});
    assert.equal(result.status, 1);
    assert.match(result.stderr, /An external prepared manifest digest is required/);
    await assert.rejects(stat(output), {code: 'ENOENT'});
    assert.equal((await stat(path.join(value.prepared, 'builds/baseline/tidemq'))).mode & 0o777, 0o644);
  } finally { await rm(value.root, {recursive: true, force: true}); }
});

test('the measurement CLI parses both artifact options and rejects a mismatched digest before execution', async () => {
  const value = await fixture();
  try {
    const settings = {...value.settings, '--manifest-sha256': '0'.repeat(64)};
    assert.notEqual(settings['--manifest-sha256'], value.settings['--manifest-sha256']);
    const output = path.join(value.root, 'measurement');
    const result = spawnSync(process.execPath, [new URL('../performance-gate.mjs', import.meta.url).pathname,
      'measure', ...Object.entries(settings).flat(), '--output', output], {encoding: 'utf8', timeout: 5000});
    assert.equal(result.status, 1);
    assert.match(result.stderr, /Prepared manifest digest differs from the build job/);
    await assert.rejects(stat(output), {code: 'ENOENT'});
    for (const [name, binary] of Object.entries(binaries)) {
      assert.equal((await stat(path.join(value.prepared, 'builds', name, binary))).mode & 0o777, 0o644);
    }
  } finally { await rm(value.root, {recursive: true, force: true}); }
});

test('a same-source reuse claim requires the actual service and workload identities to match', async () => {
  const value = await fixture(true);
  try {
    assert.equal((await verifyPreparedBuilds(value.settings)).manifest.same_source_reused_builds, true);
    const directory = path.join(value.prepared, 'builds/candidate-load');
    const bytes = 'Different workload binary from the same source\n';
    await writeFile(path.join(directory, 'perf_load'), bytes);
    value.manifest.builds['candidate-load'].binary_sha256 = hash(bytes);
    await writeFile(path.join(directory, 'identity.json'), JSON.stringify(value.manifest.builds['candidate-load']));
    const manifest = JSON.stringify(value.manifest);
    await writeFile(path.join(value.prepared, 'manifest.json'), manifest);
    value.settings['--manifest-sha256'] = hash(manifest);
    await assert.rejects(verifyPreparedBuilds(value.settings), /Reused SDK workload identities differ/);
  } finally { await rm(value.root, {recursive: true, force: true}); }
});
