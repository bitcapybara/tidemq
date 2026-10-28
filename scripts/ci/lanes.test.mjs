import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';

test('integration lanes select every suite once and keep reports separate', t => {
  for (const [mode, expected, external = false] of [['normal', ['integration']], ['faults', ['runtime-faults']], ['all', ['integration', 'runtime-faults']], ['faults', ['runtime-faults', 'external-services'], true]]) {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-ci-lanes-'));
    t.after(() => fs.rmSync(root, {recursive: true, force: true}));
    for (const directory of ['scripts/ci', 'bin', 'tmp']) fs.mkdirSync(path.join(root, directory), {recursive: true});
    fs.copyFileSync(new URL('./integration.sh', import.meta.url), path.join(root, 'scripts/ci/integration.sh'));
    fs.copyFileSync(new URL('./nextest.sh', import.meta.url), path.join(root, 'scripts/ci/nextest.sh'));
    fs.copyFileSync(new URL('./capture-nextest.sh', import.meta.url), path.join(root, 'scripts/ci/capture-nextest.sh'));
    fs.copyFileSync(new URL('./capture-nextest.mjs', import.meta.url), path.join(root, 'scripts/ci/capture-nextest.mjs'));
    fs.writeFileSync(path.join(root, 'bin/cargo'), '#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$TRACE"\nmkdir -p target/nextest/ci\nprintf "<testsuites/>" > target/nextest/ci/junit.xml\n', {mode: 0o755});
    fs.writeFileSync(path.join(root, 'bin/node'), '#!' + process.execPath + '\nconst {spawnSync}=require("node:child_process");if(process.argv[2]?.endsWith("/capture-nextest.mjs")){const r=spawnSync(process.execPath,process.argv.slice(2),{stdio:"inherit"});process.exit(r.status??1);}\n', {mode: 0o755});
    for (const name of ['rustfs', 'collector', 'sha256sum']) fs.writeFileSync(path.join(root, 'bin', name), '#!/usr/bin/env bash\nprintf "verified fixture\\n"\n', {mode: 0o755});
    const result = spawnSync('bash', [path.join(root, 'scripts/ci/integration.sh'), mode], {encoding: 'utf8', env: {...process.env,
      PATH: path.join(root, 'bin') + ':' + process.env.PATH, TMPDIR: path.join(root, 'tmp'), TRACE: path.join(root, 'calls'), CARGO_TARGET_DIR: path.join(root, 'target'), TIDEMQ_REQUIRE_EXTERNAL: external ? '1' : '0', TIDEMQ_NEXTEST_IMAGE: '', TIDEMQ_TEST_RUSTFS_BIN: path.join(root, 'bin/rustfs'), TIDEMQ_OTELCOL_BIN: path.join(root, 'bin/collector')}});
    assert.equal(result.status, 0, result.stderr);
    const reports = fs.readdirSync(path.join(root, 'target/ci')).filter(file => file.endsWith('-junit.xml')).map(file => file.replace('-junit.xml', '')).sort();
    assert.deepEqual(reports, expected.sort());
    const calls = fs.readFileSync(path.join(root, 'calls'), 'utf8');
    assert.equal(calls.includes('--workspace'), mode !== 'faults');
    assert.equal(calls.includes('test-hooks'), mode !== 'normal');
    assert.equal(calls.includes('--test runtime_group_watch'), mode !== 'normal');
    assert.equal(calls.includes('--test sdk_consumer_long_poll'), mode !== 'normal');
    assert.equal(calls.includes('--lib --test runtime_queue'), mode !== 'normal');
    assert.equal(calls.includes('--run-ignored only'), external);
    for (const name of ['rustfs-version.txt', 'collector-version.txt', 'external-binary-sha256.txt']) assert.equal(fs.existsSync(path.join(root, 'target/ci', name)), external);
  }
});
