import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';

test('parallel integration lanes select every suite once and keep reports separate', t => {
  for (const [mode, expected] of [['normal', ['integration']], ['faults', ['runtime-faults']], ['all', ['integration', 'runtime-faults']]]) {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-ci-lanes-'));
    t.after(() => fs.rmSync(root, {recursive: true, force: true}));
    for (const directory of ['scripts/ci', 'bin', 'tmp']) fs.mkdirSync(path.join(root, directory), {recursive: true});
    fs.copyFileSync(new URL('./integration.sh', import.meta.url), path.join(root, 'scripts/ci/integration.sh'));
    fs.writeFileSync(path.join(root, 'bin/cargo'), '#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$TRACE"\nmkdir -p target/nextest/ci\nprintf "<testsuites/>" > target/nextest/ci/junit.xml\n', {mode: 0o755});
    fs.writeFileSync(path.join(root, 'bin/node'), '#!/usr/bin/env bash\nexit 0\n', {mode: 0o755});
    const result = spawnSync('bash', [path.join(root, 'scripts/ci/integration.sh'), mode], {encoding: 'utf8', env: {...process.env,
      PATH: path.join(root, 'bin') + ':' + process.env.PATH, TMPDIR: path.join(root, 'tmp'), TRACE: path.join(root, 'calls'), TIDEMQ_REQUIRE_EXTERNAL: '0'}});
    assert.equal(result.status, 0, result.stderr);
    const reports = fs.readdirSync(path.join(root, 'target/ci')).map(file => file.replace('-junit.xml', '')).sort();
    assert.deepEqual(reports, expected.sort());
    const calls = fs.readFileSync(path.join(root, 'calls'), 'utf8');
    assert.equal(calls.includes('--workspace'), mode !== 'faults');
    assert.equal(calls.includes('test-hooks'), mode !== 'normal');
    assert.equal(calls.includes('--test runtime_group_watch'), mode !== 'normal');
    assert.equal(calls.includes('--test sdk_consumer_long_poll'), mode !== 'normal');
  }
});
