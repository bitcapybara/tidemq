import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {collectFailureEvidence} from './failure-evidence.mjs';

test('failed runtime evidence is bounded and excludes configuration, keys and symlinks', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-evidence-'));
  try {
    const directory = path.join(root, 'tidemq-runtime-0123-abcd');
    fs.mkdirSync(directory);
    fs.writeFileSync(path.join(directory, 'live-pages-send-failure.json'), '{"reason":"test"}');
    fs.writeFileSync(path.join(directory, 'node-1.log'), 'x'.repeat(300 * 1024));
    fs.writeFileSync(path.join(directory, 'process-2.log'), 'Runtime process diagnostic');
    fs.writeFileSync(path.join(directory, 'node-1.key'), 'private');
    fs.writeFileSync(path.join(directory, 'config.toml'), 'private');
    fs.symlinkSync(path.join(directory, 'node-1.key'), path.join(directory, 'linked-failure.json'));
    const output = path.join(root, 'output');
    assert.equal(collectFailureEvidence(root, output).length, 3);
    assert.equal(fs.statSync(path.join(output, 'tidemq-runtime-0123-abcd/node-1.log')).size, 256 * 1024);
    assert.equal(fs.existsSync(path.join(output, 'tidemq-runtime-0123-abcd/node-1.key')), false);
    assert.equal(fs.readFileSync(path.join(output, 'tidemq-runtime-0123-abcd/process-2.log'), 'utf8'), 'Runtime process diagnostic');
    assert.deepEqual(JSON.parse(fs.readFileSync(path.join(output, 'tidemq-runtime-0123-abcd/live-pages-send-failure.json'))), {reason: 'test'});
  } finally { fs.rmSync(root, {recursive: true, force: true}); }
});
