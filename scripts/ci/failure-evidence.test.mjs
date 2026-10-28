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

test('consumer recovery snapshots retain both known stages without allowing credential files', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-evidence-'));
  try {
    const directory = path.join(root, 'tidemq-runtime-0123-abcd');
    fs.mkdirSync(directory);
    const snapshots = {
      'consumer-recovery-after-quorum.json': {stage: 'after-quorum', nodes: [{blocked: false}]},
      'consumer-recovery-timeout.json': {stage: 'timeout', nodes: []},
    };
    for (const [name, value] of Object.entries(snapshots)) {
      fs.writeFileSync(path.join(directory, name), JSON.stringify(value));
    }
    fs.writeFileSync(path.join(directory, 'consumer-recovery-access-token.json'), '{"secret":"private"}');
    const oversized = path.join(root, 'tidemq-runtime-5678-abcd');
    fs.mkdirSync(oversized);
    fs.writeFileSync(path.join(oversized, 'consumer-recovery-timeout.json'), JSON.stringify({samples: 'x'.repeat(2 * 1024 * 1024)}));
    const output = path.join(root, 'output');
    const files = collectFailureEvidence(root, output);
    assert.deepEqual(new Set(files), new Set(Object.keys(snapshots).map(name => `tidemq-runtime-0123-abcd/${name}`)));
    for (const [name, value] of Object.entries(snapshots)) {
      assert.deepEqual(JSON.parse(fs.readFileSync(path.join(output, 'tidemq-runtime-0123-abcd', name))), value);
    }
    assert.equal(fs.existsSync(path.join(output, 'tidemq-runtime-0123-abcd/consumer-recovery-access-token.json')), false);
    assert.equal(fs.existsSync(path.join(output, 'tidemq-runtime-5678-abcd/consumer-recovery-timeout.json')), false);
  } finally { fs.rmSync(root, {recursive: true, force: true}); }
});

test('failure manifests identify the retained tail instead of claiming complete logs', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-evidence-'));
  try {
    const directory = path.join(root, 'tidemq-runtime-0123-abcd');
    fs.mkdirSync(directory);
    const prefix = 'p'.repeat(64 * 1024);
    const tail = 't'.repeat(256 * 1024);
    fs.writeFileSync(path.join(directory, 'process-1.log'), prefix + tail);
    fs.writeFileSync(path.join(directory, 'consumer-recovery-timeout.json'), '{}');
    const output = path.join(root, 'output');
    collectFailureEvidence(root, output);
    assert.equal(fs.readFileSync(path.join(output, 'tidemq-runtime-0123-abcd/process-1.log'), 'utf8'), tail);
    const manifest = JSON.parse(fs.readFileSync(path.join(output, 'manifest.json')));
    assert.deepEqual(manifest.entries.find(entry => entry.path.endsWith('/process-1.log')), {
      path: 'tidemq-runtime-0123-abcd/process-1.log', sourceBytes: prefix.length + tail.length,
      copiedBytes: tail.length, tailOffset: prefix.length, truncated: true,
    });
    assert.deepEqual(manifest.entries.find(entry => entry.path.endsWith('/consumer-recovery-timeout.json')), {
      path: 'tidemq-runtime-0123-abcd/consumer-recovery-timeout.json', sourceBytes: 2,
      copiedBytes: 2, tailOffset: 0, truncated: false,
    });
  } finally { fs.rmSync(root, {recursive: true, force: true}); }
});

test('short reads stay visibly incomplete and never add zero padding to evidence', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-evidence-'));
  const read = fs.readSync;
  try {
    const directory = path.join(root, 'tidemq-runtime-0123-abcd');
    fs.mkdirSync(directory);
    fs.writeFileSync(path.join(directory, 'process-1.log'), 'original log data');
    const output = path.join(root, 'output');
    fs.readSync = (fd, buffer, offset, length, position) => read(fd, buffer, offset, Math.min(length, 3), position);
    try { collectFailureEvidence(root, output); }
    finally { fs.readSync = read; }
    assert.equal(fs.readFileSync(path.join(output, 'tidemq-runtime-0123-abcd/process-1.log'), 'utf8'), 'ori');
    const {entries} = JSON.parse(fs.readFileSync(path.join(output, 'manifest.json')));
    assert.deepEqual(entries, [{path: 'tidemq-runtime-0123-abcd/process-1.log', sourceBytes: 17,
      copiedBytes: 3, tailOffset: 0, truncated: true}]);
  } finally {
    fs.readSync = read;
    fs.rmSync(root, {recursive: true, force: true});
  }
});
