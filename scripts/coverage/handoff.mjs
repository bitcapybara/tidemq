import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import path from 'node:path';

export const coverageStore = '/srv/tidemq/maintenance/coverage-handoff';
const scopePattern = /^[0-9]+-[0-9]+-[0-9]+-[0-9a-f]{40}$/;
const dayMs = 24 * 60 * 60 * 1000;

function privateDirectory(directory, create) {
  if (create) {
    try { fs.mkdirSync(directory, {mode: 0o700}); }
    catch (error) { if (error.code !== 'EEXIST') throw error; }
  }
  const stat = fs.lstatSync(directory, {throwIfNoEntry: false});
  if (!stat) return null;
  assert.ok(stat.isDirectory() && stat.uid === process.getuid() && (stat.mode & 0o077) === 0 &&
    fs.realpathSync(directory) === directory, 'Coverage directory must be private and owned by the runner');
  return stat;
}

function scope(root, env, create = false) {
  const ids = [env.GITHUB_REPOSITORY_ID, env.GITHUB_RUN_ID, env.GITHUB_RUN_ATTEMPT];
  assert.ok(ids.every(id => typeof id === 'string' && /^[0-9]+$/.test(id)), 'Invalid coverage workflow identity');
  assert.match(env.GITHUB_SHA ?? '', /^[0-9a-f]{40}$/, 'Invalid coverage commit');
  if (!privateDirectory(root, create)) return null;
  const directory = path.join(root, [...ids, env.GITHUB_SHA].join('-'));
  return privateDirectory(directory, create) ? directory : null;
}

export function publishCoverage(root, env, lane, pack) {
  assert.ok(lane === 'native' || lane === 'lab', 'Invalid coverage lane');
  const directory = scope(root, env, true);
  for (const entry of fs.readdirSync(root, {withFileTypes: true})) {
    if (!entry.isDirectory() || !scopePattern.test(entry.name) ||
        !entry.name.startsWith(env.GITHUB_REPOSITORY_ID + '-')) continue;
    const previous = path.join(root, entry.name);
    if (previous === directory) continue;
    const stat = privateDirectory(previous, false);
    if (stat && Date.now() - stat.mtimeMs > dayMs) fs.rmSync(previous, {recursive: true});
  }
  const destination = path.join(directory, lane);
  assert.ok(!fs.lstatSync(destination, {throwIfNoEntry: false}), 'Coverage lane already published');
  const temporary = fs.mkdtempSync(path.join(directory, '.' + lane + '-'));
  try {
    pack(temporary);
    fs.renameSync(temporary, destination);
  } finally {
    fs.rmSync(temporary, {recursive: true, force: true});
  }
}

export function coverageInputs(root, env) {
  const directory = scope(root, env);
  assert.ok(directory, 'Local coverage inputs missing. Rerun all workflow jobs.');
  return ['native', 'lab'].map(lane => {
    const input = path.join(directory, lane);
    assert.ok(privateDirectory(input, false), 'Local coverage lane missing: ' + lane + '. Rerun all workflow jobs.');
    return input;
  });
}

export function removeCoverageInputs(root, env) {
  const directory = scope(root, env);
  if (directory) fs.rmSync(directory, {recursive: true});
}
