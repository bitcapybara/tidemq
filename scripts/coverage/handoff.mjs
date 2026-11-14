import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import path from 'node:path';

export const coverageStore = '/srv/tidemq/maintenance/coverage-handoff';
const scopePattern = /^[0-9]+-[0-9]+-[0-9]+-[0-9a-f]{40}$/;
const retentionMs = 2 * 60 * 60 * 1000;
const coverageStoreByteBudget = 20 * 1024 * 1024 * 1024;

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

function validateIdentity(env) {
  const ids = [env.GITHUB_REPOSITORY_ID, env.GITHUB_RUN_ID, env.GITHUB_RUN_ATTEMPT];
  assert.ok(ids.every(id => typeof id === 'string' && /^[0-9]+$/.test(id)), 'Invalid coverage workflow identity');
  assert.match(env.GITHUB_SHA ?? '', /^[0-9a-f]{40}$/, 'Invalid coverage commit');
}

function scope(root, env, create = false) {
  validateIdentity(env);
  if (!privateDirectory(root, create)) return null;
  const directory = path.join(root, [env.GITHUB_REPOSITORY_ID, env.GITHUB_RUN_ID, env.GITHUB_RUN_ATTEMPT, env.GITHUB_SHA].join('-'));
  return privateDirectory(directory, create) ? directory : null;
}

function directoryBytes(directory) {
  let total = 0;
  for (const entry of fs.readdirSync(directory, {withFileTypes: true})) {
    if (entry.isSymbolicLink()) continue;
    const file = path.join(directory, entry.name);
    if (entry.isDirectory()) total += directoryBytes(file);
    else if (entry.isFile()) total += fs.statSync(file).size;
  }
  return total;
}

function pruneCoverageStore(root, env, current) {
  validateIdentity(env);
  if (!privateDirectory(root, false)) return;
  const prefix = `${env.GITHUB_REPOSITORY_ID}-`;
  const candidates = [];
  let total = 0;
  for (const entry of fs.readdirSync(root, {withFileTypes: true})) {
    if (!entry.isDirectory() || entry.isSymbolicLink() || !scopePattern.test(entry.name) || !entry.name.startsWith(prefix)) continue;
    const directory = path.join(root, entry.name), stat = privateDirectory(directory, false);
    if (!stat) continue;
    const bytes = directoryBytes(directory);
    candidates.push({directory, bytes, mtimeMs: stat.mtimeMs});
    total += bytes;
  }
  candidates.sort((left, right) => left.mtimeMs - right.mtimeMs);
  for (const candidate of candidates) {
    if (total <= coverageStoreByteBudget) break;
    if (candidate.directory === current) continue;
    fs.rmSync(candidate.directory, {recursive: true});
    total -= candidate.bytes;
  }
}

function scopes(root, env, current, includeFuture = false) {
  validateIdentity(env);
  if (!privateDirectory(root, false)) return [];
  const prefix = `${env.GITHUB_REPOSITORY_ID}-${env.GITHUB_RUN_ID}-`;
  const suffix = `-${env.GITHUB_SHA}`;
  const currentAttempt = BigInt(env.GITHUB_RUN_ATTEMPT);
  const candidates = current ? [{directory: current, attempt: currentAttempt}] : [];
  for (const entry of fs.readdirSync(root, {withFileTypes: true})) {
    if (!entry.isDirectory() || entry.isSymbolicLink() || !scopePattern.test(entry.name) ||
        !entry.name.startsWith(prefix) || !entry.name.endsWith(suffix)) continue;
    const attemptText = entry.name.slice(prefix.length, -suffix.length);
    if (!/^[0-9]+$/.test(attemptText) || (!includeFuture && BigInt(attemptText) > currentAttempt)) continue;
    const directory = path.join(root, entry.name);
    if (directory === current) continue;
    if (privateDirectory(directory, false)) candidates.push({directory, attempt: BigInt(attemptText)});
  }
  candidates.sort((left, right) => left.attempt === right.attempt ? 0 : left.attempt > right.attempt ? -1 : 1);
  return candidates.map(candidate => candidate.directory);
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
    if (stat && Date.now() - stat.mtimeMs > retentionMs) fs.rmSync(previous, {recursive: true});
  }
  const destination = path.join(directory, lane);
  assert.ok(!fs.lstatSync(destination, {throwIfNoEntry: false}), 'Coverage lane already published');
  const temporary = fs.mkdtempSync(path.join(directory, '.' + lane + '-'));
  try {
    pack(temporary);
    fs.renameSync(temporary, destination);
    pruneCoverageStore(root, env, directory);
  } finally {
    fs.rmSync(temporary, {recursive: true, force: true});
  }
}

export function coverageInputs(root, env) {
  const current = scope(root, env);
  const directories = scopes(root, env, current);
  assert.ok(directories.length, 'Local coverage inputs missing. Rerun all workflow jobs.');
  return ['native', 'lab'].map(lane => {
    for (const directory of directories) {
      const input = path.join(directory, lane);
      if (privateDirectory(input, false)) return input;
    }
    assert.fail('Local coverage lane missing: ' + lane + '. Rerun all workflow jobs.');
  });
}

export function removeCoverageInputs(root, env) {
  for (const directory of scopes(root, env, null, true)) fs.rmSync(directory, {recursive: true});
}
