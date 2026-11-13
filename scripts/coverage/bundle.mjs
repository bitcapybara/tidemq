import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import * as fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {coverageInputs, coverageStore, publishCoverage, removeCoverageInputs} from './handoff.mjs';

const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const json = (file, value) => fs.writeFileSync(file, JSON.stringify(value, null, 2) + '\n', {mode: 0o600});
const stages = {native: ['unit', 'sdk-default-unit', 'integration'], lab: ['runtime-faults', 'external-services']};
const coverageByteBudget = 8 * 1024 * 1024 * 1024;

export function coverageByteTotal(entries) {
  const counted = new Map();
  let total = 0;
  for (const entry of entries) {
    let hashes = counted.get(entry.file);
    if (!hashes) {
      hashes = new Set();
      counted.set(entry.file, hashes);
    }
    if (!hashes.has(entry.sha256)) {
      hashes.add(entry.sha256);
      total += entry.bytes;
    }
  }
  return total;
}

function alternateObject(file, sha256) {
  const parsed = path.parse(file), suffix = sha256.slice(0, 16);
  if (parsed.base === 'build-script-build') {
    const parent = path.parse(parsed.dir);
    return path.join(parent.dir, parent.base.replace(/-[a-f0-9]{16}$/, '') + '-' + suffix, parsed.base);
  }
  return path.join(parsed.dir, parsed.name.replace(/-[a-f0-9]{16}$/, '') + '-' + suffix + parsed.ext);
}

function boundedFile(root, relative, limit = 256 * 1024 * 1024) {
  assert.match(relative, /^[a-zA-Z0-9._/-]+$/, 'Invalid bundle path');
  const parts = relative.split('/');
  assert.ok(parts.every(part => part && part !== '.' && part !== '..'), 'Bundle path escapes its root');
  let file = root;
  for (const part of parts) { file = path.join(file, part); assert.ok(!fs.lstatSync(file).isSymbolicLink(), 'Bundle symlinks are forbidden'); }
  const stat = fs.lstatSync(file);
  assert.ok(stat.isFile() && stat.size > 0 && stat.size <= limit, 'Invalid bundle file size or type');
  return fs.readFileSync(file);
}

function objects(directory, prefix = 'debug') {
  if (!fs.existsSync(directory)) return [];
  const found = [];
  for (const entry of fs.readdirSync(directory, {withFileTypes: true}).sort((a, b) => a.name.localeCompare(b.name))) {
    if (entry.isSymbolicLink() || ['.fingerprint', 'incremental'].includes(entry.name)) continue;
    const relative = prefix + '/' + entry.name, file = path.join(directory, entry.name);
    if (entry.isDirectory()) found.push(...objects(file, relative));
    else if (entry.isFile() && (fs.statSync(file).mode & 0o111)) {
      const fd = fs.openSync(file, 'r'), magic = Buffer.alloc(4);
      try { fs.readSync(fd, magic, 0, 4, 0); } finally { fs.closeSync(fd); }
      if (magic.equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46]))) found.push(relative);
    }
  }
  return found;
}

export function packBundle(target, reports, output, identity) {
  assert.ok(stages[identity.lane], 'Unknown coverage lane');
  fs.mkdirSync(output, {recursive: true});
  assert.equal(fs.readdirSync(output).length, 0, 'Bundle destination must be empty');
  const entries = [], hashes = new Set();
  function copy(root, source, file, kind) {
    const bytes = boundedFile(root, source), sha256 = hash(bytes);
    if (kind === 'object' && hashes.has(sha256)) return;
    hashes.add(sha256);
    const destination = path.join(output, file);
    fs.mkdirSync(path.dirname(destination), {recursive: true});
    fs.writeFileSync(destination, bytes, {mode: kind === 'object' ? 0o755 : 0o600, flag: 'wx'});
    entries.push({file, bytes: bytes.length, sha256, kind});
  }
  for (const file of objects(path.join(target, 'debug'))) copy(target, file, 'objects/' + file, 'object');
  for (const file of fs.readdirSync(target).filter(file => /^[a-zA-Z0-9._-]+\.profraw$/.test(file))) {
    if (fs.lstatSync(path.join(target, file)).size) copy(target, file, 'profiles/' + file, 'profile');
  }
  for (const stage of stages[identity.lane]) copy(reports, stage + '-junit.xml', 'reports/' + stage + '-junit.xml', 'report');
  if (identity.lane === 'lab') copy(reports, 'api-coverage.json', 'reports/api-coverage.json', 'report');
  assert.ok(entries.some(file => file.kind === 'object') && entries.some(file => file.kind === 'profile'), 'Missing instrumented objects or profiles');
  const manifest = {schema_version: 1, ...identity, entries};
  json(path.join(output, 'manifest.json'), manifest);
  return manifest;
}

export function restoreBundles(bundles, target, reports, expected) {
  const manifests = bundles.map(bundle => JSON.parse(boundedFile(bundle, 'manifest.json', 4 * 1024 * 1024)));
  assert.deepEqual(manifests.map(value => value.lane).sort(), ['lab', 'native'], 'Both coverage lanes are required exactly once');
  const expectedAttempt = BigInt(expected.run_attempt);
  const verifiedEntries = [];
  for (let index = 0; index < bundles.length; index++) {
    const value = manifests[index];
    assert.equal(value.schema_version, 1, 'Unsupported coverage bundle');
    for (const key of ['source_commit', 'run_id', 'workspace', 'toolchain']) assert.deepEqual(value[key], expected[key], 'Coverage bundle identity differs: ' + key);
    assert.match(value.run_attempt ?? '', /^[0-9]+$/, 'Coverage bundle identity differs: run_attempt');
    assert.ok(BigInt(value.run_attempt) <= expectedAttempt, 'Coverage bundle run_attempt is newer than expected');
    assert.ok(value.entries.length > 0 && value.entries.length <= 10000, 'Invalid bundle entry count');
    const names = new Set();
    for (const entry of value.entries) {
      assert.ok(['object', 'profile', 'report'].includes(entry.kind), 'Unknown bundle entry');
      assert.ok(!names.has(entry.file), 'Duplicate bundle entry'); names.add(entry.file);
      const bytes = boundedFile(bundles[index], entry.file);
      assert.equal(bytes.length, entry.bytes, 'Bundle file size changed');
      assert.equal(hash(bytes), entry.sha256, 'Bundle file digest changed');
      verifiedEntries.push({file: entry.file, sha256: entry.sha256, bytes: bytes.length});
    }
  }
  assert.ok(coverageByteTotal(verifiedEntries) <= coverageByteBudget, 'Coverage bundles exceed their byte budget');
  assert.ok(!fs.existsSync(target) || fs.readdirSync(target).length === 0, 'Coverage merge target must be empty');
  const restored = new Set();
  for (let index = 0; index < bundles.length; index++) for (const entry of manifests[index].entries) {
    const bytes = boundedFile(bundles[index], entry.file);
    let destination;
    if (entry.kind === 'object') {
      assert.ok(entry.file.startsWith('objects/debug/'), 'Object is outside the debug tree');
      destination = path.join(target, entry.file.slice('objects/'.length));
      if (fs.existsSync(destination) && hash(fs.readFileSync(destination)) !== entry.sha256) {
        // Different feature builds may have the same executable name; retain both mappings.
        destination = alternateObject(destination, entry.sha256);
      }
    } else if (entry.kind === 'profile') {
      assert.match(entry.file, /^profiles\/[a-zA-Z0-9._-]+\.profraw$/, 'Invalid native profile path');
      destination = path.join(target, path.basename(entry.file));
    } else {
      assert.match(entry.file, /^reports\/(?:[a-z-]+-junit\.xml|api-coverage\.json)$/, 'Invalid report path');
      destination = path.join(reports, path.basename(entry.file));
    }
    if (fs.existsSync(destination)) assert.equal(hash(fs.readFileSync(destination)), entry.sha256, 'Conflicting coverage inputs');
    else { fs.mkdirSync(path.dirname(destination), {recursive: true}); fs.writeFileSync(destination, bytes, {mode: entry.kind === 'object' ? 0o755 : 0o600, flag: 'wx'}); }
    restored.add(destination);
  }
  for (const stage of Object.values(stages).flat()) assert.ok(restored.has(path.join(reports, stage + '-junit.xml')), 'A test stage was omitted');
  assert.ok(restored.has(path.join(reports, 'api-coverage.json')), 'API coverage was omitted');
  json(path.join(reports, 'coverage-inputs.json'), manifests);
  return manifests;
}

function identity(lane) {
  const version = args => execFileSync('cargo', args, {encoding: 'utf8'}).trim();
  execFileSync('git', ['diff', '--exit-code'], {stdio: 'pipe'});
  execFileSync('git', ['diff', '--cached', '--exit-code'], {stdio: 'pipe'});
  const source = execFileSync('git', ['rev-parse', 'HEAD'], {encoding: 'utf8'}).trim();
  assert.equal(source, process.env.GITHUB_SHA, 'Coverage checkout differs from workflow commit');
  return {lane, source_commit: source,
    run_id: process.env.GITHUB_RUN_ID, run_attempt: process.env.GITHUB_RUN_ATTEMPT, workspace: fs.realpathSync('.'),
    toolchain: {rustc: execFileSync('rustc', ['--version'], {encoding: 'utf8'}).trim(), nextest: version(['nextest', '--version']), llvm_cov: version(['llvm-cov', '--version'])}};
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const [mode, lane] = process.argv.slice(2), target = process.env.CARGO_LLVM_COV_TARGET_DIR;
  if (mode === 'clean') {
    removeCoverageInputs(coverageStore, process.env);
  } else {
    assert.ok(target && process.env.CARGO_LLVM_COV_SHOW_ENV === '1', 'Coverage environment is required');
    if (mode === 'pack') {
      publishCoverage(coverageStore, process.env, lane, output => packBundle(target, 'target/ci', output, identity(lane)));
    } else if (mode === 'restore') {
      restoreBundles(coverageInputs(coverageStore, process.env), target, 'target/ci', identity(null));
    } else throw new Error('Usage: bundle.mjs pack native|lab | restore | clean');
  }
}
