#!/usr/bin/env node
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {spawn} from 'node:child_process';
import {createWriteStream} from 'node:fs';
import * as fs from 'node:fs/promises';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import {setTimeout as sleep} from 'node:timers/promises';
import {fileURLToPath} from 'node:url';
import {comparePairs, summarizeReport, validatePolicy, workloadFor, HARNESS_FILES} from './performance/compare.mjs';

const DEFAULT_TOOLS = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const LOG_LIMIT = 32 * 1024 * 1024;
const OWNED = new Set();
const BUILD_BINARIES = {baseline: 'tidemq', candidate: 'tidemq', 'baseline-load': 'perf_load', 'candidate-load': 'perf_load'};
const policyPath = tools => path.join(tools, 'performance/baseline.json');
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const json = async file => JSON.parse(await fs.readFile(file, 'utf8'));
const save = async (file, value) => fs.writeFile(file, `${JSON.stringify(value, null, 2)}\n`, {mode: 0o600});

function options(values) {
  const result = {};
  while (values.length) {
    const key = values.shift();
    assert.match(key, /^--[a-z][a-z0-9-]*$/, 'Invalid performance gate option');
    const value = key === '--require-baseline-unchanged' ? true : values.shift();
    assert.ok(value && !String(value).startsWith('--'), `Missing ${key} value`);
    assert.ok(!(key in result), `Duplicate ${key} option`);
    result[key] = value;
  }
  return result;
}

async function command(binary, args, settings = {}) {
  const child = spawn(binary, args, {cwd: settings.cwd, env: settings.env ?? process.env, stdio: ['ignore', 'pipe', 'pipe']});
  OWNED.add(child);
  child.once('close', () => OWNED.delete(child));
  const limit = settings.log ? LOG_LIMIT : 262144;
  const chunks = [];
  let bytes = 0;
  let exceeded = false;
  const collect = chunk => {
    bytes += chunk.length;
    if (bytes <= limit) chunks.push(chunk);
    else { exceeded = true; child.kill('SIGKILL'); }
  };
  child.stdout.on('data', collect);
  child.stderr.on('data', collect);
  const timer = setTimeout(() => child.kill('SIGKILL'), settings.timeoutMs ?? 120000);
  const result = await new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => resolve({code, signal}));
  }).finally(() => clearTimeout(timer));
  const bytesOutput = Buffer.concat(chunks);
  const output = bytesOutput.toString('utf8');
  if (settings.log) await fs.writeFile(settings.log, bytesOutput, {mode: 0o600});
  assert.ok(!exceeded, `${binary} output exceeded its byte limit`);
  assert.equal(result.code, 0, `${binary} failed (${result.code ?? result.signal}); ${settings.log ?? output.slice(-4096)}`);
  return output.trim();
}

async function sourceIdentity(source) {
  await command('git', ['diff', '--quiet'], {cwd: source});
  await command('git', ['diff', '--cached', '--quiet'], {cwd: source});
  return command('git', ['rev-parse', 'HEAD'], {cwd: source});
}

function environment() {
  return {platform: os.platform(), arch: os.arch(), kernel: os.release(), node: process.version,
    hostname: os.hostname(), cpu_model: os.cpus()[0]?.model ?? 'unknown', logical_cpus: os.cpus().length,
    total_memory_bytes: os.totalmem(), github_runner_image: process.env.ImageVersion ?? null,
    github_run_id: process.env.GITHUB_RUN_ID ?? null, github_job: process.env.GITHUB_JOB ?? null};
}

async function gateInputs(settings) {
  const tools = path.resolve(settings['--tools'] ?? DEFAULT_TOOLS);
  const baseline = path.resolve(settings['--baseline']);
  const candidate = path.resolve(settings['--candidate']);
  const policyBytes = await fs.readFile(policyPath(tools));
  const policy = validatePolicy(JSON.parse(policyBytes));
  if (settings['--require-baseline-unchanged']) {
    assert.deepEqual(await fs.readFile(policyPath(candidate)), policyBytes,
      'PRs cannot replace their protected performance baseline; successful main runs promote it');
  }
  const [toolsCommit, baselineCommit, candidateCommit] = await Promise.all(
    [tools, baseline, candidate].map(sourceIdentity));
  assert.equal(baselineCommit, policy.service_source_commit, 'Baseline checkout is not the protected source');
  return {tools, baseline, candidate, policyBytes, policy, toolsCommit, baselineCommit, candidateCommit};
}

async function overlay(source, tools, directory) {
  const commit = await sourceIdentity(source);
  const toolsCommit = await sourceIdentity(tools);
  await fs.mkdir(directory, {recursive: true, mode: 0o700});
  const archive = `${directory}.tar`;
  await command('git', ['archive', '--format=tar', '--output', archive, commit], {cwd: source});
  await command('tar', ['-xf', archive, '-C', directory]);
  await fs.unlink(archive);
  const harnessHashes = {};
  for (const file of HARNESS_FILES) {
    const bytes = await fs.readFile(path.join(tools, file));
    await fs.mkdir(path.dirname(path.join(directory, file)), {recursive: true});
    await fs.writeFile(path.join(directory, file), bytes);
    harnessHashes[file] = sha256(bytes);
  }
  return {original_source: source, source_commit: commit, harness_source_commit: toolsCommit,
    harness_files_sha256: harnessHashes};
}

export function canReuseBuilds(baseline, candidate) {
  return /^[0-9a-f]{40}$/.test(baseline.source_commit)
    && baseline.source_commit === candidate.source_commit
    && HARNESS_FILES.every(file => /^[0-9a-f]{64}$/.test(baseline.harness_files_sha256?.[file])
      && baseline.harness_files_sha256[file] === candidate.harness_files_sha256?.[file]);
}

export function sameActualBinaries(report) {
  const service = [report.baseline?.service_binary_sha256, report.candidate?.service_binary_sha256];
  const load = [report.workload_binaries?.baseline?.binary_sha256, report.workload_binaries?.candidate?.binary_sha256];
  for (const digest of [...service, ...load]) assert.match(digest ?? '', /^[0-9a-f]{64}$/, 'Missing or invalid actual binary digest');
  return service[0] === service[1] && load[0] === load[1];
}

export async function reuseBuild(source, directory, binary, identity) {
  assert.ok(['tidemq', 'perf_load'].includes(binary), 'Unknown reusable binary');
  assert.deepEqual(await json(path.join(source, 'identity.json')), identity, 'Reusable build identity changed');
  const hash = binary === 'tidemq' ? identity.service_binary_sha256 : identity.binary_sha256;
  assert.equal(sha256(await fs.readFile(path.join(source, binary))), hash, 'Reusable build binary changed');
  await fs.mkdir(directory, {mode: 0o700});
  await fs.copyFile(path.join(source, binary), path.join(directory, binary));
  await fs.copyFile(path.join(source, 'identity.json'), path.join(directory, 'identity.json'));
  assert.equal(sha256(await fs.readFile(path.join(directory, binary))), hash, 'Copied build binary differs');
  await fs.writeFile(path.join(directory, 'build.log'),
    `Reused the exact baseline ${binary} binary and identity for source ${identity.source_commit}; no second compilation.\n`, {mode: 0o600});
  return identity;
}

async function build(source, directory, target, policy, loadOverlay) {
  await fs.mkdir(directory, {recursive: true, mode: 0o700});
  const loadOnly = loadOverlay !== undefined;
  const original = loadOnly ? loadOverlay.original_source : source;
  const commit = await sourceIdentity(original);
  if (loadOnly) assert.equal(commit, loadOverlay.source_commit, 'Archived SDK source changed before build');
  const args = ['build', '--manifest-path', path.join(source, 'Cargo.toml'), '--release', '--locked', '-p', 'tidemq',
    loadOnly ? '--example' : '--bin', loadOnly ? 'perf_load' : 'tidemq', '--no-default-features', '--features', policy.allocator_feature, '-j2'];
  await command('cargo', args, {cwd: source, env: {...process.env, CARGO_TARGET_DIR: target},
    timeoutMs: 1800000, log: path.join(directory, 'build.log')});
  const name = loadOnly ? 'perf_load' : 'tidemq';
  await fs.copyFile(path.join(target, 'release', ...(loadOnly ? ['examples'] : []), name), path.join(directory, name));
  assert.equal(await sourceIdentity(original), commit, 'Tracked source changed during build');
  if (loadOnly) {
    for (const [file, hash] of Object.entries(loadOverlay.harness_files_sha256)) {
      assert.equal(sha256(await fs.readFile(path.join(source, file))), hash, 'Trusted harness changed during build');
    }
    const {original_source: _original, ...identity} = loadOverlay;
    const loadIdentity = {...identity, sdk_source_commit: commit, generator_allocator: 'system',
      cargo_lock_sha256: sha256(await fs.readFile(path.join(source, 'Cargo.lock'))),
      binary_sha256: sha256(await fs.readFile(path.join(directory, name)))};
    await save(path.join(directory, 'identity.json'), loadIdentity);
    return loadIdentity;
  }
  const identity = {
    source_commit: commit,
    cargo_lock_sha256: sha256(await fs.readFile(path.join(source, 'Cargo.lock'))),
    service_binary_sha256: sha256(await fs.readFile(path.join(directory, name))),
    allocator_feature: policy.allocator_feature,
    rustc: await command('rustc', ['--version']), host: `${os.platform()} ${os.arch()} ${os.release()}`,
    profile: 'release', tracked_source_clean: true,
  };
  await save(path.join(directory, 'identity.json'), identity);
  return identity;
}

function launch(binary, args, log, workerThreads) {
  const child = spawn(binary, args, {env: {PATH: process.env.PATH ?? '/usr/bin:/bin', TOKIO_WORKER_THREADS: String(workerThreads)},
    stdio: ['ignore', 'pipe', 'pipe']});
  OWNED.add(child);
  child.once('close', () => OWNED.delete(child));
  const output = createWriteStream(log, {flags: 'wx', mode: 0o600});
  let bytes = 0;
  let outputError;
  const collect = chunk => {
    bytes += chunk.length;
    if (bytes > LOG_LIMIT) { outputError = 'Service log exceeded 32 MiB'; child.kill('SIGKILL'); return; }
    if (!output.write(chunk)) { child.stdout.pause(); child.stderr.pause(); }
  };
  output.on('drain', () => { child.stdout.resume(); child.stderr.resume(); });
  output.on('error', error => { outputError = String(error); child.kill('SIGKILL'); });
  child.stdout.on('data', collect);
  child.stderr.on('data', collect);
  const done = new Promise(resolve => {
    child.once('error', error => { outputError = String(error); resolve({code: null, signal: null}); });
    child.once('close', (code, signal) => resolve({code, signal}));
  }).then(async result => { await new Promise(resolve => output.end(resolve)); return {...result, output_error: outputError ?? null, log_bytes: bytes}; });
  return {child, done};
}

export function management(node, suffix) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let timer;
    const finish = (error, body) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (error) reject(error);
      else resolve(body);
    };
    const request = http.get(`http://127.0.0.1:${8400 + node * 10}${suffix}`, {timeout: 1000}, response => {
      let bytes = 0;
      const chunks = [];
      response.on('data', chunk => {
        bytes += chunk.length;
        if (bytes > 262144) response.destroy(new Error('Management response exceeded 256 KiB'));
        else chunks.push(chunk);
      });
      response.once('error', finish);
      response.once('end', () => response.statusCode === 200
        ? finish(null, Buffer.concat(chunks).toString('utf8')) : finish(new Error(`Node ${node} is not ready`)));
    });
    timer = setTimeout(() => {
      const error = new Error('Management absolute deadline expired');
      request.destroy(error);
      finish(error);
    }, 1000);
    request.once('timeout', () => request.destroy(new Error('Management idle deadline expired')));
    request.once('error', finish);
    request.once('close', () => finish(new Error('Management request closed before completion')));
  });
}

async function readiness(services, nodes) {
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) {
    assert.ok(services.every(service => service.child.exitCode === null && service.child.signalCode === null),
      'An owned service exited before readiness');
    try {
      await Promise.all(nodes.map(node => management(node, '/readyz')));
      await sleep(1000);
      return;
    } catch { await sleep(100); }
  }
  throw new Error('All-node readiness deadline expired');
}

async function stop(services) {
  for (const service of services) service.child.kill('SIGTERM');
  return Promise.all(services.map(async service => {
    let forced = false;
    const timer = setTimeout(() => { forced = true; service.child.kill('SIGKILL'); }, 10000);
    const result = await service.done.finally(() => clearTimeout(timer));
    return {...result, forced};
  }));
}

async function trial(tools, serviceBuild, loadBinary, output, topology, policy, scenarios = ['throughput', 'latency']) {
  assert.ok(policy.topologies.includes(topology), 'Unknown performance topology');
  for (const scenario of scenarios) workloadFor(policy, scenario);
  await fs.mkdir(output, {mode: 0o700});
  const cluster = path.join(output, 'cluster');
  await command('bash', [path.join(tools, 'deploy/local/generate.sh'), topology, cluster]);
  const nodes = topology === 'rf1' ? [1] : [1, 2, 3];
  const binary = path.join(serviceBuild, 'tidemq');
  const identity = await json(path.join(serviceBuild, 'identity.json'));
  assert.equal(sha256(await fs.readFile(binary)), identity.service_binary_sha256, 'Fixed service binary changed');
  const services = [];
  const summaries = {};
  let failure;
  try {
    for (const node of nodes) {
      const config = path.join(cluster, `node-${node}.toml`);
      const text = await fs.readFile(config, 'utf8');
      assert.match(text, /role = 'combined'/, 'Each CI node must host all roles');
      assert.match(text, new RegExp(`replication_factor = ${nodes.length}\\n`), 'Replication factor mismatch');
      await command(binary, ['validate-config', '--config', config], {log: path.join(output, `node-${node}-validated.json`)});
      services.push(launch(binary, ['serve', '--config', config], path.join(output, `node-${node}.log`), policy.server_worker_threads));
    }
    await readiness(services, nodes);
    const common = ['--endpoint', '127.0.0.1:7410', '--server-name', 'node-1.local',
      '--ca', path.join(cluster, 'ca.pem'), '--token-file', path.join(cluster, 'access.token'),
      '--management', '127.0.0.1:8410', '--service-pid', String(services[0].child.pid), '--service-binary', binary,
      '--service-config', path.join(cluster, 'node-1.toml'), '--build-identity', path.join(serviceBuild, 'identity.json')];
    for (const scenario of scenarios) {
      const reportPath = path.join(output, `report-${scenario}.json`);
      const loadArgs = [...common, '--output', reportPath];
      for (const [key, value] of Object.entries(workloadFor(policy, scenario))) loadArgs.push(`--${key.replaceAll('_', '-')}`, String(value));
      await command(loadBinary, loadArgs, {timeoutMs: 240000, env: {PATH: process.env.PATH ?? '/usr/bin:/bin'}, log: path.join(output, `load-${scenario}.log`)});
      summaries[scenario] = summarizeReport(await json(reportPath), policy, identity.source_commit, scenario);
    }
  } catch (error) { failure = error; }
  for (const node of nodes) {
    try {
      await fs.writeFile(path.join(output, `node-${node}-metrics.txt`), await management(node, '/metrics'), {mode: 0o600});
      await fs.writeFile(path.join(output, `node-${node}-diagnostics.json`), await management(node, '/diagnostics'), {mode: 0o600});
    } catch (error) { failure ??= error; }
  }
  const exits = await stop(services);
  await save(path.join(output, 'execution.json'), {topology, node_count: nodes.length,
    replication_factor: nodes.length, service_worker_threads: policy.server_worker_threads, scenarios, exits,
    failure: failure?.message ?? null});
  if (failure) throw failure;
  assert.ok(exits.every(exit => exit.code === 0 && !exit.forced && !exit.output_error), 'An owned node did not drain cleanly');
  // Successful WAL directories are disposable; failed trials retain them for diagnosis.
  for (const node of nodes) await fs.rm(path.join(cluster, `data-${node}`), {recursive: true, force: true});
  return summaries;
}

async function prepareGate(settings) {
  const inputs = await gateInputs(settings);
  const {tools, baseline, candidate, policyBytes, policy} = inputs;
  const output = path.resolve(settings['--output']);
  await fs.mkdir(output, {mode: 0o700});
  const target = path.resolve(settings['--target'] ?? path.join(tools, 'target/performance-gate'));
  const builds = path.join(output, 'builds');
  const baselineOverlay = await overlay(baseline, tools, path.join(output, 'overlays/baseline'));
  const candidateOverlay = await overlay(candidate, tools, path.join(output, 'overlays/candidate'));
  const reused = canReuseBuilds(baselineOverlay, candidateOverlay);
  const baselineLoad = await build(path.join(output, 'overlays/baseline'), path.join(builds, 'baseline-load'), target, policy, baselineOverlay);
  const candidateLoad = reused
    ? await reuseBuild(path.join(builds, 'baseline-load'), path.join(builds, 'candidate-load'), 'perf_load', baselineLoad)
    : await build(path.join(output, 'overlays/candidate'), path.join(builds, 'candidate-load'), target, policy, candidateOverlay);
  const baselineIdentity = await build(baseline, path.join(builds, 'baseline'), target, policy);
  const candidateIdentity = reused
    ? await reuseBuild(path.join(builds, 'baseline'), path.join(builds, 'candidate'), 'tidemq', baselineIdentity)
    : await build(candidate, path.join(builds, 'candidate'), target, policy);
  assert.equal(baselineIdentity.source_commit, baselineOverlay.source_commit, 'Baseline build source differs from its snapshot');
  assert.equal(candidateIdentity.source_commit, candidateOverlay.source_commit, 'Candidate build source differs from its snapshot');
  assert.equal(await sourceIdentity(baseline), baselineOverlay.source_commit, 'Baseline source changed during build');
  assert.equal(await sourceIdentity(candidate), candidateOverlay.source_commit, 'Candidate source changed during build');
  assert.equal(await sourceIdentity(tools), inputs.toolsCommit, 'Trusted tools changed during build');
  const manifest = {schema_version: 1, baseline_manifest_sha256: sha256(policyBytes), trusted_tools_commit: inputs.toolsCommit,
    same_source_reused_builds: reused, build_environment: environment(),
    builds: {baseline: baselineIdentity, candidate: candidateIdentity, 'baseline-load': baselineLoad, 'candidate-load': candidateLoad}};
  const file = path.join(output, 'manifest.json');
  await save(file, manifest);
  return sha256(await fs.readFile(file));
}

async function regularFile(file) {
  assert.ok((await fs.lstat(file)).isFile(), `Prepared artifact is not a regular file: ${file}`);
  return fs.readFile(file);
}

async function verifyIdentity(inputs, prepared, name, expected) {
  const directory = path.join(prepared, 'builds', name);
  assert.ok((await fs.lstat(directory)).isDirectory(), 'Prepared build directory is not a real directory');
  const identity = JSON.parse(await regularFile(path.join(directory, 'identity.json')));
  assert.deepEqual(identity, expected, 'Prepared build identity differs from its manifest');
  const log = await fs.lstat(path.join(directory, 'build.log'));
  assert.ok(log.isFile() && log.size <= LOG_LIMIT, 'Prepared build log is not a bounded regular file');
  const source = name.startsWith('baseline') ? inputs.baseline : inputs.candidate;
  const commit = name.startsWith('baseline') ? inputs.baselineCommit : inputs.candidateCommit;
  assert.equal(identity.source_commit, commit, 'Prepared build source does not match the clean checkout');
  assert.equal(identity.cargo_lock_sha256, sha256(await fs.readFile(path.join(source, 'Cargo.lock'))), 'Prepared lockfile differs');
  const binary = BUILD_BINARIES[name];
  if (binary === 'perf_load') {
    assert.equal(identity.sdk_source_commit, commit, 'Prepared SDK source differs');
    assert.equal(identity.harness_source_commit, inputs.toolsCommit, 'Prepared harness source differs');
    assert.equal(identity.generator_allocator, 'system', 'Prepared load allocator differs');
    for (const file of HARNESS_FILES) {
      assert.equal(identity.harness_files_sha256?.[file], sha256(await fs.readFile(path.join(inputs.tools, file))),
        'Prepared protected harness differs');
    }
  } else {
    assert.equal(identity.allocator_feature, inputs.policy.allocator_feature, 'Prepared service allocator differs');
    assert.equal(identity.profile, 'release', 'Prepared service profile differs');
    assert.equal(identity.tracked_source_clean, true, 'Prepared service source was not clean');
  }
  assert.equal(sha256(await regularFile(path.join(directory, binary))),
    binary === 'tidemq' ? identity.service_binary_sha256 : identity.binary_sha256, 'Prepared binary digest differs');
  return identity;
}

export async function verifyPreparedBuilds(settings) {
  const inputs = await gateInputs(settings);
  const prepared = path.resolve(settings['--prepared']);
  assert.ok((await fs.lstat(prepared)).isDirectory(), 'Prepared artifact root is not a real directory');
  assert.ok((await fs.lstat(path.join(prepared, 'builds'))).isDirectory(), 'Prepared builds root is not a real directory');
  assert.match(settings['--manifest-sha256'] ?? '', /^[0-9a-f]{64}$/, 'An external prepared manifest digest is required');
  const bytes = await regularFile(path.join(prepared, 'manifest.json'));
  assert.equal(sha256(bytes), settings['--manifest-sha256'], 'Prepared manifest digest differs from the build job');
  const manifest = JSON.parse(bytes);
  assert.equal(manifest.schema_version, 1, 'Unsupported prepared build manifest');
  assert.equal(manifest.baseline_manifest_sha256, sha256(inputs.policyBytes), 'Prepared protected policy differs');
  assert.equal(manifest.trusted_tools_commit, inputs.toolsCommit, 'Prepared trusted tools source differs');
  assert.equal(manifest.build_environment.platform, os.platform(), 'Prepared platform differs from measurement');
  assert.equal(manifest.build_environment.arch, os.arch(), 'Prepared architecture differs from measurement');
  for (const name of Object.keys(BUILD_BINARIES)) await verifyIdentity(inputs, prepared, name, manifest.builds[name]);
  assert.equal(manifest.same_source_reused_builds,
    canReuseBuilds(manifest.builds['baseline-load'], manifest.builds['candidate-load']), 'Prepared reuse claim differs');
  if (manifest.same_source_reused_builds) {
    assert.deepEqual(manifest.builds.baseline, manifest.builds.candidate, 'Reused service identities differ');
    assert.deepEqual(manifest.builds['baseline-load'], manifest.builds['candidate-load'], 'Reused SDK workload identities differ');
  }
  // Artifact transport resets executable permissions. Restore only the four verified static files.
  for (const [name, binary] of Object.entries(BUILD_BINARIES)) await fs.chmod(path.join(prepared, 'builds', name, binary), 0o700);
  return {...inputs, prepared, manifest};
}

async function measureGate(settings, combined = false) {
  const {tools, policy, prepared, manifest} = await verifyPreparedBuilds(settings);
  const output = path.resolve(settings['--output']);
  if (!combined) await fs.mkdir(output, {mode: 0o700});
  const builds = path.join(prepared, 'builds');
  if (output !== prepared) {
    for (const name of Object.keys(BUILD_BINARIES)) {
      await fs.mkdir(path.join(output, 'builds', name), {recursive: true, mode: 0o700});
      for (const file of ['identity.json', 'build.log']) await fs.copyFile(path.join(builds, name, file), path.join(output, 'builds', name, file));
    }
    await fs.copyFile(path.join(prepared, 'manifest.json'), path.join(output, 'build-manifest.json'));
  }
  const reused = manifest.same_source_reused_builds;
  const summary = {
    schema_version: 4, status: 'running', baseline_manifest_sha256: manifest.baseline_manifest_sha256,
    prepared_manifest_sha256: settings['--manifest-sha256'], trusted_tools_commit: manifest.trusted_tools_commit,
    same_source_reused_builds: reused,
    policy, baseline: manifest.builds.baseline, candidate: manifest.builds.candidate,
    workload_binaries: {baseline: manifest.builds['baseline-load'], candidate: manifest.builds['candidate-load']},
    build_environment: manifest.build_environment,
    environment: {...environment(), same_host_paired_trials: true, physical_availability_zone_validation: false,
      compilation_performed_in_measurement_phase: false, fixed_sync_before_trials: true},
    limitations: ['Closed-loop finite regression workload; no production saturation or availability-zone claim',
      'An interval containing one means no detected regression, not proof of zero performance difference',
      'Latency means use every raw message duration, including scheduled-offer lateness and tail stalls',
      'P99 uses all observations merged in log2/32 upper-bound buckets with up to 3.125 percent bucket width',
      'Both sides use the protected workload source overlay; matching sources reuse the exact build artifacts',
      'Ten predeclared comparisons use 99.5 percent individual intervals for nominal 95 percent simultaneous Bonferroni coverage',
      'Student t intervals assume independent paired log ratios and the paired Student t model',
      'All five metrics veto statistically resolved slowdowns; only throughput and raw latency means require precision',
      'P99 intervals can remain imprecise in a passing run; this gate does not exclude small tail regressions',
      'The primary log half-width budget is uncertainty, not a permitted percentage slowdown'],
    topologies: {},
  };
  try {
    await command('sync', [], {timeoutMs: 60000});
    for (const topology of policy.topologies) {
      const pairs = [];
      await fs.mkdir(path.join(output, topology), {mode: 0o700});
      for (let pair = 0; pair < policy.pairs; pair++) {
        const values = {};
        const order = pair % 2 === 0 ? ['baseline', 'candidate'] : ['candidate', 'baseline'];
        for (const side of order) {
          const workloads = await trial(tools, path.join(builds, side), path.join(builds, `${side}-load/perf_load`),
            path.join(output, topology, `${pair}-${side}`), topology, policy);
          values[side] = {durable_messages_per_second: workloads.throughput.durable_messages_per_second,
            produce_mean_ns: workloads.latency.produce_mean_ns,
            produce_p99_upper_ns: workloads.latency.produce_p99_upper_ns,
            offered_to_durable_ack_mean_ns: workloads.latency.offered_to_durable_ack_mean_ns,
            offered_to_durable_ack_p99_upper_ns: workloads.latency.offered_to_durable_ack_p99_upper_ns, workloads};
          console.log(`${topology} pair ${pair + 1}/${policy.pairs} ${side}: ${values[side].durable_messages_per_second.toFixed(2)} durable messages/s`);
        }
        pairs.push({pair, order, ...values});
        summary.topologies[topology] = {status: 'running', pairs};
        await save(path.join(output, 'summary.json'), summary);
      }
      summary.topologies[topology] = {...comparePairs(pairs, policy, {sameBinary: sameActualBinaries(summary)}), pairs};
    }
    const statuses = Object.values(summary.topologies).map(topology => topology.status);
    summary.status = statuses.every(status => status === 'passed') ? 'passed' : statuses.includes('regressed') ? 'regressed'
      : statuses.includes('temporal_bias_detected') ? 'temporal_bias_detected' : 'inconclusive';
  } catch (error) { summary.status = 'failed'; summary.error = error.message; }
  await save(path.join(output, 'summary.json'), summary);
  console.log(`Performance gate: ${summary.status}; ${path.join(output, 'summary.json')}`);
  assert.equal(summary.status, 'passed',
    'Performance gate failed; retain this cohort and address the regression or measurement conditions before a new declared run');
}

async function runGate(settings) {
  const digest = await prepareGate(settings);
  await measureGate({...settings, '--prepared': settings['--output'], '--manifest-sha256': digest}, true);
}

async function promote(settings) {
  const tools = path.resolve(settings['--tools'] ?? DEFAULT_TOOLS);
  const bytes = await fs.readFile(policyPath(tools));
  const policy = validatePolicy(JSON.parse(bytes));
  const report = await json(path.resolve(settings['--report']));
  assert.equal(report.schema_version, 4, 'Unsupported performance gate report schema');
  assert.equal(report.status, 'passed', 'Only successful measurements may advance the baseline');
  assert.equal(report.baseline_manifest_sha256, sha256(bytes), 'The previous baseline changed; rerun before promotion');
  assert.equal(report.baseline.source_commit, policy.service_source_commit, 'Previous source does not match');
  assert.match(report.candidate.source_commit, /^[0-9a-f]{40}$/, 'Invalid candidate source');
  assert.equal(report.candidate.source_commit, await sourceIdentity(tools),
    'Promotion candidate must match the current clean tools HEAD');
  const sameBinary = sameActualBinaries(report);
  for (const topology of policy.topologies) {
    assert.equal(comparePairs(report.topologies[topology].pairs, policy, {sameBinary}).status, 'passed', 'Recomputed gate did not pass');
  }
  policy.service_source_commit = report.candidate.source_commit;
  await save(policyPath(tools), policy);
  await save(path.join(tools, 'performance/baseline-report.json'), report);
  console.log(`Advanced the measured baseline to ${policy.service_source_commit}`);
}

async function main() {
  assert.ok(Number(process.versions.node.split('.')[0]) >= 20, 'Node.js 20 or newer is required');
  process.umask(0o077);
  const [action, ...args] = process.argv.slice(2);
  const settings = options(args);
  if (action === 'source') {
    const policy = validatePolicy(await json(policyPath(path.resolve(settings['--tools'] ?? DEFAULT_TOOLS))));
    console.log(`source_commit=${policy.service_source_commit}`);
  } else if (action === 'prepare') console.log(`manifest_sha256=${await prepareGate(settings)}`);
  else if (action === 'measure') await measureGate(settings);
  else if (action === 'run') await runGate(settings);
  else if (action === 'promote') await promote(settings);
  else if (action === 'trial') {
    const tools = path.resolve(settings['--tools'] ?? DEFAULT_TOOLS);
    const build = path.resolve(settings['--build']);
    console.log(JSON.stringify(await trial(tools, build, path.resolve(settings['--load'] ?? path.join(build, 'perf_load')),
      path.resolve(settings['--output']), settings['--topology'] ?? 'rf1', validatePolicy(await json(policyPath(tools))),
      settings['--scenario'] ? [settings['--scenario']] : ['throughput', 'latency']), null, 2));
  } else throw new Error('Usage: performance-gate.mjs source|prepare|measure|run|trial|promote --tools directory [named paths]');
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => { console.error(error.message); process.exitCode = 1; });
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => {
    for (const child of OWNED) child.kill('SIGTERM');
    setTimeout(() => { for (const child of OWNED) child.kill('SIGKILL'); }, 10000).unref();
    process.exitCode = signal === 'SIGINT' ? 130 : 143;
  });
}
