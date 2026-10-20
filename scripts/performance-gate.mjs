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
import {comparePairs, summarizeReport, validatePolicy} from './performance/compare.mjs';

const DEFAULT_TOOLS = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const LOG_LIMIT = 32 * 1024 * 1024;
const OWNED = new Set();
const policyPath = tools => path.join(tools, 'performance/baseline.json');
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const json = async file => JSON.parse(await fs.readFile(file, 'utf8'));
const save = async (file, value) => fs.writeFile(file, `${JSON.stringify(value, null, 2)}\n`, {mode: 0o600});

function options(values) {
  const result = {};
  while (values.length) {
    const key = values.shift();
    assert.match(key, /^--[a-z-]+$/, 'Invalid performance gate option');
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
  return settings.raw ? bytesOutput : output.trim();
}

async function sourceIdentity(source) {
  await command('git', ['diff', '--quiet'], {cwd: source});
  await command('git', ['diff', '--cached', '--quiet'], {cwd: source});
  return command('git', ['rev-parse', 'HEAD'], {cwd: source});
}

async function overlay(source, tools, directory, policy) {
  const commit = await sourceIdentity(source);
  const toolsCommit = await sourceIdentity(tools);
  const legacy = commit === policy.bootstrap.legacy_service_source_commit;
  const harnessCommit = legacy ? policy.bootstrap.legacy_harness_source_commit : toolsCommit;
  await fs.mkdir(directory, {recursive: true, mode: 0o700});
  const archive = `${directory}.tar`;
  await command('git', ['archive', '--format=tar', '--output', archive, commit], {cwd: source});
  await command('tar', ['-xf', archive, '-C', directory]);
  await fs.unlink(archive);
  const harnessHashes = {};
  for (const file of policy.bootstrap.harness_files) {
    const bytes = legacy ? await command('git', ['show', `${harnessCommit}:${file}`], {cwd: tools, raw: true})
      : await fs.readFile(path.join(tools, file));
    await fs.mkdir(path.dirname(path.join(directory, file)), {recursive: true});
    await fs.writeFile(path.join(directory, file), bytes);
    harnessHashes[file] = sha256(bytes);
  }
  return {original_source: source, source_commit: commit, harness_source_commit: harnessCommit,
    harness_files_sha256: harnessHashes, legacy_deadline_api_adapter: legacy};
}

async function build(source, directory, target, policy, loadOverlay) {
  await fs.mkdir(directory, {recursive: true, mode: 0o700});
  const loadOnly = loadOverlay !== undefined;
  const original = loadOnly ? loadOverlay.original_source : source;
  const commit = await sourceIdentity(original);
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
    return {...identity, sdk_source_commit: commit, generator_allocator: 'system',
      cargo_lock_sha256: sha256(await fs.readFile(path.join(source, 'Cargo.lock'))),
      binary_sha256: sha256(await fs.readFile(path.join(directory, name)))};
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

async function trial(tools, serviceBuild, loadBinary, output, topology, policy) {
  assert.ok(policy.topologies.includes(topology), 'Unknown performance topology');
  await fs.mkdir(output, {mode: 0o700});
  const cluster = path.join(output, 'cluster');
  await command('bash', [path.join(tools, 'deploy/local/generate.sh'), topology, cluster]);
  const nodes = topology === 'rf1' ? [1] : [1, 2, 3];
  const binary = path.join(serviceBuild, 'tidemq');
  const identity = await json(path.join(serviceBuild, 'identity.json'));
  assert.equal(sha256(await fs.readFile(binary)), identity.service_binary_sha256, 'Fixed service binary changed');
  const services = [];
  let report;
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
    const loadArgs = ['--endpoint', '127.0.0.1:7410', '--server-name', 'node-1.local',
      '--ca', path.join(cluster, 'ca.pem'), '--token-file', path.join(cluster, 'access.token'),
      '--management', '127.0.0.1:8410', '--service-pid', String(services[0].child.pid), '--service-binary', binary,
      '--service-config', path.join(cluster, 'node-1.toml'), '--build-identity', path.join(serviceBuild, 'identity.json'),
      '--output', path.join(output, 'report.json')];
    for (const [key, value] of Object.entries(policy.workload)) loadArgs.push(`--${key.replaceAll('_', '-')}`, String(value));
    await command(loadBinary, loadArgs, {timeoutMs: 240000, env: {PATH: process.env.PATH ?? '/usr/bin:/bin'}, log: path.join(output, 'load.log')});
    report = await json(path.join(output, 'report.json'));
    for (const node of nodes) {
      await fs.writeFile(path.join(output, `node-${node}-metrics.txt`), await management(node, '/metrics'), {mode: 0o600});
      await fs.writeFile(path.join(output, `node-${node}-diagnostics.json`), await management(node, '/diagnostics'), {mode: 0o600});
    }
  } catch (error) { failure = error; }
  const exits = await stop(services);
  await save(path.join(output, 'execution.json'), {topology, node_count: nodes.length,
    replication_factor: nodes.length, service_worker_threads: policy.server_worker_threads, exits,
    failure: failure?.message ?? null});
  if (failure) throw failure;
  assert.ok(exits.every(exit => exit.code === 0 && !exit.forced && !exit.output_error), 'An owned node did not drain cleanly');
  const summary = summarizeReport(report, policy, identity.source_commit);
  // Successful WAL directories are disposable; failed trials retain them for diagnosis.
  for (const node of nodes) await fs.rm(path.join(cluster, `data-${node}`), {recursive: true, force: true});
  return summary;
}

async function runGate(settings) {
  const tools = path.resolve(settings['--tools'] ?? DEFAULT_TOOLS);
  const baseline = path.resolve(settings['--baseline']);
  const candidate = path.resolve(settings['--candidate']);
  const output = path.resolve(settings['--output']);
  await fs.mkdir(output, {mode: 0o700});
  const policyBytes = await fs.readFile(policyPath(tools));
  const policy = validatePolicy(JSON.parse(policyBytes));
  if (settings['--require-baseline-unchanged']) {
    assert.deepEqual(await fs.readFile(policyPath(candidate)), policyBytes,
      'PRs cannot replace their protected performance baseline; successful main runs promote it');
  }
  assert.equal(await sourceIdentity(baseline), policy.service_source_commit, 'Baseline checkout is not the protected source');
  const target = path.resolve(settings['--target'] ?? path.join(tools, 'target/performance-gate'));
  const builds = path.join(output, 'builds');
  const baselineOverlay = await overlay(baseline, tools, path.join(output, 'overlays/baseline'), policy);
  const candidateOverlay = await overlay(candidate, tools, path.join(output, 'overlays/candidate'), policy);
  const baselineLoad = await build(path.join(output, 'overlays/baseline'), path.join(builds, 'baseline-load'), target, policy, baselineOverlay);
  const candidateLoad = await build(path.join(output, 'overlays/candidate'), path.join(builds, 'candidate-load'), target, policy, candidateOverlay);
  const baselineIdentity = await build(baseline, path.join(builds, 'baseline'), target, policy);
  const candidateIdentity = await build(candidate, path.join(builds, 'candidate'), target, policy);
  const summary = {
    schema_version: 1, status: 'running', baseline_manifest_sha256: sha256(policyBytes),
    policy, baseline: baselineIdentity, candidate: candidateIdentity,
    workload_binaries: {baseline: baselineLoad, candidate: candidateLoad},
    environment: {platform: os.platform(), arch: os.arch(), kernel: os.release(), node: process.version,
      cpu_model: os.cpus()[0]?.model ?? 'unknown', logical_cpus: os.cpus().length, total_memory_bytes: os.totalmem(),
      github_runner_image: process.env.ImageVersion ?? null, same_host_paired_trials: true,
      physical_availability_zone_validation: false},
    limitations: ['Closed-loop finite regression workload; no production saturation or availability-zone claim',
      'A stable interval containing one means no detected regression, not proof of zero performance difference',
      'P99 uses all observations merged in log2/32 upper-bound buckets with up to 3.125 percent bucket width',
      'Only exact original source 9fe9c95 uses the committed legacy deadline API adapter; subsequent sources share the protected harness',
      'Student t intervals assume independent paired log ratios; confidence is per metric, not simultaneous'],
    topologies: {},
  };
  try {
    for (const topology of policy.topologies) {
      const pairs = [];
      await fs.mkdir(path.join(output, topology), {mode: 0o700});
      for (let pair = 0; pair < policy.pairs; pair++) {
        const values = {};
        const order = pair % 2 === 0 ? ['baseline', 'candidate'] : ['candidate', 'baseline'];
        for (const side of order) {
          values[side] = await trial(tools, path.join(builds, side), path.join(builds, `${side}-load/perf_load`),
            path.join(output, topology, `${pair}-${side}`), topology, policy);
          console.log(`${topology} pair ${pair + 1}/${policy.pairs} ${side}: ${values[side].durable_messages_per_second.toFixed(2)} durable messages/s`);
        }
        pairs.push({pair, order, ...values});
        summary.topologies[topology] = {status: 'running', pairs};
        await save(path.join(output, 'summary.json'), summary);
      }
      summary.topologies[topology] = {...comparePairs(pairs, policy), pairs};
    }
    const statuses = Object.values(summary.topologies).map(topology => topology.status);
    summary.status = statuses.every(status => status === 'passed') ? 'passed' : statuses.includes('regressed') ? 'regressed' : 'inconclusive';
  } catch (error) { summary.status = 'failed'; summary.error = error.message; }
  await save(path.join(output, 'summary.json'), summary);
  console.log(`Performance gate: ${summary.status}; ${path.join(output, 'summary.json')}`);
  assert.equal(summary.status, 'passed', 'Performance gate failed; inspect the report and optimize or rerun an inconclusive measurement');
}

async function promote(settings) {
  const tools = path.resolve(settings['--tools'] ?? DEFAULT_TOOLS);
  const bytes = await fs.readFile(policyPath(tools));
  const policy = validatePolicy(JSON.parse(bytes));
  const report = await json(path.resolve(settings['--report']));
  assert.equal(report.status, 'passed', 'Only successful measurements may advance the baseline');
  assert.equal(report.baseline_manifest_sha256, sha256(bytes), 'The previous baseline changed; rerun before promotion');
  assert.equal(report.baseline.source_commit, policy.service_source_commit, 'Previous source does not match');
  assert.match(report.candidate.source_commit, /^[0-9a-f]{40}$/, 'Invalid candidate source');
  assert.equal(report.candidate.source_commit, await sourceIdentity(tools),
    'Promotion candidate must match the current clean tools HEAD');
  for (const topology of policy.topologies) {
    assert.equal(comparePairs(report.topologies[topology].pairs, policy).status, 'passed', 'Recomputed gate did not pass');
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
  } else if (action === 'run') await runGate(settings);
  else if (action === 'promote') await promote(settings);
  else if (action === 'trial') {
    const tools = path.resolve(settings['--tools'] ?? DEFAULT_TOOLS);
    const build = path.resolve(settings['--build']);
    console.log(JSON.stringify(await trial(tools, build, path.resolve(settings['--load'] ?? path.join(build, 'perf_load')),
      path.resolve(settings['--output']), settings['--topology'] ?? 'rf1', validatePolicy(await json(policyPath(tools)))), null, 2));
  } else throw new Error('Usage: performance-gate.mjs source|run|trial|promote --tools directory [named paths]');
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => { console.error(error.message); process.exitCode = 1; });
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => {
    for (const child of OWNED) child.kill('SIGTERM');
    setTimeout(() => { for (const child of OWNED) child.kill('SIGKILL'); }, 10000).unref();
    process.exitCode = signal === 'SIGINT' ? 130 : 143;
  });
}
