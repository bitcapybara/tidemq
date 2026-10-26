import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import * as fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {roles} from '../lab/workload.mjs';

const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const cases = ['api-contract', 'smoke', 'replay', 'worker-poweroff', 'data-isolation', 'quorum-loss', 'control-plane-fault', 'object-store-fault', 'recovered'];
export const API_CHECKS = ['identity_catalog_pagination', 'namespace_topic_quota', 'scoped_authorization', 'token_revocation',
  'queue_nack_redelivery_ack', 'stream_cumulative_ack_resume', 'subscription_inspection_deletion', 'cluster_catalog', 'resource_deletion'];

export function validateApiCoverage(value, source, runId) {
  assert.equal(value?.schema_version, 1, 'Missing API coverage manifest');
  assert.equal(value.mode, 'kubernetes', 'Expected deployed Kubernetes API coverage');
  assert.equal(value.source_commit, source, 'API binaries belong to another source commit');
  assert.match(value.namespace ?? '', /^ci-[0-9]+-[0-9]+$/, 'Invalid API coverage namespace');
  if (runId) assert.equal(value.namespace.split('-')[1], runId, 'API profiles are from another CI run');
  assert.equal(value.build_profile, 'debug', 'API binaries must use the shared instrumented build');
  assert.equal(value.instrumented, true, 'API binaries were not instrumented');
  for (const name of ['tidemq', 'deploy_validation']) assert.match(value.binary_sha256?.[name] ?? '', /^[a-f0-9]{64}$/, 'Missing deployed binary digest');
  assert.match(value.image ?? '', /^localhost\/tidemq-lab:[a-f0-9]{24}$/, 'Missing deployed image');
  assert.deepEqual(value.nodes?.map(node => node.id).sort((a, b) => a - b), roles.map(node => node.id).sort((a, b) => a - b), 'Missing or duplicate API server profiles');
  for (const node of roles) {
    const measured = value.nodes.find(entry => entry.id === node.id);
    assert.equal(measured.role, node.role, 'API role differs from inventory');
    assert.equal(measured.worker, node.worker, 'API profile placement differs from inventory');
    assert.match(measured.pod_uid ?? '', /^[a-f0-9-]{36}$/, 'Missing deployed Pod identity');
    assert.match(measured.image_id ?? '', /sha256:[a-f0-9]{64}$/, 'Missing runtime image digest');
    assert.ok(measured.profiles?.length > 0, `Missing node-${node.id} profiles`);
  }
  assert.ok(value.client_profiles?.length > 0, 'Missing remote SDK/CLI profiles');
  const profiles = [...value.nodes.flatMap(node => node.profiles), ...value.client_profiles];
  const names = new Set();
  for (const profile of profiles) {
    assert.match(profile.file ?? '', /^tidemq-api-[a-z0-9._-]+\.profraw$/, 'Invalid API profile name');
    assert.ok(!names.has(profile.file), 'Duplicate API profile');
    names.add(profile.file);
    assert.ok(Number.isSafeInteger(profile.bytes) && profile.bytes > 0 && profile.bytes <= 16 * 1024 * 1024, 'Invalid API profile size');
    assert.match(profile.sha256 ?? '', /^[a-f0-9]{64}$/, 'Invalid API profile digest');
  }
  assert.deepEqual(Object.keys(value.test_reports ?? {}).sort(), [...cases].sort(), 'Missing API or VM fault results');
  for (const hash of Object.values(value.test_reports)) assert.match(hash, /^[a-f0-9]{64}$/, 'Invalid API result digest');
  assert.deepEqual(value.api_checks, API_CHECKS, 'Missing black-box API contract assertions');
  assert.equal(value.graceful_shutdown, true, 'Server profiles were collected before controlled shutdown');
  return profiles.length;
}

export function validateProfileListing(text, prefix) {
  const entries = text.trim().split('\n').filter(Boolean).map(line => {
    const [name, size] = line.split('\t');
    assert.ok(name?.startsWith(prefix) && /^[a-zA-Z0-9._-]+\.profraw$/.test(name), 'Invalid remote profile name');
    const bytes = Number(size);
    assert.ok(Number.isSafeInteger(bytes) && bytes > 0 && bytes <= 16 * 1024 * 1024, 'Invalid remote profile size');
    return {name, bytes};
  });
  assert.ok(entries.length > 0 && entries.length <= 64, 'Missing or excessive remote profiles');
  assert.equal(new Set(entries.map(entry => entry.name)).size, entries.length, 'Duplicate remote profile name');
  return entries;
}

function remote(worker, args, output) {
  return execFileSync('bash', ['-c', 'source scripts/lab/guest.sh; lab_ssh "$@"', '--', worker, ...args],
    {encoding: output === undefined ? 'utf8' : undefined, maxBuffer: 1024 * 1024, timeout: 30_000,
      stdio: output === undefined ? ['ignore', 'pipe', 'pipe'] : ['ignore', output, 'pipe']});
}

function collect(worker, directory, prefix, label, output, target) {
  const listing = remote(worker, ['sudo', 'find', directory, '-mindepth', '1', '-maxdepth', '1', '-type', 'f', '-name', '*.profraw', '-size', '+0c', '-printf', '%f\t%s\n']);
  return validateProfileListing(listing, prefix).map(entry => {
    const file = `tidemq-api-${label}-${entry.name}`;
    const destination = path.join(output, 'profiles', file);
    const fd = fs.openSync(destination, 'wx', 0o600);
    try { remote(worker, ['sudo', 'cat', '--', `${directory}/${entry.name}`], fd); }
    finally { fs.closeSync(fd); }
    assert.equal(fs.statSync(destination).size, entry.bytes, 'Remote profile changed during collection');
    fs.copyFileSync(destination, path.join(target, file), fs.constants.COPYFILE_EXCL);
    return {file, bytes: entry.bytes, sha256: sha(fs.readFileSync(destination))};
  });
}

function main() {
  const [namespace, build, report, target] = process.argv.slice(2);
  assert.match(namespace ?? '', /^ci-[0-9]+-[0-9]+$/, 'Expected a numbered CI namespace');
  assert.equal(process.env.CARGO_LLVM_COV_SHOW_ENV, '1', 'API collection requires the shared llvm-cov environment');
  const output = path.join(report, 'coverage');
  fs.mkdirSync(path.join(output, 'profiles'), {recursive: true, mode: 0o700});
  const text = name => fs.readFileSync(path.join(build, name), 'utf8').trim();
  const pods = JSON.parse(fs.readFileSync(path.join(report, 'pods-before-shutdown.json'))).items;
  const value = {schema_version: 1, mode: 'kubernetes', namespace, source_commit: text('source-commit.txt'),
    build_profile: text('profile.txt'), instrumented: true, image: text('image.txt'), image_id: text('image-id.txt'),
    binary_sha256: Object.fromEntries(['tidemq', 'deploy_validation'].map(name => [name, sha(fs.readFileSync(path.join(build, 'bin', name)))])),
    nodes: [], client_profiles: [], test_reports: {}, api_checks: [], graceful_shutdown: true};
  for (const node of roles) {
    const pod = pods.find(item => item.metadata.name === `node-${node.id}-0`);
    assert.equal(pod?.spec.nodeName, node.worker, 'Pod placement differs from profile source');
    const server = pod.spec.containers.find(container => container.name === 'tidemq');
    assert.equal(server.image, value.image, 'Deployed image differs from the instrumented build');
    assert.equal(server.env?.find(item => item.name === 'LLVM_PROFILE_FILE')?.value,
      `/coverage/node-${node.id}-%h-%p-%m.profraw`, 'Deployed process lacks its coverage destination');
    const profiles = collect(node.worker, `/var/lib/tidemq/coverage/${namespace}/node-${node.id}`, `node-${node.id}-`, `server-${node.id}`, output, target);
    value.nodes.push({id: node.id, role: node.role, worker: node.worker, pod_uid: pod.metadata.uid,
      image_id: pod.status.containerStatuses.find(container => container.name === 'tidemq')?.imageID, profiles});
  }
  value.client_profiles = collect('tidemq-loadgen', `/srv/tidemq/validation/${namespace}/profiles`, 'client-', 'client', output, target);
  for (const name of cases) {
    const bytes = fs.readFileSync(path.join(report, name, 'report.json'));
    const result = JSON.parse(bytes);
    assert.equal(result.status, 'passed', `API case ${name} failed`);
    assert.equal(result.client_close_completed, true, `API client ${name} did not close`);
    if (name === 'api-contract') value.api_checks = result.api_checks;
    value.test_reports[name] = sha(bytes);
  }
  const count = validateApiCoverage(value, value.source_commit, process.env.GITHUB_RUN_ID);
  fs.writeFileSync(path.join(output, 'manifest.json'), `${JSON.stringify(value, null, 2)}\n`, {mode: 0o600});
  fs.copyFileSync(path.join(output, 'manifest.json'), 'target/ci/api-coverage.json', fs.constants.COPYFILE_EXCL);
  console.log(`Collected ${count} deployed API profiles from all ${roles.length} server instances and the remote client`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) main();
