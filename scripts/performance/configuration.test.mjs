import assert from 'node:assert/strict';
import path from 'node:path';
import test from 'node:test';
import {configurationEvidence, normalizeConfiguration, validateConfigurationEvidence} from './configuration.mjs';

function legacy(cluster, node = 1, nodes = [1, 2, 3]) {
  return {node_id: node, cluster_id: '12345678-1234-4234-8234-123456789012', role: 'combined',
    listen: `127.0.0.1:${7400 + node * 10}`, management_listen: `127.0.0.1:${8400 + node * 10}`,
    data_dir: path.join(cluster, `data-${node}`), replication_factor: nodes.length,
    memory: {total_bytes: 201326592, control_reserved_bytes: 33554432, max_connections: 128,
      max_streams_per_connection: 64, max_pending_requests: 256},
    protocol: {max_frame_bytes: 1048576, max_control_frame_bytes: 16384, max_message_bytes: 262144,
      max_batch_messages: 1024, max_payload_fields: 1100, max_traceparent_bytes: 55},
    logging: {filter: 'warn,tidemq=info', queue_capacity: 8192}, tracing: {enabled: false, endpoint: null},
    tls: {certificate: path.join(cluster, `node-${node}.pem`), private_key: path.join(cluster, `node-${node}.key`),
      trust_roots: path.join(cluster, 'ca.pem')},
    runtime: {internal_listen: `127.0.0.1:${7401 + node * 10}`, metadata_group: 1, metadata_voters: nodes,
      groups: [{group_id: 2, voters: nodes, standby: []}], bootstrap: true, objects: null,
      retention: {minimum_age_ms: 3600000, max_time_boundaries: 8192}, archive_interval_ms: 1000,
      capacity: {max_sample_age_ms: 1000, logical_backlog: {low: 100663296, high: 134217728},
        physical_storage: {low: 201326592, high: 268435456}, process_memory: {low: 201326592, high: 268435456},
        shared_reclaim_lag: {low: 50331648, high: 67108864}},
      access_file: path.join(cluster, 'access.toml'), delivery_key_file: path.join(cluster, 'delivery.key'),
      peers: nodes.map(peer => ({node_id: peer, role: 'combined', zone: `zone-${peer % 3}`,
        internal_address: `127.0.0.1:${7401 + peer * 10}`, server_name: `node-${peer}.local`,
        certificate_der: path.join(cluster, `node-${peer}.der`)}))}};
}

function roles(cluster, node, nodes) {
  const value = legacy(cluster, node, nodes);
  value.authorization = {access_file: value.runtime.access_file, delivery_key_file: value.runtime.delivery_key_file};
  value.gateway = {listen: value.listen};
  value.storage = {retention: value.runtime.retention, archive_interval_ms: value.runtime.archive_interval_ms};
  delete value.listen;
  for (const field of ['access_file', 'delivery_key_file', 'objects', 'retention', 'archive_interval_ms']) delete value.runtime[field];
  return value;
}

test('test-harness normalization permits only role layout and per-trial identity changes', () => {
  for (const nodes of [[1], [1, 2, 3]]) for (const node of nodes) {
    const old = normalizeConfiguration(legacy('/old', node, nodes), '/old', node, nodes);
    const proposed = roles('/new', node, nodes);
    proposed.cluster_id = '87654321-4321-4321-8321-210987654321';
    const next = normalizeConfiguration(proposed, '/new', node, nodes);
    assert.deepEqual(old, next);
    assert.deepEqual(configurationEvidence(old), configurationEvidence(next));
    assert.ok(!JSON.stringify(old).includes('/old/'));
  }
});

test('missing fields, mixed schemas, topology and resource changes fail before launch', () => {
  const mutations = [
    value => delete value.authorization.access_file,
    value => delete value.gateway.listen,
    value => delete value.storage.retention,
    value => {value.listen = '127.0.0.1:7410';},
    value => {value.runtime.access_file = '/other';},
    value => {value.runtime.unreviewed_budget = 100;},
    value => {value.storage.unreviewed_budget = 100;},
    value => {value.node_id = 2;},
    value => {value.replication_factor = 1;},
    value => {value.role = 'gateway';},
    value => {value.memory.total_bytes *= 2;},
    value => {value.protocol.max_message_bytes *= 2;},
    value => {value.runtime.metadata_voters = [1];},
    value => {value.runtime.groups[0].voters = [1];},
    value => value.runtime.peers.pop(),
    value => {value.tls.trust_roots = '/other/ca.pem';},
  ];
  for (const mutate of mutations) {
    const value = roles('/new');
    mutate(value);
    assert.throws(() => normalizeConfiguration(value, '/new', 1, [1, 2, 3]));
  }
});

test('effective defaults for quotas, retention, logging and tracing are retained for exact comparison', () => {
  const baseline = normalizeConfiguration(legacy('/old'), '/old', 1, [1, 2, 3]);
  for (const mutate of [
    value => {value.runtime.capacity.max_sample_age_ms++;},
    value => {value.storage.retention.minimum_age_ms++;},
    value => {value.storage.archive_interval_ms++;},
    value => {value.logging.queue_capacity++;},
    value => {value.tracing.enabled = true;},
    value => {value.storage.objects = {bucket: 'unexpected-object-backend'};},
  ]) {
    const value = roles('/new');
    mutate(value);
    assert.notDeepEqual(normalizeConfiguration(value, '/new', 1, [1, 2, 3]), baseline);
  }
});

test('promotion rechecks both frozen generator identities and every recorded effective budget', () => {
  const generators = {baseline: {source_commit: '1'.repeat(40), script_sha256: '2'.repeat(64)},
    candidate: {source_commit: '3'.repeat(40), script_sha256: '4'.repeat(64)}};
  const evidence = [1, 2, 3].map(node => configurationEvidence(normalizeConfiguration(legacy('/old', node), '/old', node, [1, 2, 3])));
  const report = {configuration_generators: generators, topologies: {'rf3-combined': {pairs: [{
    baseline: {workloads: {effective_configurations: evidence}}, candidate: {workloads: {effective_configurations: structuredClone(evidence)}}}]}}};
  validateConfigurationEvidence(report, generators, ['rf3-combined']);
  assert.throws(() => validateConfigurationEvidence(report, {...generators, baseline: generators.candidate}, ['rf3-combined']), /provenance/);
  for (const mutate of [
    value => value.pop(),
    value => {value[1].node_id = 1;},
    value => {value[0].configuration_sha256 = 'invalid';},
    value => {delete value[0].memory;},
    value => {value[0].archive_interval_ms = 1001;},
    value => {value[0].capacity.max_sample_age_ms++;},
  ]) {
    const changed = structuredClone(report);
    mutate(changed.topologies['rf3-combined'].pairs[0].candidate.workloads.effective_configurations);
    assert.throws(() => validateConfigurationEvidence(changed, generators, ['rf3-combined']));
  }
});
