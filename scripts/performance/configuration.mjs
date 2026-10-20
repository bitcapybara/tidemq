import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import path from 'node:path';

function keys(value, expected, label) {
  assert.ok(value && typeof value === 'object' && !Array.isArray(value), `Missing ${label}`);
  assert.deepEqual(Object.keys(value).sort(), [...expected].sort(), `Unexpected ${label} fields`);
}

export function normalizeConfiguration(configuration, cluster, node, nodes) {
  const value = structuredClone(configuration);
  // Only the comparison harness maps historical layouts; production accepts the current schema.
  if (value.authorization !== undefined) {
    keys(value.authorization, ['access_file', 'delivery_key_file'], 'authorization');
    keys(value.gateway, ['listen'], 'gateway');
    assert.ok(value.storage, 'Missing combined storage configuration');
    keys(value.storage, value.storage.objects === undefined ? ['retention', 'archive_interval_ms']
      : ['objects', 'retention', 'archive_interval_ms'], 'storage');
    assert.ok(!('listen' in value), 'Mixed configuration layouts');
    const oldFields = ['access_file', 'delivery_key_file', 'objects', 'retention', 'archive_interval_ms'];
    assert.ok(oldFields.every(field => !(field in value.runtime)), 'Mixed runtime configuration layouts');
    value.listen = value.gateway.listen;
    Object.assign(value.runtime, value.authorization, {objects: value.storage.objects ?? null,
      retention: value.storage.retention, archive_interval_ms: value.storage.archive_interval_ms});
    delete value.authorization;
    delete value.gateway;
    delete value.storage;
  }
  keys(value, ['node_id', 'cluster_id', 'role', 'listen', 'management_listen', 'data_dir', 'replication_factor',
    'memory', 'protocol', 'logging', 'tracing', 'tls', 'runtime'], 'effective node configuration');
  keys(value.runtime, ['internal_listen', 'metadata_group', 'metadata_voters', 'groups', 'peers', 'bootstrap',
    'access_file', 'delivery_key_file', 'objects', 'retention', 'archive_interval_ms', 'capacity'], 'effective runtime');
  assert.equal(value.node_id, node, 'Node identity changed');
  assert.match(value.cluster_id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-8[0-9a-f]{3}-[0-9a-f]{12}$/, 'Invalid generated cluster identity');
  assert.equal(value.role, 'combined', 'Each CI node must host all roles');
  assert.equal(value.replication_factor, nodes.length, 'Replication factor changed');
  assert.equal(value.listen, `127.0.0.1:${7400 + node * 10}`, 'External listener changed');
  assert.equal(value.management_listen, `127.0.0.1:${8400 + node * 10}`, 'Management listener changed');
  assert.equal(value.data_dir, path.join(cluster, `data-${node}`), 'Node data directory changed');
  assert.deepEqual(value.memory, {total_bytes: 201326592, control_reserved_bytes: 33554432,
    max_connections: 128, max_streams_per_connection: 64, max_pending_requests: 256}, 'Memory resource budget changed');
  assert.deepEqual(value.protocol, {max_frame_bytes: 1048576, max_control_frame_bytes: 16384,
    max_message_bytes: 262144, max_batch_messages: 1024, max_payload_fields: 1100, max_traceparent_bytes: 55}, 'Protocol budget changed');
  assert.deepEqual(value.tls, {certificate: path.join(cluster, `node-${node}.pem`),
    private_key: path.join(cluster, `node-${node}.key`), trust_roots: path.join(cluster, 'ca.pem')}, 'TLS topology changed');
  assert.equal(value.runtime.internal_listen, `127.0.0.1:${7401 + node * 10}`, 'Internal listener changed');
  assert.equal(value.runtime.metadata_group, 1, 'Metadata group changed');
  assert.equal(value.runtime.bootstrap, true, 'Bootstrap policy changed');
  assert.deepEqual(value.runtime.metadata_voters, nodes, 'Metadata quorum changed');
  assert.deepEqual(value.runtime.groups, [{group_id: 2, voters: nodes, standby: []}], 'Data quorum changed');
  assert.deepEqual(value.runtime.peers, nodes.map(peer => ({node_id: peer, role: 'combined', zone: `zone-${peer % 3}`,
    internal_address: `127.0.0.1:${7401 + peer * 10}`, server_name: `node-${peer}.local`,
    certificate_der: path.join(cluster, `node-${peer}.der`)})), 'Peer topology changed');
  assert.equal(value.runtime.access_file, path.join(cluster, 'access.toml'), 'Authorization source changed');
  assert.equal(value.runtime.delivery_key_file, path.join(cluster, 'delivery.key'), 'Delivery signing source changed');
  const mask = item => {
    if (item === value.cluster_id) return '<cluster-id>';
    if (typeof item === 'string' && item.startsWith(`${cluster}${path.sep}`)) return `<cluster>/${path.relative(cluster, item)}`;
    if (Array.isArray(item)) return item.map(mask);
    if (item && typeof item === 'object') return Object.fromEntries(Object.keys(item).sort().map(key => [key, mask(item[key])]));
    return item;
  };
  return mask(value);
}

export function configurationEvidence(value) {
  return {node_id: value.node_id, configuration_sha256: createHash('sha256').update(JSON.stringify(value)).digest('hex'),
    memory: value.memory, protocol: value.protocol, capacity: value.runtime.capacity,
    retention: value.runtime.retention, archive_interval_ms: value.runtime.archive_interval_ms};
}

export function validateConfigurationEvidence(report, expectedGenerators, topologies) {
  assert.deepEqual(report.configuration_generators, expectedGenerators, 'Configuration generator provenance changed');
  for (const topology of topologies) {
    const nodes = topology === 'rf1' ? [1] : [1, 2, 3];
    let expected;
    for (const pair of report.topologies[topology].pairs) {
      for (const side of ['baseline', 'candidate']) {
        const actual = pair[side].workloads?.effective_configurations;
        assert.ok(Array.isArray(actual), 'Missing effective configuration evidence');
        assert.deepEqual(actual.map(item => item.node_id), nodes, 'Configuration evidence node count changed');
        for (const item of actual) {
          keys(item, ['node_id', 'configuration_sha256', 'memory', 'protocol', 'capacity', 'retention', 'archive_interval_ms'], 'configuration evidence');
          assert.match(item.configuration_sha256 ?? '', /^[0-9a-f]{64}$/, 'Invalid effective configuration digest');
          for (const key of ['memory', 'protocol', 'capacity', 'retention']) assert.ok(item[key] && typeof item[key] === 'object', `Missing ${key} evidence`);
          assert.equal(item.archive_interval_ms, 1000, 'Archive interval changed');
        }
        expected ??= actual;
        assert.deepEqual(actual, expected, 'Effective configuration evidence changed across paired trials');
      }
    }
  }
}
