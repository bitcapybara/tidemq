import test from 'node:test';
import assert from 'node:assert/strict';
import {roles, runtimeConfig, workload, persistentVolume, sidecarConfig, queueService} from './workload.mjs';

const addresses = Object.fromEntries(roles.map((node, index) => [node.id, `10.96.1.${index + 1}`]));
test('RF3 storage and metadata quorums span separate guest kernels and failure zones', () => {
  for (const role of ['metadata', 'storage']) {
    const voters = roles.filter(node => node.role === role);
    assert.equal(new Set(voters.map(node => node.worker)).size, 3);
    assert.equal(new Set(voters.map(node => node.zone)).size, 3);
  }
});
test('only storage roles receive S3 configuration and only metadata receives bootstrap authority', () => {
  for (const node of roles) {
    const config = runtimeConfig(node, 'test-cluster', addresses, 'ci-test', 'https://192.168.241.34:36443');
    assert.equal(config.includes('[storage.objects]'), node.role === 'storage');
    assert.equal(config.includes('bootstrap_admin_token_file'), node.role === 'metadata');
    assert.equal(config.includes('[gateway]'), node.role === 'gateway');
  }
  assert.throws(() => runtimeConfig(roles[0], 'test', {}, 'ci-test', 'https://example.test:443'), /stable lab Service IP/);
});
test('persistent state uses named data disks and retained, prebound claims', () => {
  for (const node of roles) {
    const pod = workload(node, 'ci-test', 'localhost/tidemq-lab:0123456789ab');
    assert.equal(pod.spec.template.spec.automountServiceAccountToken, false);
    const pv = persistentVolume(node, 'ci-test');
    if (!pv) { assert.equal(pod.spec.volumeClaimTemplates, undefined); continue; }
    assert.equal(pv.spec.persistentVolumeReclaimPolicy, 'Retain');
    assert.equal(pv.spec.claimRef.name, `data-node-${node.id}-0`);
    assert.match(pv.spec.local.path, /^\/var\/lib\/tidemq\/(metadata|storage)\/ci-test\//);
    assert.equal(pod.spec.volumeClaimTemplates[0].spec.volumeName, pv.metadata.name);
  }
});
test('application exporters stay on loopback and the sidecar authenticates the central gateway', () => {
  const config = sidecarConfig(roles[0], 'ci-test');
  assert.equal(config.receivers.otlp.protocols.http.endpoint, '127.0.0.1:4318');
  assert.equal(config.exporters.otlp_http.tls.key_file, '/telemetry/client.key');
  assert.deepEqual(config.processors.resource.attributes.find(attribute => attribute.key === 'service.instance.id'),
    {key: 'service.instance.id', value: 'ci-test-node-11', action: 'upsert'});
  assert.ok(config.exporters.otlp_http.sending_queue.queue_size <= 128);
});
test('the UDP load balancer uses local gateways and distinct public and CI addresses', () => {
  const publicService = queueService('tidemq-dev').spec;
  const ciService = queueService('ci-test').spec;
  assert.equal(publicService.externalTrafficPolicy, 'Local');
  assert.equal(ciService.externalTrafficPolicy, 'Local');
  assert.equal(ciService.ports[0].protocol, 'UDP');
  assert.notEqual(publicService.loadBalancerIP, ciService.loadBalancerIP);
});
