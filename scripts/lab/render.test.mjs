import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { cloudConfig, domainXML, networkConfig, resourceBudget, validateInventory } from './render.mjs';

const inventory = JSON.parse(fs.readFileSync(new URL('../../deploy/lab/inventory.json', import.meta.url), 'utf8'));

test('the bounded lab preserves three fault zones and fixed resource headroom', () => {
  validateInventory(inventory);
  assert.deepEqual(resourceBudget(inventory), { memoryMiB: 38912, vcpus: 32, diskGiB: 242 });
  assert.equal(new Set(inventory.nodes.filter(node => node.role === 'worker').map(node => node.zone)).size, 3);
});

test('duplicate network identities and unsafe disk paths are rejected', () => {
  const duplicate = structuredClone(inventory);
  duplicate.nodes[1].managementIP = duplicate.nodes[0].managementIP;
  assert.throws(() => validateInventory(duplicate), /Duplicate/);
  const unsafe = structuredClone(inventory);
  unsafe.nodes.find(node => node.role === 'storage').disks[0].mount = '/etc';
  assert.throws(() => validateInventory(unsafe), /mount/);
});

test('guest data routes have no default route and persistent disks keep flushes', () => {
  const worker = inventory.nodes.find(node => node.role === 'worker');
  const network = networkConfig(worker);
  assert.equal((network.match(/to: default/g) ?? []).length, 1);
  assert.match(network, /set-name: data0/);
  const domain = domainXML(inventory, worker);
  assert.match(domain, /type='kvm'/);
  assert.match(domain, /cache='none'/);
  assert.doesNotMatch(domain, /cache='unsafe'/);
  assert.match(domain, /<memballoon model='none'/);
});

test('cloud configuration does not format the root disk or accept extra key lines', () => {
  const worker = inventory.nodes.find(node => node.role === 'worker');
  const config = cloudConfig(worker, 'ssh-ed25519 AAAA lab');
  assert.match(config, /device: \/dev\/disk\/by-id\/virtio-tmq-wal/);
  assert.doesNotMatch(config, /device: \/dev\/vda/);
  assert.match(config, /overwrite: false/);
  assert.throws(() => cloudConfig(worker, 'ssh-ed25519 AAAA lab\nssh-rsa BBBB'), /public key/);
});
