import fs from 'node:fs';
import path from 'node:path';
import { isIPv4 } from 'node:net';
import { pathToFileURL } from 'node:url';

const requireValue = (condition, message) => {
  if (!condition) throw new Error(message);
};
const xml = value => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('"', '&quot;').replaceAll("'", '&apos;');

export function validateInventory(inventory) {
  requireValue(inventory.schemaVersion === 1 && inventory.lab === 'tidemq-dev', 'Unsupported lab inventory');
  requireValue(inventory.stateDirectory === '/var/lib/tidemq-lab', 'Unexpected lab state directory');
  requireValue(inventory.nodes.length === 11, 'The baseline requires eleven virtual machines');
  requireValue(/^https:\/\//.test(inventory.image.url) && /^[a-f0-9]{64}$/.test(inventory.image.sha256), 'A verified HTTPS image is required');
  const identities = new Set();
  for (const node of inventory.nodes) {
    requireValue(/^tidemq-[a-z][a-z0-9-]+$/.test(node.name), 'Invalid virtual machine name');
    requireValue(Number.isInteger(node.vcpus) && node.vcpus >= 1 && node.vcpus <= 8, 'Invalid virtual CPU budget');
    requireValue(Number.isInteger(node.memoryMiB) && node.memoryMiB >= 2048, 'Invalid virtual memory budget');
    requireValue(Number.isInteger(node.rootGiB) && node.rootGiB >= 8, 'Invalid root disk budget');
    for (const [key, prefix] of [['managementIP', '192.168.240.'], ['dataIP', '192.168.241.']]) {
      requireValue(isIPv4(node[key]) && node[key].startsWith(prefix), 'Unexpected virtual machine network');
    }
    for (const value of [node.name, node.managementIP, node.dataIP, node.managementMAC, node.dataMAC]) {
      requireValue(!identities.has(value), 'Duplicate virtual machine identity');
      identities.add(value);
    }
    for (const disk of node.disks) {
      requireValue(/^[a-z]+$/.test(disk.name) && Number.isInteger(disk.sizeGiB) && disk.sizeGiB > 0, 'Invalid data disk');
      requireValue(/^\/(srv|var\/lib)\/tidemq(?:\/[a-z]+)?$/.test(disk.mount), 'Unexpected data mount');
    }
  }
  requireValue(inventory.nodes.filter(node => node.role === 'control-plane').length === 3, 'Three control planes are required');
  requireValue(inventory.nodes.filter(node => node.role === 'worker').length === 3, 'Three workers are required');
  return inventory;
}

export function resourceBudget(inventory) {
  return inventory.nodes.reduce((total, node) => ({
    memoryMiB: total.memoryMiB + node.memoryMiB,
    vcpus: total.vcpus + node.vcpus,
    diskGiB: total.diskGiB + node.rootGiB + node.disks.reduce((sum, disk) => sum + disk.sizeGiB, 0),
  }), { memoryMiB: 0, vcpus: 0, diskGiB: 0 });
}

export function networkXML(inventory, type) {
  const network = inventory.networks[type];
  const records = inventory.nodes.map(node => `    <host ip='${xml(type === 'management' ? node.managementIP : node.dataIP)}'><hostname>${xml(node.name)}</hostname></host>`);
  if (type === 'management') {
    for (const [name, endpoint] of Object.entries(inventory.endpoints)) {
      if (name === 'queue') continue;
      const node = inventory.nodes.find(candidate => candidate.role === endpoint.node);
      const address = name === 's3' ? node.dataIP : node.managementIP;
      records.push(`    <host ip='${xml(address)}'><hostname>${xml(endpoint.host)}</hostname></host>`);
    }
  }
  return `<network><name>${xml(network.name)}</name><bridge name='${xml(network.bridge)}' stp='on' delay='0'/>
  ${network.nat ? "<forward mode='nat'/>" : ''}
  <domain name='vm.lab.tidemq.test' localOnly='yes'/><dns>${records.join('\n')}</dns>
  <ip address='${xml(network.address)}' prefix='${network.prefix}'/>
</network>\n`;
}

export function networkConfig(node) {
  const entry = (name, mac, address, gateway) => `  ${name}:
    match: {macaddress: '${mac}'}
    set-name: ${name}
    dhcp4: false
    addresses: ['${address}/24']
    ${gateway ? "routes: [{to: default, via: 192.168.240.1}]\n    nameservers: {addresses: [192.168.240.1]}" : ''}`;
  return `version: 2\nethernets:\n${entry('mgmt0', node.managementMAC, node.managementIP, true)}\n${entry('data0', node.dataMAC, node.dataIP, false)}\n`;
}

export function cloudConfig(node, publicKey) {
  requireValue(/^ssh-ed25519 [A-Za-z0-9+/=]+(?: [^\r\n]+)?$/.test(publicKey), 'Expected one Ed25519 public key');
  const lines = ['#cloud-config', `hostname: ${node.name}`, 'manage_etc_hosts: true', 'ssh_pwauth: false', 'disable_root: true', 'growpart: {mode: auto, devices: [/]}', 'resize_rootfs: true', 'users:', '  - name: lab', '    groups: [adm, sudo]', '    sudo: ALL=(ALL) NOPASSWD:ALL', '    shell: /bin/bash', '    lock_passwd: true', '    ssh_authorized_keys:', `      - ${JSON.stringify(publicKey)}`, 'package_update: true', 'packages: [ca-certificates, curl, jq, openssl, qemu-guest-agent, socat, conntrack, sysstat, gpg]'];
  if (node.disks.length) {
    lines.push('fs_setup:');
    for (const disk of node.disks) lines.push(`  - {label: tmq-${disk.name}, filesystem: ext4, device: /dev/disk/by-id/virtio-tmq-${disk.name}, overwrite: false}`);
    lines.push('mounts:');
    for (const disk of node.disks) lines.push(`  - [/dev/disk/by-id/virtio-tmq-${disk.name}, ${disk.mount}, ext4, 'defaults,noatime', '0', '2']`);
  }
  lines.push('write_files:', '  - path: /etc/sysctl.d/90-tidemq-lab.conf', "    permissions: '0644'", '    content: |', '      net.ipv4.ip_forward=1', '      fs.inotify.max_user_instances=8192', '      fs.inotify.max_user_watches=1048576', '  - path: /etc/tidemq-lab-role', "    permissions: '0644'", `    content: '${node.role}'`, 'runcmd:', '  - [sysctl, --system]', '  - [systemctl, enable, --now, qemu-guest-agent]', '  - [install, -d, -m, "0755", /srv/tidemq]');
  return lines.join('\n') + '\n';
}

export function domainXML(inventory, node) {
  const state = inventory.stateDirectory;
  const directory = `${state}/images`;
  const disk = (source, device, format, serial) => `<disk type='file' device='disk'><driver name='qemu' type='${format}' cache='none' io='threads'/><source file='${xml(source)}'/><target dev='${device}' bus='virtio'/><serial>${serial}</serial></disk>`;
  const disks = node.disks.map((item, index) => disk(`${directory}/${node.name}-${item.name}.raw`, `vd${String.fromCharCode(98 + index)}`, 'raw', `tmq-${item.name}`));
  const interfaces = [['management', node.managementMAC], ['data', node.dataMAC]].map(([type, mac]) => `<interface type='network'><mac address='${mac}'/><source network='${inventory.networks[type].name}'/><model type='virtio'/></interface>`);
  return `<domain type='kvm'><name>${node.name}</name><description>Managed by TideMQ lab ${inventory.lab}</description>
  <memory unit='MiB'>${node.memoryMiB}</memory><currentMemory unit='MiB'>${node.memoryMiB}</currentMemory><vcpu>${node.vcpus}</vcpu>
  <os><type arch='x86_64' machine='q35'>hvm</type><boot dev='hd'/></os><features><acpi/><apic/></features><cpu mode='host-passthrough' check='none'/><clock offset='utc'/>
  <on_poweroff>destroy</on_poweroff><on_reboot>restart</on_reboot><on_crash>destroy</on_crash>
  <devices><emulator>/usr/bin/qemu-system-x86_64</emulator>
  ${disk(`${directory}/${node.name}-root.qcow2`, 'vda', 'qcow2', 'tmq-root')}
  ${disks.join('\n')}
  <disk type='file' device='cdrom'><driver name='qemu' type='raw'/><source file='${directory}/${node.name}-seed.iso'/><target dev='sda' bus='sata'/><readonly/></disk>
  ${interfaces.join('\n')}
  <serial type='pty'><target port='0'/></serial><console type='pty'><target type='serial' port='0'/></console>
  <channel type='unix'><target type='virtio' name='org.qemu.guest_agent.0'/></channel><rng model='virtio'><backend model='random'>/dev/urandom</backend></rng><memballoon model='none'/>
  </devices></domain>\n`;
}

function main() {
  const [inventoryFile, output, keyFile] = process.argv.slice(2);
  requireValue(inventoryFile && output && keyFile, 'Usage: node render.mjs inventory.json output-directory public-key-file');
  const inventory = validateInventory(JSON.parse(fs.readFileSync(inventoryFile, 'utf8')));
  const key = fs.readFileSync(keyFile, 'utf8').trim();
  fs.mkdirSync(output, { recursive: true, mode: 0o700 });
  for (const type of ['management', 'data']) fs.writeFileSync(path.join(output, `network-${type}.xml`), networkXML(inventory, type), { mode: 0o600 });
  for (const node of inventory.nodes) {
    for (const [suffix, content] of [['network.yaml', networkConfig(node)], ['user.yaml', cloudConfig(node, key)], ['domain.xml', domainXML(inventory, node)], ['meta.yaml', `instance-id: ${inventory.lab}-${node.name}\nlocal-hostname: ${node.name}\n`]]) {
      fs.writeFileSync(path.join(output, `${node.name}-${suffix}`), content, { mode: 0o600 });
    }
  }
  console.log(JSON.stringify(resourceBudget(inventory)));
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) main();
