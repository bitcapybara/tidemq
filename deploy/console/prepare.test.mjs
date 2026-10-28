import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import test, {after} from 'node:test';

import {prepare, validate} from './prepare.mjs';

const identity = fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-console-identity-'));
after(() => fs.rmSync(identity, {recursive: true, force: true}));
const certificateFile = path.join(identity, 'server.pem'), privateKeyFile = path.join(identity, 'server.key');
const issued = spawnSync('openssl', ['req', '-new', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256', '-nodes', '-days', '1', '-subj', '/CN=console.example.test', '-addext', 'subjectAltName=DNS:console.example.test,IP:127.0.0.1,IP:::1', '-keyout', privateKeyFile, '-out', certificateFile], {encoding: 'utf8'});
assert.equal(issued.status, 0, issued.stderr);
const base = {certificateFile, privateKeyFile, image: `registry.example.test/tidemq@sha256:${'a'.repeat(64)}`, publicOrigin: 'https://console.example.test', clusterAddress: 'mq.tidemq.svc:7443', clusterServerName: 'mq.tidemq.svc'};
const kubernetes = {...base, namespace: 'tidemq', privateSecret: 'console-private', tlsSecret: 'console-tls'};

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'tidemq-console-deploy-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  return root;
}

test('Kubernetes Console is stateless, has no platform token, and probes loopback by exec', t => {
  const root = fixture(t), output = path.join(root, 'kubernetes');
  prepare('kubernetes', kubernetes, output);
  const resources = JSON.parse(fs.readFileSync(path.join(output, 'resources.json')));
  assert.deepEqual(resources.items.map(item => item.kind), ['ConfigMap', 'Secret', 'Deployment', 'Service']);
  const deployment = resources.items.find(item => item.kind === 'Deployment');
  assert.equal(deployment.spec.replicas, 2);
  const pod = deployment.spec.template.spec;
  assert.equal(pod.automountServiceAccountToken, false);
  assert.equal(pod.securityContext.runAsUser, 10001);
  assert.equal(pod.securityContext.fsGroup, 10001);
  assert.ok(pod.volumes.every(volume => !volume.persistentVolumeClaim && !volume.hostPath));
  assert.equal(pod.volumes.find(volume => volume.secret).secret.defaultMode, 0o440);
  const server = pod.containers[0];
  assert.equal(server.securityContext.readOnlyRootFilesystem, true);
  assert.deepEqual(server.ports.map(port => port.containerPort), [8443]);
  for (const name of ['startupProbe', 'readinessProbe', 'livenessProbe']) {
    assert.ok(!server[name].httpGet);
    assert.match(server[name].exec.command.at(-1), /^http:\/\/127\.0\.0\.1:9420\/health\/(ready|live)$/);
  }
  const service = resources.items.find(item => item.kind === 'Service');
  assert.deepEqual(service.spec.ports, [{name: 'https', port: 443, targetPort: 'https'}]);
  const tls = resources.items.find(item => item.kind === 'Secret');
  assert.equal(tls.type, 'kubernetes.io/tls');
  assert.equal(Buffer.from(tls.data['tls.crt'], 'base64').toString(), fs.readFileSync(certificateFile, 'utf8'));
  assert.ok(pod.volumes.some(volume => volume.name === 'tls' && volume.secret.secretName === kubernetes.tlsSecret));
  const config = resources.items.find(item => item.kind === 'ConfigMap').data['console.toml'];
  assert.match(config, /^role = "console"$/m);
  assert.ok(!config.includes('node_id'));
  assert.ok(!config.includes('data_dir'));
  assert.ok(!config.includes('replication_factor'));
  if (process.env.TIDEMQ_BIN) {
    const file = path.join(root, 'console.toml'); fs.writeFileSync(file, config);
    const result = spawnSync(process.env.TIDEMQ_BIN, ['validate-config', '--config', file], {encoding: 'utf8'});
    assert.equal(result.status, 0, result.stderr);
  }
});

test('Compose exposes only a loopback page port and read-only files, with native HTTPS', t => {
  const root = fixture(t);
  const ca = path.join(root, 'ca.pem'), key = path.join(root, 'session.key');
  fs.writeFileSync(ca, 'CA placeholder'); fs.writeFileSync(key, Buffer.alloc(32, 1), {mode: 0o440});
  const settings = {...base, hostPort: 18080, trustRootsFile: ca, sessionKeyFile: key};
  const output = path.join(root, 'docker'); prepare('docker', settings, output);
  const document = JSON.parse(fs.readFileSync(path.join(output, 'compose.json')));
  const server = document.services.console;
  assert.equal(server.user, '10001:10001');
  assert.equal(server.read_only, true);
  assert.deepEqual(server.ports, ['127.0.0.1:18080:8443']);
  assert.ok(server.volumes.every(volume => volume.type === 'bind' && volume.read_only && !volume.bind.create_host_path));
  assert.ok(!JSON.stringify(document).includes('docker.sock'));
  assert.ok(!document.volumes);
  assert.equal(document.networks, undefined);
  assert.equal(fs.statSync(path.join(output, 'console.toml')).mode & 0o777, 0o644);
  assert.throws(() => prepare('docker', settings, output), /Output must be a new directory/);
  const overlay = path.join(root, 'overlay');
  prepare('docker', {...settings, networkService: 'network', clusterAddress: '127.0.0.1:7410'}, overlay);
  const shared = JSON.parse(fs.readFileSync(path.join(overlay, 'compose.json')));
  assert.equal(shared.services.console.network_mode, 'service:network');
  assert.equal(shared.services.console.ports, undefined);
  assert.deepEqual(shared.services.network.ports, ['127.0.0.1:18080:8443']);
  assert.deepEqual(shared.services.network.networks, ['console-ingress']);
  assert.deepEqual(shared.networks, {'console-ingress': {driver: 'bridge', internal: false}});
});

test('Compose configuration remains readable to UID 10001 under a private umask', t => {
  const root = fixture(t);
  const ca = path.join(root, 'ca.pem'), key = path.join(root, 'session.key');
  fs.writeFileSync(ca, 'CA placeholder', {mode: 0o600});
  fs.writeFileSync(key, Buffer.alloc(32, 1), {mode: 0o400});
  const output = path.join(root, 'private-docker');
  const settings = {...base, hostPort: 18080, trustRootsFile: ca, sessionKeyFile: key};
  const result = spawnSync(process.execPath, ['--input-type=module', '-e', `
    process.umask(0o077);
    const {prepare} = await import(process.argv[1]);
    prepare('docker', JSON.parse(process.argv[2]), process.argv[3]);
  `, new URL('./prepare.mjs', import.meta.url).href, JSON.stringify(settings), output], {encoding: 'utf8'});
  assert.equal(result.status, 0, result.stderr);
  const document = JSON.parse(fs.readFileSync(path.join(output, 'compose.json')));
  assert.equal(document.services.console.user, '10001:10001');
  assert.equal(fs.statSync(path.join(output, 'console.toml')).mode & 0o777, 0o644);
  assert.equal(fs.statSync(output).mode & 0o777, 0o700);
  assert.equal(fs.statSync(path.join(output, 'compose.json')).mode & 0o777, 0o600);
  assert.equal(fs.existsSync(path.join(output, 'nginx.conf')), false);
  assert.equal(fs.statSync(key).mode & 0o777, 0o400);
});

test('unsafe image, origin, upstream, and inactive settings fail before creating deployment output', t => {
  const root = fixture(t);
  for (const [index, patch] of [
    {image: 'example/tidemq:latest'}, {image: 'example/tidemq:main'},
    {publicOrigin: 'http://console.example.test'}, {publicOrigin: 'https://user:pass@console.example.test'},
    {publicOrigin: 'https://console.example.test/path'}, {publicOrigin: 'https://console.example.test:8443'},
    {clusterAddress: 'https://mq:7443'}, {clusterAddress: 'mq:0'}, {clusterAddress: 'mq:7443/path'},
    {clusterAddress: '0.0.0.0:7443'}, {clusterAddress: '[:::1]:7443'}, {clusterAddress: '[::]:7443'},
    {hostPort: 18080}, {privateSecret: 'invalid/name'}, {privateSecret: kubernetes.tlsSecret},
  ].entries()) {
    const output = path.join(root, `rejected-${index}`);
    assert.throws(() => prepare('kubernetes', {...kubernetes, ...patch}, output));
    assert.equal(fs.existsSync(output), false);
  }
  validate('kubernetes', {...kubernetes, image: `localhost/tidemq-lab:${'b'.repeat(24)}`});
});

test('shared verification keys and fixed observation sources render without credentials or platform access', t => {
  const root = fixture(t);
  const observability = {components: [{id: 'prometheus', name: 'Prometheus', probeUrl: 'http://prometheus:9090/-/ready', detailUrl: 'https://metrics.example.test/graph?g0.expr=up'}], alertsUrl: 'http://prometheus:9090/api/v1/alerts', alertsDetailUrl: 'https://metrics.example.test/alerts'};
  const settings = {...kubernetes, previousSessionKeyNames: ['session-old.key', 'session-older.key'], observability};
  const output = path.join(root, 'rotation-kubernetes');
  prepare('kubernetes', settings, output);
  const resources = JSON.parse(fs.readFileSync(path.join(output, 'resources.json')));
  const config = resources.items.find(item => item.kind === 'ConfigMap').data['console.toml'];
  assert.match(config, /previous_key_files = \["\/private\/session-old.key","\/private\/session-older.key"\]/);
  assert.match(config, /\[\[observability.components\]\]/);
  assert.match(config, /probe_url = "http:\/\/prometheus:9090\/-\/ready"/);
  assert.match(config, /alerts_url = "http:\/\/prometheus:9090\/api\/v1\/alerts"/);
  if (process.env.TIDEMQ_BIN) {
    const file = path.join(root, 'observability.toml'); fs.writeFileSync(file, config);
    const result = spawnSync(process.env.TIDEMQ_BIN, ['validate-config', '--config', file], {encoding: 'utf8'});
    assert.equal(result.status, 0, result.stderr);
  }
  const ca = path.join(root, 'ca.pem'), current = path.join(root, 'session.key'), old = path.join(root, 'old.key');
  fs.writeFileSync(ca, 'CA placeholder'); fs.writeFileSync(current, Buffer.alloc(32, 1), {mode: 0o440}); fs.writeFileSync(old, Buffer.alloc(32, 2), {mode: 0o440});
  const dockerOutput = path.join(root, 'rotation-docker');
  prepare('docker', {...base, hostPort: 18080, trustRootsFile: ca, sessionKeyFile: current, previousSessionKeyFiles: [old], observability}, dockerOutput);
  const compose = JSON.parse(fs.readFileSync(path.join(dockerOutput, 'compose.json')));
  assert.ok(compose.services.console.volumes.some(volume => volume.source === old && volume.target === '/private/session-previous-1.key' && volume.read_only));
  assert.match(fs.readFileSync(path.join(dockerOutput, 'console.toml'), 'utf8'), /previous_key_files = \["\/private\/session-previous-1.key"\]/);
  for (const patch of [
    {previousSessionKeyNames: ['session.key']}, {previousSessionKeyNames: ['ca.pem']}, {previousSessionKeyNames: ['../old.key']},
    {previousSessionKeyNames: ['old.key', 'old.key']}, {previousSessionKeyNames: ['one.key', 'two.key', 'three.key']},
    {observability: {...observability, alertsUrl: 'http://prometheus/api/v1/admin/tsdb/delete_series'}},
    {observability: {...observability, components: [{...observability.components[0], id: 'metadata'}]}},
    {observability: {...observability, components: [{...observability.components[0], probeUrl: 'http://user:secret@prometheus/-/ready'}]}},
    {observability: {...observability, components: [{...observability.components[0], probeUrl: 'http://prometheus/-/ready?token=secret'}]}},
    {observability: {...observability, components: [{...observability.components[0], detailUrl: 'http://metrics.example.test/'}]}},
    {observability: {alertsDetailUrl: 'https://metrics.example.test/alerts'}},
  ]) assert.throws(() => validate('kubernetes', {...settings, ...patch}));
  assert.throws(() => validate('docker', {...base, hostPort: 18080, trustRootsFile: ca, sessionKeyFile: current, previousSessionKeyFiles: [current]}));
});

test('the complete dependency example uses each deployed component health path', () => {
  const settings = JSON.parse(fs.readFileSync(new URL('./observability.example.json', import.meta.url)));
  const {observability} = settings;
  validate('kubernetes', {...kubernetes, ...settings});
  const paths = Object.fromEntries(observability.components.map(component => [component.id, new URL(component.probeUrl).pathname]));
  assert.deepEqual(paths, {
    'object-storage': '/health/ready', 'otel-collector': '/', 'otel-agent-node-1': '/', 'otel-agent-node-2': '/',
    prometheus: '/-/ready', loki: '/ready', tempo: '/ready', grafana: '/api/health', alertmanager: '/-/ready',
  });
  const agents = Array.from({length: 12}, (_, index) => ({id: `otel-agent-${index + 1}`, name: `OpenTelemetry agent ${index + 1}`, probeUrl: `http://otel-agent-${index + 1}:13133/`}));
  const dependencies = observability.components.filter(component => !component.id.startsWith('otel-agent-'));
  validate('kubernetes', {...kubernetes, observability: {...observability, components: [...dependencies, ...agents]}});
  assert.throws(() => validate('kubernetes', {...kubernetes, observability: {components: Array.from({length: 33}, (_, index) => ({id: `service-${index}`, name: 'Service', probeUrl: 'http://service/ready'}))}}));
});

test('fixed tracing endpoints render bounded OTLP export in both deployment platforms', t => {
  const root = fixture(t);
  const endpoint = 'https://192.0.2.10:4318/v1/traces';
  const ca = path.join(root, 'ca.pem'), key = path.join(root, 'session.key');
  fs.writeFileSync(ca, 'CA placeholder'); fs.writeFileSync(key, Buffer.alloc(32, 1), {mode: 0o440});
  for (const [platform, settings] of [
    ['kubernetes', kubernetes],
    ['docker', {...base, hostPort: 18080, trustRootsFile: ca, sessionKeyFile: key}],
  ]) {
    const output = path.join(root, platform);
    prepare(platform, {...settings, tracingEndpoint: endpoint}, output);
    const config = platform === 'docker'
      ? fs.readFileSync(path.join(output, 'console.toml'), 'utf8')
      : JSON.parse(fs.readFileSync(path.join(output, 'resources.json'))).items.find(item => item.kind === 'ConfigMap').data['console.toml'];
    assert.match(config, /\[tracing\]\nenabled = true\nendpoint = "https:\/\/192\.0\.2\.10:4318\/v1\/traces"\n/);
    for (const [name, value] of Object.entries({sample_per_mille: 10, spans_per_second: 128, queue_spans: 256, batch_spans: 32, export_interval_ms: 500, export_timeout_ms: 500})) {
      assert.match(config, new RegExp(`^${name} = ${value}$`, 'm'));
    }
    assert.ok(!config.includes('headers ='));
    if (process.env.TIDEMQ_BIN) {
      const file = path.join(root, `${platform}-tracing.toml`); fs.writeFileSync(file, config);
      const result = spawnSync(process.env.TIDEMQ_BIN, ['validate-config', '--config', file], {encoding: 'utf8'});
      assert.equal(result.status, 0, result.stderr);
    }
  }
});

test('tracing requires a fixed literal IP, protected scheme, exact path, and no URL secrets', t => {
  const root = fixture(t);
  for (const endpoint of ['https://192.0.2.10:4318/v1/traces', 'https://[2001:db8::1]:4318/v1/traces', 'http://127.42.1.2:4318/v1/traces', 'http://[::1]:4318/v1/traces']) {
    validate('kubernetes', {...kubernetes, tracingEndpoint: endpoint});
  }
  for (const [index, tracingEndpoint] of [
    null, false, 4318, '', 'not-a-url', 'https://collector:4318/v1/traces',
    'http://192.0.2.10:4318/v1/traces', 'http://[::ffff:127.0.0.1]:4318/v1/traces',
    'https://user:secret@192.0.2.10:4318/v1/traces', 'https://192.0.2.10:4318/other',
    'https://192.0.2.10:4318/v1/traces?token=secret', 'https://192.0.2.10:4318/v1/traces?',
    'https://192.0.2.10:4318/v1/traces#fragment', 'https://192.0.2.10:4318/v1/traces#',
    'https://192.0.2.10:65536/v1/traces', 'https://192.0.2.10:4318/v1/traces\n',
    `https://192.0.2.10:4318/${'x'.repeat(512)}`,
  ].entries()) {
    const output = path.join(root, `tracing-rejected-${index}`);
    assert.throws(() => prepare('kubernetes', {...kubernetes, tracingEndpoint}, output));
    assert.equal(fs.existsSync(output), false);
  }
  const output = path.join(root, 'disabled'); prepare('kubernetes', kubernetes, output);
  const config = JSON.parse(fs.readFileSync(path.join(output, 'resources.json'))).items.find(item => item.kind === 'ConfigMap').data['console.toml'];
  assert.ok(!config.includes('[tracing]'));
});

test('certificate SAN and key pair are validated before output is created', t => {
  const root = fixture(t);
  for (const patch of [{publicOrigin: 'https://other.example.test'}, {privateKeyFile: certificateFile}]) {
    const output = path.join(root, 'rejected');
    assert.throws(() => prepare('kubernetes', {...kubernetes, ...patch}, output));
    assert.equal(fs.existsSync(output), false);
  }
  for (const publicOrigin of ['https://127.0.0.1', 'https://[::1]']) validate('kubernetes', {...kubernetes, publicOrigin});
});
