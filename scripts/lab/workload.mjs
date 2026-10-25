import fs from 'node:fs';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {pathToFileURL} from 'node:url';

export const roles = [
  [11, 'metadata', 'a', 'zone-1'], [12, 'metadata', 'b', 'zone-2'], [13, 'metadata', 'c', 'zone-0'],
  [21, 'storage', 'a', 'zone-1'], [22, 'storage', 'b', 'zone-2'], [23, 'storage', 'c', 'zone-0'],
  [31, 'broker', 'a', 'zone-1'], [32, 'broker', 'b', 'zone-2'],
  [41, 'gateway', 'a', 'zone-1'], [42, 'gateway', 'b', 'zone-2'],
].map(([id, role, worker, zone]) => ({id, role, worker: `tidemq-worker-${worker}`, zone}));
const toml = value => JSON.stringify(value);
const resource = (apiVersion, kind, name, spec) => ({apiVersion, kind, metadata: {name}, spec});

export function runtimeConfig(node, cluster, addresses, namespace, endpoint) {
  const lines = [`node_id = ${node.id}`, `cluster_id = ${toml(cluster)}`, `role = ${toml(node.role)}`,
    'management_listen = "127.0.0.1:9419"', 'data_dir = "/var/lib/tidemq"', 'replication_factor = 3',
    '[memory]', 'total_bytes = 201326592', 'control_reserved_bytes = 33554432',
    '[logging]', 'filter = "warn,tidemq=info"',
    '[tracing]', 'enabled = true', 'sample_per_mille = 1000', 'spans_per_second = 16',
    'queue_spans = 128', 'batch_spans = 1', 'export_interval_ms = 100', 'export_timeout_ms = 500',
    '[tls]', `certificate = "/private/node-${node.id}.pem"`, `private_key = "/private/node-${node.id}.key"`,
    'trust_roots = "/private/ca.pem"', '[authorization]'];
  if (node.role === 'metadata') lines.push('bootstrap_admin_token_file = "/private/bootstrap.token"');
  if (node.role === 'gateway') lines.push('[gateway]', 'listen = "0.0.0.0:7443"');
  if (node.role === 'storage') lines.push('[storage]', 'archive_interval_ms = 1000', '[storage.objects]',
    `endpoint = ${toml(endpoint)}`, 'region = "us-east-1"', `bucket = ${toml(namespace === 'tidemq-dev' ? 'tidemq-dev' : 'tidemq-ci')}`,
    `prefix = ${toml(namespace)}`, 'credentials_file = "/private/objects.toml"', 'allow_http_loopback = false');
  lines.push('[runtime]', 'internal_listen = "0.0.0.0:7444"', 'metadata_group = 1',
    'metadata_voters = [11,12,13]', 'bootstrap = true', '[[runtime.groups]]', 'group_id = 2', 'voters = [21,22,23]');
  for (const peer of roles) {
    if (!/^10\.96\.[0-9]+\.[0-9]+$/.test(addresses[peer.id] ?? '')) throw new Error('Every peer requires a stable lab Service IP');
    lines.push('[[runtime.peers]]', `node_id = ${peer.id}`, `role = ${toml(peer.role)}`, `zone = ${toml(peer.zone)}`,
      `internal_address = "${addresses[peer.id]}:7444"`, `server_name = "node-${peer.id}.local"`,
      `certificate_der = "/private/node-${peer.id}.der"`);
  }
  return lines.join('\n') + '\n';
}

export function sidecarConfig(node, namespace) {
  return {
    receivers: {otlp: {protocols: {http: {endpoint: '127.0.0.1:4318', max_request_body_size: 65536}}},
      prometheus: {config: {scrape_configs: [{job_name: 'tidemq', scrape_interval: '15s', static_configs: [{targets: ['127.0.0.1:9419']}]}]}}},
    processors: {memory_limiter: {check_interval: '1s', limit_mib: 96, spike_limit_mib: 24},
      resource: {attributes: [
        {key: 'service.name', value: 'tidemq', action: 'upsert'},
        {key: 'service.namespace', value: 'tidemq-lab', action: 'upsert'},
        {key: 'service.instance.id', value: `${namespace}-node-${node.id}`, action: 'upsert'},
        {key: 'k8s.namespace.name', value: namespace, action: 'upsert'},
        {key: 'tidemq.node.id', value: `${node.id}`, action: 'upsert'},
        {key: 'ci.run.id', value: namespace, action: 'upsert'},
      ]}, batch: {send_batch_size: 128, send_batch_max_size: 256, timeout: '1s'}},
    exporters: {otlp_http: {endpoint: 'https://otlp.lab.tidemq.test:4318',
      tls: {ca_file: '/telemetry/ca.pem', cert_file: '/telemetry/client.pem', key_file: '/telemetry/client.key'},
      sending_queue: {enabled: true, queue_size: 128}, retry_on_failure: {enabled: true, max_elapsed_time: '1m'}}},
    service: {telemetry: {logs: {level: 'warn', encoding: 'json'}, metrics: {level: 'none'}},
      pipelines: {traces: {receivers: ['otlp'], processors: ['memory_limiter', 'resource', 'batch'], exporters: ['otlp_http']},
        metrics: {receivers: ['prometheus'], processors: ['memory_limiter', 'resource', 'batch'], exporters: ['otlp_http']}}},
  };
}

export function workload(node, namespace, image) {
  const name = `node-${node.id}`;
  const persistent = ['metadata', 'storage'].includes(node.role);
  const labels = {app: 'tidemq', role: node.role, 'tidemq-node': name};
  const security = {runAsUser: 10001, runAsGroup: 10001, runAsNonRoot: true, readOnlyRootFilesystem: true,
    allowPrivilegeEscalation: false, capabilities: {drop: ['ALL']}};
  const pod = {
    nodeSelector: {'kubernetes.io/hostname': node.worker}, automountServiceAccountToken: false,
    terminationGracePeriodSeconds: 30, securityContext: {seccompProfile: {type: 'RuntimeDefault'}},
    initContainers: [{name: 'initialize', image, imagePullPolicy: 'Never', command: ['/bin/sh', '-ec'],
      args: ['umask 077; cp /seed/* /private/; chmod 600 /private/*; chown -R 10001:10001 /private; chmod 700 /private; chown 10001:10001 /var/lib/tidemq; chmod 700 /var/lib/tidemq'],
      securityContext: {runAsUser: 0, runAsNonRoot: false, readOnlyRootFilesystem: true, allowPrivilegeEscalation: false,
        capabilities: {drop: ['ALL'], add: ['CHOWN', 'FOWNER', 'DAC_OVERRIDE']}},
      volumeMounts: [{name: 'seed', mountPath: '/seed', readOnly: true}, {name: 'private', mountPath: '/private'}, {name: 'data', mountPath: '/var/lib/tidemq'}]}],
    containers: [
      {name: 'tidemq', image, imagePullPolicy: 'Never', command: ['/usr/local/bin/tidemq', 'serve', '--config', '/config/config.toml'],
        env: [{name: 'TOKIO_WORKER_THREADS', value: '2'}], securityContext: security,
        resources: {requests: {cpu: '100m', memory: '256Mi'}, limits: {cpu: '2', memory: '512Mi'}},
        ports: [{name: 'peer', containerPort: 7444, protocol: 'UDP'}, ...(node.role === 'gateway' ? [{name: 'quic', containerPort: 7443, protocol: 'UDP'}] : [])],
        readinessProbe: {exec: {command: ['curl', '--fail', '--silent', '--max-time', '1', 'http://127.0.0.1:9419/readyz']}, periodSeconds: 2, timeoutSeconds: 2},
        volumeMounts: [{name: 'config', mountPath: '/config', readOnly: true}, {name: 'private', mountPath: '/private', readOnly: true}, {name: 'data', mountPath: '/var/lib/tidemq'}, {name: 'tmp', mountPath: '/tmp'}]},
      {name: 'otelcol', image, imagePullPolicy: 'Never', command: ['/usr/local/bin/otelcol-contrib', '--config=/config/collector.json'],
        env: [{name: 'GOMEMLIMIT', value: '128MiB'}, {name: 'GOMAXPROCS', value: '1'}], securityContext: security,
        resources: {requests: {cpu: '25m', memory: '64Mi'}, limits: {cpu: '500m', memory: '192Mi'}},
        volumeMounts: [{name: 'config', mountPath: '/config', readOnly: true}, {name: 'telemetry', mountPath: '/telemetry', readOnly: true}]},
    ],
    volumes: [{name: 'config', configMap: {name}}, {name: 'seed', secret: {secretName: name, defaultMode: 256}},
      {name: 'private', emptyDir: {medium: 'Memory', sizeLimit: '1Mi'}}, {name: 'tmp', emptyDir: {medium: 'Memory', sizeLimit: '16Mi'}},
      {name: 'telemetry', secret: {secretName: 'telemetry', defaultMode: 292}}],
  };
  const spec = {serviceName: name, replicas: 1, selector: {matchLabels: {'tidemq-node': name}},
    updateStrategy: {type: 'OnDelete'}, persistentVolumeClaimRetentionPolicy: {whenDeleted: 'Retain', whenScaled: 'Retain'},
    template: {metadata: {labels}, spec: pod}};
  if (persistent) {
    const size = node.role === 'storage' ? (namespace === 'tidemq-dev' ? '4Gi' : '1Gi') : '512Mi';
    spec.volumeClaimTemplates = [{metadata: {name: 'data'}, spec: {accessModes: ['ReadWriteOnce'], storageClassName: 'tidemq-local',
      volumeName: `${namespace}-${name}`, resources: {requests: {storage: size}}}}];
  } else pod.volumes.push({name: 'data', emptyDir: {sizeLimit: '64Mi'}});
  return resource('apps/v1', 'StatefulSet', name, spec);
}

export function persistentVolume(node, namespace) {
  if (!['storage', 'metadata'].includes(node.role)) return null;
  return resource('v1', 'PersistentVolume', `${namespace}-node-${node.id}`, {
    capacity: {storage: node.role === 'storage' ? (namespace === 'tidemq-dev' ? '4Gi' : '1Gi') : '512Mi'}, volumeMode: 'Filesystem', accessModes: ['ReadWriteOnce'],
    persistentVolumeReclaimPolicy: 'Retain', storageClassName: 'tidemq-local',
    claimRef: {namespace, name: `data-node-${node.id}-0`},
    local: {path: `/var/lib/tidemq/${node.role}/${namespace}/node-${node.id}`},
    nodeAffinity: {required: {nodeSelectorTerms: [{matchExpressions: [{key: 'kubernetes.io/hostname', operator: 'In', values: [node.worker]}]}]}},
  });
}

export function queueService(namespace) {
  const publicVIP = namespace === 'tidemq-dev' ? '192.168.241.200' : '192.168.241.201';
  // Keep QUIC traffic on the announcing gateway node instead of another node's stale UDP backend.
  return resource('v1', 'Service', 'mq', {type: 'LoadBalancer', loadBalancerIP: publicVIP,
    selector: {app: 'tidemq', role: 'gateway'}, externalTrafficPolicy: 'Local', ports: [{name: 'quic', port: 7443, protocol: 'UDP'}]});
}

function run(command, args, input) {
  const result = spawnSync(command, args, {input, encoding: 'utf8', maxBuffer: 16 * 1024 * 1024});
  if (result.error || result.status !== 0) throw new Error(`${command} failed: ${result.error?.message ?? result.stderr}`);
  return result.stdout;
}

function main() {
  const [context, namespace, image, endpoint, credentials, output] = process.argv.slice(2);
  if (!context || !/^[a-z][a-z0-9-]{0,48}[a-z0-9]$/.test(namespace ?? '') || !/^localhost\/tidemq-lab:[a-f0-9]{12,64}$/.test(image ?? '') ||
      !/^https:\/\/[a-z0-9.-]+:[0-9]+$/.test(endpoint ?? '') || !credentials || !output || fs.existsSync(output)) {
    throw new Error('Usage: workload.mjs context namespace localhost/tidemq-lab:hash https-endpoint credentials-file new-private-directory');
  }
  const home = process.env.TIDEMQ_LAB_HOME;
  if (!home) throw new Error('TIDEMQ_LAB_HOME is required');
  fs.mkdirSync(output, {recursive: true, mode: 0o700});
  const material = path.resolve(output, 'material');
  const kube = args => run('kubectl', ['--context', context, '--namespace', namespace, ...args]);
  const apply = items => run('kubectl', ['--context', context, 'apply', '-f', '-'], JSON.stringify({apiVersion: 'v1', kind: 'List', items}));
  kube(['create', 'namespace', namespace]);
  const services = roles.map(node => resource('v1', 'Service', `node-${node.id}`, {publishNotReadyAddresses: true,
    selector: {'tidemq-node': `node-${node.id}`}, ports: [{name: 'peer', port: 7444, protocol: 'UDP'}]}));
  const publicVIP = namespace === 'tidemq-dev' ? '192.168.241.200' : '192.168.241.201';
  services.push(queueService(namespace));
  for (const service of services) service.metadata.namespace = namespace;
  apply(services);
  run('bash', [new URL('../../deploy/local/generate.sh', import.meta.url).pathname, 'rf3', material]);
  run('openssl', ['req', '-new', '-x509', '-key', `${material}/ca.key`, '-sha256', '-days', '365', '-config', `${material}/ca.cnf`, '-out', `${material}/ca.pem`]);
  const cluster = fs.readFileSync(`${material}/node-11.toml`, 'utf8').match(/^cluster_id = '([^']+)'$/m)?.[1];
  if (!cluster) throw new Error('Generated cluster identity is missing');
  const addresses = Object.fromEntries(roles.map(node => [node.id, kube(['get', 'service', `node-${node.id}`, '-o', 'jsonpath={.spec.clusterIP}'])]));
  const secret = (name, data) => ({apiVersion: 'v1', kind: 'Secret', metadata: {name, namespace}, type: 'Opaque',
    data: Object.fromEntries(Object.entries(data).map(([key, value]) => [key, Buffer.from(value).toString('base64')]))});
  const items = [secret('telemetry', Object.fromEntries(['ca.pem', 'client.pem', 'client.key'].map(name => [name, fs.readFileSync(path.join(home, 'telemetry', name))])))];
  for (const node of roles) {
    const extensions = `${material}/node-${node.id}.ext`;
    fs.writeFileSync(extensions, `basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:node-${node.id}.local,DNS:localhost,DNS:mq.lab.tidemq.test,DNS:mq.${namespace}.svc,IP:${addresses[node.id]},IP:${publicVIP},IP:192.168.126.8,IP:127.0.0.1\n`, {mode: 0o600});
    run('openssl', ['x509', '-req', '-sha256', '-in', `${material}/node-${node.id}.csr`, '-CA', `${material}/ca.pem`, '-CAkey', `${material}/ca.key`, '-set_serial', `${node.id}`, '-days', '30', '-extfile', extensions, '-out', `${material}/node-${node.id}.pem`]);
    run('openssl', ['x509', '-in', `${material}/node-${node.id}.pem`, '-outform', 'DER', '-out', `${material}/node-${node.id}.der`]);
  }
  for (const node of roles) {
    const config = runtimeConfig(node, cluster, addresses, namespace, endpoint);
    fs.writeFileSync(`${material}/node-${node.id}.toml`, config, {mode: 0o600});
    const files = {'ca.pem': fs.readFileSync(`${material}/ca.pem`)};
    for (const suffix of ['pem', 'key']) files[`node-${node.id}.${suffix}`] = fs.readFileSync(`${material}/node-${node.id}.${suffix}`);
    for (const peer of roles) files[`node-${peer.id}.der`] = fs.readFileSync(`${material}/node-${peer.id}.der`);
    if (node.role === 'metadata') files['bootstrap.token'] = fs.readFileSync(`${material}/bootstrap.token`);
    if (node.role === 'storage') files['objects.toml'] = fs.readFileSync(credentials);
    items.push(secret(`node-${node.id}`, files), {apiVersion: 'v1', kind: 'ConfigMap', metadata: {name: `node-${node.id}`, namespace},
      data: {'config.toml': config, 'collector.json': JSON.stringify(sidecarConfig(node, namespace))}});
    const pv = persistentVolume(node, namespace);
    if (pv) items.push(pv);
    const statefulSet = workload(node, namespace, image);
    statefulSet.metadata.namespace = namespace;
    items.push(statefulSet);
  }
  for (const [role, minimum] of [['metadata', 2], ['storage', 2], ['gateway', 1], ['broker', 1]]) {
    const pdb = resource('policy/v1', 'PodDisruptionBudget', role, {minAvailable: minimum, selector: {matchLabels: {app: 'tidemq', role}}});
    pdb.metadata.namespace = namespace;
    items.push(pdb);
  }
  fs.writeFileSync(path.join(output, 'resources.json'), JSON.stringify({apiVersion: 'v1', kind: 'List', items}), {mode: 0o600});
  apply(items);
  console.log(`Created ten RF3 role Pods in ${namespace}; private materials are in ${output}`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) main();
