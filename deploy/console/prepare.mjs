import fs from 'node:fs';
import path from 'node:path';
import {isIP} from 'node:net';
import {pathToFileURL} from 'node:url';

const identifier = value => typeof value === 'string' && /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(value);
const dnsName = value => typeof value === 'string' && value.length <= 253 && value.split('.').every(identifier);
const quote = value => JSON.stringify(value);
const reservedComponents = new Set(['console', 'metadata', 'storage', 'broker', 'gateway']);
const privateKeyName = value => typeof value === 'string' && value.length <= 128 && /^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(value) && !['session.key', 'ca.pem'].includes(value);

function observationUrl(value, detail = false) {
  let url;
  try { url = new URL(value); } catch { throw new Error('Observability endpoints must be fixed absolute HTTP or HTTPS URLs'); }
  if (typeof value !== 'string' || value.length > 2048 || /[\u0000-\u001f\u007f]/.test(value) || !['http:', 'https:'].includes(url.protocol) || detail && url.protocol !== 'https:' || !url.hostname || url.username || url.password || url.hash || !detail && url.search || url.port === '0') throw new Error('Observability URLs cannot contain credentials, fragments, or probe queries; detail links require HTTPS');
  return url;
}

function validateObservability(config) {
  if (config === undefined) return;
  if (!config || typeof config !== 'object' || Array.isArray(config) || Object.keys(config).some(key => !['components', 'alertsUrl', 'alertsDetailUrl'].includes(key))) throw new Error('Unknown observability setting');
  const components = config.components ?? [];
  if (!Array.isArray(components) || components.length > 32) throw new Error('At most 32 configured components are supported');
  const ids = new Set();
  for (const component of components) {
    if (!component || typeof component !== 'object' || Array.isArray(component) || Object.keys(component).some(key => !['id', 'name', 'probeUrl', 'detailUrl'].includes(key)) || !identifier(component.id) || reservedComponents.has(component.id) || ids.has(component.id) || typeof component.name !== 'string' || !component.name.trim() || Buffer.byteLength(component.name) > 160 || /[\u0000-\u001f\u007f]/.test(component.name)) throw new Error('Components require distinct nonreserved IDs, bounded names, and fixed endpoints');
    ids.add(component.id);
    observationUrl(component.probeUrl);
    if (component.detailUrl !== undefined) observationUrl(component.detailUrl, true);
  }
  if (config.alertsUrl !== undefined && !observationUrl(config.alertsUrl).pathname.endsWith('/api/v1/alerts')) throw new Error('alertsUrl must address a Prometheus /api/v1/alerts endpoint');
  if (config.alertsDetailUrl !== undefined) {
    observationUrl(config.alertsDetailUrl, true);
    if (config.alertsUrl === undefined) throw new Error('alertsDetailUrl requires alertsUrl');
  }
}

function validateTracingEndpoint(value) {
  if (value === undefined) return;
  if (typeof value !== 'string' || Buffer.byteLength(value) > 512 || /[\u0000-\u0020\u007f]/.test(value)) throw new Error('tracingEndpoint must be a bounded fixed OTLP HTTP URL');
  let url;
  try { url = new URL(value); } catch { throw new Error('tracingEndpoint must be an absolute HTTP or HTTPS URL'); }
  const host = url.hostname.replace(/^\[|\]$/g, '');
  const ip = isIP(host), loopback = ip === 4 && host.startsWith('127.') || host === '::1';
  if (!ip || value.includes('@') || value.includes('?') || value.includes('#') || url.pathname !== '/v1/traces' || !(url.protocol === 'https:' || url.protocol === 'http:' && loopback)) throw new Error('tracingEndpoint requires a literal IP, /v1/traces, HTTPS or loopback HTTP, and no credentials, query, or fragment');
}

export function validate(platform, settings) {
  if (!['docker', 'kubernetes'].includes(platform)) throw new Error('Platform must be docker or kubernetes');
  const shared = ['image', 'publicOrigin', 'clusterAddress', 'clusterServerName', 'observability', 'tracingEndpoint'];
  const extra = platform === 'docker' ? ['hostPort', 'trustRootsFile', 'sessionKeyFile', 'previousSessionKeyFiles', 'networkService'] : ['namespace', 'privateSecret', 'previousSessionKeyNames', 'tlsSecret', 'ingressClass'];
  if (!settings || typeof settings !== 'object' || Array.isArray(settings) || Object.keys(settings).some(key => ![...shared, ...extra].includes(key))) throw new Error('Unknown or inactive deployment setting');
  if (typeof settings.image !== 'string' || !/^(?:[a-zA-Z0-9][a-zA-Z0-9./:_-]*@sha256:[a-f0-9]{64}|localhost\/tidemq-lab:[a-f0-9]{24,64})$/.test(settings.image)) throw new Error('Use an image digest or a content-addressed local lab image');
  let origin;
  try { origin = new URL(settings.publicOrigin); } catch { throw new Error('publicOrigin must be an HTTPS origin'); }
  if (origin.protocol !== 'https:' || !dnsName(origin.hostname) || origin.username || origin.password || origin.pathname !== '/' || origin.search || origin.hash) throw new Error('publicOrigin must contain only an HTTPS scheme, DNS host, and optional port');
  if (!dnsName(settings.clusterServerName)) throw new Error('clusterServerName must be a DNS name');
  const address = typeof settings.clusterAddress === 'string' && settings.clusterAddress.match(/^(\[[0-9a-fA-F:]+\]|[a-zA-Z0-9.-]+):([0-9]{1,5})$/);
  if (!address || Number(address[2]) < 1 || Number(address[2]) > 65535) throw new Error('clusterAddress must be a fixed host:port');
  const host = address[1];
  if (host.startsWith('[') ? isIP(host.slice(1, -1)) !== 6 || host === '[::]' : !dnsName(host.toLowerCase()) || host === '0.0.0.0') throw new Error('clusterAddress must use a concrete IP or DNS host');
  if (platform === 'docker') {
    if (!Number.isInteger(settings.hostPort) || settings.hostPort < 1024 || settings.hostPort > 65535) throw new Error('hostPort must be an unprivileged port');
    if (Number(origin.port || 443) === settings.hostPort) throw new Error('The HTTPS proxy port must differ from the loopback backend port');
    for (const name of ['trustRootsFile', 'sessionKeyFile']) {
      if (typeof settings[name] !== 'string' || !path.isAbsolute(settings[name]) || !fs.statSync(settings[name]).isFile()) throw new Error(`${name} must name an existing absolute file`);
    }
    const previous = settings.previousSessionKeyFiles ?? [];
    if (!Array.isArray(previous) || previous.length > 2 || new Set([settings.sessionKeyFile, ...previous]).size !== previous.length + 1 || previous.some(file => typeof file !== 'string' || !path.isAbsolute(file) || !fs.statSync(file).isFile())) throw new Error('previousSessionKeyFiles must contain at most two distinct existing absolute files');
    if (settings.networkService !== undefined && (!identifier(settings.networkService) || settings.networkService === 'console')) throw new Error('networkService must name an existing network holder service');
  } else {
    for (const name of ['namespace', 'privateSecret', 'tlsSecret', 'ingressClass']) if (!identifier(settings[name])) throw new Error(`${name} must be a DNS label`);
    if (origin.port && origin.port !== '443') throw new Error('The Kubernetes HTTPS Ingress example uses port 443');
    const previous = settings.previousSessionKeyNames ?? [];
    if (!Array.isArray(previous) || previous.length > 2 || new Set(previous).size !== previous.length || previous.some(name => !privateKeyName(name))) throw new Error('previousSessionKeyNames must contain at most two distinct additional Secret key names');
  }
  validateObservability(settings.observability);
  validateTracingEndpoint(settings.tracingEndpoint);
  return origin;
}

function observationConfig(config) {
  if (!config) return '';
  let text = '[observability]\n';
  if (!config.components?.length) text += 'components = []\n';
  if (config.alertsUrl !== undefined) text += `alerts_url = ${quote(config.alertsUrl)}\n`;
  if (config.alertsDetailUrl !== undefined) text += `alerts_detail_url = ${quote(config.alertsDetailUrl)}\n`;
  for (const component of config.components ?? []) {
    text += `[[observability.components]]\nid = ${quote(component.id)}\nname = ${quote(component.name)}\nprobe_url = ${quote(component.probeUrl)}\n`;
    if (component.detailUrl !== undefined) text += `detail_url = ${quote(component.detailUrl)}\n`;
  }
  return text;
}

function tracingConfig(endpoint) {
  if (endpoint === undefined) return '';
  return `[tracing]
enabled = true
endpoint = ${quote(endpoint)}
sample_per_mille = 10
spans_per_second = 128
queue_spans = 256
batch_spans = 32
export_interval_ms = 500
export_timeout_ms = 500
`;
}

export function consoleConfig(settings) {
  const previous = settings.previousSessionKeyNames?.map(name => `/private/${name}`) ?? settings.previousSessionKeyFiles?.map((_, index) => `/private/session-previous-${index + 1}.key`) ?? [];
  return `role = "console"
listen = "0.0.0.0:8080"
management_listen = "127.0.0.1:9420"
static_dir = "/opt/tidemq/console"
[cluster]
address = ${quote(settings.clusterAddress)}
server_name = ${quote(settings.clusterServerName)}
trust_roots = "/private/ca.pem"
[session]
key_file = "/private/session.key"
previous_key_files = ${quote(previous)}
public_origin = ${quote(new URL(settings.publicOrigin).origin)}
max_age_seconds = 1800
${observationConfig(settings.observability)}${tracingConfig(settings.tracingEndpoint)}[logging]
json = true
`;
}

const probe = endpoint => ({exec: {command: ['curl', '--fail', '--silent', '--max-time', '1', `http://127.0.0.1:9420/health/${endpoint}`]}, periodSeconds: 5, timeoutSeconds: 2});

export function kubernetesResources(settings) {
  const labels = {app: 'tidemq-console'};
  const metadata = name => ({name, namespace: settings.namespace});
  const resource = (apiVersion, kind, name, spec) => ({apiVersion, kind, metadata: metadata(name), spec});
  const deployment = resource('apps/v1', 'Deployment', 'tidemq-console', {
    replicas: 2, selector: {matchLabels: labels}, template: {metadata: {labels}, spec: {
      automountServiceAccountToken: false, terminationGracePeriodSeconds: 10,
      securityContext: {runAsNonRoot: true, runAsUser: 10001, runAsGroup: 10001, fsGroup: 10001, seccompProfile: {type: 'RuntimeDefault'}},
      containers: [{name: 'console', image: settings.image, imagePullPolicy: 'IfNotPresent',
        command: ['/usr/local/bin/tidemq', 'serve', '--config', '/config/console.toml'],
        securityContext: {readOnlyRootFilesystem: true, allowPrivilegeEscalation: false, capabilities: {drop: ['ALL']}},
        resources: {requests: {cpu: '100m', memory: '128Mi'}, limits: {cpu: '500m', memory: '256Mi'}},
        ports: [{name: 'http', containerPort: 8080}],
        startupProbe: {...probe('ready'), failureThreshold: 30}, readinessProbe: probe('ready'), livenessProbe: probe('live'),
        volumeMounts: [{name: 'config', mountPath: '/config', readOnly: true}, {name: 'private', mountPath: '/private', readOnly: true}],
      }],
      volumes: [{name: 'config', configMap: {name: 'tidemq-console'}}, {name: 'private', secret: {secretName: settings.privateSecret, defaultMode: 0o440}}],
    }},
  });
  const ingress = resource('networking.k8s.io/v1', 'Ingress', 'tidemq-console', {
    ingressClassName: settings.ingressClass,
    tls: [{hosts: [new URL(settings.publicOrigin).hostname], secretName: settings.tlsSecret}],
    rules: [{host: new URL(settings.publicOrigin).hostname, http: {paths: [{path: '/', pathType: 'Prefix', backend: {service: {name: 'tidemq-console', port: {name: 'http'}}}}]}}],
  });
  ingress.metadata.annotations = {'nginx.ingress.kubernetes.io/force-ssl-redirect': 'true', 'nginx.ingress.kubernetes.io/proxy-body-size': '1500000', 'nginx.ingress.kubernetes.io/proxy-read-timeout': '30'};
  return {apiVersion: 'v1', kind: 'List', items: [
    {apiVersion: 'v1', kind: 'ConfigMap', metadata: metadata('tidemq-console'), data: {'console.toml': consoleConfig(settings)}},
    deployment,
    resource('v1', 'Service', 'tidemq-console', {selector: labels, ports: [{name: 'http', port: 8080, targetPort: 'http'}]}),
    ingress,
  ]};
}

export function compose(settings, output) {
  const document = {services: {console: {
    image: settings.image, user: '10001:10001', read_only: true, cap_drop: ['ALL'], security_opt: ['no-new-privileges:true'],
    command: ['/usr/local/bin/tidemq', 'serve', '--config', '/config/console.toml'],
    ports: [`127.0.0.1:${settings.hostPort}:8080`], restart: 'unless-stopped', stop_grace_period: '10s',
    cpus: 0.5, mem_limit: '256m', memswap_limit: '256m', pids_limit: 128,
    logging: {driver: 'local', options: {'max-size': '10m', 'max-file': '3'}},
    healthcheck: {test: ['CMD', 'curl', '--fail', '--silent', '--max-time', '1', 'http://127.0.0.1:9420/health/ready'], interval: '5s', timeout: '2s', retries: 6},
    volumes: [
      {type: 'bind', source: path.join(output, 'console.toml'), target: '/config/console.toml', read_only: true, bind: {create_host_path: false}},
      {type: 'bind', source: settings.trustRootsFile, target: '/private/ca.pem', read_only: true, bind: {create_host_path: false}},
      {type: 'bind', source: settings.sessionKeyFile, target: '/private/session.key', read_only: true, bind: {create_host_path: false}},
      ...(settings.previousSessionKeyFiles ?? []).map((source, index) => ({type: 'bind', source, target: `/private/session-previous-${index + 1}.key`, read_only: true, bind: {create_host_path: false}})),
    ],
  }}};
  if (settings.networkService) {
    document.services[settings.networkService] = {ports: document.services.console.ports, networks: ['console-ingress']};
    document.networks = {'console-ingress': {driver: 'bridge', internal: false}};
    delete document.services.console.ports;
    document.services.console.network_mode = `service:${settings.networkService}`;
  }
  return document;
}

export function prepare(platform, settings, output) {
  const origin = validate(platform, settings);
  if (fs.existsSync(output) || !fs.statSync(path.dirname(path.resolve(output))).isDirectory()) throw new Error('Output must be a new directory with an existing parent');
  fs.mkdirSync(output, {mode: 0o700});
  if (platform === 'kubernetes') {
    fs.writeFileSync(path.join(output, 'resources.json'), JSON.stringify(kubernetesResources(settings), null, 2) + '\n', {mode: 0o600});
  } else {
    fs.writeFileSync(path.join(output, 'console.toml'), consoleConfig(settings), {mode: 0o644});
    // File bind mounts must stay readable to UID 10001 even with the caller's private umask.
    fs.chmodSync(path.join(output, 'console.toml'), 0o644);
    fs.writeFileSync(path.join(output, 'compose.json'), JSON.stringify(compose(settings, path.resolve(output)), null, 2) + '\n', {mode: 0o600});
    fs.writeFileSync(path.join(output, 'nginx.conf'), `server {
  listen ${origin.port || 443} ssl;
  server_name ${origin.hostname};
  ssl_certificate /etc/console-tls/tls.crt;
  ssl_certificate_key /etc/console-tls/tls.key;
  access_log off;
  client_max_body_size 1500000;
  location / {
    proxy_set_header Host $http_host;
    proxy_set_header X-Forwarded-Proto https;
    proxy_read_timeout 30s;
    proxy_pass http://127.0.0.1:${settings.hostPort};
  }
}
`, {mode: 0o600});
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const [platform, settingsFile, output] = process.argv.slice(2);
  if (!platform || !settingsFile || !output) throw new Error('Usage: prepare.mjs docker|kubernetes settings.json new-output-directory');
  prepare(platform, JSON.parse(fs.readFileSync(settingsFile, 'utf8')), output);
  console.log(`Prepared Console foundation deployment files in ${path.resolve(output)}; no platform changes were applied`);
}
