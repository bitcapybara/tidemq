export const ids = {
  tenant: "018bcfe5-6800-7000-8000-000000000001",
  namespace: "018bcfe5-6800-7000-8000-000000000002",
  topic: "018bcfe5-6800-7000-8000-000000000003",
  shard: "018bcfe5-6800-7000-8000-000000000004",
  principal: "018bcfe5-6800-7000-8000-000000000005",
  credential: "018bcfe5-6800-7000-8000-000000000006",
  subscription: "018bcfe5-6800-7000-8000-000000000007",
  other: "018bcfe5-6800-7000-8000-000000000008",
};
export const observation = {
  observedAt: "2026-10-27T04:00:00.000Z",
  source: "metadataLinearizable",
  revision: "7",
  partial: false,
};
export const page = <T,>(items: T[], nextCursor: string | null = null) => ({
  ...observation, items, nextCursor,
});
export const session = {
  principal: { id: ids.principal, name: "Test operator" },
  cluster: { id: ids.tenant, name: "Test cluster" },
  tenant: ids.tenant,
  namespace: ids.namespace,
  operator: true,
  capabilities: { execution: "remote" as "local" | "remote", clusterAdministration: true, replicaMembership: true, nodeMigration: true, raftDiagnostics: true, businessShardScaling: true },
  permissions: ["inspect", "inspect_message_content", "produce", "consume", "manage_topics", "manage_credentials", "manage_tenants"],
  resourceGrants: [],
  csrfToken: "test-csrf-token",
  credentialId: ids.credential,
  expiresAt: "2100-01-01T00:00:00.000Z",
  credentialExpiresAt: "2100-01-01T00:00:00.000Z",
};
export const topic = {
  id: ids.topic, name: "Orders", tenant: ids.tenant, namespace: ids.namespace,
  writable: true, shards: "2",
};
export const shard = {
  id: ids.shard, sealed: false, retainedStart: "0", committedEnd: "100", group: "1", reason: null,
};
export const topicDetail = {
  ...topic, ...observation,
  effectivePermissions: session.permissions,
  configuration: [{ name: "retention", value: "3600", scope: "topic", source: "core" }],
  shardDetails: [shard], nextShardCursor: null, totalShards: "1",
};
export const message = {
  id: "message-42", shard: ids.shard, offset: "42", payloadBase64: "eyJvcmRlciI6NDJ9",
  keyBase64: "a2V5", propertiesBase64: { region: "ZWFzdA==" }, tier: "hot" as const,
  payloadBytes: "12", payloadTruncated: false, metadataTruncated: false,
};
export const messages = {
  ...page([message], "scan-cursor"), shard: ids.shard, retainedStart: "0", observedEnd: "100",
  scannedStart: "0", scannedEnd: "43", stopReason: "limit", nextOffset: "43",
};
export const progressObservation = {
  source: "broker", observedFromMs: "1793073600000", observedUntilMs: "1793073600000",
  observedFrom: observation.observedAt, observedUntil: observation.observedAt,
  sampledShards: "1", partial: false, reason: null,
};
export const overview = {
  ...observation, topics: "1", unconfirmed: "3", consumers: "1", alerts: "1", progressObservation,
  anomalies: [{ id: "lag", summary: "Queue needs attention", topicId: ids.topic, subscriptionId: ids.subscription }],
};
export const subscription = {
  id: ids.subscription, name: "Order processors", topicId: ids.topic, topicName: topic.name,
  model: "queue", members: "1", unconfirmed: "3", progressObservation, reason: "backlog",
};
export const progress = {
  shard: ids.shard, sealed: true, retainedStart: "0", committedEnd: "100", frontier: "42",
  windowEnd: "45", ready: "1", inFlight: "1", acked: "0", paused: "1", unconfirmed: "3", tail: "55",
  reason: "waiting_for_ack", componentId: "data-group:1", observedAt: observation.observedAt,
  source: "broker", revision: "7",
};
export const member = {
  id: "worker-1", leaseValid: true, expiresAt: session.expiresAt, lastHeartbeat: observation.observedAt,
  reason: null, shards: [ids.shard], epoch: "3", status: "active",
};
export const principal = { id: ids.principal, name: "Test operator", kind: "principal", enabled: true, tenant: ids.tenant };
export const credential = { id: ids.credential, principalId: ids.principal, name: null, enabled: true, expiresAt: session.expiresAt, createdAt: null };
export const grant = {
  principalId: ids.principal, principalName: "Test operator", resourceKind: "topic", resourceId: ids.topic,
  version: "7", direct: ["produce"], inherited: [{ permission: "produce", source: "tenant" }, { permission: "inspect", source: "namespace" }],
  effective: ["produce", "inspect"], tenantId: ids.tenant, namespaceId: ids.namespace, topicId: ids.topic,
};
export const component = {
  ...observation, id: "storage", name: "Storage", health: "healthy", detail: "Ready", instances: "1",
  registered: "1", ready: "1", links: [{ label: "Metrics", url: "https://example.test/metrics" }],
};
export const instance = {
  ...observation, id: "storage-1", nodeId: "1", role: "storage", zone: "zone-a", version: "test",
  health: "healthy", detail: "Ready",
};
export const node = {
  id: "1", endpoint: "127.0.0.1:9092", roles: ["storage", "broker"], schedulable: true, retired: false,
  live: true, reason: null, generation: "1", zone: "zone-a", version: "test", instance: "storage-1",
  ready: true, sampledAt: observation.observedAt,
  groups: [{ id: "1", role: "storage", leader: "1", voters: ["1"], quorumConfirmed: true, hotBytes: "128", hotMessages: "1" }],
  groupsPartial: false, logDroppedEvents: "0", logSinkErrors: "0",
};
export const localSession = {
  ...session,
  capabilities: { execution: "local" as const, clusterAdministration: false, replicaMembership: false, nodeMigration: false, raftDiagnostics: false, businessShardScaling: true },
};
export const localNode = {
  id: "1", execution: "local", instance: "combined-1", roles: ["metadata", "storage", "broker", "gateway", "console"],
  version: "test", ready: true, phase: "ready", sampledAt: observation.observedAt,
  metadataGroups: "1", dataGroups: "2", logDroppedEvents: "0", logSinkErrors: "0",
};
export const operation = (id: string, overrides: object = {}) => ({
  ...observation, id, actor: session.principal.name, target: topic.name, scope: "topic",
  status: "succeeded", receipt: "Applied revision 8", reason: null, resourceId: ids.topic,
  secretUnavailable: false, ...overrides,
});
