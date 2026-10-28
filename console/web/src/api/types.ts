import { z } from "zod";

export const decimal = z.string().regex(/^(0|[1-9]\d*)$/);
export const resourceIdSchema = z
  .string()
  .regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);
export const observationSchema = z.object({
  observedAt: z.string().datetime(),
  source: z.string(),
  partial: z.boolean(),
  revision: decimal.nullable().optional(),
});
export type Observation = z.infer<typeof observationSchema>;

export const capabilitiesSchema = z.object({
  execution: z.enum(["local", "remote"]),
  clusterAdministration: z.boolean(),
  replicaMembership: z.boolean(),
  nodeMigration: z.boolean(),
  raftDiagnostics: z.boolean(),
  businessShardScaling: z.boolean(),
});

export const sessionSchema = z.object({
  principal: z.object({ id: z.string(), name: z.string() }),
  cluster: z.object({ id: z.string(), name: z.string() }),
  tenant: z.string().nullable(),
  namespace: z.string().nullable(),
  operator: z.boolean(),
  capabilities: capabilitiesSchema,
  permissions: z.array(z.string()),
  csrfToken: z.string().min(1),
  credentialId: z.string(),
  expiresAt: z.string().datetime(),
  credentialExpiresAt: z.string().datetime(),
  resourceGrants: z.array(z.object({
    resourceKind: z.enum(["cluster", "tenant", "namespace", "topic"]),
    resourceId: z.string(),
    tenantId: z.string().nullable(),
    namespaceId: z.string().nullable(),
    topicId: z.string().nullable(),
    permissions: z.array(z.string()),
  })),
});
export type Session = z.infer<typeof sessionSchema>;

export const topicSchema = z.object({
  id: z.string(),
  name: z.string(),
  tenant: z.string(),
  namespace: z.string(),
  writable: z.boolean(),
  shards: decimal,
});
export type Topic = z.infer<typeof topicSchema>;
const progressCounter = decimal
  .max(20)
  .refine((value) => BigInt(value) <= 18_446_744_073_709_551_615n);
export const progressObservationSchema = z
  .object({
    source: z.string(),
    observedFromMs: progressCounter.nullable(),
    observedUntilMs: progressCounter.nullable(),
    observedFrom: z.string().datetime().nullable(),
    observedUntil: z.string().datetime().nullable(),
    sampledShards: progressCounter.nullable(),
    partial: z.boolean(),
    reason: z.string().nullable(),
  })
  .refine(
    (value) =>
      value.observedFromMs === null ||
      value.observedUntilMs === null ||
      BigInt(value.observedFromMs) <= BigInt(value.observedUntilMs),
    "Invalid progress observation range",
  );
export type ProgressObservation = z.infer<typeof progressObservationSchema>;
export const subscriptionSchema = z.object({
  id: z.string(),
  name: z.string(),
  topicId: z.string(),
  topicName: z.string(),
  model: z.enum(["stream", "queue"]),
  members: decimal,
  unconfirmed: decimal.nullable(),
  progressObservation: progressObservationSchema,
  reason: z.string().nullable(),
});
export type Subscription = z.infer<typeof subscriptionSchema>;
export const principalSchema = z.object({
  id: z.string(),
  name: z.string(),
  kind: z.literal("principal").nullable(),
  enabled: z.boolean(),
  tenant: z.string().nullable(),
});
export type Principal = z.infer<typeof principalSchema>;
export const componentSchema = z
  .object({
    id: z.string(),
    name: z.string(),
    health: z.enum(["healthy", "degraded", "unavailable", "unknown"]),
    detail: z.string().nullable(),
    instances: decimal.nullable(),
  })
  .merge(observationSchema);
export type Component = z.infer<typeof componentSchema>;
export const operationSchema = z
  .object({
    id: z.string(),
    actor: z.string(),
    target: z.string(),
    scope: z.string(),
    status: z.enum(["succeeded", "rejected", "unknown", "expired"]),
    receipt: z.string().nullable(),
    reason: z.string().nullable(),
    resourceId: z.string().nullable(),
    secretUnavailable: z.boolean(),
  })
  .merge(observationSchema);
export type Operation = z.infer<typeof operationSchema>;

export function pageSchema<T extends z.ZodType>(item: T) {
  return z
    .object({
      items: z.array(item),
      nextCursor: z.string().nullable(),
    })
    .merge(observationSchema);
}
export type Page<T> = Observation & { items: T[]; nextCursor: string | null };

export type ResourceKind = "tenant" | "namespace" | "topic";
export interface CreateTopicInput {
  tenantId: string;
  namespaceId: string;
  name: string;
}
export interface CreatePrincipalInput {
  name: string;
  tenantId: string;
  namespaceId: string | null;
  permissions: string[];
}
export interface UpdatePrincipalInput {
  enabled: boolean;
}
export interface CreateCredentialInput {
  principalId: string;
  lifetimeMs: string;
}
export interface UpdateGrantInput {
  principalId: string;
  tenantId: string | null;
  namespaceId: string | null;
  topicId: string | null;
  permissions: string[];
  expectedVersion: string;
}
export interface SendMessageInput {
  payloadBase64: string;
  keyBase64: string | null;
  propertiesBase64: Record<string, string>;
}
export type WriteInput = (
  | CreateTopicInput
  | CreatePrincipalInput
  | UpdatePrincipalInput
  | CreateCredentialInput
  | UpdateGrantInput
  | SendMessageInput
  | Record<string, never>
) & { operationId: string };

export const configurationSchema = z.object({
  name: z.string(),
  value: z.string().nullable(),
  scope: z.string(),
  source: z.string(),
});
export const shardSchema = z.object({
  id: z.string(),
  sealed: z.boolean(),
  retainedStart: decimal.nullable(),
  committedEnd: decimal.nullable(),
  group: decimal.nullable(),
  reason: z.string().nullable(),
});
export const topicDetailSchema = topicSchema
  .extend({
    effectivePermissions: z.array(z.string()),
    configuration: z.array(configurationSchema),
    shardDetails: z.array(shardSchema),
    nextShardCursor: z.string().nullable(),
    totalShards: decimal,
  })
  .merge(observationSchema);

export const messageSchema = z.object({
  id: z.string(),
  shard: z.string(),
  offset: decimal,
  payloadBase64: z.string(),
  keyBase64: z.string().nullable(),
  propertiesBase64: z.record(z.string(), z.string()),
  tier: z.enum(["hot", "cold"]),
  payloadBytes: decimal,
  payloadTruncated: z.boolean(),
  metadataTruncated: z.boolean(),
});
export type Message = z.infer<typeof messageSchema>;
export const messagePageSchema = pageSchema(messageSchema).extend({
  shard: z.string(),
  retainedStart: decimal,
  observedEnd: decimal,
  scannedStart: decimal,
  scannedEnd: decimal,
  stopReason: z.string(),
  nextOffset: decimal.nullable(),
});
export type MessagePage = z.infer<typeof messagePageSchema>;
export interface MessageQuery {
  shard: string;
  startOffset?: string;
  endOffset?: string;
  messageId?: string;
  cursor?: string;
  keyBase64?: string;
  propertiesBase64?: Record<string, string>;
  limit: number;
  observedEnd?: string;
}
const leaseSchema = z.object({
  id: z.string(),
  leaseValid: z.boolean(),
  expiresAt: z.string().datetime(),
  lastHeartbeat: z.string().nullable(),
  reason: z.string().nullable(),
});
export const memberSchema = leaseSchema.extend({
  shards: z.array(z.string()),
  epoch: decimal,
  status: z.string(),
});
export type Member = z.infer<typeof memberSchema>;
export const producerSchema = leaseSchema.extend({ registration: decimal });
export type Producer = z.infer<typeof producerSchema>;
export const progressSchema = z.object({
  shard: z.string(),
  sealed: z.boolean(),
  retainedStart: decimal.nullable(),
  committedEnd: decimal.nullable(),
  frontier: decimal.nullable(),
  windowEnd: decimal.nullable(),
  ready: decimal.nullable(),
  inFlight: decimal.nullable(),
  acked: decimal.nullable(),
  paused: decimal.nullable(),
  unconfirmed: decimal.nullable(),
  tail: decimal.nullable(),
  reason: z.string().nullable(),
  componentId: z.string().nullable(),
  observedAt: z.string().nullable(),
  source: z.string(),
  revision: decimal.nullable(),
});
export type Progress = z.infer<typeof progressSchema>;
export const grantSchema = z.object({
  principalId: z.string(),
  principalName: z.string(),
  resourceKind: z.enum(["tenant", "namespace", "topic"]),
  resourceId: z.string(),
  version: z.string(),
  direct: z.array(z.string()),
  inherited: z.array(z.object({ permission: z.string(), source: z.string() })),
  effective: z.array(z.string()),
  tenantId: z.string().nullable(),
  namespaceId: z.string().nullable(),
  topicId: z.string().nullable(),
});
export type Grant = z.infer<typeof grantSchema>;
export const credentialSchema = z.object({
  id: z.string(),
  principalId: z.string(),
  name: z.string().nullable(),
  enabled: z.boolean(),
  expiresAt: z.string().nullable(),
  createdAt: z.string().nullable(),
});
export type Credential = z.infer<typeof credentialSchema>;
export const alertSchema = z.object({
  id: z.string(),
  name: z.string(),
  state: z.enum(["firing", "pending"]),
  detail: z.string().nullable(),
  url: z.string().nullable(),
  runbookUrl: z.string().nullable(),
});
export type Alert = z.infer<typeof alertSchema>;
export const overviewSchema = observationSchema.extend({
  topics: decimal.nullable(),
  unconfirmed: decimal.nullable(),
  progressObservation: progressObservationSchema,
  consumers: decimal.nullable(),
  alerts: decimal.nullable(),
  anomalies: z.array(
    z.object({
      id: z.string(),
      summary: z.string(),
      topicId: z.string().nullable(),
      subscriptionId: z.string().nullable(),
    }),
  ),
});
export const componentDetailSchema = componentSchema.extend({
  links: z.array(z.object({ label: z.string(), url: z.string() })),
  registered: decimal.nullable(),
  ready: decimal.nullable(),
});
export const instanceSchema = z
  .object({
    id: z.string(),
    nodeId: z.string().nullable(),
    role: z.string(),
    zone: z.string().nullable(),
    version: z.string().nullable(),
    health: z.enum(["healthy", "degraded", "unavailable", "unknown"]),
    detail: z.string().nullable(),
  })
  .merge(observationSchema);
export const resourceOptionSchema = z.object({
  id: z.string(),
  name: z.string(),
  tenantId: z.string().nullable().optional(),
});
export const nodeGroupSchema = z.object({
  id: decimal,
  role: z.string(),
  leader: decimal.nullable(),
  voters: z.array(decimal),
  quorumConfirmed: z.boolean().nullable(),
  hotBytes: decimal.nullable(),
  hotMessages: decimal.nullable(),
});
export const nodeSchema = z.object({
  id: decimal,
  endpoint: z.string(),
  roles: z.array(z.string()),
  schedulable: z.boolean(),
  retired: z.boolean(),
  live: z.boolean().nullable(),
  reason: z.string().nullable(),
  generation: decimal,
  zone: z.string(),
  version: z.string().nullable(),
  instance: z.string().nullable(),
  ready: z.boolean().nullable(),
  sampledAt: z.string().nullable(),
  groups: z.array(nodeGroupSchema),
  groupsPartial: z.boolean(),
  logDroppedEvents: decimal.nullable(),
  logSinkErrors: decimal.nullable(),
});
export type Node = z.infer<typeof nodeSchema>;

export const localNodeSchema = z.object({
  id: decimal,
  execution: z.literal("local"),
  instance: z.string(),
  roles: z.array(z.string()),
  version: z.string(),
  phase: z.enum(["starting", "recovering", "ready", "draining", "failed", "stopped"]),
  ready: z.boolean(),
  sampledAt: z.string().nullable(),
  metadataGroups: decimal,
  dataGroups: decimal,
  logDroppedEvents: decimal.nullable(),
  logSinkErrors: decimal.nullable(),
});
