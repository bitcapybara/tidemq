import { expect, it } from "vitest";
import { normalizeTimes } from "./client";
import {
  overviewSchema,
  pageSchema,
  progressObservationSchema,
  subscriptionSchema,
  sessionSchema,
  topicSchema,
  topicDetailSchema,
  memberSchema,
  producerSchema,
  localNodeSchema,
} from "./types";
import { localNode, localSession, session as remoteSession } from "../../tests/network/data";

const progress = {
  source: "perShardLinearizableGroupCursorSamples",
  observedFromMs: "1700000000000",
  observedUntilMs: "1700000000123",
  sampledShards: "2",
  partial: false,
  reason: null,
};
const metadata = {
  observedAtMs: "1700000099999",
  source: "metadataLinearizable",
  partial: false,
  revision: "42",
};

it("keeps progress sample ranges and large counters separate from directory metadata", () => {
  const page = pageSchema(subscriptionSchema).parse(
    normalizeTimes({
      ...metadata,
      nextCursor: null,
      items: [
        {
          id: "subscription",
          name: "fulfillment",
          topicId: "topic",
          topicName: "acme/app/orders",
          model: "queue",
          members: "1",
          unconfirmed: "9007199254740993",
          reason: null,
          progressObservation: progress,
        },
      ],
    }),
  );
  const sample = page.items.at(0)?.progressObservation;
  expect(page.items.at(0)?.unconfirmed).toBe("9007199254740993");
  expect(page.observedAt).toBe("2023-11-14T22:14:59.999Z");
  expect(sample?.observedFrom).toBe("2023-11-14T22:13:20.000Z");
  expect(sample?.observedUntil).toBe("2023-11-14T22:13:20.123Z");
  expect(sample?.source).not.toBe(page.source);
  const overview = overviewSchema.parse(
    normalizeTimes({
      ...metadata,
      topics: "1",
      unconfirmed: "9007199254740993",
      consumers: "1",
      alerts: null,
      anomalies: [],
      progressObservation: progress,
    }),
  );
  expect(overview.progressObservation).toEqual(sample);
});

it("rejects incomplete authority responses instead of manufacturing current permissions", () => {
  const session = normalizeTimes({
    principal: { id: "principal", name: "Operator" },
    cluster: { id: "cluster", name: "TideMQ" },
    tenant: null,
    namespace: null,
    operator: true,
    capabilities: remoteSession.capabilities,
    permissions: ["inspect"],
    csrfToken: "csrf",
    credentialId: "credential",
    expiresAtMs: "1700000100000",
    credentialExpiresAtMs: "1700000200000",
    resourceGrants: [],
  }) as Record<string, unknown>;
  expect(sessionSchema.safeParse(session).success).toBe(true);
  for (const field of ["namespace", "resourceGrants", "credentialId", "expiresAt", "capabilities"])
    expect(
      sessionSchema.safeParse(Object.fromEntries(
        Object.entries(session).filter(([key]) => key !== field),
      )).success,
    ).toBe(false);
  const topic = {
    id: "topic", name: "orders", tenant: "tenant", namespace: "namespace",
    writable: true, shards: "1",
  };
  expect(topicSchema.safeParse(topic).success).toBe(true);
  const detail = {
    ...topic, observedAt: "2023-11-14T22:13:20.000Z", source: "metadataLinearizable", partial: false,
    configuration: [], shardDetails: [], nextShardCursor: null, totalShards: "1",
  };
  expect(topicDetailSchema.safeParse(detail).success).toBe(false);
  expect(topicDetailSchema.safeParse({ ...detail, effectivePermissions: [] }).success).toBe(true);
});

it("requires explicit local execution capabilities and process facts", () => {
  expect(sessionSchema.parse(localSession).capabilities).toEqual(localSession.capabilities);
  expect(localNodeSchema.parse(localNode).phase).toBe("ready");
  expect(localNodeSchema.safeParse({ ...localNode, execution: "remote" }).success).toBe(false);
  expect(localNodeSchema.safeParse({ ...localNode, phase: "leader" }).success).toBe(false);
  expect(sessionSchema.safeParse({ ...localSession, capabilities: {} }).success).toBe(false);
});

it("requires current producer and consumer facts without filling absent state", () => {
  const lease = normalizeTimes({
    id: "identity", leaseValid: true, expiresAtMs: "1700000200000",
    lastHeartbeatMs: null, reason: null,
  });
  const producer = { ...(lease as object), registration: "1" };
  const member = { ...(lease as object), epoch: "2", status: "active", shards: ["shard"] };
  expect(producerSchema.safeParse(producer).success).toBe(true);
  expect(memberSchema.safeParse(member).success).toBe(true);
  expect(memberSchema.safeParse(producer).success).toBe(false);
  expect(producerSchema.safeParse(member).success).toBe(false);
});

it("preserves missing progress samples and rejects numeric, overflowing or inverted ranges", () => {
  const missing = progressObservationSchema.parse(
    normalizeTimes({
      ...progress,
      observedFromMs: null,
      observedUntilMs: null,
      sampledShards: null,
      partial: true,
      reason: "dataObservationUnavailable",
    }),
  );
  expect(missing.observedFrom).toBeNull();
  expect(missing.observedUntil).toBeNull();
  expect(missing.sampledShards).toBeNull();
  expect(missing.partial).toBe(true);
  for (const fields of [
    { sampledShards: 0 },
    { observedFromMs: 1700000000000 },
    { sampledShards: "18446744073709551616" },
    { sampledShards: "00" },
    { observedFromMs: "9007199254740993", observedUntilMs: "9007199254740992" },
  ])
    expect(
      progressObservationSchema.safeParse(
        normalizeTimes({ ...progress, ...fields }),
      ).success,
    ).toBe(false);
});
