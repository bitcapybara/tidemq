import { QueryClient } from "@tanstack/react-query";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, expect, it, vi } from "vitest";
import { z } from "zod";
import { api } from "./client";
import { SessionProvider, useSession } from "./session";
import type { Session } from "./types";

const query = vi.hoisted(() => ({
  client: null as QueryClient | null,
  refresh: null as ((context: { signal: AbortSignal }) => Promise<Session>) | null,
}));

vi.mock("@tanstack/react-query", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@tanstack/react-query")>()),
  useQueryClient: () => query.client,
  useQuery: (options: {
    queryFn: (context: { signal: AbortSignal }) => Promise<Session>;
  }) => {
    query.refresh = options.queryFn;
    return { isPending: false, data: undefined, error: null };
  },
}));

const topic = "018bcfe5-6800-7000-8000-000000000001";
const tenant = "018bcfe5-6800-7000-8000-000000000002";
const namespace = "018bcfe5-6800-7000-8000-000000000003";
const session = {
  principal: { id: topic, name: "Test user" },
  cluster: { id: tenant, name: "Test cluster" },
  tenant,
  namespace,
  operator: false,
  capabilities: { execution: "remote", clusterAdministration: true, replicaMembership: true, nodeMigration: true, raftDiagnostics: true, businessShardScaling: true },
  permissions: [],
  resourceGrants: [{
    resourceKind: "topic",
    resourceId: topic,
    tenantId: tenant,
    namespaceId: namespace,
    topicId: topic,
    permissions: ["inspect_message_content"],
  }],
  csrfToken: "session-csrf-token",
  credentialId: namespace,
  expiresAtMs: "1900000000000",
  credentialExpiresAtMs: "1900000000000",
  revision: "1",
};

afterEach(() => {
  api.clearIdentity();
  query.client?.clear();
  query.client = null;
  query.refresh = null;
  vi.unstubAllGlobals();
});

it.each(["resourceGrants", "namespace", "capabilities"] as const)(
  "clears protected data and rejects late messages when only %s changes after login",
  async (field) => {
    const client = new QueryClient();
    query.client = client;
    const capture = vi.fn<(value: ReturnType<typeof useSession>) => void>();
    function Probe() {
      capture(useSession());
      return null;
    }
    renderToStaticMarkup(<SessionProvider><Probe /></SessionProvider>);
    const controls = capture.mock.calls.at(0)?.at(0);
    let finish: ((response: Response) => void) | undefined;
    let signal: AbortSignal | null | undefined;
    const next = {
      ...session,
      ...(field === "resourceGrants"
        ? { resourceGrants: [] }
        : field === "namespace" ? { namespace: null }
        : { capabilities: { execution: "local", clusterAdministration: false, replicaMembership: false, nodeMigration: false, raftDiagnostics: false, businessShardScaling: true } }),
    };
    const fetcher = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(new Response(JSON.stringify(session)))
      .mockImplementationOnce((_input, options) => {
        signal = options?.signal;
        return new Promise((resolve) => { finish = resolve; });
      })
      .mockResolvedValueOnce(new Response(JSON.stringify(next)));
    vi.stubGlobal("fetch", fetcher);
    expect(controls).toBeDefined();
    await controls?.login("personal-test-token");
    const key = ["protected", 0, `topic:${topic}`, "messages"];
    client.setQueryData(key, { payloadBase64: "cHJpdmF0ZQ==" });
    client.setQueryData(["unprotected"], "retained");
    const late = api.request(`/api/v1/topics/${topic}/messages`, z.string(), {
      scope: `topic:${topic}`,
    });
    expect(query.refresh).toBeTypeOf("function");
    await query.refresh?.({ signal: new AbortController().signal });
    expect(client.getQueryData(key)).toBeUndefined();
    expect(client.getQueryData(["unprotected"])).toBe("retained");
    expect(signal?.aborted).toBe(true);
    finish?.(new Response(JSON.stringify("private late body")));
    await expect(late).rejects.toMatchObject({ kind: "stale" });
  },
);
