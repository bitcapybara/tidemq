import { test as base, expect } from "@playwright/test";
import * as data from "../../tests/network/data";

export interface MockNetwork {
  authenticated: boolean;
  identity: typeof data.session;
  denied: Set<string>;
  overviewStatus: number;
  writeMode: "success" | "lost" | "conflict";
  writes: Record<string, unknown>[];
  scans: Record<string, unknown>[];
  operations: Map<string, ReturnType<typeof data.operation>>;
}

// This fixture intercepts API traffic; it is UI coverage, not a Rust/backend acceptance test.
export const test = base.extend<{ mockNetwork: MockNetwork }>({
  mockNetwork: [async ({ page }, use) => {
    const state: MockNetwork = {
      authenticated: true, identity: structuredClone(data.session), denied: new Set(),
      overviewStatus: 200, writeMode: "success", writes: [], scans: [], operations: new Map(),
    };
    const unhandled: string[] = [];
    await page.route("**/api/v1/**", async (route) => {
      const request = route.request();
      const url = new URL(request.url());
      const path = url.pathname.replace("/api/v1", "");
      const method = request.method();
      const reply = (value: unknown, status = 200) => route.fulfill({ status, json: value, headers: { "Cache-Control": "no-store" } });
      if (path === "/session" && method === "POST") {
        const body: unknown = request.postDataJSON();
        if (typeof body === "object" && body !== null && "token" in body && body.token === "bad-token") return reply({ code: "invalid_token" }, 401);
        state.authenticated = true;
        return reply(state.identity);
      }
      if (path === "/session" && method === "DELETE") {
        state.authenticated = false;
        return route.fulfill({ status: 204 });
      }
      if (!state.authenticated) return reply({ code: "authentication_required" }, 401);
      if (state.denied.has(path)) return reply({ code: "permission_denied" }, 403);
      if (path === "/me") return reply(state.identity);
      if (path.endsWith("/messages") && method === "POST") {
        state.scans.push(request.postDataJSON() as Record<string, unknown>);
        return reply(data.messages);
      }
      if (method !== "GET") {
        const body = request.postDataJSON() as Record<string, unknown>;
        state.writes.push(body);
        const id = String(body.operationId);
        const operation = data.operation(id, state.writeMode === "conflict"
          ? { status: "rejected", reason: "content_conflict" }
          : { status: state.writeMode === "lost" ? "unknown" : "succeeded", secretUnavailable: path === "/credentials" });
        state.operations.set(id, operation);
        if (state.writeMode === "lost") return route.abort("failed");
        return reply({ ...operation, ...(path === "/credentials" ? { secret: "synthetic-one-time-token" } : {}) });
      }
      if (path.startsWith("/operations/")) {
        const id = path.slice("/operations/".length);
        return reply(state.operations.get(id) ?? data.operation(id));
      }
      const responses: Record<string, unknown> = {
        "/overview": data.overview,
        "/topics": url.searchParams.has("cursor") ? data.page([{ ...data.topic, id: data.ids.other, name: "Invoices" }]) : data.page([data.topic], "page2"),
        [`/topics/${data.ids.topic}`]: data.topicDetail,
        [`/topics/${data.ids.topic}/producers`]: data.page([{ ...data.member, registration: "1" }]),
        "/subscriptions": data.page([data.subscription]),
        [`/subscriptions/${data.ids.subscription}/progress`]: data.page([data.progress]),
        [`/subscriptions/${data.ids.subscription}/members`]: data.page([data.member]),
        "/principals": data.page([data.principal]),
        "/credentials": data.page([data.credential]),
        "/grants": data.page([data.grant]),
        "/tenants": data.page([{ id: data.ids.tenant, name: "Tenant" }]),
        "/namespaces": data.page([{ id: data.ids.namespace, name: "Namespace" }]),
        "/components": data.page([data.component]),
        "/components/storage/instances": data.page([data.instance]),
        "/cluster": data.page([data.node]),
        "/alerts": data.page([]),
      };
      if (path === "/overview" && state.overviewStatus !== 200) return reply({ code: "upstream_unavailable" }, state.overviewStatus);
      if (path in responses) return reply(responses[path]);
      unhandled.push(`${method} ${path}`);
      return reply({ code: "unhandled_mock_route" }, 501);
    });
    await use(state);
    expect(unhandled, "Every mocked API request must have an explicit fixture").toEqual([]);
  }, { auto: true }],
});
export { expect };
