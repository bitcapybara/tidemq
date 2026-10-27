import { describe, expect, it, vi } from "vitest";
import { z } from "zod";
import { ApiClient, normalizeTimes } from "./client";

describe("API trust and identity boundaries", () => {
  it("exposes only validated correlation IDs from error response headers", async () => {
    const requestId = "018bcfe5-6800-7000-8000-000000000000";
    const traceId = "0af7651916cd43dd8448eb211c80319c";
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response("{}", {
          status: 503,
          headers: {
            "x-request-id": requestId,
            traceparent: `00-${traceId}-b7ad6b7169203331-01`,
          },
        }),
      )
      .mockResolvedValueOnce(
        new Response("{}", {
          status: 403,
          headers: {
            "x-request-id": '<script>alert("secret")</script>',
            traceparent: `00-${traceId}-0000000000000000-01`,
          },
        }),
      );
    const client = new ApiClient(fetcher);
    await expect(
      client.request("/api/v1/overview", z.unknown()),
    ).rejects.toMatchObject({ kind: "unavailable", requestId, traceId });
    await expect(
      client.request("/api/v1/topics", z.unknown()),
    ).rejects.toMatchObject({
      kind: "forbidden",
      requestId: null,
      traceId: null,
    });
  });
  it("requires CSRF for authenticated writes, exempts only login, and clears it with the identity", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockImplementation(() => Promise.resolve(new Response("null")));
    const client = new ApiClient(fetcher);
    await expect(
      client.request("/api/v1/topics", z.null(), { method: "POST", body: {} }),
    ).rejects.toMatchObject({ code: "csrf_token_required" });
    expect(fetcher).not.toHaveBeenCalled();
    await client.request("/api/v1/session", z.null(), {
      method: "POST",
      publicSession: true,
      body: { token: "test-token" },
    });
    expect(
      fetcher.mock.calls.map(([, options]) => options?.headers).at(0),
    ).not.toHaveProperty("X-CSRF-Token");
    client.setCsrfToken("csrf-test");
    await client.request("/api/v1/session", z.null(), { method: "DELETE" });
    expect(
      fetcher.mock.calls.map(([, options]) => options?.headers).at(1),
    ).toHaveProperty("X-CSRF-Token", "csrf-test");
    client.clearIdentity();
    await expect(
      client.request("/api/v1/session", z.null(), { method: "DELETE" }),
    ).rejects.toMatchObject({ code: "csrf_token_required" });
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("normalizes nullable millisecond observations without interpreting user property names", () => {
    const properties = { observedAtMs: "dGVzdA==", __proto__: null };
    expect(
      normalizeTimes({
        observedAtMs: "1700000000000",
        expiresAtMs: null,
        nested: [{ lastHeartbeatMs: "8640000000000001" }],
        propertiesBase64: properties,
      }),
    ).toEqual({
      observedAtMs: "1700000000000",
      observedAt: "2023-11-14T22:13:20.000Z",
      expiresAtMs: null,
      expiresAt: null,
      nested: [{ lastHeartbeatMs: "8640000000000001", lastHeartbeat: null }],
      propertiesBase64: properties,
    });
  });
  it("calls the native fetch with its global receiver", async () => {
    vi.stubGlobal("fetch", function (this: unknown) {
      expect(this).toBe(globalThis);
      return Promise.resolve(new Response('"connected"'));
    });
    try {
      await expect(
        new ApiClient().request("/api/v1/me", z.string()),
      ).resolves.toBe("connected");
    } finally {
      vi.unstubAllGlobals();
    }
  });
  it("distinguishes an upstream failure from invalid credentials and never retries", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(JSON.stringify({ code: "upstream_unavailable" }), {
        status: 503,
      }),
    );
    const client = new ApiClient(fetcher);
    await expect(
      client.request("/api/v1/me", z.unknown()),
    ).rejects.toMatchObject({ kind: "unavailable", status: 503 });
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("discards a late success after an identity change even if the transport ignores cancellation", async () => {
    let complete: ((response: Response) => void) | undefined;
    const fetcher = vi.fn<typeof fetch>().mockImplementation(
      () =>
        new Promise((resolve) => {
          complete = resolve;
        }),
    );
    const client = new ApiClient(fetcher);
    const request = client.request("/api/v1/topics", z.array(z.string()), {
      scope: "topics",
    });
    client.clearIdentity();
    complete?.(new Response(JSON.stringify(["private-topic"])));
    await expect(request).rejects.toMatchObject({ kind: "stale" });
  });

  it("clears only the forbidden resource generation and rejects its late responses", async () => {
    let complete: ((response: Response) => void) | undefined;
    const fetcher = vi
      .fn<typeof fetch>()
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            complete = resolve;
          }),
      )
      .mockResolvedValueOnce(new Response("{}", { status: 403 }))
      .mockResolvedValueOnce(new Response('"allowed"'));
    const client = new ApiClient(fetcher);
    const events = vi.fn();
    client.onAccessFailure(events);
    const first = client.request("/api/v1/topics/a/messages", z.string(), {
      scope: "topic:a",
    });
    await expect(
      client.request("/api/v1/topics/a", z.string(), { scope: "topic:a" }),
    ).rejects.toMatchObject({ kind: "forbidden" });
    complete?.(new Response('"private-message"'));
    await expect(first).rejects.toMatchObject({ kind: "stale" });
    await expect(
      client.request("/api/v1/topics/b", z.string(), { scope: "topic:b" }),
    ).resolves.toBe("allowed");
    expect(events).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: "forbidden",
        scope: "topic:a",
      }),
    );
  });

  it("notifies identity invalidation on 401 and rejects malformed success data", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(new Response("{}", { status: 401 }))
      .mockResolvedValueOnce(new Response('{"offset":9007199254740993}'));
    const client = new ApiClient(fetcher);
    const events = vi.fn();
    client.onAccessFailure(events);
    await expect(
      client.request("/api/v1/me", z.unknown()),
    ).rejects.toMatchObject({ kind: "authentication" });
    expect(events).toHaveBeenCalledWith(
      expect.objectContaining({ kind: "authentication" }),
    );
    await expect(
      client.request("/api/v1/topics/a", z.object({ offset: z.string() })),
    ).rejects.toMatchObject({ code: "invalid_response" });
  });

  it("limits requests to same-origin internal API paths without secret query parameters", async () => {
    const fetcher = vi.fn<typeof fetch>();
    const client = new ApiClient(fetcher);
    await expect(
      client.request("https://elsewhere/api/v1/me", z.unknown()),
    ).rejects.toMatchObject({ kind: "invalid" });
    await expect(
      client.request("/api/v1/topics?key=secret", z.unknown()),
    ).rejects.toMatchObject({ kind: "invalid" });
    expect(fetcher).not.toHaveBeenCalled();
  });
});
