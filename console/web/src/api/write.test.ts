import { describe, expect, it, vi } from "vitest";
import { ApiClient } from "./client";
import { operationSchema } from "./types";
import { submitOperation } from "./write";
import { operationId } from "@/features/logic";

const receipt = (id: string) => ({
  id,
  actor: "actor-id",
  target: "tenant/topic",
  scope: "topic-id",
  status: "succeeded",
  receipt: "message-id",
  reason: null,
  resourceId: null,
  secretUnavailable: false,
  source: "core",
  observedAtMs: "1700000000000",
  partial: false,
});

describe("original operation recovery", () => {
  it("preserves an operation link before sending bytes and recovers a lost reply with a read only", async () => {
    const id = operationId(1700000000000, new Uint8Array(16));
    const fetcher = vi
      .fn<typeof fetch>()
      .mockRejectedValueOnce(new TypeError("Connection lost after submission"))
      .mockResolvedValueOnce(new Response(JSON.stringify(receipt(id))));
    const client = new ApiClient(fetcher);
    client.setCsrfToken("csrf-test");
    const preserved: string[] = [];
    await expect(
      submitOperation(
        {
          id,
          path: "/api/v1/topics/topic-id/send",
          method: "POST",
          body: { payloadBase64: "aGVsbG8=" },
          beforeSubmit: (value) => {
            expect(fetcher).not.toHaveBeenCalled();
            preserved.push(`/operations/${value}`);
          },
        },
        client,
      ),
    ).rejects.toMatchObject({ kind: "unavailable" });
    expect(preserved).toEqual([`/operations/${id}`]);
    await expect(
      client.request(`/api/v1/operations/${id}`, operationSchema),
    ).resolves.toMatchObject({ id, status: "succeeded" });
    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(fetcher.mock.calls.map(([, options]) => options?.method)).toEqual([
      "POST",
      "GET",
    ]);
    expect(
      JSON.parse(
        String(fetcher.mock.calls.map(([, options]) => options?.body).at(0)),
      ),
    ).toEqual({ payloadBase64: "aGVsbG8=", operationId: id });
  });

  it("keeps a newly issued secret out of result-query data and rejects a mismatched receipt", async () => {
    const id = operationId(1700000000000, new Uint8Array(16));
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ ...receipt(id), secret: "first-response-only" }),
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ ...receipt(id), secret: "unexpected-extra" }),
        ),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify(receipt("different-id"))),
      );
    const client = new ApiClient(fetcher);
    client.setCsrfToken("csrf-test");
    const input = {
      id,
      path: "/api/v1/credentials",
      method: "POST" as const,
      body: { principalId: "principal-id", lifetimeMs: "60000" },
      beforeSubmit: vi.fn(),
    };
    await expect(submitOperation(input, client)).resolves.toMatchObject({
      secret: "first-response-only",
    });
    const queried = await client.request(
      `/api/v1/operations/${id}`,
      operationSchema,
    );
    expect(queried).not.toHaveProperty("secret");
    await expect(submitOperation(input, client)).rejects.toMatchObject({
      code: "operation_identity_mismatch",
    });
  });

  it("does not retry a stale-version rejection or generate another identity", async () => {
    const id = operationId();
    const fetcher = vi.fn<typeof fetch>().mockResolvedValueOnce(
      new Response(JSON.stringify({ code: "operation_content_conflict" }), {
        status: 409,
      }),
    );
    const client = new ApiClient(fetcher);
    client.setCsrfToken("csrf-test");
    await expect(
      submitOperation(
        {
          id,
          path: "/api/v1/grants",
          method: "PUT",
          body: { expectedVersion: "3", permissions: [] },
          beforeSubmit: vi.fn(),
        },
        client,
      ),
    ).rejects.toMatchObject({ kind: "conflict", status: 409 });
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(
      JSON.parse(
        String(fetcher.mock.calls.map(([, options]) => options?.body).at(0)),
      ).operationId,
    ).toBe(id);
  });
});
