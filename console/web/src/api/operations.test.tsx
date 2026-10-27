import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter, Route, Routes } from "react-router";
import { beforeEach, expect, it, vi } from "vitest";
import { ApiError } from "./client";
import type { LocalOperation } from "./operations";
import { redactDeniedOperations } from "./operation-access";
import type { Operation } from "./types";
import { OperationPage } from "@/features/operations";

const state = vi.hoisted(() => ({
  entries: {} as Record<string, LocalOperation>,
  denied: new Set<string>(),
  data: undefined as Operation | undefined,
  error: null as ApiError | null,
}));

vi.mock("./session", () => ({
  useSession: () => ({ session: {}, denied: state.denied, refresh: () => undefined }),
}));
vi.mock("./query", () => ({
  useResource: () => ({
    data: state.data,
    error: state.error,
    isPending: false,
    isFetching: false,
    isError: Boolean(state.error),
    refetch: () => undefined,
  }),
}));
vi.mock("./operations", async (original) => ({
  ...(await original<typeof import("./operations")>()),
  useOperations: () => ({ entries: state.entries, forgetSecret: () => undefined }),
}));

const id = "00000000-0000-7000-8000-000000000001";
const result: Operation = {
  id,
  actor: "private-actor",
  target: "private-target",
  scope: "private-scope",
  status: "succeeded",
  receipt: "private-receipt",
  reason: null,
  resourceId: "private-resource",
  secretUnavailable: false,
  observedAt: "2026-10-27T00:00:00Z",
  source: "metadataLinearizable",
  partial: false,
  revision: null,
};
const entry: LocalOperation = {
  phase: "result",
  result,
  secret: "one-time-token-secret",
  error: new ApiError("unavailable", "private-error"),
  returnTo: "/identities",
  resourceTo: "/identities?principalId=private-resource",
  epoch: 3,
};

beforeEach(() => {
  state.entries = { [id]: entry };
  state.denied = new Set();
  state.data = undefined;
  state.error = null;
});

function page() {
  return renderToStaticMarkup(
    <MemoryRouter initialEntries={[`/operations/${id}`]}>
      <Routes>
        <Route path="/operations/:id" element={<OperationPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

it("permanently scrubs denied operation data while keeping safe recovery identity and unrelated operations", () => {
  const entries = { [id]: entry, unrelated: entry };
  const denied = new Set([`operation:${id}`]);
  const redacted = redactDeniedOperations(entries, denied);
  expect(redacted[id]).toEqual({ phase: "recover", returnTo: "/identities", epoch: 3 });
  expect(redacted.unrelated).toBe(entry);
  expect(entries[id]).toBe(entry);
  expect(redactDeniedOperations(redacted, denied)).toBe(redacted);
  expect(redactDeniedOperations(redacted, new Set())[id]).toEqual(redacted[id]);
  const late = redactDeniedOperations({ ...redacted, [id]: entry }, denied);
  expect(late[id]).toEqual(redacted[id]);
});

it.each(["scope", "response"])("hides cached receipts and secrets immediately on %s denial before stored cleanup", (kind) => {
  state.data = result;
  if (kind === "scope") state.denied.add(`operation:${id}`);
  else state.error = new ApiError("forbidden", "permission_denied", 403);
  const html = page();
  for (const value of [result.actor, result.target, result.receipt, entry.secret, entry.resourceTo]) {
    expect(html).not.toContain(value);
  }
  expect(html).not.toContain("Copy token");
  expect(html).toContain(id);
  expect(html).toContain('href="/identities"');
});

it("keeps the first response only during temporary unavailability", () => {
  state.error = new ApiError("unavailable", "upstream_unavailable", 503);
  const html = page();
  expect(html).toContain("private-receipt");
  expect(html).toContain("one-time-token-secret");
  expect(html).toContain("Stale");
  state.error = new ApiError("invalid", "invalid_response", 400);
  const invalid = page();
  expect(invalid).not.toContain("private-receipt");
  expect(invalid).not.toContain("one-time-token-secret");
});
