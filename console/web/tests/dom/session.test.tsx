import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { useLocation } from "react-router";
import { expect, it, vi } from "vitest";
import { z } from "zod";
import { api } from "@/api/client";
import { LoginPage } from "@/features/login";
import { renderConsole } from "../render";
import { apiUrl, server } from "../network/server";
import * as data from "../network/data";

function Location() { return <output aria-label="Current route">{useLocation().pathname}</output>; }

it.each(["/topics", "//example.invalid", "/\\example.invalid", "/login", "relative"])("validates the post-login destination %s", async (returnTo) => {
  server.use(http.get(apiUrl("/me"), () => new HttpResponse(null, { status: 401 })));
  const user = userEvent.setup();
  renderConsole(<><LoginPage /><Location /></>, "/login", "*", { returnTo });
  await waitFor(() => expect(screen.getByRole("button", { name: "Sign in" })).toBeEnabled());
  await user.type(screen.getByLabelText("Personal token"), "test-token");
  await user.click(screen.getByRole("button", { name: "Sign in" }));
  await screen.findByRole("heading", { name: "Current identity" });
  expect(screen.getByLabelText("Current route")).toHaveTextContent(returnTo === "/topics" ? "/topics" : "/");
});

it("validates login, keeps a rejected token out of the DOM/storage, signs in and signs out", async () => {
  server.use(http.get(apiUrl("/me"), () => new HttpResponse(null, { status: 401 })));
  let received: unknown;
  server.use(http.post(apiUrl("/session"), async ({ request }) => {
    received = await request.json();
    return HttpResponse.json({ code: "invalid_token" }, { status: 401 });
  }));
  const user = userEvent.setup();
  renderConsole(<><LoginPage /><Location /></>, "/login");
  const submit = screen.getByRole("button", { name: "Sign in" });
  await waitFor(() => expect(submit).toBeEnabled());
  await user.click(submit);
  expect(await screen.findByRole("alert")).toHaveTextContent("Enter your personal token.");
  await user.type(screen.getByLabelText("Personal token"), "rejected-test-token");
  await user.click(submit);
  expect(await screen.findByText(/The token is invalid or expired/)).toBeVisible();
  expect(received).toEqual({ token: "rejected-test-token" });
  expect(screen.getByLabelText("Personal token")).toHaveValue("");
  expect(JSON.stringify(localStorage)).not.toContain("rejected-test-token");
  expect(window.location.href).not.toContain("rejected-test-token");
  server.use(http.post(apiUrl("/session"), () => HttpResponse.json(data.session)));
  await user.type(screen.getByLabelText("Personal token"), "valid-test-token");
  await user.click(submit);
  expect(await screen.findByRole("heading", { name: "Current identity" })).toBeVisible();
  expect(screen.getByLabelText("Current route")).toHaveTextContent("/");
  await user.click(screen.getByRole("button", { name: "Sign out" }));
  expect(await screen.findByLabelText("Personal token")).toBeVisible();
  expect(screen.getByLabelText("Current route")).toHaveTextContent("/login");
});

it("keeps sign-in disabled while the initial session request is pending", async () => {
  let finish: (() => void) | undefined;
  const pending = new Promise<void>((resolve) => { finish = resolve; });
  server.use(http.get(apiUrl("/me"), async () => { await pending; return new HttpResponse(null, { status: 503 }); }));
  renderConsole(<LoginPage />);
  expect(screen.getByRole("button", { name: "Sign in" })).toBeDisabled();
  expect(screen.getByText(/The session API is not connected/)).toBeVisible();
  await act(async () => { finish?.(); });
  await waitFor(() => expect(screen.getByRole("button", { name: "Sign in" })).toBeEnabled());
  await userEvent.setup().click(screen.getByRole("button", { name: "Refresh" }));
});

it("clears protected data at session expiry without treating missing data as a new identity", async () => {
  const expires = Date.now() + 60_000;
  server.use(http.get(apiUrl("/me"), () => HttpResponse.json({ ...data.session, expiresAt: new Date(expires).toISOString() })));
  const view = renderConsole(<LoginPage />);
  await screen.findByRole("heading", { name: "Current identity" });
  view.client.setQueryData(["protected", 0, "topics"], "private");
  view.client.setQueryDefaults(["public"], { gcTime: Infinity });
  view.client.setQueryData(["public"], "retained");
  vi.useFakeTimers();
  act(() => view.client.setQueryData(["session"], { ...data.session, expiresAt: new Date(Date.now() + 60_000).toISOString() }));
  await act(async () => { await vi.advanceTimersByTimeAsync(60_001); });
  expect(view.session().session).toBeNull();
  expect(view.client.getQueryData(["protected", 0, "topics"])).toBeUndefined();
  expect(view.client.getQueryData(["public"])).toBe("retained");
  expect(screen.getByLabelText("Personal token")).toBeVisible();
});

it.each(["permissions", "namespace", "resourceGrants"] as const)("invalidates protected cache and in-flight responses when %s changes", async (field) => {
  const view = renderConsole(<LoginPage />);
  await screen.findByRole("heading", { name: "Current identity" });
  view.client.setQueryData(["protected", 0, "topic:orders"], "private");
  let finish: (() => void) | undefined;
  const pending = new Promise<void>((resolve) => { finish = resolve; });
  server.use(http.get(apiUrl("/private"), async () => { await pending; return HttpResponse.json("late private data"); }));
  const late = api.request("/api/v1/private", z.string(), { scope: "topic:orders" }).catch((e: unknown) => e);
  const changed = { ...data.session,
    ...(field === "permissions" ? { permissions: [] } : field === "namespace" ? { namespace: null } : { resourceGrants: [{ resourceKind: "topic", resourceId: data.ids.topic, tenantId: data.ids.tenant, namespaceId: data.ids.namespace, topicId: data.ids.topic, permissions: ["inspect"] }] }),
  };
  server.use(http.get(apiUrl("/me"), () => HttpResponse.json(changed)));
  const epoch = view.session().epoch;
  act(() => view.session().refresh());
  await waitFor(() => expect(view.session().epoch).toBeGreaterThan(epoch));
  expect(view.client.getQueryData(["protected", 0, "topic:orders"])).toBeUndefined();
  finish?.();
  expect(await late).toMatchObject({ kind: "stale" });
});

it("preserves logout failures for retry and refreshes CSRF before a second deletion", async () => {
  const user = userEvent.setup();
  let attempts = 0;
  let csrf: string | null = null;
  server.use(http.delete(apiUrl("/session"), ({ request }) => {
    attempts += 1;
    csrf = request.headers.get("X-CSRF-Token");
    return attempts === 1 ? new HttpResponse(null, { status: 503 }) : new HttpResponse(null, { status: 204 });
  }));
  const view = renderConsole(<LoginPage />);
  await user.click(await screen.findByRole("button", { name: "Sign out" }));
  expect(await screen.findByText(/Local protected data has been cleared/)).toBeVisible();
  expect(view.session().session).toBeNull();
  server.use(http.get(apiUrl("/me"), () => HttpResponse.json({ ...data.session, csrfToken: "new-csrf" })));
  await user.click(screen.getByRole("button", { name: "Retry sign out" }));
  await waitFor(() => expect(attempts).toBe(2));
  expect(csrf).toBe("new-csrf");
  await waitFor(() => expect(screen.queryByRole("button", { name: "Retry sign out" })).not.toBeInTheDocument());
});

it("treats an already-expired server session as a successful local logout", async () => {
  server.use(http.delete(apiUrl("/session"), () => new HttpResponse(null, { status: 401 })));
  const view = renderConsole(<LoginPage />);
  await screen.findByRole("heading", { name: "Current identity" });
  await act(async () => { await view.session().logout(); });
  expect(view.session().session).toBeNull();
  expect(view.session().logoutFailure).toBeNull();
});

it("rejects concurrent sign-ins and does not submit the second credential", async () => {
  let finish: (() => void) | undefined;
  const pending = new Promise<void>((resolve) => { finish = resolve; });
  let count = 0;
  server.use(http.post(apiUrl("/session"), async () => { count++; await pending; return HttpResponse.json(data.session); }));
  const view = renderConsole(<LoginPage />);
  await screen.findByRole("heading", { name: "Current identity" });
  let first: Promise<void> | undefined;
  await act(async () => {
    first = view.session().login("first");
    await expect(view.session().login("second")).rejects.toMatchObject({ code: "login_in_progress" });
  });
  await waitFor(() => expect(count).toBe(1));
  expect(view.session().authenticating).toBe(true);
  await act(async () => { finish?.(); await first; });
  expect(view.session().authenticating).toBe(false);
});

it("receives cross-tab logout/login markers, clears protected cache, and broadcasts no secrets", async () => {
  class Channel {
    static all: Channel[] = [];
    onmessage: ((event: MessageEvent) => void) | null = null;
    sent: unknown[] = [];
    closed = false;
    name: string;
    constructor(name: string) { this.name = name; Channel.all.push(this); }
    postMessage(message: unknown) { this.sent.push(message); }
    close() { this.closed = true; }
  }
  vi.stubGlobal("BroadcastChannel", Channel);
  const view = renderConsole(<LoginPage />);
  await screen.findByRole("heading", { name: "Current identity" });
  const channel = Channel.all.at(0)!;
  view.client.setQueryData(["protected", 0, "topics"], "private");
  act(() => channel.onmessage?.(new MessageEvent("message", { data: "irrelevant" })));
  expect(view.session().session).not.toBeNull();
  act(() => channel.onmessage?.(new MessageEvent("message", { data: "logout" })));
  await waitFor(() => expect(view.session().session).toBeNull());
  expect(view.client.getQueryData(["protected", 0, "topics"])).toBeUndefined();
  act(() => channel.onmessage?.(new MessageEvent("message", { data: "login" })));
  await screen.findByRole("heading", { name: "Current identity" });
  await act(async () => { await view.session().login("private-token"); await view.session().logout(); });
  expect(channel.sent).toEqual(["logout", "login", "logout"]);
  view.unmount();
  expect(channel.closed).toBe(true);
});
