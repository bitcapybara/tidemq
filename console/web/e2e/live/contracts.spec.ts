import { test, expect } from "@playwright/test";
import { randomUUID } from "node:crypto";
import type { Operation, Session, Topic } from "../../src/api/types";
import { operationId as newOperationId } from "../../src/features/logic";
import { liveContext, login, request } from "./context";

test("real Rust backend: a restricted identity cannot read another namespace or tenant", async ({ page }) => {
  const seed = await liveContext();
  await login(page, seed.restricted.tokenFile);
  const allowed = await request<Topic>(page, "GET", `/api/v1/topics/${seed.allowedTopicId}`);
  expect(allowed.status).toBe(200);
  expect(allowed.value.id).toBe(seed.allowedTopicId);
  for (const id of [seed.sameTenantDeniedTopicId, seed.otherTenantTopicId]) {
    const refused = await request(page, "GET", `/api/v1/topics/${id}`);
    expect(refused.status).toBe(403);
    expect("id" in refused.value).toBe(false);
  }
  const listing = await request<{ items: Topic[] }>(page, "GET", "/api/v1/topics");
  expect(listing.status).toBe(200);
  expect(listing.value.items.some(item => item.id === seed.allowedTopicId)).toBe(true);
  expect(listing.value.items.some(item => [seed.sameTenantDeniedTopicId, seed.otherTenantTopicId].includes(item.id))).toBe(false);
});

test("real Rust backend: revocation invalidates an existing Cookie and clears protected UI", async ({ page, browser, browserName, baseURL }) => {
  const seed = await liveContext();
  const credential = seed.revocable[browserName];
  if (!credential) throw new Error("The browser requires an independent revocable credential");
  await login(page, credential.tokenFile);
  await page.getByRole("navigation", { name: "Main navigation" }).getByRole("link", { name: "Topics", exact: true }).click();
  await expect(page.getByRole("table", { name: "Authorized topics" })).toBeVisible();
  const protectedTopic = page.locator(`a[href="/topics/${seed.allowedTopicId}"]`);
  await expect(protectedTopic).toBeVisible();
  const originalCookie = (await page.context().cookies()).filter(cookie => cookie.httpOnly).map(cookie => cookie.value).join(":");
  expect(originalCookie.length > 0).toBe(true);
  const admin = await browser.newContext({ baseURL, ignoreHTTPSErrors: true });
  try {
    const root = await admin.newPage();
    await login(root);
    const revoked = await request<Operation>(root, "DELETE", `/api/v1/credentials/${credential.credentialId}`, { operationId: newOperationId() });
    expect(revoked.status).toBe(200);
    expect(revoked.value.status).toBe("succeeded");
    const retainedCookie = (await page.context().cookies()).filter(cookie => cookie.httpOnly).map(cookie => cookie.value).join(":");
    expect(retainedCookie === originalCookie).toBe(true);
    expect((await request(page, "GET", "/api/v1/me")).status).toBe(401);
    const denied = page.waitForResponse(response => response.url().includes("/api/v1/topics") && response.status() === 401);
    await page.getByRole("button", { name: "Refresh", exact: true }).click();
    await denied;
    await expect(protectedTopic).toHaveCount(0);
    await page.getByRole("link", { name: "Sign in", exact: true }).click();
    await expect(page.getByLabel("Personal token", { exact: true })).toBeVisible();
  } finally {
    await admin.close();
  }
});

test("real Rust backend: missing or forged CSRF and a foreign Origin cause no mutation", async ({ page, browserName }) => {
  const seed = await liveContext();
  await login(page);
  const before = await request<{ items: Topic[]; nextCursor: string | null }>(page, "GET", "/api/v1/topics?limit=32");
  expect(before.status).toBe(200);
  expect(before.value.nextCursor).toBeNull();
  const ids = before.value.items.map(item => item.id).sort();
  for (const headers of [{ csrf: null }, { csrf: "forged-csrf" }, { origin: "https://foreign.invalid" }]) {
    const refused = await request(page, "POST", "/api/v1/topics", {
      operationId: newOperationId(), tenantId: seed.tenantId, namespaceId: seed.namespaceId,
      name: `csrf-${browserName}-${randomUUID()}`,
    }, headers);
    expect(refused.status).toBe(403);
    expect(refused.value.code).toBe(headers.origin ? "origin_rejected" : "csrf_rejected");
    const after = await request<{ items: Topic[]; nextCursor: string | null }>(page, "GET", "/api/v1/topics?limit=32");
    expect(after.status).toBe(200);
    expect(after.value.nextCursor).toBeNull();
    expect(after.value.items.map(item => item.id).sort()).toEqual(ids);
  }
});

test("real Rust backend: a committed response is lost and Unknown recovers only its original operation", async ({ page, browserName }) => {
  const seed = await liveContext();
  await login(page);
  let writes = 0;
  let committed = false;
  let operationId = "";
  let resourceId = "";
  const queries: string[] = [];
  let release = () => {};
  const recovery = new Promise<void>(resolve => { release = resolve; });
  await page.route("**/api/v1/operations/*", async route => {
    queries.push(new URL(route.request().url()).pathname.split("/").at(-1) ?? "");
    await recovery;
    await route.continue();
  });
  await page.route("**/api/v1/topics", async route => {
    if (route.request().method() !== "POST") return route.continue();
    writes++;
    operationId = (route.request().postDataJSON() as { operationId: string }).operationId;
    try {
      const response = await route.fetch({ maxRetries: 0 });
      const result = await response.json() as Operation;
      committed = response.status() === 200 && result.status === "succeeded" && result.id === operationId;
      resourceId = result.resourceId ?? "";
    } finally {
      await route.abort("failed");
    }
  });
  try {
    await page.goto("/topics");
    await page.getByRole("button", { name: "Create topic", exact: true }).click();
    const dialog = page.getByRole("dialog", { name: "Create topic", exact: true });
    await dialog.getByLabel("Topic name").fill(`unknown-${browserName}-${Date.now()}`);
    await dialog.getByLabel("Tenant UUID").fill(seed.tenantId);
    await dialog.getByLabel("Namespace UUID").fill(seed.namespaceId);
    await dialog.getByRole("button", { name: "Confirm and submit" }).click();
    await expect(page.getByText(/The submission result is unknown/)).toBeVisible();
    expect(committed).toBe(true);
    expect(resourceId.length > 0).toBe(true);
    await expect(page).toHaveURL(new RegExp(`/operations/${operationId}$`));
    release();
    await expect(page.getByText("Succeeded", { exact: true })).toBeVisible();
    await page.reload();
    await expect(page.getByText("Succeeded", { exact: true })).toBeVisible();
    await expect(page).toHaveURL(new RegExp(`/operations/${operationId}$`));
    expect(writes).toBe(1);
    expect(queries.length > 0 && queries.every(id => id === operationId)).toBe(true);
    const result = await request<Operation>(page, "GET", `/api/v1/operations/${operationId}`);
    expect(result.value.status).toBe("succeeded");
    expect(result.value.resourceId).toBe(resourceId);
    expect((await request(page, "GET", `/api/v1/topics/${resourceId}`)).status).toBe(200);
  } finally {
    release();
  }
});

test("real Rust backend: execution facts and cluster capabilities match the actual topology", async ({ page }) => {
  const seed = await liveContext();
  await login(page);
  const session = await request<Pick<Session, "capabilities">>(page, "GET", "/api/v1/me");
  expect(session.status).toBe(200);
  await page.goto("/cluster");
  if (seed.topology === "combined") {
    expect(session.value.capabilities.execution).toBe("local");
    for (const key of ["clusterAdministration", "replicaMembership", "nodeMigration", "raftDiagnostics"] as const)
      expect(session.value.capabilities[key]).toBe(false);
    await expect(page.getByRole("heading", { name: "Local process", exact: true })).toBeVisible();
    await expect(page.getByRole("table", { name: "Local process state" })).toContainText("ready");
    for (const name of ["Replica membership", "Node migration", "Raft diagnostics"])
      await expect(page.getByRole("button", { name, exact: true })).toBeDisabled();
    await expect(page.getByRole("table", { name: "Sampled data groups" })).toHaveCount(0);
    await expect(page.getByRole("columnheader", { name: "Leader node" })).toHaveCount(0);
    expect((await request(page, "POST", "/api/v1/cluster/drain", { operationId: newOperationId() })).status).toBe(501);
  } else {
    expect(session.value.capabilities.execution).toBe("remote");
    expect(session.value.capabilities.raftDiagnostics).toBe(true);
    await expect(page.getByRole("heading", { name: "Cluster", exact: true })).toBeVisible();
    await expect(page.getByRole("table", { name: "Local process state" })).toHaveCount(0);
    await expect(page.getByRole("table", { name: "Registered nodes" })).toContainText("41");
  }
});
