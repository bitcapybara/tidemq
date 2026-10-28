import { expect, type Page } from "@playwright/test";
import { readFile } from "node:fs/promises";
import type { Session } from "../../src/api/types";

export interface LiveContext {
  topology: "combined" | "separated-rf1";
  tenantId: string;
  namespaceId: string;
  allowedTopicId: string;
  sameTenantDeniedTopicId: string;
  otherTenantTopicId: string;
  restricted: { tokenFile: string; credentialId: string };
  revocable: Record<string, { tokenFile: string; credentialId: string }>;
}

export async function liveContext(): Promise<LiveContext> {
  const file = process.env.TIDEMQ_LIVE_CONTEXT_FILE;
  if (!file) throw new Error("The native live fixture context is required");
  return JSON.parse(await readFile(file, "utf8")) as LiveContext;
}

export async function login(page: Page, file = process.env.TIDEMQ_LIVE_TOKEN_FILE) {
  if (!file) throw new Error("The private live token file is required");
  await page.goto("/login");
  await page.getByLabel("Personal token", { exact: true }).fill((await readFile(file, "utf8")).trim());
  const reply = page.waitForResponse(response => response.url().endsWith("/api/v1/session") && response.request().method() === "POST");
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  expect((await reply).status()).toBe(200);
  await expect(page.getByRole("navigation", { name: "Main navigation" })).toBeVisible();
}

export async function request<T = Record<string, unknown>>(
  page: Page, method: string, resource: string, data?: object,
  overrides: { csrf?: string | null; origin?: string } = {},
): Promise<{ status: number; value: T }> {
  const headers: Record<string, string> = { origin: overrides.origin ?? new URL(page.url()).origin };
  if (method !== "GET") {
    const me = await request<Pick<Session, "csrfToken">>(page, "GET", "/api/v1/me");
    const csrf = overrides.csrf === undefined ? me.value.csrfToken : overrides.csrf;
    if (csrf !== null) headers["x-csrf-token"] = csrf;
  }
  try {
    const reply = await page.request.fetch(resource, { method, data, headers, timeout: 15_000 });
    return { status: reply.status(), value: await reply.json() as T };
  } catch {
    // Playwright transport errors can include private request headers.
    throw new Error("The native HTTPS fixture request failed");
  }
}
