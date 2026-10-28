import { test, expect } from "./fixture";
import { ids, operation, localSession, session as remoteSession } from "../../tests/network/data";

test.describe("mocked network UI interactions", () => {
  test("shows local execution without Raft state and disables cluster-only controls", async ({ page, mockNetwork }, testInfo) => {
    mockNetwork.identity = structuredClone(localSession);
    await page.goto("/cluster?nodeId=1&groupId=1");
    await expect(page.getByRole("heading", { name: "Local process", exact: true })).toBeVisible();
    await expect(page.getByRole("table", { name: "Local process state" })).toContainText("ready");
    for (const name of ["Replica membership", "Node migration", "Raft diagnostics"])
      await expect(page.getByRole("button", { name, exact: true })).toBeDisabled();
    await expect(page.getByRole("table", { name: "Sampled data groups" })).toHaveCount(0);
    await expect(page.getByRole("columnheader", { name: "Leader node" })).toHaveCount(0);
    await expect(page.getByText("Logical shard business controls remain available.")).toBeVisible();
    await page.screenshot({ path: testInfo.outputPath("local-process.png"), fullPage: true });
    const nav = page.getByRole("navigation", { name: "Main navigation" });
    await nav.getByRole("link", { name: "Topics", exact: true }).click();
    await expect(page.getByRole("button", { name: "Create topic", exact: true })).toBeEnabled();
    expect(mockNetwork.writes).toEqual([]);
    mockNetwork.identity = structuredClone(remoteSession);
    await page.goto("/cluster?nodeId=1");
    await expect(page.getByRole("table", { name: "Sampled data groups" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "Leader node" })).toBeVisible();
    await page.screenshot({ path: testInfo.outputPath("remote-cluster.png"), fullPage: true });
  });
  test("validates login and rejects a bad token, signs in, navigates and signs out", async ({ page, mockNetwork }) => {
    mockNetwork.authenticated = false;
    await page.goto("/login");
    await expect(page.getByRole("button", { name: "Sign in", exact: true })).toBeEnabled();
    await page.getByRole("button", { name: "Sign in", exact: true }).click();
    await expect(page.getByRole("alert")).toHaveText("Enter your personal token.");
    await page.getByLabel("Personal token", { exact: true }).fill("bad-token");
    await page.getByRole("button", { name: "Sign in", exact: true }).click();
    await expect(page.getByText(/The token is invalid or expired/)).toBeVisible();
    await expect(page.getByLabel("Personal token", { exact: true })).toHaveValue("");
    await page.getByLabel("Personal token", { exact: true }).fill("synthetic-test-token");
    await page.getByRole("button", { name: "Sign in", exact: true }).click();
    await expect(page.getByRole("heading", { name: "Overview", exact: true })).toBeVisible();
    const nav = page.getByRole("navigation", { name: "Main navigation" });
    await nav.getByRole("link", { name: "Topics", exact: true }).click();
    await expect(page.getByRole("link", { name: "Orders", exact: true })).toBeVisible();
    await nav.getByRole("link", { name: "Consumers", exact: true }).click();
    await expect(page.getByRole("link", { name: "Order processors" })).toBeVisible();
    await page.getByRole("link", { name: "Test operator", exact: true }).click();
    await page.getByRole("button", { name: "Sign out", exact: true }).click();
    await expect(page.getByLabel("Personal token", { exact: true })).toBeVisible();
    expect(await page.evaluate(() => JSON.stringify(localStorage))).not.toContain("synthetic-test-token");
  });

  test("makes the background inert, supports keyboard navigation and Escape, and restores focus", async ({ page }) => {
    await page.goto("/topics");
    const open = page.getByRole("button", { name: "Create topic", exact: true });
    await open.focus();
    await page.keyboard.press("Enter");
    const dialog = page.getByRole("dialog", { name: "Create topic", exact: true });
    await expect(dialog).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Close", exact: true })).toBeFocused();
    await open.evaluate((element) => element.focus());
    await expect(dialog.getByRole("button", { name: "Close", exact: true })).toBeFocused();
    await dialog.getByLabel("Topic name").focus();
    await page.keyboard.press("Tab");
    await expect(dialog.getByLabel("Tenant UUID")).toBeFocused();
    await page.keyboard.press("Shift+Tab");
    await expect(dialog.getByLabel("Topic name")).toBeFocused();
    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();
    await expect(open).toBeFocused();
  });

  test("paginates topics and filters a bounded message range with private filters", async ({ page, mockNetwork }) => {
    await page.goto("/topics");
    await page.getByRole("button", { name: "Next page" }).click();
    await expect(page.getByRole("link", { name: "Invoices", exact: true })).toBeVisible();
    await page.getByRole("button", { name: "Previous", exact: true }).click();
    await page.getByRole("link", { name: "Orders", exact: true }).click();
    await page.getByRole("button", { name: "Messages", exact: true }).click();
    await expect(page.getByText("message-42", { exact: true })).toBeVisible();
    await page.getByLabel("Start offset (inclusive)").fill("40");
    await page.getByLabel("End offset (exclusive)").fill("50");
    await page.getByText(/Exact key \/ property filters/).click();
    await page.getByRole("checkbox", { name: "Match exact key (including empty)" }).check();
    await page.getByLabel("Exact key value").fill("private-filter");
    await page.getByRole("button", { name: "Inspect range" }).click();
    await expect.poll(() => mockNetwork.scans.at(-1)).toMatchObject({ startOffset: "40", endOffset: "50", shard: ids.shard, keyBase64: "cHJpdmF0ZS1maWx0ZXI=", limit: 32 });
    expect(page.url()).not.toContain("private-filter");
    await page.getByRole("button", { name: "42", exact: true }).click();
    const preview = page.getByRole("region", { name: "Message content", exact: true });
    await preview.getByLabel("Payload format").selectOption("json");
    await expect(preview).toContainText('"order": 42');
    await preview.getByRole("button", { name: "Close", exact: true }).click();
    await page.getByRole("button", { name: "Continue", exact: true }).click();
    await expect.poll(() => mockNetwork.scans.at(-1)?.observedEnd).toBe("100");
  });

  test("recovers a lost write under the same ID and blocks another operation until resolved", async ({ page, mockNetwork }) => {
    mockNetwork.writeMode = "lost";
    await page.goto("/topics");
    await page.getByRole("button", { name: "Create topic", exact: true }).click();
    const dialog = page.getByRole("dialog");
    await dialog.getByLabel("Topic name").fill("Invoices");
    await dialog.getByLabel("Namespace UUID").fill(ids.namespace);
    await dialog.getByRole("button", { name: "Confirm and submit" }).click();
    await expect(page.getByText(/The submission result is unknown/)).toBeVisible();
    const id = String(mockNetwork.writes.at(0)?.operationId);
    await expect(page).toHaveURL(new RegExp(`/operations/${id}$`));
    await page.getByRole("link", { name: "Return to resource" }).click();
    await page.getByRole("button", { name: "Create topic", exact: true }).click();
    await expect(page.getByRole("button", { name: "Confirm and submit" })).toBeDisabled();
    await page.getByRole("link", { name: /Recover the previous result/ }).click();
    mockNetwork.operations.set(id, operation(id));
    await page.getByRole("button", { name: "Refresh", exact: true }).click();
    await expect(page.getByText("Succeeded", { exact: true })).toBeVisible();
    expect(mockNetwork.writes).toHaveLength(1);
  });

  test("previews inherited access and reports a revision conflict without replacing state", async ({ page, mockNetwork }) => {
    mockNetwork.writeMode = "conflict";
    await page.goto("/identities");
    await page.getByRole("button", { name: "Edit", exact: true }).click();
    await page.getByRole("checkbox", { name: "produce", exact: true }).uncheck();
    await page.getByRole("checkbox", { name: "consume", exact: true }).check();
    await expect(page.getByRole("dialog")).toContainText("Still inherited");
    await page.getByRole("button", { name: "Confirm and submit" }).click();
    await expect(page.getByText(/The resource version changed/)).toBeVisible();
    expect(mockNetwork.writes.at(0)).toMatchObject({ permissions: ["consume"], expectedVersion: "7" });
  });

  test("redacts cached messages immediately after a forbidden read", async ({ page, mockNetwork }) => {
    await page.goto(`/topics/${ids.topic}?tab=messages`);
    await page.getByRole("button", { name: "42", exact: true }).click();
    await expect(page.getByRole("region", { name: "Message content", exact: true })).toContainText("order");
    mockNetwork.denied.add(`/topics/${ids.topic}/messages`);
    await page.getByRole("button", { name: "Latest window" }).click();
    await expect(page.getByText(/You do not have access to this resource/)).toBeVisible();
    await expect(page.getByRole("region", { name: "Message content", exact: true })).toHaveCount(0);
    await expect(page.getByText("message-42", { exact: true })).toHaveCount(0);
  });

  test("keeps stale observations visible during service failure and clears identity on expiry", async ({ page, mockNetwork }) => {
    mockNetwork.identity.expiresAt = new Date(Date.now() + 120_000).toISOString();
    await page.clock.install();
    await page.goto("/");
    await expect(page.getByText("Queue needs attention", { exact: true })).toBeVisible();
    mockNetwork.overviewStatus = 503;
    await page.getByRole("button", { name: "Refresh", exact: true }).click();
    await expect(page.getByText(/Stale observation/).first()).toBeVisible();
    await expect(page.getByText("Queue needs attention", { exact: true })).toBeVisible();
    await page.clock.fastForward(120_001);
    await expect(page.getByText("Not authenticated", { exact: true })).toBeVisible();
    await expect(page.getByText("Queue needs attention", { exact: true })).toHaveCount(0);
  });

  test("one-time secrets disappear after leaving the result page", async ({ page }) => {
    await page.goto("/identities");
    await page.getByRole("button", { name: "Issue token", exact: true }).click();
    await page.getByRole("button", { name: "Confirm and submit" }).click();
    const resultUrl = page.url();
    await expect(page.getByLabel("New token secret")).toHaveValue("synthetic-one-time-token");
    await page.getByRole("button", { name: "Show", exact: true }).click();
    await expect(page.getByLabel("New token secret")).toHaveAttribute("type", "text");
    await page.getByRole("link", { name: "Return to resource" }).click();
    await page.goto(resultUrl);
    await expect(page.getByText(/The original token secret cannot be retrieved again/)).toBeVisible();
    await expect(page.getByLabel("New token secret")).toHaveCount(0);
  });

  test("mobile navigation closes with Escape and language preference survives reload", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto("/");
    const toggle = page.getByRole("button", { name: "Toggle navigation" });
    await toggle.click();
    await expect(toggle).toHaveAttribute("aria-expanded", "true");
    await page.keyboard.press("Escape");
    await expect(toggle).toHaveAttribute("aria-expanded", "false");
    await expect(toggle).toBeFocused();
    await page.getByLabel("Language", { exact: true }).selectOption("zh-CN");
    await page.reload();
    await expect(page.getByRole("heading", { name: "总览", exact: true })).toBeVisible();
    await expect(page.locator("html")).toHaveAttribute("lang", "zh-CN");
  });
});
