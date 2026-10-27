import { defineConfig, devices } from "@playwright/test";

if (!process.env.TIDEMQ_LIVE_BASE_URL || !process.env.TIDEMQ_LIVE_TOKEN_FILE)
  throw new Error("Live backend smoke requires TIDEMQ_LIVE_BASE_URL and TIDEMQ_LIVE_TOKEN_FILE from the Rust fixture");

export default defineConfig({
  testDir: "./e2e/live",
  forbidOnly: Boolean(process.env.CI),
  workers: 1,
  retries: 0,
  reporter: [
    ["list"],
    ["html", { outputFolder: "playwright-report/live", open: "never" }],
    ["json", { outputFile: "playwright-report/live/results.json" }],
  ],
  outputDir: "test-results/live",
  use: {
    baseURL: process.env.TIDEMQ_LIVE_BASE_URL,
    locale: "en-US",
    ignoreHTTPSErrors: true,
    // Live login uses a private bootstrap credential; artifacts must not record it.
    trace: "off",
    screenshot: "off",
    video: "off",
  },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
    { name: "firefox", use: { ...devices["Desktop Firefox"] } },
    { name: "webkit", use: { ...devices["Desktop Safari"] } },
  ],
});
