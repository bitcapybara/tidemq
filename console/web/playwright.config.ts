import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e/mock-network",
  fullyParallel: true,
  forbidOnly: Boolean(process.env.CI),
  failOnFlakyTests: Boolean(process.env.CI),
  retries: process.env.CI ? 1 : 0,
  workers: process.env.CI ? 2 : undefined,
  timeout: 30_000,
  outputDir: "test-results/mock-network",
  reporter: [
    ["list"],
    ["html", { outputFolder: "playwright-report/mock-network", open: "never" }],
    ["json", { outputFile: "playwright-report/mock-network/results.json" }],
  ],
  use: {
    baseURL: "http://127.0.0.1:4173",
    locale: "en-US",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
    { name: "firefox", use: { ...devices["Desktop Firefox"] } },
    { name: "webkit", use: { ...devices["Desktop Safari"] } },
  ],
  webServer: {
    command: "npm run preview -- --host 127.0.0.1 --port 4173 --strictPort",
    url: "http://127.0.0.1:4173",
    reuseExistingServer: false,
  },
});
