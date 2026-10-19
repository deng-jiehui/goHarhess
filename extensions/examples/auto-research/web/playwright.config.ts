import { defineConfig } from "@playwright/test";

const port = Number(process.env.AUTORESEARCH_E2E_PORT || 48781);

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  expect: { timeout: 7_000 },
  workers: 1,
  reporter: "list",
  use: {
    baseURL: `http://127.0.0.1:${port}`,
    browserName: "chromium",
    headless: false,
    viewport: { width: 1440, height: 960 },
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    launchOptions: {
      executablePath: process.env.AUTORESEARCH_BROWSER_PATH || undefined,
    },
  },
  webServer: {
    command: "node scripts/test-server.mjs",
    url: `http://127.0.0.1:${port}`,
    timeout: 120_000,
    reuseExistingServer: false,
  },
});
