import { defineConfig, devices } from "@playwright/test";
import path from "node:path";
import { fileURLToPath } from "node:url";

const baseURL = process.env.PLAYWRIGHT_BASE_URL || "http://127.0.0.1:4174";
const apiBase = process.env.PLAYWRIGHT_API_BASE || "http://127.0.0.1:8015";
const configDir = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  workers: 1,
  timeout: 30 * 60 * 1000,
  expect: {
    timeout: 60 * 1000,
  },
  reporter: [["list"], ["html", { open: "never" }]],
  use: {
    baseURL,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
      },
    },
  ],
  webServer: {
    command: "npm.cmd run dev -- --host 127.0.0.1 --port 4174",
    cwd: configDir,
    url: baseURL,
    reuseExistingServer: !process.env.CI,
    env: {
      ...process.env,
      VITE_PROXY_API_TARGET: apiBase,
    },
    timeout: 2 * 60 * 1000,
  },
});
