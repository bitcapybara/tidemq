import { fileURLToPath, URL } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } },
  test: {
    environment: "jsdom",
    environmentOptions: { jsdom: { url: "http://localhost:5173" } },
    include: ["src/**/*.test.{ts,tsx}", "tests/dom/**/*.test.{ts,tsx}"],
    setupFiles: ["./tests/setup.ts"],
    restoreMocks: true,
    coverage: {
      provider: "v8",
      include: ["src/**/*.{ts,tsx}"],
      exclude: ["src/**/*.test.{ts,tsx}", "src/**/*.d.ts"],
      reportsDirectory: process.env.COVERAGE_DIR ?? "coverage/unit-integration",
      reporter: ["text", "html", "lcov", "json", "json-summary"],
      reportOnFailure: true,
      thresholds: {
        lines: 80,
        statements: 80,
        functions: 80,
        branches: 70,
        "src/api/{client,session,query,operations,operation-access,write}.{ts,tsx}": {
          lines: 90,
          branches: 80,
          perFile: true,
        },
        "src/features/{identities,logic,login,permissions,operations}.{ts,tsx}": {
          lines: 90,
          branches: 80,
          perFile: true,
        },
        "src/components/forms.tsx": { lines: 90, branches: 80 },
      },
    },
  },
});
