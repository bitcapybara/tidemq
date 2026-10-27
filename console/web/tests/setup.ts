import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterAll, afterEach, beforeAll, beforeEach, vi } from "vitest";
import { server } from "./network/server";
import { api } from "@/api/client";

beforeAll(() => server.listen({ onUnhandledFrame: "error" }));
beforeEach(() => {
  const interceptedFetch = globalThis.fetch;
  vi.stubGlobal("fetch", (input: RequestInfo | URL, init?: RequestInit) =>
    interceptedFetch(
      typeof input === "string" ? new URL(input, window.location.origin) : input,
      init,
    ),
  );
  vi.stubGlobal("BroadcastChannel", undefined);
  localStorage.clear();
  window.history.replaceState(null, "", "/");
});
afterEach(() => {
  cleanup();
  api.clearIdentity();
  server.resetHandlers();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});
afterAll(() => server.close());

// jsdom lacks top-layer layout; native focus trapping and Escape are tested in Playwright.
HTMLDialogElement.prototype.showModal = function () { this.open = true; };
HTMLDialogElement.prototype.close = function () { this.open = false; };
Element.prototype.scrollIntoView = () => undefined;
