import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it } from "vitest";
import App from "@/App";
import { http, HttpResponse } from "msw";
import { apiUrl, server } from "../network/server";
import * as data from "../network/data";

it("navigates the real application shell, focuses main content, changes language and closes the navigation", async () => {
  const user = userEvent.setup();
  render(<App />);
  await screen.findByRole("heading", { name: "Overview", level: 1 });
  const nav = screen.getByRole("navigation", { name: "Main navigation" });
  await user.click(within(nav).getByRole("link", { name: "Topics" }));
  await screen.findByRole("link", { name: "Orders" });
  expect(screen.getByRole("main")).toHaveFocus();
  await user.click(screen.getByRole("link", { name: "Orders" }));
  await screen.findByText("retention");
  await user.click(within(nav).getByRole("link", { name: "Consumers" }));
  await user.click(await screen.findByRole("link", { name: "Order processors" }));
  await screen.findByRole("heading", { name: "Consumer progress" });
  await user.click(within(nav).getByRole("link", { name: "Identity & access" }));
  await screen.findByRole("heading", { name: "Principals" });
  await user.click(within(nav).getByRole("link", { name: "Cluster" }));
  await screen.findByRole("heading", { name: "MQ node registry" });
  await user.selectOptions(screen.getByLabelText("Language"), "zh-CN");
  expect(document.documentElement.lang).toBe("zh-CN");
  await user.selectOptions(screen.getByLabelText("语言"), "en");
  const toggle = screen.getByRole("button", { name: "Toggle navigation" });
  await user.click(toggle);
  expect(toggle).toHaveAttribute("aria-expanded", "true");
  await user.keyboard("{Escape}");
  expect(toggle).toHaveFocus();
  expect(toggle).toHaveAttribute("aria-expanded", "false");
  await user.click(toggle);
  await user.click(screen.getByRole("button", { name: "Close navigation" }));
  expect(toggle).toHaveAttribute("aria-expanded", "false");
  await user.click(screen.getByRole("link", { name: "Test operator" }));
  await screen.findByRole("heading", { name: "Current identity" });
  await user.click(screen.getByRole("button", { name: "Sign out" }));
  await waitFor(() => expect(screen.getByLabelText("Personal token")).toBeVisible());
});

it("routes local operators to the process view and keeps business navigation available", async () => {
  server.use(
    http.get(apiUrl("/me"), () => HttpResponse.json(data.localSession)),
    http.post(apiUrl("/session"), () => HttpResponse.json(data.localSession)),
    http.get(apiUrl("/cluster"), () => HttpResponse.json(data.page([data.localNode]))),
  );
  const user = userEvent.setup();
  window.history.replaceState(null, "", "/login");
  render(<App />);
  await user.type(await screen.findByLabelText("Personal token"), "synthetic-local-token");
  await user.click(screen.getByRole("button", { name: "Sign in" }));
  expect(await screen.findByRole("link", { name: "View local process" })).toBeVisible();
  const nav = screen.getByRole("navigation", { name: "Main navigation" });
  expect(within(nav).queryByRole("link", { name: "Cluster" })).not.toBeInTheDocument();
  await user.click(within(nav).getByRole("link", { name: "Local process" }));
  expect(await screen.findByRole("table", { name: "Local process state" })).toBeVisible();
  await user.click(within(nav).getByRole("link", { name: "Topics" }));
  expect(await screen.findByRole("link", { name: "Orders" })).toBeVisible();
});
