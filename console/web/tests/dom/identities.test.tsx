import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { Route, Routes } from "react-router";
import { expect, it } from "vitest";
import { IdentitiesPage } from "@/features/identities";
import { OperationPage } from "@/features/operations";
import { renderConsole } from "../render";
import { apiUrl, server } from "../network/server";
import * as data from "../network/data";

const routes = <Routes>
  <Route path="/operations/:id" element={<OperationPage />} />
  <Route path="*" element={<IdentitiesPage />} />
</Routes>;

it("limits a non-manager to their own identity and session metadata without requesting the credential directory", async () => {
  server.use(http.get(apiUrl("/me"), () => HttpResponse.json({ ...data.session, operator: false, permissions: [] })));
  let credentialReads = 0;
  server.use(http.get(apiUrl("/credentials"), () => { credentialReads++; return HttpResponse.json(data.page([])); }));
  renderConsole(<IdentitiesPage />, `/identities?principalId=${data.ids.other}`);
  expect(await screen.findByRole("heading", { name: "My identity" })).toBeVisible();
  expect(await screen.findByRole("heading", { name: "Current session credential" })).toBeVisible();
  expect(screen.getByText(data.ids.credential)).toBeVisible();
  expect(screen.queryByRole("button", { name: "Issue token" })).not.toBeInTheDocument();
  expect(credentialReads).toBe(0);
});

it("clears the namespace suggestion when tenant scope changes and selects a principal within the selected scope", async () => {
  const user = userEvent.setup();
  const reads: URL[] = [];
  server.use(http.get(apiUrl("/principals"), ({ request }) => { reads.push(new URL(request.url)); return HttpResponse.json(data.page([{ ...data.principal, id: data.ids.other, name: "Another user", tenant: null }])); }));
  renderConsole(<IdentitiesPage />);
  await user.click(await screen.findByRole("button", { name: "Another user" }));
  await user.clear(screen.getByLabelText("Tenant UUID"));
  await user.type(screen.getByLabelText("Tenant UUID"), data.ids.other);
  await user.click(screen.getByRole("button", { name: "Apply scope" }));
  await waitFor(() => expect(reads.at(-1)?.searchParams.get("tenantId")).toBe(data.ids.other));
  await user.click(screen.getByRole("button", { name: "Create principal" }));
  const dialog = screen.getByRole("dialog");
  await user.type(within(dialog).getByLabelText("Namespace UUID"), data.ids.namespace);
  await user.clear(within(dialog).getByLabelText("Tenant UUID"));
  expect(within(dialog).getByLabelText("Namespace UUID")).toHaveValue("");
  await user.click(within(dialog).getByRole("button", { name: "Cancel" }));
});

it.each([true, false])("reviews principal enable/disable (currently enabled=%s) and submits its new state", async (enabled) => {
  const user = userEvent.setup();
  let input: unknown;
  server.use(http.get(apiUrl("/principals"), () => HttpResponse.json(data.page([{ ...data.principal, enabled }]))),
    http.patch(apiUrl("/principals/:id"), async ({ request }) => {
      input = await request.json();
      return HttpResponse.json(data.operation(String((input as { operationId: string }).operationId)));
    }));
  renderConsole(routes);
  await user.click(await screen.findByRole("button", { name: enabled ? "Disable" : "Enable" }));
  expect(screen.getByRole("dialog")).toHaveTextContent("This changes your current identity");
  await user.click(screen.getByRole("button", { name: "Confirm and submit" }));
  expect(await screen.findByText("Succeeded")).toBeVisible();
  expect(input).toMatchObject({ enabled: !enabled });
});

it("revokes the current credential only after showing its session impact", async () => {
  const user = userEvent.setup();
  let path = "";
  server.use(http.delete(apiUrl("/credentials/:id"), async ({ request }) => {
    path = new URL(request.url).pathname;
    const input = await request.json() as { operationId: string };
    return HttpResponse.json(data.operation(input.operationId));
  }));
  const view = renderConsole(routes);
  await screen.findByRole("button", { name: "Revoke" });
  await waitFor(() => expect(view.client.isFetching()).toBe(0));
  await user.click(screen.getByRole("button", { name: "Revoke" }));
  expect(await screen.findByRole("dialog")).toHaveTextContent("This is your current login credential");
  await user.click(screen.getByRole("button", { name: "Cancel" }));
  await user.click(screen.getByRole("button", { name: "Revoke" }));
  await user.click(screen.getByRole("button", { name: "Confirm and submit" }));
  expect(await screen.findByText("Succeeded")).toBeVisible();
  expect(path).toBe(`/api/v1/credentials/${data.ids.credential}`);
});

it.each(["hours", "minutes"])("issues credential lifetime expressed in %s", async (unit) => {
  const user = userEvent.setup();
  let input: unknown;
  server.use(http.post(apiUrl("/credentials"), async ({ request }) => {
    input = await request.json();
    return HttpResponse.json(data.operation((input as { operationId: string }).operationId));
  }));
  renderConsole(routes);
  await user.click(await screen.findByRole("button", { name: "Issue token" }));
  await user.click(screen.getByRole("button", { name: "Cancel" }));
  await user.click(screen.getByRole("button", { name: "Issue token" }));
  await user.selectOptions(screen.getByLabelText("Unit"), unit);
  await user.click(screen.getByRole("button", { name: "Confirm and submit" }));
  expect(await screen.findByText("Succeeded")).toBeVisible();
  expect(input).toMatchObject({ lifetimeMs: unit === "hours" ? "86400000" : "1440000" });
});
