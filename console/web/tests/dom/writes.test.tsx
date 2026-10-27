import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { Route, Routes } from "react-router";
import { expect, it, vi } from "vitest";
import { TopicsPage } from "@/features/topics";
import { OperationPage, OperationLookup } from "@/features/operations";
import { IdentitiesPage } from "@/features/identities";
import { GrantsPanel } from "@/features/permissions";
import { useOperations, useOperationRunner } from "@/api/operations";
import type { Operation } from "@/api/types";
import { renderConsole } from "../render";
import { apiUrl, server } from "../network/server";
import * as data from "../network/data";

const id = "018bcfe5-6800-7000-8000-000000000099";
function WriteRoutes({ resource = <TopicsPage /> }: { resource?: React.ReactNode }) {
  return <Routes>
    <Route path="/operations/:id" element={<OperationPage />} />
    <Route path="*" element={resource} />
  </Routes>;
}
async function createTopic(user: ReturnType<typeof userEvent.setup>) {
  await user.click(await screen.findByRole("button", { name: "Create topic" }));
  const dialog = screen.getByRole("dialog", { name: "Create topic" });
  await user.type(within(dialog).getByLabelText("Topic name"), "Invoices");
  await user.type(within(dialog).getByLabelText("Namespace UUID"), data.ids.namespace);
  return dialog;
}

it("preserves the first UUID before sending, recovers a lost reply by reading it, and never resends the write", async () => {
  let submitted: Record<string, unknown> | undefined;
  let writes = 0;
  let observedSubmitting = false;
  const reads: string[] = [];
  let status = "unknown";
  server.use(
    http.post(apiUrl("/topics"), async ({ request }) => {
      writes++;
      submitted = await request.json() as Record<string, unknown>;
      observedSubmitting = Boolean(screen.queryByText(/The original operation is being submitted/));
      return HttpResponse.error();
    }),
    http.get(apiUrl("/operations/:id"), ({ params }) => {
      reads.push(String(params.id));
      return HttpResponse.json(data.operation(String(params.id), { status }));
    }),
  );
  const user = userEvent.setup();
  renderConsole(<WriteRoutes />, "/topics");
  const dialog = await createTopic(user);
  await user.dblClick(within(dialog).getByRole("button", { name: "Confirm and submit" }));
  expect(await screen.findByText(/The submission result is unknown/)).toBeVisible();
  expect(submitted).toMatchObject({ name: "Invoices", tenantId: data.ids.tenant, namespaceId: data.ids.namespace });
  expect(submitted?.operationId).toMatch(/^[0-9a-f-]{14}7[0-9a-f-]{21}$/);
  expect(observedSubmitting).toBe(true);
  expect(reads).toEqual([submitted?.operationId]);
  expect(writes).toBe(1);
  await user.click(screen.getByRole("link", { name: "Return to resource" }));
  await createTopic(user);
  expect(screen.getByRole("button", { name: "Confirm and submit" })).toBeDisabled();
  await user.click(screen.getByRole("link", { name: /Recover the previous result/ }));
  status = "succeeded";
  await user.click(screen.getByRole("button", { name: "Refresh" }));
  expect(await screen.findByText("Succeeded")).toBeVisible();
  expect(reads.every((value) => value === submitted?.operationId)).toBe(true);
  expect(writes).toBe(1);
});

it("validates UUID fields before a write and records the created resource on success", async () => {
  const user = userEvent.setup();
  let body: Record<string, unknown> | undefined;
  server.use(http.post(apiUrl("/topics"), async ({ request }) => {
    body = await request.json() as Record<string, unknown>;
    return HttpResponse.json(data.operation(String(body.operationId)));
  }));
  renderConsole(<WriteRoutes />, "/topics");
  const dialog = await createTopic(user);
  const namespace = within(dialog).getByLabelText("Namespace UUID");
  await user.clear(namespace);
  await user.type(namespace, "invalid-uuid");
  await user.click(within(dialog).getByRole("button", { name: "Confirm and submit" }));
  expect(screen.getByRole("alert")).toHaveTextContent("Check the entered values");
  expect(body).toBeUndefined();
  await user.clear(namespace);
  await user.type(namespace, data.ids.namespace);
  await user.click(within(dialog).getByRole("button", { name: "Confirm and submit" }));
  expect(await screen.findByRole("link", { name: "Open created resource" })).toHaveAttribute("href", `/topics/${data.ids.topic}`);
});

it("reviews inherited permissions and submits exactly the loaded revision, then explains a content conflict", async () => {
  const user = userEvent.setup();
  let body: Record<string, unknown> | undefined;
  server.use(http.put(apiUrl("/grants"), async ({ request }) => {
    body = await request.json() as Record<string, unknown>;
    return HttpResponse.json(data.operation(String(body.operationId), { status: "rejected", reason: "content_conflict" }));
  }));
  renderConsole(<WriteRoutes resource={<GrantsPanel principalId={data.ids.principal} />} />, "/identities");
  await user.click(await screen.findByRole("button", { name: "Edit" }));
  await user.click(screen.getByRole("checkbox", { name: "produce" }));
  await user.click(screen.getByRole("checkbox", { name: "consume" }));
  const dialog = screen.getByRole("dialog");
  expect(within(dialog).getByText("Still inherited").nextElementSibling).toHaveTextContent("produce");
  await user.click(within(dialog).getByRole("button", { name: "Confirm and submit" }));
  expect(await screen.findByText(/The resource version changed/)).toBeVisible();
  expect(body).toMatchObject({ expectedVersion: "7", permissions: ["consume"], principalId: data.ids.principal, topicId: data.ids.topic });
});

it("requires a matching grant preview, invalidates it after target edits and supports cancel", async () => {
  const user = userEvent.setup();
  renderConsole(<GrantsPanel principalId={data.ids.principal} topicId={data.ids.topic} tenantId={data.ids.tenant} namespaceId={data.ids.namespace} />);
  await user.click(await screen.findByRole("button", { name: "Add grant" }));
  const submit = screen.getByRole("button", { name: "Confirm and submit" });
  expect(submit).toBeDisabled();
  await user.click(screen.getByRole("button", { name: "Load effective access" }));
  await waitFor(() => expect(submit).toBeEnabled());
  await user.click(screen.getByRole("checkbox", { name: "produce" }));
  await user.clear(screen.getByLabelText("Topic UUID (blank for an ancestor scope)"));
  expect(submit).toBeDisabled();
  await user.click(screen.getByRole("button", { name: "Load effective access" }));
  expect(await screen.findByText(/The request could not be accepted/)).toBeVisible();
  expect(submit).toBeDisabled();
  await user.click(screen.getByRole("button", { name: "Close" }));
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
});

it("does not accept an invalid new grant scope or stale preview responses", async () => {
  const user = userEvent.setup();
  renderConsole(<GrantsPanel principalId={data.ids.principal} />);
  await user.click(await screen.findByRole("button", { name: "Add grant" }));
  await user.type(screen.getByLabelText("Topic UUID (blank for an ancestor scope)"), data.ids.topic);
  await user.click(screen.getByRole("button", { name: "Load effective access" }));
  expect(await screen.findByText(/The request could not be accepted/)).toBeVisible();
  await user.type(screen.getByLabelText("Namespace UUID"), data.ids.namespace);
  let finish: (() => void) | undefined;
  const pending = new Promise<void>((resolve) => { finish = resolve; });
  server.use(http.get(apiUrl("/grants"), async () => { await pending; return HttpResponse.json(data.page([data.grant])); }));
  await user.click(screen.getByRole("button", { name: "Load effective access" }));
  expect(screen.getByRole("button", { name: "Loading access…" })).toBeDisabled();
  await user.clear(screen.getByLabelText("Principal UUID"));
  await user.type(screen.getByLabelText("Principal UUID"), data.ids.other);
  await act(async () => { finish?.(); });
  await waitFor(() => expect(screen.getByRole("button", { name: "Load effective access" })).toBeEnabled());
  expect(screen.getByRole("button", { name: "Confirm and submit" })).toBeDisabled();
});

function SeedResult() {
  const store = useOperations();
  return <button onClick={() => store.set(id, { phase: "result", result: data.operation(id) as Operation, secret: "one-time-secret", returnTo: "/identities", epoch: 0 })}>Seed first response</button>;
}

it("shows a one-time credential, reports clipboard failure, and discards it on dismissal", async () => {
  const user = userEvent.setup();
  const clipboard = vi.spyOn(navigator.clipboard, "writeText");
  clipboard.mockRejectedValue(new Error("Clipboard denied"));
  renderConsole(<><SeedResult /><OperationPage /></>, `/operations/${id}`, "/operations/:id");
  await screen.findByText("Succeeded");
  await user.click(screen.getByRole("button", { name: "Seed first response" }));
  expect(screen.getByLabelText("New token secret")).toHaveAttribute("type", "password");
  await user.click(screen.getByRole("button", { name: "Show" }));
  expect(screen.getByLabelText("New token secret")).toHaveAttribute("type", "text");
  await user.click(screen.getByRole("button", { name: "Hide" }));
  await user.click(screen.getByRole("button", { name: "Copy token" }));
  expect(await screen.findByText(/Copy failed/)).toBeVisible();
  clipboard.mockResolvedValue();
  await user.click(screen.getByRole("button", { name: "Copy token" }));
  expect(await screen.findByText("Token copied.")).toBeVisible();
  await user.click(screen.getByRole("button", { name: "Copy result link" }));
  expect(await screen.findByRole("button", { name: "Copied" })).toBeVisible();
  await user.click(screen.getByRole("button", { name: "Saved, dismiss secret" }));
  expect(screen.queryByLabelText("New token secret")).not.toBeInTheDocument();
});

it("permanently redacts a cached secret and receipt when an operation becomes forbidden", async () => {
  const user = userEvent.setup();
  renderConsole(<><SeedResult /><OperationPage /></>, `/operations/${id}`, "/operations/:id");
  await screen.findByText("Succeeded");
  await user.click(screen.getByRole("button", { name: "Seed first response" }));
  expect(screen.getByLabelText("New token secret")).toHaveValue("one-time-secret");
  server.use(http.get(apiUrl("/operations/:id"), () => new HttpResponse(null, { status: 403 })));
  await user.click(screen.getByRole("button", { name: "Refresh" }));
  await waitFor(() => expect(screen.queryByLabelText("New token secret")).not.toBeInTheDocument());
  expect(screen.queryByText("Applied revision 8")).not.toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Return to resource" })).toHaveAttribute("href", "/identities");
});

it("validates result links and reads expired results without authorizing a replacement write", async () => {
  const user = userEvent.setup();
  server.use(http.get(apiUrl("/operations/:id"), ({ params }) => HttpResponse.json(data.operation(String(params.id), { status: "expired", secretUnavailable: true }))));
  renderConsole(<WriteRoutes resource={<OperationLookup />} />);
  await user.type(screen.getByLabelText("Recover an original result"), "not-an-id");
  await user.click(screen.getByRole("button", { name: "Open original result" }));
  expect(screen.getByRole("alert")).toHaveTextContent("Enter a valid operation identifier");
  await user.clear(screen.getByLabelText("Recover an original result"));
  await user.type(screen.getByLabelText("Recover an original result"), id);
  await user.click(screen.getByRole("button", { name: "Open original result" }));
  expect(await screen.findByText("Result expired")).toBeVisible();
  expect(screen.getByText(/The original token secret cannot be retrieved again/)).toBeVisible();
  expect(screen.getByText(/Unknown or expired results do not authorize a new operation ID/)).toBeVisible();
});

it("surfaces a conflict response under its original operation ID", async () => {
  function Run() { const { run } = useOperationRunner(); return <button onClick={() => void run("/api/v1/grants", "PUT", {})}>Write grant</button>; }
  server.use(http.put(apiUrl("/grants"), () => HttpResponse.json({ code: "content_conflict" }, { status: 409 })),
    http.get(apiUrl("/operations/:id"), ({ params }) => HttpResponse.json(data.operation(String(params.id), { status: "unknown" }))));
  const user = userEvent.setup();
  const view = renderConsole(<WriteRoutes resource={<Run />} />);
  await waitFor(() => expect(view.session().session).not.toBeNull());
  await user.click(screen.getByRole("button", { name: "Write grant" }));
  expect(await screen.findByText(/This resource changed after it was read/)).toBeVisible();
  expect(await screen.findByText("Unknown result")).toBeVisible();
});

it("creates a scoped principal with selected permissions and opens its new resource", async () => {
  const user = userEvent.setup();
  let body: Record<string, unknown> | undefined;
  server.use(http.post(apiUrl("/principals"), async ({ request }) => {
    body = await request.json() as Record<string, unknown>;
    return HttpResponse.json(data.operation(String(body.operationId), { resourceId: data.ids.other }));
  }));
  renderConsole(<WriteRoutes resource={<IdentitiesPage />} />, "/identities");
  await user.click(await screen.findByRole("button", { name: "Create principal" }));
  await user.type(screen.getByLabelText("Principal name"), "Invoice service");
  await user.type(screen.getByLabelText("Namespace UUID"), data.ids.namespace);
  await user.click(screen.getByRole("checkbox", { name: "produce" }));
  await user.click(screen.getByRole("checkbox", { name: "manage_credentials" }));
  await user.click(screen.getByRole("button", { name: "Confirm and submit" }));
  expect(await screen.findByRole("link", { name: "Open created resource" })).toHaveAttribute("href", `/identities?principalId=${data.ids.other}`);
  expect(body).toMatchObject({ name: "Invoice service", permissions: ["produce", "manage_credentials"], namespaceId: data.ids.namespace });
});

it("validates credential lifetime and converts days before the first submission", async () => {
  const user = userEvent.setup();
  let body: Record<string, unknown> | undefined;
  server.use(http.post(apiUrl("/credentials"), async ({ request }) => {
    body = await request.json() as Record<string, unknown>;
    return HttpResponse.json(data.operation(String(body.operationId)));
  }));
  renderConsole(<WriteRoutes resource={<IdentitiesPage />} />, "/identities");
  await user.click(await screen.findByRole("button", { name: "Issue token" }));
  const duration = screen.getByLabelText("Validity duration");
  await user.clear(duration);
  await user.type(duration, "0");
  await user.click(screen.getByRole("button", { name: "Confirm and submit" }));
  expect(screen.getByRole("alert")).toHaveTextContent("Check the entered values");
  expect(body).toBeUndefined();
  await user.clear(duration);
  await user.type(duration, "999999999999");
  await user.selectOptions(screen.getByLabelText("Unit"), "days");
  await user.click(screen.getByRole("button", { name: "Confirm and submit" }));
  expect(body).toBeUndefined();
  await user.clear(duration);
  await user.type(duration, "2");
  await user.selectOptions(screen.getByLabelText("Unit"), "days");
  await user.click(screen.getByRole("button", { name: "Confirm and submit" }));
  expect(await screen.findByText("Succeeded")).toBeVisible();
  expect(body).toMatchObject({ principalId: data.ids.principal, lifetimeMs: "172800000" });
});
