import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { expect, it } from "vitest";
import { TopicsPage, TopicPage } from "@/features/topics";
import { ConsumersPage, ConsumerPage } from "@/features/consumers";
import { ClusterPage } from "@/features/cluster";
import { OverviewPage } from "@/features/overview";
import { renderConsole } from "../render";
import { apiUrl, server } from "../network/server";
import * as data from "../network/data";

it("paginates the directory, restores the previous page, and resets its cursor when scope changes", async () => {
  const requests: URL[] = [];
  server.use(http.get(apiUrl("/topics"), ({ request }) => {
    const url = new URL(request.url);
    requests.push(url);
    return HttpResponse.json(url.searchParams.has("cursor")
      ? data.page([{ ...data.topic, id: data.ids.other, name: "Invoices", writable: false }])
      : data.page([data.topic], "next-page"));
  }));
  const user = userEvent.setup();
  renderConsole(<TopicsPage />);
  expect(await screen.findByRole("link", { name: "Orders" })).toBeVisible();
  expect(screen.getByRole("button", { name: "Previous" })).toBeDisabled();
  await user.click(screen.getByRole("button", { name: "Next page" }));
  expect(await screen.findByRole("link", { name: "Invoices" })).toBeVisible();
  expect(screen.getByText("Not writable")).toBeVisible();
  await user.click(screen.getByRole("button", { name: "Previous" }));
  expect(await screen.findByRole("link", { name: "Orders" })).toBeVisible();
  await user.click(screen.getByRole("button", { name: "Next page" }));
  await user.type(screen.getByLabelText("Namespace UUID"), data.ids.namespace);
  await user.click(screen.getByRole("button", { name: "Apply" }));
  await waitFor(() => expect(requests.at(-1)?.searchParams.get("namespaceId")).toBe(data.ids.namespace));
  expect(requests.at(-1)?.searchParams.has("cursor")).toBe(false);
  expect(requests.every((url) => url.searchParams.get("limit") === "32")).toBe(true);
});

it("shows a stale directory with a failed refresh, then removes its protected data on permission denial", async () => {
  const user = userEvent.setup();
  const view = renderConsole(<TopicsPage />);
  await screen.findByRole("link", { name: "Orders" });
  server.use(http.get(apiUrl("/topics"), () => HttpResponse.json({ code: "upstream_down" }, { status: 503 })));
  await user.click(screen.getByRole("button", { name: "Refresh" }));
  expect(await screen.findByText(/Stale observation/)).toBeVisible();
  expect(screen.getByRole("link", { name: "Orders" })).toBeVisible();
  server.use(http.get(apiUrl("/topics"), () => new HttpResponse(null, { status: 403 })));
  await user.click(screen.getByRole("button", { name: "Refresh" }));
  await waitFor(() => expect(screen.queryByRole("link", { name: "Orders" })).not.toBeInTheDocument());
  expect(view.client.getQueriesData({ predicate: (q) => q.queryKey[0] === "protected" && q.queryKey[2] === "topics" }).every(([, value]) => value === undefined)).toBe(true);
  expect(view.session().denied.has("topics")).toBe(true);
});

it("loads more shards and navigates topic sections without issuing consumer or producer writes", async () => {
  const user = userEvent.setup();
  server.use(http.get(apiUrl("/topics/:id"), () => HttpResponse.json({ ...data.topicDetail, nextShardCursor: "more-shards", totalShards: "2" })));
  renderConsole(<TopicPage />, `/topics/${data.ids.topic}`, "/topics/:id");
  await screen.findByText("retention");
  await user.click(screen.getByRole("button", { name: "Load more shards" }));
  await waitFor(() => expect(screen.getByText("Loaded shards: 2 / 2")).toBeVisible());
  await user.click(screen.getByRole("button", { name: "Subscriptions" }));
  expect(await screen.findByRole("link", { name: "Order processors" })).toBeVisible();
  await user.click(screen.getByRole("button", { name: "Producers" }));
  expect(await screen.findByRole("table", { name: "Producers" })).toHaveTextContent("worker-1");
  await user.click(screen.getByRole("button", { name: "Permissions" }));
  expect(await screen.findByRole("table", { name: "Resource grants" })).toHaveTextContent("produce ← tenant");
  await user.click(screen.getByRole("button", { name: "Delete topic" }));
  expect(screen.getByRole("dialog", { name: "Delete topic" })).toHaveTextContent("All subscriptions must be deleted first");
  await user.click(screen.getByRole("button", { name: "Cancel" }));
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
});

it("filters subscriptions and keeps member allocation separate from the complete shard progress", async () => {
  const user = userEvent.setup();
  const directory = renderConsole(<ConsumersPage />);
  await screen.findByRole("link", { name: "Order processors" });
  await user.type(screen.getByLabelText("Topic UUID (blank for authorized directory)"), data.ids.topic);
  await user.click(screen.getByRole("button", { name: "Apply" }));
  directory.unmount();
  server.use(http.get(apiUrl("/subscriptions/:id/progress"), () => HttpResponse.json(data.page([data.progress, { ...data.progress, shard: data.ids.other, sealed: false, componentId: null, frontier: null }]))));
  renderConsole(<ConsumerPage />, `/consumers/${data.ids.subscription}?topicId=${data.ids.topic}`, "/consumers/:id");
  await screen.findByRole("button", { name: "worker-1" });
  const progress = screen.getByRole("table", { name: "Shard progress" });
  expect(within(progress).getAllByRole("link")).toHaveLength(3);
  await user.click(screen.getByRole("button", { name: "worker-1" }));
  expect(within(progress).queryByRole("link", { name: data.ids.other })).not.toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Show all shards" }));
  expect(within(progress).getByRole("link", { name: data.ids.other })).toBeVisible();
  await user.click(screen.getByRole("button", { name: "Refresh" }));
});

it("requires the owning topic before reading consumer progress", async () => {
  const user = userEvent.setup();
  renderConsole(<ConsumerPage />, `/consumers/${data.ids.subscription}`, "/consumers/:id");
  await user.type(screen.getByLabelText("Owning Topic UUID"), data.ids.topic);
  await user.click(screen.getByRole("button", { name: "Open progress" }));
  expect(await screen.findByRole("button", { name: "worker-1" })).toBeVisible();
});

it("selects component instances, samples a node and pauses/resumes automatic observation", async () => {
  const user = userEvent.setup();
  renderConsole(<ClusterPage />, "/cluster");
  await user.click(await screen.findByRole("button", { name: "Storage" }));
  const instances = await screen.findByRole("table", { name: "Component instances" });
  await user.click(await within(instances).findByRole("button", { name: "1" }));
  expect(await screen.findByRole("table", { name: "Sampled data groups" })).toHaveTextContent("Confirmed");
  expect(screen.getByRole("img", { name: /Hot bytes/ })).toBeVisible();
  await user.click(screen.getByRole("button", { name: "Pause refresh" }));
  await user.click(screen.getByRole("button", { name: "Resume refresh" }));
  await user.click(screen.getByRole("checkbox", { name: "Only anomalies on this page" }));
  expect(within(screen.getByRole("table", { name: "Component state" })).queryByRole("button", { name: "Storage" })).not.toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Refresh" }));
});

it("shows local process facts, disables cluster operations and retains logical shard capabilities", async () => {
  const user = userEvent.setup();
  let observations = 0;
  server.use(
    http.get(apiUrl("/me"), () => HttpResponse.json(data.localSession)),
    http.get(apiUrl("/cluster"), () => { observations++; return HttpResponse.json(data.page([data.localNode])); }),
  );
  renderConsole(<ClusterPage />, "/cluster?nodeId=1&groupId=1");
  expect(await screen.findByRole("heading", { name: "Local process", level: 1 })).toBeVisible();
  const table = await screen.findByRole("table", { name: "Local process state" });
  await waitFor(() => expect(table).toHaveTextContent("ready"));
  expect(table).toHaveTextContent("test");
  for (const name of ["Replica membership", "Node migration", "Raft diagnostics"])
    expect(screen.getByRole("button", { name })).toBeDisabled();
  expect(screen.getByText("Logical shard business controls remain available.")).toBeVisible();
  expect(screen.queryByRole("table", { name: "Sampled data groups" })).not.toBeInTheDocument();
  expect(screen.queryByRole("columnheader", { name: "Leader node" })).not.toBeInTheDocument();
  expect(screen.queryByText("MQ node registry")).not.toBeInTheDocument();
  const before = observations;
  await user.click(screen.getByRole("button", { name: "Refresh" }));
  await waitFor(() => expect(observations).toBeGreaterThan(before));
  await user.click(screen.getByRole("button", { name: "Pause refresh" }));
  await user.click(screen.getByRole("button", { name: "Resume refresh" }));
});

it("preserves failed local observations and reports draining without inventing readiness", async () => {
  const user = userEvent.setup();
  server.use(
    http.get(apiUrl("/me"), () => HttpResponse.json({ ...data.localSession, capabilities: { ...data.localSession.capabilities, businessShardScaling: false } })),
    http.get(apiUrl("/cluster"), () => HttpResponse.json(data.page([{ ...data.localNode, phase: "draining", ready: false }]))),
  );
  renderConsole(<ClusterPage />);
  expect(await screen.findByText("draining")).toBeVisible();
  expect(screen.getByText("Not ready")).toBeVisible();
  expect(screen.queryByText("Logical shard business controls remain available.")).not.toBeInTheDocument();
  server.use(http.get(apiUrl("/cluster"), () => HttpResponse.json({ code: "upstream_unavailable" }, { status: 503 })));
  await user.click(screen.getByRole("button", { name: "Refresh" }));
  expect(await screen.findByText(/Stale observation/)).toBeVisible();
  expect(screen.getByText("draining")).toBeVisible();
});

it("retains uncertainty for missing groups and incomplete observations", async () => {
  server.use(http.get(apiUrl("/components"), () => HttpResponse.json(data.page([{ ...data.component, partial: true, health: "degraded" }]))));
  renderConsole(<ClusterPage />, "/cluster?componentId=storage&groupId=999");
  expect(await screen.findByText(/This registry page has no sample/)).toBeVisible();
  expect(await screen.findByText(/These counts cover a bounded/)).toBeVisible();
});

it("shows overview evidence and avoids operator endpoints for a tenant identity", async () => {
  server.use(http.get(apiUrl("/me"), () => HttpResponse.json({ ...data.session, operator: false, permissions: [] })));
  const view = renderConsole(<OverviewPage />);
  expect(await screen.findByRole("link", { name: "View consumer" })).toHaveAttribute("href", `/consumers/${data.ids.subscription}?topicId=${data.ids.topic}`);
  expect(screen.queryByRole("heading", { name: "Current component status" })).not.toBeInTheDocument();
  expect(view.client.getQueryData(["protected", 0, "components", "/api/v1/components", {}])).toBeUndefined();
  view.unmount();
  renderConsole(<ClusterPage />);
  expect(await screen.findByText(/You do not have access to this resource/)).toBeVisible();
});

it("renders actual overview alerts and refreshes the operator's samples", async () => {
  const user = userEvent.setup();
  const view = renderConsole(<OverviewPage />);
  expect(await screen.findByRole("link", { name: "View cluster" })).toBeVisible();
  expect(await screen.findByRole("link", { name: "Runbook" })).toHaveAttribute("rel", "noopener noreferrer");
  await user.click(screen.getByRole("button", { name: "Refresh" }));
  await act(async () => { await view.client.cancelQueries(); });
});
