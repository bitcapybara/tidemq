import { useState } from "react";
import { Plus, Send, Trash2 } from "lucide-react";
import { useParams, useSearchParams } from "react-router";
import { z } from "zod";
import { useInfiniteQuery } from "@tanstack/react-query";
import { useResource } from "@/api/query";
import { api, ApiError } from "@/api/client";
import {
  pageSchema,
  resourceIdSchema,
  shardSchema,
  topicDetailSchema,
  topicSchema,
  producerSchema,
  type Topic,
  type Producer,
  type CreateTopicInput,
} from "@/api/types";
import { useSession } from "@/api/session";
import {
  DataTable,
  FailureNotice,
  ObservationLabel,
  PageHeading,
  Panel,
  ReadState,
  RefreshButton,
  ResourceLink,
} from "@/components/console";
import { Field, WriteForm, field } from "@/components/forms";
import { Pagination, usePageCursor } from "@/components/pagination";
import { ScopeFields } from "@/components/scope-fields";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useLocale } from "@/locales/locale";
import { formatDecimal } from "./logic";
import { ConsumersTable } from "./consumers";
import { GrantsPanel } from "./permissions";
import { MessageBrowser, SendMessage } from "./messages";

export function TopicsPage() {
  const { t } = useLocale();
  const { session, refresh } = useSession();
  const cursor = usePageCursor();
  const [filters, setFilters] = useState<{
    tenantId?: string;
    namespaceId?: string;
  }>({});
  const [create, setCreate] = useState(false);
  const query = useResource(
    "topics",
    "/api/v1/topics",
    pageSchema(topicSchema),
    false,
    true,
    { ...filters, cursor: cursor.cursor, limit: "32" },
  );
  const canManage = Boolean(session?.permissions.includes("manage_topics"));
  return (
    <>
      <PageHeading
        title="Topics"
        description={t(
          "Browse authorized topics, inspect messages and manage access.",
          "浏览授权 Topic，排查消息并管理访问权限。",
        )}
      >
        <RefreshButton
          onClick={() => (session ? void query.refetch() : refresh())}
          loading={query.isFetching}
        />
        {canManage && (
          <Button onClick={() => setCreate(true)}>
            <Plus size={15} />
            {t("Create topic", "创建 Topic")}
          </Button>
        )}
      </PageHeading>
      <Panel
        title={t("Topic directory", "Topic 目录")}
        description={t(
          "Only resources within your current access scope.",
          "仅列举当前访问范围内的资源。",
        )}
      >
        <form
          className="filter-bar"
          onSubmit={(event) => {
            event.preventDefault();
            const form = new FormData(event.currentTarget);
            cursor.reset();
            setFilters({
              tenantId: field(form, "tenantId") || undefined,
              namespaceId: field(form, "namespaceId") || undefined,
            });
          }}
        >
          <Field
            name="tenantId"
            label={t("Tenant UUID", "租户 UUID")}
            value={session?.tenant ?? ""}
          />
          <Field
            name="namespaceId"
            label={t("Namespace UUID", "命名空间 UUID")}
          />
          <Button type="submit" variant="outline">
            {t("Apply", "应用筛选")}
          </Button>
        </form>
        {query.data && query.error && (
          <FailureNotice error={query.error} compact />
        )}
        <DataTable<Topic>
          caption={t("Authorized topics", "授权 Topic")}
          data={query.data?.items ?? []}
          state={
            <ReadState
              loading={query.isPending}
              error={query.error}
              empty={Boolean(query.data)}
            />
          }
          columns={[
            {
              accessorKey: "name",
              header: "Topic",
              cell: ({ row }) => (
                <ResourceLink to={`/topics/${row.original.id}`}>
                  {row.original.name}
                </ResourceLink>
              ),
            },
            { accessorKey: "tenant", header: t("Tenant UUID", "租户 UUID") },
            {
              accessorKey: "namespace",
              header: t("Namespace UUID", "命名空间 UUID"),
            },
            {
              accessorKey: "shards",
              header: "Shards",
              cell: ({ row }) => formatDecimal(row.original.shards, "en"),
            },
            {
              accessorKey: "writable",
              header: t("Write state", "写入状态"),
              cell: ({ row }) => (
                <Badge variant="outline">
                  {row.original.writable
                    ? t("Writable", "可写")
                    : t("Not writable", "不可写")}
                </Badge>
              ),
            },
          ]}
        />
        <div className="table-footer">
          <ObservationLabel
            observation={query.data}
            stale={query.isError && Boolean(query.data)}
          />
          <Pagination
            cursor={cursor}
            nextCursor={query.data?.nextCursor}
            busy={query.isFetching}
          />
        </div>
      </Panel>
      {create && (
        <WriteForm
          title={t("Create topic", "创建 Topic")}
          target={session?.tenant ?? t("Selected scope", "所选作用域")}
          path="/api/v1/topics"
          onClose={() => setCreate(false)}
          readBody={(form): CreateTopicInput => ({
            tenantId: resourceIdSchema.parse(field(form, "tenantId")),
            namespaceId: resourceIdSchema.parse(field(form, "namespaceId")),
            name: z.string().min(1).max(200).parse(field(form, "name")),
          })}
        >
          <Field name="name" label={t("Topic name", "Topic 名称")} required />
          <ScopeFields />
          <p className="footnote">
            {t(
              "Use an existing tenant and namespace. Other creation settings use the core defaults shown in the resulting topic configuration.",
              "使用已存在的租户和命名空间。其他创建参数采用核心默认值，成功后可在有效配置中查看。",
            )}
          </p>
        </WriteForm>
      )}
    </>
  );
}

const tabs = [
  "overview",
  "messages",
  "subscriptions",
  "producers",
  "permissions",
] as const;
export function TopicPage() {
  const { id = "" } = useParams();
  return <TopicDetails key={id} id={id} />;
}

function TopicDetails({ id }: { id: string }) {
  const { t } = useLocale();
  const { session, denied, deniedErrors, epoch } = useSession();
  const [search, setSearch] = useSearchParams();
  const tab = tabs.find((value) => value === search.get("tab")) ?? "overview";
  const [action, setAction] = useState<"send" | "delete" | null>(null);
  const query = useResource(
    `topic:${id}`,
    `/api/v1/topics/${encodeURIComponent(id)}`,
    topicDetailSchema,
  );
  const firstCursor = query.data?.nextShardCursor ?? undefined;
  const moreShards = useInfiniteQuery({
    queryKey: [
      "protected",
      epoch,
      `topic:${id}`,
      "shards",
      query.data?.observedAt,
      firstCursor,
    ],
    queryFn: ({ signal, pageParam }) =>
      api.request(
        `/api/v1/topics/${encodeURIComponent(id)}/shards`,
        pageSchema(shardSchema),
        {
          scope: `topic:${id}`,
          signal,
          query: { cursor: pageParam, limit: "32" },
        },
      ),
    initialPageParam: firstCursor,
    getNextPageParam: (last) => last.nextCursor ?? undefined,
    enabled: false,
    retry: false,
    refetchOnWindowFocus: false,
  });
  const shards = [
    ...new Map(
      [
        ...(query.data?.shardDetails ?? []),
        ...(moreShards.data?.pages.flatMap((page) => page.items) ?? []),
      ].map((shard) => [shard.id, shard]),
    ).values(),
  ];
  const hasMore = moreShards.data
    ? moreShards.hasNextPage
    : Boolean(firstCursor);
  if (denied.has(`topic:${id}`))
    return (
      <FailureNotice
        error={
          deniedErrors.get(`topic:${id}`) ??
          new ApiError("forbidden", "permission_denied")
        }
      />
    );
  const labels = {
    overview: t("Overview / configuration", "概览 / 配置"),
    messages: t("Messages", "消息"),
    subscriptions: t("Subscriptions", "订阅"),
    producers: t("Producers", "生产者"),
    permissions: t("Permissions", "权限"),
  };
  const permissions = query.data?.effectivePermissions ?? [];
  const canProduce = permissions.includes("produce");
  const canManage = permissions.includes("manage_topics");
  return (
    <>
      <PageHeading
        eyebrow="TOPIC"
        title={query.data?.name ?? t("Topic details", "Topic 详情")}
        description={id}
      >
        <RefreshButton
          onClick={() => void query.refetch()}
          loading={query.isFetching}
        />
        {canProduce && (
          <Button onClick={() => setAction("send")}>
            <Send size={14} />
            {t("Send message", "发送消息")}
          </Button>
        )}
        {canManage && (
          <Button variant="destructive" onClick={() => setAction("delete")}>
            <Trash2 size={14} />
            {t("Delete topic", "删除 Topic")}
          </Button>
        )}
      </PageHeading>
      <nav className="tabs" aria-label={t("Topic sections", "Topic 页面区域")}>
        {tabs.map((item) => (
          <button
            key={item}
            type="button"
            aria-current={tab === item ? "page" : undefined}
            onFocus={(event) =>
              event.currentTarget.scrollIntoView({
                block: "nearest",
                inline: "nearest",
              })
            }
            onClick={() =>
              setSearch((previous) => {
                previous.set("tab", item);
                return previous;
              })
            }
          >
            {labels[item]}
          </button>
        ))}
      </nav>
      {query.error && <FailureNotice error={query.error} compact />}
      {tab === "overview" && (
        <>
          <Panel
            title={t("Effective configuration", "有效配置")}
            description={t(
              "Read-only values and their actual source scope.",
              "只读展示真实生效值与来源作用域。",
            )}
          >
            <dl className="detail-grid">
              {[
                ["Topic UUID", id],
                [t("Tenant UUID", "租户 UUID"), query.data?.tenant],
                [t("Namespace UUID", "命名空间 UUID"), query.data?.namespace],
                [
                  t("Write state", "写入状态"),
                  query.data
                    ? query.data.writable
                      ? t("Writable", "可写")
                      : t("Not writable", "不可写")
                    : null,
                ],
              ].map(([label, value]) => (
                <div key={label}>
                  <dt>{label}</dt>
                  <dd>{value ?? t("Unknown", "未知")}</dd>
                </div>
              ))}
            </dl>
            <DataTable
              caption={t("Configuration sources", "配置与来源")}
              data={query.data?.configuration ?? []}
              state={
                <ReadState
                  loading={query.isPending}
                  error={query.error}
                  empty={Boolean(query.data)}
                />
              }
              columns={[
                { accessorKey: "name", header: t("Setting", "配置项") },
                {
                  accessorKey: "value",
                  header: t("Effective value", "生效值"),
                  cell: ({ row }) => row.original.value ?? t("Unknown", "未知"),
                },
                { accessorKey: "scope", header: t("Scope", "作用域") },
                { accessorKey: "source", header: t("Source", "来源") },
              ]}
            />
            <div className="panel-footer">
              <ObservationLabel observation={query.data} />
            </div>
          </Panel>
          <Panel title={t("Current and sealed shards", "当前与封存 Shard")}>
            <DataTable
              caption="Shards"
              data={shards}
              state={
                <ReadState
                  loading={query.isPending}
                  error={query.error}
                  empty={Boolean(query.data)}
                />
              }
              columns={[
                {
                  accessorKey: "id",
                  header: "Shard",
                  cell: ({ row }) => (
                    <ResourceLink
                      to={`/topics/${id}?tab=messages&shard=${row.original.id}`}
                    >
                      {row.original.id}
                    </ResourceLink>
                  ),
                },
                {
                  accessorKey: "sealed",
                  header: t("Layout state", "布局状态"),
                  cell: ({ row }) =>
                    row.original.sealed
                      ? t("Sealed", "封存")
                      : t("Current", "当前"),
                },
                {
                  accessorKey: "retainedStart",
                  header: t("Retained start", "保留起点"),
                  cell: ({ row }) =>
                    formatDecimal(row.original.retainedStart, "en"),
                },
                {
                  accessorKey: "committedEnd",
                  header: t("Committed end (exclusive)", "已提交末端（排他）"),
                  cell: ({ row }) =>
                    formatDecimal(row.original.committedEnd, "en"),
                },
                {
                  accessorKey: "group",
                  header: t("Data group", "数据组"),
                  cell: ({ row }) => row.original.group ?? t("Unknown", "未知"),
                },
                {
                  accessorKey: "reason",
                  header: t("Observation detail", "观测说明"),
                  cell: ({ row }) => row.original.reason ?? "—",
                },
              ]}
            />
          </Panel>
        </>
      )}
      {tab === "messages" && query.data ? (
        <MessageBrowser
          key={id}
          topicId={id}
          shards={shards}
          allowed={permissions.includes("inspect_message_content")}
        />
      ) : tab === "messages" ? (
        <ReadState loading={query.isPending} error={query.error} />
      ) : null}
      {(tab === "overview" || tab === "messages") && (
        <div className="shard-pagination">
          <p className="footnote">
            {t("Loaded shards", "已加载 Shard")}: {shards.length} /{" "}
            {query.data?.totalShards ?? t("Unknown", "未知")}
          </p>
          {hasMore && (
            <Button
              variant="outline"
              onClick={() => void moreShards.fetchNextPage()}
              disabled={moreShards.isFetching || !session}
            >
              {t("Load more shards", "加载更多 Shard")}
            </Button>
          )}
          {moreShards.error && (
            <FailureNotice error={moreShards.error} compact />
          )}
        </div>
      )}
      {tab === "subscriptions" && <ConsumersTable topicId={id} />}
      {tab === "producers" && <Producers topicId={id} />}
      {tab === "permissions" && (
        <GrantsPanel
          topicId={id}
          tenantId={query.data?.tenant}
          namespaceId={query.data?.namespace}
          canManage={permissions.includes("manage_credentials")}
          principalId={
            permissions.includes("manage_credentials")
              ? undefined
              : session?.principal.id
          }
        />
      )}
      {action === "send" && (
        <SendMessage
          topicId={id}
          topicName={query.data?.name ?? id}
          onClose={() => setAction(null)}
        />
      )}
      {action === "delete" && (
        <WriteForm
          title={t("Delete topic", "删除 Topic")}
          target={query.data?.name ?? id}
          path={`/api/v1/topics/${id}`}
          method="DELETE"
          onClose={() => setAction(null)}
          readBody={() => ({})}
        >
          <p className="danger-note">
            {t(
              "This removes the topic. All subscriptions must be deleted first using the CLI. The server will reject deletion while blockers remain; subscriptions are never deleted automatically.",
              "此操作删除 Topic。必须先在 CLI 中删除全部订阅；存在阻塞项时核心会拒绝删除，不会自动连带删除订阅。",
            )}
          </p>
        </WriteForm>
      )}
    </>
  );
}

function Producers({ topicId }: { topicId: string }) {
  const { t } = useLocale();
  const cursor = usePageCursor();
  const query = useResource(
    `topic:${topicId}`,
    `/api/v1/topics/${topicId}/producers`,
    pageSchema(producerSchema),
    true,
    true,
    { cursor: cursor.cursor, limit: "32" },
  );
  return (
    <Panel
      title={t("Producer registrations & leases", "生产者登记与租约")}
      description={t(
        "Registration is durable; online state requires an active lease. Viewing does not renew a lease.",
        "登记是持久事实，在线状态以有效租约为依据，查看不会续租。",
      )}
      action={
        <RefreshButton
          onClick={() => void query.refetch()}
          loading={query.isFetching}
        />
      }
    >
      <DataTable<Producer>
        caption={t("Producers", "生产者")}
        data={query.data?.items ?? []}
        state={
          <ReadState
            error={query.error}
            loading={query.isPending}
            empty={Boolean(query.data)}
          />
        }
        columns={[
          { accessorKey: "id", header: t("Producer identity", "生产者身份") },
          {
            accessorKey: "registration",
            header: t("Registration", "登记版本"),
            cell: ({ row }) => row.original.registration ?? "—",
          },
          {
            accessorKey: "leaseValid",
            header: t("Lease", "租约"),
            cell: ({ row }) =>
              row.original.leaseValid
                ? t("Valid", "有效")
                : t("Expired", "过期"),
          },
          {
            accessorKey: "lastHeartbeat",
            header: t("Last heartbeat", "最近心跳"),
            cell: ({ row }) =>
              row.original.lastHeartbeat ?? t("Unknown", "未知"),
          },
          { accessorKey: "expiresAt", header: t("Expires at", "过期时间") },
          {
            accessorKey: "reason",
            header: t("Observation detail", "观测说明"),
            cell: ({ row }) => row.original.reason ?? "—",
          },
        ]}
      />
      <div className="table-footer">
        <ObservationLabel
          observation={query.data}
          stale={query.isError && Boolean(query.data)}
        />
        <Pagination
          cursor={cursor}
          nextCursor={query.data?.nextCursor}
          busy={query.isFetching}
        />
      </div>
    </Panel>
  );
}
