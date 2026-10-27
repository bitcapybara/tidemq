import { useState } from "react";
import { useParams, useSearchParams } from "react-router";
import { useResource } from "@/api/query";
import {
  pageSchema,
  subscriptionSchema,
  progressSchema,
  memberSchema,
  type Subscription,
  type Progress,
  type Member,
} from "@/api/types";
import { useSession } from "@/api/session";
import {
  DataTable,
  FailureNotice,
  ObservationLabel,
  ProgressObservationLabel,
  PageHeading,
  Panel,
  ReadState,
  RefreshButton,
  ResourceLink,
} from "@/components/console";
import { Field, field } from "@/components/forms";
import { Pagination, usePageCursor } from "@/components/pagination";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useLocale } from "@/locales/locale";
import { componentPath, formatDecimal } from "./logic";

export function ConsumersPage() {
  const { t } = useLocale();
  return (
    <>
      <PageHeading
        title={t("Consumers", "消费者")}
        description={t(
          "Persistent subscriptions, current members and durable ACK progress.",
          "查看持久订阅、当前成员与持久 ACK 进度。",
        )}
      />
      <ConsumersTable />
    </>
  );
}

export function ConsumersTable({ topicId }: { topicId?: string }) {
  const { t, language } = useLocale();
  const { session, refresh } = useSession();
  const cursor = usePageCursor();
  const [topic, setTopic] = useState(topicId ?? "");
  const query = useResource(
    topicId ? `topic:${topicId}` : "subscriptions",
    "/api/v1/subscriptions",
    pageSchema(subscriptionSchema),
    true,
    true,
    { topicId: topic || undefined, cursor: cursor.cursor, limit: "32" },
  );
  return (
    <Panel
      title={t("Persistent subscriptions", "持久订阅")}
      description={t(
        "Subscriptions remain visible when no members are online.",
        "没有在线成员时，持久订阅仍保留在目录中。",
      )}
      action={
        <RefreshButton
          onClick={() => (session ? void query.refetch() : refresh())}
          loading={query.isFetching}
        />
      }
    >
      {!topicId && (
        <form
          className="filter-bar"
          onSubmit={(event) => {
            event.preventDefault();
            cursor.reset();
            setTopic(field(new FormData(event.currentTarget), "topicId"));
          }}
        >
          <Field
            name="topicId"
            label={t(
              "Topic UUID (blank for authorized directory)",
              "Topic UUID（留空查看授权目录）",
            )}
          />
          <Button type="submit" variant="outline">
            {t("Apply", "应用筛选")}
          </Button>
        </form>
      )}
      {query.data && query.error && (
        <FailureNotice error={query.error} compact />
      )}
      <DataTable<Subscription>
        caption={t("Subscriptions", "订阅列表")}
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
            header: t("Subscription", "订阅"),
            cell: ({ row }) => (
              <ResourceLink
                to={`/consumers/${row.original.id}?topicId=${row.original.topicId}`}
              >
                {row.original.name}
              </ResourceLink>
            ),
          },
          {
            accessorKey: "topicName",
            header: "Topic",
            cell: ({ row }) => (
              <ResourceLink
                to={`/topics/${row.original.topicId}?tab=subscriptions`}
              >
                {row.original.topicName}
              </ResourceLink>
            ),
          },
          {
            accessorKey: "model",
            header: t("Model", "模型"),
            cell: ({ row }) => (
              <Badge variant="outline">
                {row.original.model === "stream" ? "Stream" : "Queue"}
              </Badge>
            ),
          },
          {
            accessorKey: "members",
            header: t("Current members", "当前成员"),
            cell: ({ row }) => formatDecimal(row.original.members, language),
          },
          {
            accessorKey: "unconfirmed",
            header: t("Unconfirmed", "未确认"),
            cell: ({ row }) =>
              formatDecimal(row.original.unconfirmed, language),
          },
          {
            id: "progressObservation",
            header: t("Unconfirmed sample details", "未确认数采样详情"),
            cell: ({ row }) => (
              <ProgressObservationLabel
                observation={row.original.progressObservation}
                stale={query.isError}
              />
            ),
          },
          {
            accessorKey: "reason",
            header: t("Reported blocker", "上游报告的阻塞原因"),
            cell: ({ row }) => row.original.reason ?? t("Unknown", "未知"),
          },
        ]}
      />
      <div className="table-footer">
        <div>
          <span>{t("Directory metadata", "目录元数据")}</span>
          <ObservationLabel
            observation={query.data}
            stale={query.isError && Boolean(query.data)}
          />
        </div>
        <Pagination
          cursor={cursor}
          nextCursor={query.data?.nextCursor}
          busy={query.isFetching}
        />
      </div>
    </Panel>
  );
}

export function ConsumerPage() {
  const { id = "" } = useParams();
  const [search] = useSearchParams();
  return (
    <ConsumerDetails key={`${id}:${search.get("topicId") ?? ""}`} id={id} />
  );
}

function ConsumerDetails({ id }: { id: string }) {
  const [search, setSearch] = useSearchParams();
  const topicId = search.get("topicId") ?? "";
  const { t, language } = useLocale();
  const { session, denied } = useSession();
  const progressCursor = usePageCursor(),
    memberCursor = usePageCursor();
  const [memberId, setMemberId] = useState<string | null>(null);
  const parameters = { topicId, limit: "32" };
  const progress = useResource(
    `subscription:${id}`,
    `/api/v1/subscriptions/${id}/progress`,
    pageSchema(progressSchema),
    true,
    Boolean(topicId),
    { ...parameters, cursor: progressCursor.cursor },
  );
  const members = useResource(
    `subscription:${id}`,
    `/api/v1/subscriptions/${id}/members`,
    pageSchema(memberSchema),
    true,
    Boolean(topicId),
    { ...parameters, cursor: memberCursor.cursor },
  );
  const member = members.data?.items.find((item) => item.id === memberId);
  const rows = memberId
    ? member
      ? progress.data?.items.filter((row) => member.shards.includes(row.shard))
      : []
    : progress.data?.items;
  if (denied.has(`subscription:${id}`))
    return <FailureNotice error={progress.error ?? members.error} />;
  return (
    <>
      <PageHeading
        eyebrow={t("SUBSCRIPTION", "订阅")}
        title={t("Consumer progress", "消费进度")}
        description={id}
      >
        <RefreshButton
          onClick={() => {
            void progress.refetch();
            void members.refetch();
          }}
          loading={progress.isFetching || members.isFetching}
        />
      </PageHeading>
      {!topicId && (
        <form
          className="filter-bar"
          onSubmit={(event) => {
            event.preventDefault();
            setSearch({
              topicId: field(new FormData(event.currentTarget), "topicId"),
            });
          }}
        >
          <Field
            name="topicId"
            label={t("Owning Topic UUID", "所属 Topic UUID")}
            required
          />
          <Button type="submit">{t("Open progress", "查看进度")}</Button>
        </form>
      )}
      <div
        className="diagnostic-path"
        role="group"
        aria-label={t("Troubleshooting path", "排查路径")}
      >
        {[
          t("Subscription", "订阅"),
          "Shard",
          t("Member / blocker", "成员 / 阻塞项"),
          t("Message / component", "消息 / 组件"),
          t("Observation details", "观测详情"),
        ].map((label, index) => (
          <span key={label}>
            <b>{String(index + 1).padStart(2, "0")}</b>
            {label}
          </span>
        ))}
      </div>
      <Panel
        title={t("Durable ACK progress by shard", "逐 Shard 持久 ACK 进度")}
        description={t(
          "Includes sealed shards. Boundaries are exclusive. Window counts are separate from the tail and do not describe the whole topic.",
          "包含封存 Shard，边界均为排他位置；窗口内计数与尾部分开，不代表整个 Topic。",
        )}
        action={
          memberId && (
            <Button variant="outline" onClick={() => setMemberId(null)}>
              {t("Show all shards", "显示全部 Shard")}
            </Button>
          )
        }
      >
        {memberId && (
          <p className="subtle-note">
            {member
              ? `${t("Assigned shards for member", "成员已分配 Shard")}: ${memberId}`
              : t(
                  "The selected member is absent from the current member page. Clear the selection or reload; its previous allocation is not treated as current.",
                  "当前成员页未包含所选成员，请清除选择或刷新；不会将旧分配当作当前分配。",
                )}
          </p>
        )}
        {progress.data && progress.error && (
          <FailureNotice error={progress.error} compact />
        )}
        <DataTable<Progress>
          caption={t("Shard progress", "Shard 进度")}
          data={rows ?? []}
          state={
            <ReadState
              loading={progress.isPending && Boolean(topicId)}
              error={progress.error}
              empty={Boolean(progress.data)}
            />
          }
          columns={[
            {
              accessorKey: "shard",
              header: "Shard",
              cell: ({ row }) => (
                <>
                  <ResourceLink
                    to={`/topics/${topicId}?tab=messages&shard=${row.original.shard}${row.original.frontier ? `&offset=${row.original.frontier}` : ""}`}
                  >
                    {row.original.shard}
                  </ResourceLink>
                  {row.original.sealed && (
                    <Badge variant="outline">{t("Sealed", "封存")}</Badge>
                  )}
                </>
              ),
            },
            ...(
              [
                ["retainedStart", t("Retained start", "保留起点")],
                ["committedEnd", t("Committed end", "已提交末端")],
                ["frontier", t("Durable ACK frontier", "连续持久 ACK 前沿")],
                ["windowEnd", t("Window end", "投递窗口末端")],
                ["unconfirmed", t("Unconfirmed", "未确认")],
                ["tail", t("Outside window", "尚未入窗")],
                ["ready", t("Ready / retry", "待投递 / 重试")],
                ["inFlight", t("In flight", "处理中")],
                ["acked", t("ACKed in window", "窗口内已确认")],
                ["paused", t("Paused", "暂停")],
              ] as const
            ).map(([key, label]) => ({
              accessorKey: key,
              header: label,
              cell: ({ row }: { row: { original: Progress } }) =>
                formatDecimal(row.original[key], language),
            })),
            {
              accessorKey: "reason",
              header: t("Reported blocker", "上游报告的阻塞原因"),
              cell: ({ row }) => row.original.reason ?? t("Unknown", "未知"),
            },
            {
              id: "component",
              header: t("Component", "组件"),
              cell: ({ row }) =>
                row.original.componentId && session?.operator ? (
                  <ResourceLink to={componentPath(row.original.componentId)}>
                    {row.original.componentId}
                  </ResourceLink>
                ) : (
                  "—"
                ),
            },
            {
              accessorKey: "observedAt",
              header: t("Shard sample time", "Shard 采样时间"),
              cell: ({ row }) =>
                row.original.observedAt
                  ? new Date(row.original.observedAt).toLocaleString(language)
                  : t("Unknown", "未知"),
            },
            {
              accessorKey: "source",
              header: t("Shard sample source", "Shard 采样来源"),
            },
            {
              accessorKey: "revision",
              header: t("Applied revision", "已应用版本"),
              cell: ({ row }) => row.original.revision ?? t("Unknown", "未知"),
            },
          ]}
        />
        <div className="table-footer">
          <ObservationLabel
            observation={progress.data}
            stale={progress.isError && Boolean(progress.data)}
          />
          <Pagination
            cursor={progressCursor}
            nextCursor={progress.data?.nextCursor}
            busy={progress.isFetching}
          />
        </div>
      </Panel>
      <Panel
        title={t("Members & allocation", "成员与分配")}
        description={t(
          "Lease validity, expiration and reported blockers are separate facts. Select a member to view its assigned shards on this page.",
          "租约有效性、过期与上游报告的阻塞项分别呈现；选择成员可查看本页中其被分配的 Shard。",
        )}
      >
        <DataTable<Member>
          caption={t("Current members", "当前成员")}
          data={members.data?.items ?? []}
          state={
            <ReadState
              loading={members.isPending && Boolean(topicId)}
              error={members.error}
              empty={Boolean(members.data)}
            />
          }
          columns={[
            {
              accessorKey: "id",
              header: t("Member identity", "成员身份"),
              cell: ({ row }) => (
                <button
                  className="text-button"
                  onClick={() => setMemberId(row.original.id)}
                >
                  {row.original.id}
                </button>
              ),
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
              accessorKey: "epoch",
              header: t("Member epoch", "成员 epoch"),
              cell: ({ row }) => row.original.epoch ?? "—",
            },
            {
              accessorKey: "status",
              header: t("Reported state", "上游报告状态"),
              cell: ({ row }) => row.original.status ?? t("Unknown", "未知"),
            },
            {
              accessorKey: "shards",
              header: t("Assigned shards", "已分配 Shard"),
              cell: ({ row }) => row.original.shards.join(", ") || "—",
            },
            { accessorKey: "expiresAt", header: t("Expires at", "过期时间") },
            {
              accessorKey: "lastHeartbeat",
              header: t("Last heartbeat", "最近心跳"),
              cell: ({ row }) =>
                row.original.lastHeartbeat ?? t("Unknown", "未知"),
            },
            {
              accessorKey: "reason",
              header: t("Reported blocker", "上游报告的阻塞原因"),
              cell: ({ row }) => row.original.reason ?? t("Unknown", "未知"),
            },
          ]}
        />
        <div className="table-footer">
          <ObservationLabel observation={members.data} />
          <Pagination
            cursor={memberCursor}
            nextCursor={members.data?.nextCursor}
            busy={members.isFetching}
          />
        </div>
      </Panel>
      <p className="footnote">
        {t(
          "Unknown samples are never counted as zero. Observation does not join groups, renew leases, advance delivery or ACK messages. Subscription deletion and position reset remain in the CLI.",
          "缺失样本不计为零。查看不会加入消费组、续租、推进投递或确认消息，订阅删除与位置重置仍使用 CLI。",
        )}
      </p>
    </>
  );
}
