import { useState } from "react";
import { useSearchParams } from "react-router";
import { Pause, Play } from "lucide-react";
import { useResource } from "@/api/query";
import { useSession } from "@/api/session";
import {
  alertSchema,
  componentDetailSchema,
  instanceSchema,
  nodeSchema,
  pageSchema,
  type Component,
  type Node,
} from "@/api/types";
import {
  DataTable,
  FailureNotice,
  ObservationLabel,
  PageHeading,
  Panel,
  ReadState,
  RefreshButton,
} from "@/components/console";
import { Pagination, usePageCursor } from "@/components/pagination";
import { ExternalLink } from "@/components/external-link";
import { MetricTrend } from "@/components/metric-trend";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useLocale } from "@/locales/locale";
import { ApiError } from "@/api/client";
import { formatDecimal, isOffset } from "./logic";
import { healthLabel } from "./overview";

export function ClusterPage() {
  const { t, language } = useLocale();
  const { session } = useSession();
  const [search, setSearch] = useSearchParams();
  const [paused, setPaused] = useState(false);
  const [anomaliesOnly, setAnomaliesOnly] = useState(false);
  const componentCursor = usePageCursor(),
    nodeCursor = usePageCursor(),
    instanceCursor = usePageCursor();
  const allowed = Boolean(session?.operator);
  const query = useResource(
    "components",
    "/api/v1/components",
    pageSchema(componentDetailSchema),
    !paused,
    allowed,
    { cursor: componentCursor.cursor, limit: "32" },
  );
  const nodes = useResource(
    "cluster",
    "/api/v1/cluster",
    pageSchema(nodeSchema),
    !paused,
    allowed,
    { cursor: nodeCursor.cursor, limit: "32" },
  );
  const alerts = useResource(
    "alerts",
    "/api/v1/alerts",
    pageSchema(alertSchema),
    !paused,
    allowed,
  );
  const selected = query.data?.items.find(
    (item) => item.id === search.get("componentId"),
  );
  const requestedGroup = search.get("groupId");
  const groupId =
    requestedGroup && isOffset(requestedGroup) ? requestedGroup : null;
  const nodeId = search.get("nodeId");
  const selectedNode = nodeId
    ? nodes.data?.items.find((item) => item.id === nodeId)
    : groupId
      ? nodes.data?.items.find((item) =>
          item.groups.some((group) => group.id === groupId),
        )
      : undefined;
  const instances = useResource(
    `component:${selected?.id ?? "none"}`,
    `/api/v1/components/${encodeURIComponent(selected?.id ?? "none")}/instances`,
    pageSchema(instanceSchema),
    !paused,
    allowed && Boolean(selected),
    { cursor: instanceCursor.cursor, limit: "32" },
  );
  if (session && !session.operator)
    return (
      <FailureNotice error={new ApiError("forbidden", "operator_required")} />
    );
  return (
    <>
      <PageHeading
        title={t("Cluster", "集群")}
        description={t(
          "Read-only components, instances, registered nodes and observation sources.",
          "只读查看组件、实例、注册节点与观测来源。",
        )}
      >
        <Button variant="outline" onClick={() => setPaused(!paused)}>
          {paused ? <Play size={14} /> : <Pause size={14} />}
          {paused
            ? t("Resume refresh", "恢复刷新")
            : t("Pause refresh", "暂停刷新")}
        </Button>
        <RefreshButton
          onClick={() => {
            void query.refetch();
            void nodes.refetch();
            void alerts.refetch();
            if (selected) void instances.refetch();
          }}
          loading={query.isFetching || nodes.isFetching}
        />
      </PageHeading>
      <Panel
        title={t("Configured components & dependencies", "已配置组件与依赖")}
        description={t(
          "Unknown observations are not healthy or zero. Observability health is separate from message persistence.",
          "缺失观测不等于正常或零值，观测系统健康与消息持久化状态分开判断。",
        )}
        action={
          <label className="check-label">
            <input
              type="checkbox"
              checked={anomaliesOnly}
              onChange={(event) => setAnomaliesOnly(event.target.checked)}
            />
            {t("Only anomalies on this page", "仅看本页异常")}
          </label>
        }
      >
        {query.data && query.error && (
          <FailureNotice error={query.error} compact />
        )}
        <DataTable<Component>
          caption={t("Component state", "组件状态")}
          data={(query.data?.items ?? []).filter(
            (item) => !anomaliesOnly || item.health !== "healthy",
          )}
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
              header: t("Component", "组件"),
              cell: ({ row }) => (
                <button
                  className="text-button"
                  onClick={() => {
                    instanceCursor.reset();
                    setSearch({ componentId: row.original.id });
                  }}
                >
                  {row.original.name}
                </button>
              ),
            },
            {
              accessorKey: "health",
              header: t("Current health", "当前健康"),
              cell: ({ row }) => (
                <Badge variant="outline">
                  {healthLabel(row.original.health, t)}
                </Badge>
              ),
            },
            {
              accessorKey: "instances",
              header: t("Observed instances", "观测实例"),
              cell: ({ row }) =>
                formatDecimal(row.original.instances, language),
            },
            {
              accessorKey: "detail",
              header: t("Main anomaly / evidence", "主要异常 / 依据"),
              cell: ({ row }) => row.original.detail ?? t("Unknown", "未知"),
            },
            { accessorKey: "source", header: t("Source", "来源") },
            {
              accessorKey: "observedAt",
              header: t("Observed at", "观测时间"),
              cell: ({ row }) =>
                new Date(row.original.observedAt).toLocaleString(language),
            },
          ]}
        />
        <div className="table-footer">
          <ObservationLabel
            observation={query.data}
            stale={query.isError && Boolean(query.data)}
          />
          <Pagination
            cursor={componentCursor}
            nextCursor={query.data?.nextCursor}
            busy={query.isFetching}
          />
        </div>
      </Panel>
      {selected && (
        <Panel
          title={selected.name}
          description={t(
            "Instance state and resource observations",
            "实例状态与资源观测",
          )}
          action={
            <div className="inline-actions">
              {selected.links.map((link) => (
                <ExternalLink key={link.url} href={link.url}>
                  {link.label}
                </ExternalLink>
              ))}
            </div>
          }
        >
          {selected.partial && (
            <p className="notice">
              {t(
                "These counts cover a bounded or partial registry observation. Use the node directory pages for the complete available registry; unavailable samples remain unknown.",
                "这些计数来自有界或不完整的注册观测，请通过节点目录分页查看可用的完整注册表；缺失采样仍为未知。",
              )}
            </p>
          )}
          <div className="metric-strip">
            {[
              [t("Registered", "注册数"), selected.registered],
              [t("Observed", "观测数"), selected.instances],
              [t("Ready", "就绪数"), selected.ready],
            ].map(([name, value]) => (
              <div className="metric" key={String(name)}>
                <p>{name}</p>
                <strong>{formatDecimal(value, language)}</strong>
              </div>
            ))}
          </div>
          <DataTable
            caption={t("Component instances", "组件实例")}
            data={instances.data?.items ?? []}
            state={
              !instances.error && instances.data?.nextCursor ? (
                <p className="panel-copy">
                  {t(
                    "No matching instance occurs on this registry page. Continue to the next page to inspect the remaining registry.",
                    "本页注册记录中没有匹配实例，可继续下一页查看其余注册记录。",
                  )}
                </p>
              ) : (
                <ReadState
                  error={instances.error}
                  loading={instances.isPending}
                  empty={Boolean(instances.data)}
                />
              )
            }
            columns={[
              { accessorKey: "id", header: t("Instance identity", "实例身份") },
              {
                accessorKey: "nodeId",
                header: t("Node ID", "节点 ID"),
                cell: ({ row }) =>
                  row.original.nodeId ? (
                    <button
                      className="text-button"
                      onClick={() =>
                        setSearch((previous) => {
                          previous.set("nodeId", row.original.nodeId ?? "");
                          return previous;
                        })
                      }
                    >
                      {row.original.nodeId}
                    </button>
                  ) : (
                    "—"
                  ),
              },
              { accessorKey: "role", header: t("Role", "角色") },
              {
                accessorKey: "zone",
                header: "Zone",
                cell: ({ row }) => row.original.zone ?? t("Unknown", "未知"),
              },
              {
                accessorKey: "version",
                header: t("Version", "版本"),
                cell: ({ row }) => row.original.version ?? t("Unknown", "未知"),
              },
              {
                accessorKey: "health",
                header: t("Health", "健康"),
                cell: ({ row }) => healthLabel(row.original.health, t),
              },
              { accessorKey: "detail", header: t("Evidence", "依据") },
            ]}
          />
          <div className="table-footer">
            <ObservationLabel
              observation={instances.data}
              stale={instances.isError && Boolean(instances.data)}
            />
            <Pagination
              cursor={instanceCursor}
              nextCursor={instances.data?.nextCursor}
              busy={instances.isFetching}
            />
          </div>
        </Panel>
      )}
      <Panel
        title={t("MQ node registry", "MQ 节点注册表")}
        description={t(
          "Combined roles are shown on the same node identity. Registration, liveness and scheduling mode are separate facts.",
          "Combined 角色按同一节点身份展示，注册、存活与调度模式分别呈现。",
        )}
      >
        <DataTable
          caption={t("Registered MQ nodes", "已注册 MQ 节点")}
          data={nodes.data?.items ?? []}
          state={
            <ReadState
              error={nodes.error}
              loading={nodes.isPending}
              empty={Boolean(nodes.data)}
            />
          }
          columns={[
            {
              accessorKey: "id",
              header: t("Node ID", "节点 ID"),
              cell: ({ row }) => (
                <button
                  className="text-button"
                  onClick={() =>
                    setSearch((previous) => {
                      previous.set("nodeId", row.original.id);
                      return previous;
                    })
                  }
                >
                  {row.original.id}
                </button>
              ),
            },
            { accessorKey: "endpoint", header: t("Endpoint", "端点") },
            {
              accessorKey: "roles",
              header: t("Roles", "角色"),
              cell: ({ row }) => row.original.roles.join(", "),
            },
            {
              accessorKey: "live",
              header: t("Observed liveness", "观测存活"),
              cell: ({ row }) =>
                row.original.live === null
                  ? t("Unknown", "未知")
                  : row.original.live
                    ? t("Live", "存活")
                    : t("Not live", "不存活"),
            },
            {
              accessorKey: "ready",
              header: t("Ready", "就绪"),
              cell: ({ row }) =>
                row.original.ready === null
                  ? t("Unknown", "未知")
                  : row.original.ready
                    ? t("Ready", "就绪")
                    : t("Not ready", "未就绪"),
            },
            {
              accessorKey: "schedulable",
              header: t("Scheduling mode", "调度模式"),
              cell: ({ row }) =>
                row.original.schedulable
                  ? t("Schedulable", "可调度")
                  : t("Not schedulable", "不可调度"),
            },
            {
              accessorKey: "retired",
              header: t("Retirement", "退休配置"),
              cell: ({ row }) =>
                row.original.retired
                  ? t("Retired", "已退休")
                  : t("Not retired", "未退休"),
            },
            {
              accessorKey: "reason",
              header: t("Reported reason", "上游报告的原因"),
              cell: ({ row }) => row.original.reason ?? t("Unknown", "未知"),
            },
          ]}
        />
        <div className="table-footer">
          <ObservationLabel
            observation={nodes.data}
            stale={nodes.isError && Boolean(nodes.data)}
          />
          <Pagination
            cursor={nodeCursor}
            nextCursor={nodes.data?.nextCursor}
            busy={nodes.isFetching}
          />
        </div>
      </Panel>
      {groupId && !selectedNode && (
        <p className="notice" role="status">
          {t(
            "This registry page has no sample for the requested group. Refresh or continue the node pages; this does not confirm that the group is absent.",
            "本页节点中没有所选数据组的采样，请刷新或继续节点分页；这不代表数据组不存在。",
          )}{" "}
          {t("Group", "数据组")}: {groupId}
        </p>
      )}
      {selectedNode && (
        <NodeDetails
          key={selectedNode.id}
          node={selectedNode}
          groupId={groupId}
        />
      )}
      <Panel
        title={t("Current alerts", "当前告警")}
        description={t(
          "From existing rules and alert backends. Detailed metrics and tracing use configured observation links.",
          "来自既有规则和告警后端，详细 metrics 与 tracing 使用已配置的观测链接。",
        )}
      >
        <DataTable
          caption={t("Alerts", "告警")}
          data={alerts.data?.items ?? []}
          state={
            alerts.data?.partial && !alerts.error ? (
              <p className="panel-copy">
                {t(
                  "Alert observations are partial or unconfigured. No alert in this response does not confirm the absence of alerts.",
                  "告警观测不完整或尚未配置，本次未返回告警不代表没有告警。",
                )}
              </p>
            ) : (
              <ReadState
                error={alerts.error}
                loading={alerts.isPending}
                empty={Boolean(alerts.data)}
              />
            )
          }
          columns={[
            { accessorKey: "name", header: t("Alert", "告警") },
            { accessorKey: "state", header: t("State", "状态") },
            { accessorKey: "detail", header: t("Evidence", "依据") },
            {
              id: "links",
              header: t("Details", "详情"),
              cell: ({ row }) => (
                <div className="inline-actions">
                  {row.original.url && (
                    <ExternalLink href={row.original.url}>
                      {t("Details", "详情")}
                    </ExternalLink>
                  )}
                  {row.original.runbookUrl && (
                    <ExternalLink href={row.original.runbookUrl}>
                      Runbook
                    </ExternalLink>
                  )}
                </div>
              ),
            },
          ]}
        />
        <div className="panel-footer">
          <ObservationLabel
            observation={alerts.data}
            stale={alerts.isError && Boolean(alerts.data)}
          />
        </div>
      </Panel>
      <p className="footnote">
        {t(
          "Scaling, draining, scheduling and instance changes remain in the CLI or deployment platform. Viewing never performs cluster operations.",
          "扩缩容、排空、调度和实例调整继续使用 CLI 或部署平台，查看不会执行集群运维操作。",
        )}
      </p>
    </>
  );
}

function NodeDetails({
  node,
  groupId,
}: {
  node: Node;
  groupId: string | null;
}) {
  const { t, language } = useLocale();
  const sampledGroup = groupId
    ? node.groups.find((group) => group.id === groupId)
    : node.groups.find((group) => group.role === "storage");
  return (
    <Panel
      title={`${t("Node", "节点")} ${node.id}`}
      description={t(
        "Persistent registry identity and the latest directly sampled runtime state are shown separately.",
        "持久注册信息与最近一次直接采样的运行状态分别显示。",
      )}
    >
      <dl className="detail-grid">
        {[
          [t("Instance identity", "实例身份"), node.instance],
          ["Zone", node.zone],
          [t("Version", "版本"), node.version],
          [t("Registry generation", "注册版本"), node.generation],
          [
            t("Runtime sampled at", "运行状态采样时间"),
            node.sampledAt
              ? new Date(node.sampledAt).toLocaleString(language)
              : null,
          ],
          [
            t("Dropped log events", "丢弃日志事件"),
            formatDecimal(node.logDroppedEvents, language),
          ],
          [
            t("Log sink errors", "日志输出错误"),
            formatDecimal(node.logSinkErrors, language),
          ],
        ].map(([name, value]) => (
          <div key={name}>
            <dt>{name}</dt>
            <dd>{value ?? t("Unknown", "未知")}</dd>
          </div>
        ))}
      </dl>
      {sampledGroup && node.sampledAt && (
        <div className="trend-grid">
          <MetricTrend
            key={`${node.id}:${sampledGroup.id}:bytes`}
            name={`${t("Group", "数据组")} ${sampledGroup.id} · ${t("Hot bytes", "热数据字节")}`}
            value={sampledGroup.hotBytes}
            observedAt={node.sampledAt}
            unit="bytes"
          />
          <MetricTrend
            key={`${node.id}:${sampledGroup.id}:messages`}
            name={`${t("Group", "数据组")} ${sampledGroup.id} · ${t("Hot messages", "热数据消息")}`}
            value={sampledGroup.hotMessages}
            observedAt={node.sampledAt}
          />
        </div>
      )}
      {groupId && !sampledGroup && (
        <p className="notice">
          {t(
            "The requested group has no runtime sample on this node.",
            "所选数据组在此节点没有运行状态采样。",
          )}
        </p>
      )}
      {node.groupsPartial && (
        <p className="notice">
          {t(
            "The sampled group list is partial. Missing groups are not confirmed absent.",
            "采样到的数据组列表不完整，未显示不代表不存在。",
          )}
        </p>
      )}
      <DataTable
        caption={t("Sampled data groups", "采样数据组")}
        data={node.groups}
        state={
          <p className="panel-copy">
            {t("No sampled groups were provided.", "没有提供数据组采样。")}
          </p>
        }
        columns={[
          {
            accessorKey: "id",
            header: t("Group", "数据组"),
            cell: ({ row }) =>
              row.original.id === groupId ? (
                <Badge variant="outline">
                  {row.original.id} · {t("Selected", "已选择")}
                </Badge>
              ) : (
                row.original.id
              ),
          },
          { accessorKey: "role", header: t("Local role", "本地角色") },
          {
            accessorKey: "leader",
            header: t("Leader node", "Leader 节点"),
            cell: ({ row }) => row.original.leader ?? t("Unknown", "未知"),
          },
          {
            accessorKey: "voters",
            header: t("Voter nodes", "投票节点"),
            cell: ({ row }) => row.original.voters.join(", ") || "—",
          },
          {
            accessorKey: "quorumConfirmed",
            header: t("Quorum confirmed", "Quorum 确认"),
            cell: ({ row }) =>
              row.original.quorumConfirmed === null
                ? t("Unknown", "未知")
                : row.original.quorumConfirmed
                  ? t("Confirmed", "已确认")
                  : t("Not confirmed", "未确认"),
          },
          {
            accessorKey: "hotBytes",
            header: t("Hot bytes", "热数据字节"),
            cell: ({ row }) => formatDecimal(row.original.hotBytes, language),
          },
          {
            accessorKey: "hotMessages",
            header: t("Hot messages", "热数据消息"),
            cell: ({ row }) =>
              formatDecimal(row.original.hotMessages, language),
          },
        ]}
      />
    </Panel>
  );
}
