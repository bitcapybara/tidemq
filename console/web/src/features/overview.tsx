import {
  Activity,
  ArrowRight,
  Database,
  Layers3,
  Network,
  Radio,
  Server,
} from "lucide-react";
import { Link } from "react-router";
import { useResource } from "@/api/query";
import {
  alertSchema,
  componentDetailSchema,
  overviewSchema,
  pageSchema,
} from "@/api/types";
import { useSession } from "@/api/session";
import {
  DataTable,
  FailureNotice,
  ObservationLabel,
  ProgressObservationLabel,
  Panel,
  ReadState,
  RefreshButton,
} from "@/components/console";
import { ExternalLink } from "@/components/external-link";
import { Badge } from "@/components/ui/badge";
import { useLocale } from "@/locales/locale";
import { formatDecimal } from "./logic";

const components = [
  {
    name: "Metadata",
    icon: Database,
    en: "Cluster metadata & quorum",
    zh: "集群元数据与 quorum",
  },
  {
    name: "Storage",
    icon: Layers3,
    en: "Replication & persistence",
    zh: "副本复制与消息持久化",
  },
  {
    name: "Broker",
    icon: Radio,
    en: "Shard ownership & delivery",
    zh: "Shard 所有权与消息投递",
  },
  {
    name: "Gateway",
    icon: Network,
    en: "Connections & admission",
    zh: "连接与准入状态",
  },
];

export function OverviewPage() {
  const { t, language } = useLocale();
  const { session, refresh } = useSession();
  const local = session?.capabilities.execution === "local";
  const overview = useResource(
    "overview",
    "/api/v1/overview",
    overviewSchema,
    true,
  );
  const query = useResource(
    "components",
    "/api/v1/components",
    pageSchema(componentDetailSchema),
    true,
    Boolean(session?.operator),
  );
  const alerts = useResource(
    "alerts",
    "/api/v1/alerts",
    pageSchema(alertSchema),
    true,
    Boolean(session?.operator),
    { limit: "32" },
  );
  const values = overview.data;
  const cards = query.data
    ? query.data.items.map((actual) => ({
        actual,
        template: components.find(
          (item) => item.name.toLowerCase() === actual.name.toLowerCase(),
        ) ?? {
          name: actual.name,
          icon: Server,
          en: "Configured dependency",
          zh: "已配置依赖",
        },
      }))
    : components.map((template) => ({ actual: undefined, template }));
  return (
    <>
      <div className="page-heading">
        <div>
          <p className="eyebrow">TIDEMQ CONSOLE</p>
          <h1>{t("Overview", "总览")}</h1>
          <p className="description">
            {t(
              "Availability and business state within your authorized scope.",
              "查看当前授权范围内的可用性与业务状态。",
            )}
          </p>
        </div>
        <RefreshButton
          onClick={() => {
            if (!session) refresh();
            else {
              void overview.refetch();
              if (session.operator) void query.refetch();
            }
          }}
          loading={overview.isFetching || query.isFetching}
        />
      </div>
      {overview.error && <FailureNotice error={overview.error} compact />}
      <div className="metric-strip">
        {[
          [
            t("Authorized topics", "授权 Topic"),
            values?.topics,
            t("Authorized resources", "授权范围内的资源"),
          ],
          [
            t("Unconfirmed messages", "未确认消息"),
            values?.unconfirmed,
            t("Independent Shard samples below", "下方独立 Shard 采样"),
          ],
          [
            t("Persistent subscriptions", "持久订阅"),
            values?.consumers,
            t("Durable subscription directory", "持久订阅目录"),
          ],
          [
            t("Current alerts", "当前告警"),
            values?.alerts,
            t("Configured alert source", "已配置告警来源"),
          ],
        ].map(([label, value, note]) => (
          <div key={String(label)} className="metric">
            <p>{label}</p>
            <strong>{formatDecimal(value, language)}</strong>
            <small>{note}</small>
          </div>
        ))}
      </div>
      <div className="info-ribbon">
        <span>{t("Unconfirmed-message samples", "未确认消息采样")}</span>
        <ProgressObservationLabel
          observation={values?.progressObservation}
          stale={overview.isError && Boolean(values)}
        />
      </div>
      <div className="observation">
        <span>{t("Directory metadata", "目录元数据")}</span>
        <ObservationLabel
          observation={overview.data}
          stale={overview.isError && Boolean(overview.data)}
        />
      </div>
      {(!session || session.operator) && (
        <>
          <div className="section-heading">
            <h2>
              <Activity size={17} />
              {t("Current component status", "组件当前状态")}
            </h2>
            <Link to="/cluster">
              {local ? t("View local process", "查看本机进程") : t("View cluster", "查看集群")}
              <ArrowRight size={14} />
            </Link>
          </div>
          <div className="component-grid">
            {cards.map(({ actual, template: { name, icon: Icon, en, zh } }) => (
              <Link
                key={actual?.id ?? name}
                to={`/cluster${actual ? `?componentId=${encodeURIComponent(actual.id)}` : ""}`}
                className="component-card"
              >
                <div className="component-card-top">
                  <Icon size={20} />
                  <Badge variant="outline">
                    {actual
                      ? healthLabel(actual.health, t)
                      : t("Unknown", "未知")}
                  </Badge>
                </div>
                <h3>{name}</h3>
                <p>{actual?.detail ?? (local ? t("Local durable execution and shared process readiness", "本地持久执行与共享进程就绪状态") : t(en, zh))}</p>
                <ObservationLabel
                  observation={actual}
                  stale={query.isError && Boolean(actual)}
                />
                <div className="component-card-bottom">
                  <span>{t("Observed instances", "观测实例")}</span>
                  <strong>{formatDecimal(actual?.instances, language)}</strong>
                </div>
              </Link>
            ))}
          </div>
          <ObservationLabel
            observation={query.data}
            stale={query.isError && Boolean(query.data)}
          />
          <Panel
            title={t("Current alerts", "当前告警")}
            description={t(
              "Only configured sources can establish alert state. Missing telemetry does not confirm that the cluster is healthy.",
              "告警状态以已配置来源为准，遥测缺失不代表集群健康。",
            )}
          >
            <DataTable
              caption={t("Alert observations", "告警观测列表")}
              data={alerts.data?.items ?? []}
              state={
                alerts.data?.partial ? (
                  <p className="panel-copy">
                    {t(
                      "Alert observations are partial. No alert in this response does not confirm the absence of alerts.",
                      "告警观测不完整，本次未返回告警不代表没有告警。",
                    )}
                  </p>
                ) : (
                  <ReadState
                    loading={alerts.isPending}
                    error={alerts.error}
                    empty={Boolean(alerts.data)}
                  />
                )
              }
              columns={[
                { accessorKey: "name", header: t("Alert", "告警") },
                { accessorKey: "state", header: t("State", "状态") },
                { accessorKey: "detail", header: t("Evidence", "依据") },
                {
                  id: "details",
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
        </>
      )}
      <div className="two-columns">
        <Panel
          title={t("Needs attention", "需要关注")}
          description={t(
            "Availability, backlog and reported blockers",
            "资源可用性、积压与上游报告的阻塞项",
          )}
        >
          {values?.anomalies.length ? (
            <div className="quick-links">
              {values.anomalies.map((item) => (
                <div key={item.id} className="anomaly">
                  <p>{item.summary}</p>
                  {item.subscriptionId && item.topicId ? (
                    <Link
                      className="resource-link"
                      to={`/consumers/${item.subscriptionId}?topicId=${item.topicId}`}
                    >
                      {t("View consumer", "查看消费者")}
                      <ArrowRight size={14} />
                    </Link>
                  ) : item.topicId ? (
                    <Link
                      className="resource-link"
                      to={`/topics/${item.topicId}`}
                    >
                      {t("View topic", "查看 Topic")}
                    </Link>
                  ) : null}
                </div>
              ))}
            </div>
          ) : values ? (
            <p className="panel-copy">
              {values.partial
                ? t(
                    "Some sources are unavailable. An empty anomaly list does not establish that all resources are healthy.",
                    "部分来源不可用，异常列表为空不代表所有资源正常。",
                  )
                : t(
                    "No anomaly was reported in this observation.",
                    "本次观测未报告异常。",
                  )}
            </p>
          ) : (
            <ReadState error={overview.error} loading={overview.isPending} />
          )}
        </Panel>
        <Panel
          title={t("Troubleshooting paths", "排查入口")}
          description={t(
            "Keep resource context as you investigate.",
            "沿资源上下文逐步定位问题。",
          )}
        >
          <div className="quick-links">
            <Link to="/consumers">
              <Radio size={18} />
              <div>
                <strong>{t("Consumer progress", "消费者进度")}</strong>
                <small>
                  {t(
                    "Subscriptions → shards → members → offsets",
                    "订阅 → Shard → 成员 → 消息位置",
                  )}
                </small>
              </div>
              <ArrowRight size={16} />
            </Link>
            <Link to="/topics">
              <Layers3 size={18} />
              <div>
                <strong>{t("Inspect messages", "排查消息")}</strong>
                <small>
                  {t(
                    "Topic → shard → bounded offset window",
                    "Topic → Shard → 有界 offset 窗口",
                  )}
                </small>
              </div>
              <ArrowRight size={16} />
            </Link>
            {(!session || session.operator) && (
              <Link to="/cluster">
                <Server size={18} />
                <div>
                  <strong>{t("Component health", "组件健康")}</strong>
                  <small>
                    {t(
                      "State → source → observation details",
                      "状态 → 来源 → 观测详情",
                    )}
                  </small>
                </div>
                <ArrowRight size={16} />
              </Link>
            )}
          </div>
        </Panel>
      </div>
    </>
  );
}

export function healthLabel(
  health: string,
  t: (en: string, zh: string) => string,
) {
  switch (health) {
    case "healthy":
      return t("Healthy", "正常");
    case "degraded":
      return t("Degraded", "降级");
    case "unavailable":
      return t("Unavailable", "不可用");
    default:
      return t("Unknown", "未知");
  }
}
