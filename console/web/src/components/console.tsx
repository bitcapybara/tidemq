import type { ReactNode } from "react";
import {
  AlertCircle,
  ArrowUpRight,
  CircleHelp,
  LoaderCircle,
  LockKeyhole,
  RefreshCw,
} from "lucide-react";
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
} from "@tanstack/react-table";
import { Link } from "react-router";
import { ApiError } from "@/api/client";
import { useSession } from "@/api/session";
import type { Observation, ProgressObservation } from "@/api/types";
import { useLocale } from "@/locales/locale";
import { formatDecimal } from "@/features/logic";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "./ui/table";

export function PageHeading({
  eyebrow,
  title,
  description,
  children,
}: {
  eyebrow?: string;
  title: string;
  description: string;
  children?: ReactNode;
}) {
  return (
    <div className="page-heading">
      <div>
        {eyebrow && <p className="eyebrow">{eyebrow}</p>}
        <h1>{title}</h1>
        <p className="description">{description}</p>
      </div>
      {children && <div className="page-actions">{children}</div>}
    </div>
  );
}

export function Panel({
  title,
  description,
  action,
  children,
  className = "",
}: {
  title: string;
  description?: string;
  action?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section className={`panel ${className}`} aria-label={title}>
      <div className="panel-header">
        <div>
          <h2>{title}</h2>
          {description && <p>{description}</p>}
        </div>
        {action}
      </div>
      {children}
    </section>
  );
}

export function FailureNotice({
  error,
  compact = false,
  message: customMessage,
}: {
  error: unknown;
  compact?: boolean;
  message?: string;
}) {
  const { t } = useLocale();
  const kind = error instanceof ApiError ? error.kind : "unavailable";
  const invalidResponse =
    error instanceof ApiError && error.code === "invalid_response";
  const message =
    customMessage ??
    (error instanceof ApiError && error.code === "offset_expired"
      ? t(
          "These messages are outside the retained range. Refresh the shard boundaries and select another offset.",
          "消息已超出保留范围，请刷新 Shard 边界并重新选择 offset。",
        )
      : error instanceof ApiError && error.code === "recovery_expired"
        ? t(
            "The original result recovery window has expired. Do not replay this action automatically or submit it with a new operation ID.",
            "原操作结果的恢复窗口已到期，请勿自动重放或另建操作 ID 提交。",
          )
        : {
            authentication: t(
              "Sign in to access protected cluster data.",
              "请登录后访问受保护的集群数据。",
            ),
            forbidden: t(
              "You do not have access to this resource. Its cached data has been cleared.",
              "你没有此资源的访问权限，其缓存数据已清理。",
            ),
            unavailable: t(
              "The service is temporarily unreachable. This does not mean the token is invalid.",
              "服务暂时不可达，这不代表 token 无效。",
            ),
            invalid: invalidResponse
              ? t(
                  "The service returned an unsupported response. Refresh after the service is available.",
                  "服务返回了无法识别的响应，请在服务就绪后刷新。",
                )
              : t(
                  "The request could not be accepted. Check its values, resource UUIDs, range and encoding. A write with an unknown outcome must be recovered through its original result.",
                  "请求未被接受，请检查输入值、资源 UUID、范围和编码；结果未知的写操作必须通过原结果恢复。",
                ),
            conflict: t(
              "This resource changed after it was read. Reload and review the new state before submitting a new operation.",
              "资源已被其他操作更新。请重新加载并核对最新状态，再提交新操作。",
            ),
            stale: t(
              "This read was cancelled. Refresh to read within the current access scope.",
              "此读取已取消，请在当前访问范围内刷新。",
            ),
          }[kind]);
  return (
    <div className={`notice ${compact ? "notice-compact" : ""}`} role="status">
      {kind === "authentication" || kind === "forbidden" ? (
        <LockKeyhole size={16} />
      ) : (
        <AlertCircle size={16} />
      )}
      <span>
        {message}
        {error instanceof ApiError && (error.requestId || error.traceId) && (
          <small className="failure-identifiers">
            {error.requestId && (
              <span>
                {t("Request ID", "请求 ID")}: <code>{error.requestId}</code>
              </span>
            )}
            {error.traceId && (
              <span>
                Trace ID: <code>{error.traceId}</code>
              </span>
            )}
          </small>
        )}
      </span>
    </div>
  );
}

export function ReadState({
  loading = false,
  error,
  empty = false,
}: {
  loading?: boolean;
  error?: unknown;
  empty?: boolean;
}) {
  const session = useSession();
  const { t } = useLocale();
  if (error || session.error)
    return <FailureNotice error={error ?? session.error} compact />;
  if (loading && session.session)
    return (
      <div className="empty-state" role="status">
        <LoaderCircle className="animate-spin" size={18} />
        {t("Loading…", "正在加载…")}
      </div>
    );
  if (!session.session)
    return (
      <div className="empty-state">
        <LockKeyhole size={22} />
        <span>{t("No authenticated data", "尚无授权数据")}</span>
        <small>
          {t("Sign in to query this resource.", "登录后查询此资源。")}
        </small>
      </div>
    );
  if (empty)
    return (
      <div className="empty-state">
        <CircleHelp size={22} />
        <span>
          {t(
            "No resources in this authorized result.",
            "当前授权查询结果中没有资源。",
          )}
        </span>
      </div>
    );
  return null;
}

export function ObservationLabel({
  observation,
  stale = false,
}: {
  observation?: Observation;
  stale?: boolean;
}) {
  const { language, t } = useLocale();
  return (
    <div className={`observation ${stale ? "is-stale" : ""}`}>
      <span className="status-dot" />
      {observation ? (
        <>
          <span>
            {stale
              ? t("Stale observation", "观测已过期")
              : t("Observed", "观测于")}{" "}
            {new Date(observation.observedAt).toLocaleString(language)}
          </span>
          <span>· {observation.source}</span>
          {observation.revision && (
            <span>· revision {observation.revision}</span>
          )}
          {observation.partial && (
            <Badge variant="outline">{t("Partial", "部分结果")}</Badge>
          )}
        </>
      ) : (
        <span>
          {t("No observation · source not provided", "尚无观测 · 来源未提供")}
        </span>
      )}
    </div>
  );
}

export function ProgressObservationLabel({
  observation,
  stale = false,
}: {
  observation?: ProgressObservation;
  stale?: boolean;
}) {
  const { language, t } = useLocale();
  if (!observation)
    return (
      <div className="observation">
        {t("No progress observation provided", "未提供进度观测")}
      </div>
    );
  const date = (value: string | null) =>
    value ? new Date(value).toLocaleString(language) : t("Unknown", "未知");
  return (
    <div
      className={`observation progress-observation ${stale ? "is-stale" : ""}`}
      role="group"
      aria-label={t("Unconfirmed-message samples", "未确认消息采样")}
    >
      <span>
        {stale
          ? t("Stale sample range", "采样范围已过期")
          : t("Sample range", "采样时间范围")}
        :{" "}
        {observation.observedFrom || observation.observedUntil ? (
          <>
            {date(observation.observedFrom)} → {date(observation.observedUntil)}
          </>
        ) : (
          t("No sample time provided", "未提供采样时间")
        )}
      </span>
      <span>
        {t("Source", "来源")}: {observation.source}
      </span>
      <span>
        {t("Sampled Shards", "已采样 Shard")}:{" "}
        {observation.sampledShards === null
          ? t("Unknown", "未知")
            : formatDecimal(observation.sampledShards, language)}
      </span>
      <span>
        {t(
          "Independent Shard samples; not an atomic snapshot.",
          "独立 Shard 采样，非一致快照。",
        )}
      </span>
      {observation.partial && (
        <span>
          <Badge variant="outline">{t("Partial", "部分结果")}</Badge>
        </span>
      )}
      {observation.reason && (
        <span>{progressReason(observation.reason, t)}</span>
      )}
    </div>
  );
}

function progressReason(reason: string, t: (en: string, zh: string) => string) {
  switch (reason) {
    case "observationBudgetExhausted":
      return t(
        "The progress sampling time budget was exhausted.",
        "进度采样时间预算已用尽。",
      );
    case "dataObservationUnavailable":
      return t(
        "Some data-group samples are unavailable.",
        "部分数据组采样不可用。",
      );
    case "cursorNotMaterialized":
      return t(
        "A durable cursor sample is not available yet.",
        "持久游标尚未提供采样。",
      );
    case "historicalShardPageIncomplete":
      return t(
        "Only a bounded page of historical Shards was sampled.",
        "仅采样了有界的一页历史 Shard。",
      );
    case "shardSetChanged":
      return t(
        "The Shard set changed during sampling.",
        "采样期间 Shard 集合已变化。",
      );
    case "counterOverflow":
      return t("The total exceeds the counter range.", "汇总值超出计数范围。");
    case "subscriptionProgressIncomplete":
      return t(
        "Some subscription progress samples are incomplete.",
        "部分订阅进度采样不完整。",
      );
    default:
      return `${t("Reported reason", "上游报告的原因")}: ${reason}`;
  }
}

export function DataTable<T>({
  columns,
  data,
  state,
  caption,
}: {
  columns: ColumnDef<T>[];
  data: T[];
  state?: ReactNode;
  caption: string;
}) {
  const table = useReactTable({
    data,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });
  return (
    <Table aria-label={caption}>
      <TableHeader>
        {table.getHeaderGroups().map((group) => (
          <TableRow key={group.id}>
            {group.headers.map((header) => (
              <TableHead key={header.id}>
                {flexRender(
                  header.column.columnDef.header,
                  header.getContext(),
                )}
              </TableHead>
            ))}
          </TableRow>
        ))}
      </TableHeader>
      <TableBody>
        {data.length ? (
          table.getRowModel().rows.map((row) => (
            <TableRow key={row.id}>
              {row.getVisibleCells().map((cell) => (
                <TableCell key={cell.id}>
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </TableCell>
              ))}
            </TableRow>
          ))
        ) : (
          <TableRow>
            <TableCell colSpan={columns.length} className="empty-cell">
              {state}
            </TableCell>
          </TableRow>
        )}
      </TableBody>
    </Table>
  );
}

export function DisabledAction({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  const { t } = useLocale();
  return (
    <span
      title={t(
        "This action is unavailable until its service API is connected.",
        "此操作需等待服务接口接入。",
      )}
      className={className}
    >
      <Button disabled variant="outline">
        {children}
      </Button>
    </span>
  );
}

export function RefreshButton({
  onClick,
  loading = false,
}: {
  onClick: () => void;
  loading?: boolean;
}) {
  const { t } = useLocale();
  return (
    <Button variant="outline" onClick={onClick} disabled={loading}>
      <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
      {t("Refresh", "刷新")}
    </Button>
  );
}

export function ResourceLink({
  to,
  children,
}: {
  to: string;
  children: ReactNode;
}) {
  return (
    <Link className="resource-link" to={to}>
      {children}
      <ArrowUpRight size={13} />
    </Link>
  );
}

export function QueryUnavailable() {
  const { t } = useLocale();
  return (
    <div className="subtle-note">
      <CircleHelp size={15} />
      {t(
        "The required query API has not been connected. No request or business action will be submitted from this form.",
        "所需查询接口尚未接入，此表单不会提交查询或业务操作。",
      )}
    </div>
  );
}
