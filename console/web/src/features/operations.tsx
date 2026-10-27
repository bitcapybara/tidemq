import { useEffect, useState } from "react";
import { Copy, Search } from "lucide-react";
import { Link, useNavigate, useParams } from "react-router";
import { useResource } from "@/api/query";
import { operationSchema } from "@/api/types";
import { useSession } from "@/api/session";
import { useOperations } from "@/api/operations";
import { ApiError } from "@/api/client";
import { FailureNotice } from "@/components/console";
import {
  ObservationLabel,
  PageHeading,
  Panel,
  ReadState,
  RefreshButton,
} from "@/components/console";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useLocale } from "@/locales/locale";
import { operationPath } from "./logic";

export function OperationPage() {
  const { id = "" } = useParams();
  const { t } = useLocale();
  const { session, refresh, denied } = useSession();
  const [copied, setCopied] = useState(false);
  const store = useOperations();
  const local = store.entries[id];
  const { forgetSecret } = store;
  useEffect(() => () => forgetSecret(id), [id, forgetSecret]);
  const query = useResource(
    `operation:${id}`,
    `/api/v1/operations/${encodeURIComponent(id)}`,
    operationSchema,
    true,
    local?.phase !== "submitting",
  );
  const accessDenied =
    denied.has(`operation:${id}`) ||
    (query.error instanceof ApiError &&
      (query.error.kind === "forbidden" || query.error.kind === "authentication"));
  const canShowResult =
    !accessDenied &&
    (!query.error ||
      (query.error instanceof ApiError && query.error.kind === "unavailable"));
  const result = canShowResult ? (query.data ?? local?.result) : undefined;
  const secret = canShowResult ? local?.secret : undefined;
  const [secretVisible, setSecretVisible] = useState(false);
  const [secretCopied, setSecretCopied] = useState<boolean | null>(null);
  const statuses = {
    succeeded: t("Succeeded", "已知成功"),
    rejected: t("Rejected", "明确拒绝"),
    unknown: t("Unknown result", "结果未知"),
    expired: t("Result expired", "结果已过期"),
  };
  async function copyLink() {
    try {
      await navigator.clipboard.writeText(window.location.href);
      setCopied(true);
    } catch {
      setCopied(false);
    }
  }
  return (
    <>
      <PageHeading
        eyebrow={t("ORIGINAL OPERATION", "原操作")}
        title={t("Operation result", "操作结果")}
        description={id}
      >
        <Button variant="outline" onClick={() => void copyLink()}>
          <Copy size={14} />
          {copied
            ? t("Copied", "已复制")
            : t("Copy result link", "复制结果链接")}
        </Button>
        <RefreshButton
          onClick={() => (session ? void query.refetch() : refresh())}
          loading={query.isFetching}
        />
      </PageHeading>
      <Panel
        title={t("Authoritative core receipt", "核心权威回执")}
        action={
          <Badge variant="outline">
            {local?.phase === "submitting"
              ? t("Submitting", "正在提交")
              : result
                ? statuses[result.status]
                : t("Not queried", "未查询")}
          </Badge>
        }
      >
        {local?.phase === "submitting" && (
          <p className="panel-copy" role="status">
            {t(
              "The original operation is being submitted. This result link is already preserved; do not repeat the action.",
              "正在提交原操作。结果链接已经保留，请勿重复执行。",
            )}
          </p>
        )}
        {!accessDenied && local?.phase === "recover" &&
          (!result || result.status === "unknown") && (
            <>
              <p className="panel-copy" role="status">
                {t(
                  "The submission result is unknown. Query this original operation before taking another action.",
                  "提交结果未知。请先查询此原操作，再进行其他动作。",
                )}
              </p>
              <FailureNotice error={local.error} compact />
            </>
          )}
        {result ? (
          <dl className="detail-grid">
            {[
              [t("Actor", "操作者"), result.actor],
              [t("Target", "目标"), result.target],
              [t("Scope", "作用域"), result.scope],
              [t("Operation ID", "操作 ID"), result.id],
              [t("Receipt", "回执"), result.receipt],
              [t("Reason", "原因"), result.reason],
              [t("Result resource UUID", "结果资源 UUID"), result.resourceId],
            ].map(([label, value]) => (
              <div key={label}>
                <dt>{label}</dt>
                <dd className="break-words">
                  {value || t("Not provided", "未提供")}
                </dd>
              </div>
            ))}
          </dl>
        ) : local?.phase !== "submitting" ? (
          <ReadState loading={query.isPending} error={query.error} />
        ) : null}
        {result?.status === "rejected" &&
          result.reason === "content_conflict" && (
            <p className="notice">
              {t(
                "The resource version changed. This operation was rejected without replacing the newer state. Reload the resource and review its effective access before another operation.",
                "资源版本已变化，本操作被拒绝，未覆盖最新状态。请重新加载资源并核对有效权限后再执行其他操作。",
              )}
            </p>
          )}
        {secret && (
          <section
            className="secret-card"
            aria-label={t("One-time credential", "一次性凭据")}
          >
            <h3>{t("Save this token now", "请现在保存 token")}</h3>
            <p>
              {t(
                "This secret is available only on this page. Leaving, refreshing or querying the result cannot retrieve it.",
                "秘密仅在此页面提供，离开、刷新或查询原结果均不能再次获取。",
              )}
            </p>
            <Input
              readOnly
              type={secretVisible ? "text" : "password"}
              value={secret}
              aria-label={t("New token secret", "新 token 秘密")}
              autoComplete="off"
            />
            <div className="inline-actions">
              <Button
                variant="outline"
                onClick={() => setSecretVisible(!secretVisible)}
              >
                {secretVisible ? t("Hide", "隐藏") : t("Show", "显示")}
              </Button>
              <Button
                variant="outline"
                onClick={() => {
                  void navigator.clipboard
                    .writeText(secret)
                    .then(() => setSecretCopied(true))
                    .catch(() => setSecretCopied(false));
                }}
              >
                {t("Copy token", "复制 token")}
              </Button>
              <Button variant="outline" onClick={() => store.forgetSecret(id)}>
                {t("Saved, dismiss secret", "已保存，关闭秘密")}
              </Button>
            </div>
            {secretCopied !== null && (
              <p role="status">
                {secretCopied
                  ? t("Token copied.", "token 已复制。")
                  : t(
                      "Copy failed. Select and save the visible token.",
                      "复制失败，请显示后手动选择并保存 token。",
                    )}
              </p>
            )}
          </section>
        )}
        {result?.secretUnavailable && !secret && (
          <p className="notice">
            {t(
              "The original token secret cannot be retrieved again. Use the credential ID to revoke it if its first response was not saved.",
              "原 token 秘密无法再次获取；若未保存首次响应，可根据凭据 ID 撤销该凭据。",
            )}
          </p>
        )}
        <div className="panel-footer">
          <ObservationLabel
            observation={result}
            stale={query.isError && Boolean(result)}
          />
        </div>
      </Panel>
      {local?.returnTo && (
        <Button variant="outline" asChild>
          <Link to={local.returnTo} state={{ pendingOperationId: id }}>
            {t("Return to resource", "返回资源页面")}
          </Link>
        </Button>
      )}
      {canShowResult && local?.resourceTo && (
        <Button asChild>
          <Link to={local.resourceTo}>
            {t("Open created resource", "打开新建资源")}
          </Link>
        </Button>
      )}
      <p className="footnote">
        {t(
          "Reopening this page only queries the original result. It never resends the operation. Unknown or expired results do not authorize a new operation ID. Token secrets cannot be retrieved from result queries.",
          "重新打开此页面只查询原结果，不会重发操作。结果未知或过期，不代表可以另建操作 ID。token 秘密不能通过结果查询再次获取。",
        )}
      </p>
    </>
  );
}

export function OperationLookup() {
  const { t } = useLocale();
  const navigate = useNavigate();
  const [id, setId] = useState("");
  const [invalid, setInvalid] = useState(false);
  return (
    <form
      className="operation-lookup"
      onSubmit={(event) => {
        event.preventDefault();
        const path = operationPath(id.trim());
        if (path) navigate(path);
        else setInvalid(true);
      }}
    >
      <label htmlFor="operation-id">
        {t("Recover an original result", "查询原操作结果")}
      </label>
      <div>
        <Input
          id="operation-id"
          value={id}
          onChange={(event) => {
            setId(event.target.value);
            setInvalid(false);
          }}
          autoComplete="off"
          aria-invalid={invalid}
          placeholder={t("Operation ID", "操作 ID")}
        />
        <Button
          variant="outline"
          type="submit"
          size="icon"
          aria-label={t("Open original result", "打开原结果")}
        >
          <Search size={15} />
        </Button>
      </div>
      {invalid && (
        <small role="alert">
          {t("Enter a valid operation identifier.", "请输入有效的操作标识。")}
        </small>
      )}
    </form>
  );
}
