import { useState } from "react";
import { api, ApiError } from "@/api/client";
import { useResource } from "@/api/query";
import {
  grantSchema,
  pageSchema,
  resourceIdSchema,
  type Grant,
  type UpdateGrantInput,
} from "@/api/types";
import { useSession } from "@/api/session";
import {
  DataTable,
  FailureNotice,
  ObservationLabel,
  Panel,
  ReadState,
  RefreshButton,
} from "@/components/console";
import { WriteForm, field, nullableField } from "@/components/forms";
import { Pagination, usePageCursor } from "@/components/pagination";
import { ScopeFields } from "@/components/scope-fields";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useLocale } from "@/locales/locale";
import { permissionPreview, RESOURCE_PERMISSIONS, resourceGrantPermissions } from "./logic";

const permissions = [
  ...RESOURCE_PERMISSIONS,
  "manage_credentials",
  "manage_tenants",
];
export function PermissionFields({
  value,
  onChange,
  name = "permissions",
  resourceOnly = false,
}: {
  value: string[];
  onChange: (value: string[]) => void;
  name?: string;
  resourceOnly?: boolean;
}) {
  const { t } = useLocale();
  return (
    <fieldset className="permission-options">
      <legend>{t("Permissions", "权限")}</legend>
      {(resourceOnly ? RESOURCE_PERMISSIONS : permissions).map((permission) => (
        <label key={permission}>
          <input
            type="checkbox"
            name={name}
            value={permission}
            checked={value.includes(permission)}
            onChange={(event) =>
              onChange(
                event.target.checked
                  ? [...value, permission]
                  : value.filter((item) => item !== permission),
              )
            }
          />
          <span>{permission}</span>
        </label>
      ))}
    </fieldset>
  );
}

export function GrantsPanel({
  principalId,
  topicId,
  tenantId,
  namespaceId,
  canManage: resourceCanManage,
}: {
  principalId?: string;
  topicId?: string;
  tenantId?: string;
  namespaceId?: string;
  canManage?: boolean;
}) {
  const { t } = useLocale();
  const { session } = useSession();
  const cursor = usePageCursor();
  const query = useResource(
    topicId ? `topic:${topicId}` : `principal:${principalId ?? "self"}`,
    "/api/v1/grants",
    pageSchema(grantSchema),
    false,
    true,
    { principalId, topicId, cursor: cursor.cursor, limit: "32" },
  );
  const [edit, setEdit] = useState<Grant | "new" | null>(null);
  const canManage =
    resourceCanManage ??
    Boolean(session?.permissions.includes("manage_credentials"));
  return (
    <>
      <Panel
        title={
          topicId
            ? t("Who can access this topic", "谁可以访问此 Topic")
            : t("Direct & inherited grants", "直接与继承授权")
        }
        description={t(
          "Allow permissions inherit from tenant and namespace scopes. Ordinary users see only their own access.",
          "允许权限从租户和命名空间继承，普通用户仅查看自己的权限。",
        )}
        action={
          <div className="inline-actions">
            <RefreshButton
              onClick={() => void query.refetch()}
              loading={query.isFetching}
            />
            {canManage && (
              <Button variant="outline" onClick={() => setEdit("new")}>
                {t("Add grant", "添加授权")}
              </Button>
            )}
          </div>
        }
      >
        <DataTable<Grant>
          caption={t("Resource grants", "资源授权")}
          data={query.data?.items ?? []}
          state={
            <ReadState
              error={query.error}
              loading={query.isPending}
              empty={Boolean(query.data)}
            />
          }
          columns={[
            { accessorKey: "principalName", header: t("Principal", "主体") },
            { accessorKey: "resourceKind", header: t("Scope", "作用域") },
            {
              accessorKey: "resourceId",
              header: t("Resource UUID", "资源 UUID"),
            },
            {
              accessorKey: "direct",
              header: t("Direct", "直接授权"),
              cell: ({ row }) => row.original.direct.join(", ") || "—",
            },
            {
              accessorKey: "inherited",
              header: t("Inherited / source", "继承 / 来源"),
              cell: ({ row }) =>
                row.original.inherited
                  .map((grant) => `${grant.permission} ← ${grant.source}`)
                  .join("; ") || "—",
            },
            {
              accessorKey: "effective",
              header: t("Effective", "有效权限"),
              cell: ({ row }) => row.original.effective.join(", ") || "—",
            },
            {
              id: "edit",
              header: t("Action", "操作"),
              cell: ({ row }) =>
                canManage ? (
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => setEdit(row.original)}
                  >
                    {t("Edit", "编辑")}
                  </Button>
                ) : null,
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
      {edit && (
        <GrantEditor
          grant={edit === "new" ? null : edit}
          principalId={principalId}
          topicId={topicId}
          tenantId={tenantId}
          namespaceId={namespaceId}
          onClose={() => setEdit(null)}
        />
      )}
    </>
  );
}

function grantTarget(form: FormData) {
  const optionalId = (key: string) => {
    const value = nullableField(form, key);
    return value === null ? null : resourceIdSchema.parse(value);
  };
  return {
    principalId: resourceIdSchema.parse(field(form, "principalId")),
    tenantId: optionalId("tenantId"),
    namespaceId: optionalId("namespaceId"),
    topicId: optionalId("topicId"),
  };
}
function targetIdentity(value: ReturnType<typeof grantTarget>) {
  return JSON.stringify([
    value.principalId,
    value.tenantId,
    value.namespaceId,
    value.topicId,
  ]);
}

function GrantEditor({
  grant,
  principalId,
  topicId,
  tenantId,
  namespaceId,
  onClose,
}: {
  grant: Grant | null;
  principalId?: string;
  topicId?: string;
  tenantId?: string;
  namespaceId?: string;
  onClose: () => void;
}) {
  const { t } = useLocale();
  const [proposed, setProposed] = useState(resourceGrantPermissions(grant?.direct ?? []));
  const [loaded, setLoaded] = useState<{
    grant: Grant;
    identity: string;
  } | null>(grant ? { grant, identity: targetIdentity(grant) } : null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const preview = loaded
    ? permissionPreview(
        resourceGrantPermissions(loaded.grant.direct),
        loaded.grant.inherited.map((item) => item.permission),
        proposed,
      )
    : null;
  const invalidate = () => {
    setLoaded(null);
    setError(null);
  };
  async function load(form: HTMLFormElement | null) {
    if (!form || loading) return;
    setLoaded(null);
    setError(null);
    setLoading(true);
    try {
      const target = grantTarget(new FormData(form));
      if (!target.tenantId || (target.topicId && !target.namespaceId))
        throw new ApiError("invalid", "invalid_grant_scope");
      const result = await api.request(
        "/api/v1/grants",
        pageSchema(grantSchema),
        {
          scope: topicId
            ? `topic:${topicId}`
            : `principal:${target.principalId}`,
          query: {
            ...(Object.fromEntries(
              Object.entries(target).filter(([, value]) => value !== null),
            ) as Record<string, string>),
            limit: "32",
          },
        },
      );
      const current = result.items.find(
        (item) =>
          item.principalId === target.principalId &&
          item.resourceId ===
            (target.topicId ?? target.namespaceId ?? target.tenantId),
      );
      if (!current) throw new ApiError("invalid", "grant_preview_missing");
      if (
        targetIdentity(grantTarget(new FormData(form))) !==
        targetIdentity(target)
      )
        return;
      setLoaded({ grant: current, identity: targetIdentity(target) });
      setProposed(resourceGrantPermissions(current.direct));
    } catch (failure) {
      setError(failure);
    } finally {
      setLoading(false);
    }
  }
  return (
    <WriteForm
      title={t("Permission change preview", "授权变更预览")}
      target={
        loaded?.grant.principalName ??
        principalId ??
        t("Selected principal", "所选主体")
      }
      path="/api/v1/grants"
      method="PUT"
      onClose={onClose}
      canSubmit={Boolean(loaded) && !loading}
      readBody={(form): UpdateGrantInput => {
        const target = grantTarget(form);
        if (!loaded || loaded.identity !== targetIdentity(target))
          throw new Error("The grant preview no longer matches this target");
        return {
          ...target,
          permissions: proposed,
          expectedVersion: loaded.grant.version,
        };
      }}
    >
      {grant ? (
        <>
          <input type="hidden" name="principalId" value={grant.principalId} />
          <input type="hidden" name="tenantId" value={grant.tenantId ?? ""} />
          <input
            type="hidden"
            name="namespaceId"
            value={grant.namespaceId ?? ""}
          />
          <input type="hidden" name="topicId" value={grant.topicId ?? ""} />
          <dl className="detail-grid">
            <div>
              <dt>{t("Principal UUID", "主体 UUID")}</dt>
              <dd>{grant.principalId}</dd>
            </div>
            <div>
              <dt>{t("Fixed resource scope", "固定资源作用域")}</dt>
              <dd>
                {grant.resourceKind} · {grant.resourceId}
              </dd>
            </div>
          </dl>
        </>
      ) : (
        <>
          <label className="form-field">
            <span>{t("Principal UUID", "主体 UUID")}</span>
            <Input
              name="principalId"
              defaultValue={principalId}
              onChange={invalidate}
              required
              autoComplete="off"
            />
          </label>
          <ScopeFields
            tenantId={tenantId}
            namespaceId={namespaceId}
            namespaceOptional
            onChange={invalidate}
          />
          <label className="form-field">
            <span>
              {t(
                "Topic UUID (blank for an ancestor scope)",
                "Topic UUID（上层授权留空）",
              )}
            </span>
            <Input
              name="topicId"
              defaultValue={topicId}
              onChange={invalidate}
              autoComplete="off"
            />
          </label>
          <Button
            type="button"
            variant="outline"
            disabled={loading}
            onClick={(event) => void load(event.currentTarget.form)}
          >
            {loading
              ? t("Loading access…", "正在读取权限…")
              : t("Load effective access", "读取有效权限")}
          </Button>
        </>
      )}
      {Boolean(error) && <FailureNotice error={error} compact />}
      {!loaded && (
        <p className="footnote">
          {t(
            "Read the authoritative grant and inherited access for this exact target before submitting.",
            "提交前必须读取该目标的真实授权版本及继承权限。",
          )}
        </p>
      )}
      <p className="panel-copy">
        {t(
          "Only resource permissions change here. Identity administration permissions keep their current scope.",
          "此处仅修改资源权限，身份管理权限保持原有作用域。",
        )}
      </p>
      <PermissionFields resourceOnly value={proposed} onChange={setProposed} />
      <div className="grant-preview">
        <h3>{t("Expected changes", "预计变更")}</h3>
        <dl>
          {[
            [t("Direct additions", "新增直接权限"), preview?.added],
            [t("Direct removals", "撤销直接权限"), preview?.removed],
            [t("Still inherited", "仍然继承的权限"), preview?.stillInherited],
            [
              t("Expected effective access", "预计有效权限"),
              preview?.effective,
            ],
          ].map(([label, values]) => (
            <div key={String(label)}>
              <dt>{label}</dt>
              <dd>
                {Array.isArray(values)
                  ? values.join(", ") || "—"
                  : t("Access not loaded", "尚未读取权限")}
              </dd>
            </div>
          ))}
        </dl>
        {loaded && (
          <p>
            {t("Inherited sources", "继承来源")}:{" "}
            {loaded.grant.inherited
              .map((item) => `${item.permission} ← ${item.source}`)
              .join("; ") || "—"}
          </p>
        )}
        <p>
          {t(
            "Deleting a direct grant does not remove inherited access. A stale version is rejected, then you must reload and review before creating a new operation.",
            "撤销直接授权不会移除继承权限。版本过期时核心拒绝变更，必须重新加载并核对后才能创建新操作。",
          )}
        </p>
        <p>
          {t("Expected version", "预期版本")}: {loaded?.grant.version ?? "—"}
        </p>
      </div>
    </WriteForm>
  );
}
