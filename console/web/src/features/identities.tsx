import { useState } from "react";
import { useSearchParams } from "react-router";
import { z } from "zod";
import { useResource } from "@/api/query";
import {
  credentialSchema,
  pageSchema,
  principalSchema,
  resourceIdSchema,
  type Principal,
  type Credential,
  type CreatePrincipalInput,
  type CreateCredentialInput,
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
} from "@/components/console";
import { Field, WriteForm, field, nullableField } from "@/components/forms";
import { ScopeFields } from "@/components/scope-fields";
import { Pagination, usePageCursor } from "@/components/pagination";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useLocale } from "@/locales/locale";
import { GrantsPanel, PermissionFields } from "./permissions";

export function IdentitiesPage() {
  const { t } = useLocale();
  const { session, refresh } = useSession();
  const [search, setSearch] = useSearchParams();
  const canManage = Boolean(
    session?.permissions.includes("manage_credentials"),
  );
  const selected = canManage
    ? (search.get("principalId") ?? session?.principal.id)
    : session?.principal.id;
  const cursor = usePageCursor();
  const [tenant, setTenant] = useState(session?.tenant ?? "");
  const query = useResource(
    "principals",
    "/api/v1/principals",
    pageSchema(principalSchema),
    false,
    canManage,
    { tenantId: tenant || undefined, cursor: cursor.cursor, limit: "32" },
  );
  const [creating, setCreating] = useState(false);
  const [changing, setChanging] = useState<Principal | null>(null);
  return (
    <>
      <PageHeading
        title={t("Identity & access", "身份与权限")}
        description={t(
          "People and applications use the same principal and scoped token mechanism.",
          "用户与应用使用同一类 Principal 和作用域 token 机制。",
        )}
      >
        <RefreshButton
          onClick={() => (session ? void query.refetch() : refresh())}
          loading={query.isFetching}
        />
        {canManage && (
          <Button onClick={() => setCreating(true)}>
            {t("Create principal", "创建主体")}
          </Button>
        )}
      </PageHeading>
      {canManage ? (
        <Panel
          title={t("Principals", "身份主体")}
          description={
            tenant
              ? `${t("Tenant scope", "租户作用域")}: ${tenant}`
              : t(
                  "Platform identities. Select a tenant to browse its principals.",
                  "当前为平台身份列表，选择租户可查看其主体。",
                )
          }
        >
          <form
            className="filter-bar"
            onSubmit={(event) => {
              event.preventDefault();
              cursor.reset();
              setTenant(field(new FormData(event.currentTarget), "tenantId"));
            }}
          >
            <ScopeFields tenantId={tenant} namespace={false} optional />
            <Button type="submit" variant="outline">
              {t("Apply scope", "应用作用域")}
            </Button>
          </form>
          <DataTable<Principal>
            caption={t("Principals", "身份主体")}
            data={query.data?.items ?? []}
            state={
              <ReadState
                error={query.error}
                loading={query.isPending}
                empty={Boolean(query.data)}
              />
            }
            columns={[
              {
                accessorKey: "name",
                header: t("Name", "名称"),
                cell: ({ row }) => (
                  <button
                    className="text-button"
                    onClick={() => setSearch({ principalId: row.original.id })}
                  >
                    {row.original.name}
                  </button>
                ),
              },
              { accessorKey: "id", header: t("Principal UUID", "主体 UUID") },
              {
                accessorKey: "tenant",
                header: t("Tenant UUID", "租户 UUID"),
                cell: ({ row }) =>
                  row.original.tenant ?? t("Platform scope", "平台作用域"),
              },
              {
                accessorKey: "enabled",
                header: t("State", "状态"),
                cell: ({ row }) => (
                  <Badge variant="outline">
                    {row.original.enabled
                      ? t("Enabled", "启用")
                      : t("Disabled", "停用")}
                  </Badge>
                ),
              },
              {
                id: "action",
                header: t("Action", "操作"),
                cell: ({ row }) => (
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => setChanging(row.original)}
                  >
                    {row.original.enabled
                      ? t("Disable", "停用")
                      : t("Enable", "启用")}
                  </Button>
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
      ) : (
        <Panel title={t("My identity", "我的身份")}>
          <dl className="detail-grid">
            <div>
              <dt>{t("Principal", "主体")}</dt>
              <dd>{session?.principal.name ?? "—"}</dd>
            </div>
            <div>
              <dt>{t("Principal UUID", "主体 UUID")}</dt>
              <dd>{session?.principal.id ?? "—"}</dd>
            </div>
            <div>
              <dt>{t("Tenant UUID", "租户 UUID")}</dt>
              <dd>{session?.tenant ?? "—"}</dd>
            </div>
          </dl>
        </Panel>
      )}
      {selected && (
        <>
          <GrantsPanel key={`grants:${selected}`} principalId={selected} />
          <Credentials key={`credentials:${selected}`} principalId={selected} />
        </>
      )}
      {creating && <CreatePrincipal onClose={() => setCreating(false)} />}
      {changing && (
        <WriteForm
          title={
            changing.enabled
              ? t("Disable principal", "停用主体")
              : t("Enable principal", "启用主体")
          }
          target={`${changing.name} · ${changing.id}`}
          path={`/api/v1/principals/${changing.id}`}
          method="PATCH"
          readBody={() => ({ enabled: !changing.enabled })}
          onClose={() => setChanging(null)}
        >
          <p className="danger-note">
            {changing.id === session?.principal.id
              ? t(
                  "This changes your current identity. Disabling it will invalidate your session and all its credentials.",
                  "此操作变更当前登录身份，停用后会使当前会话及该主体全部凭据失效。",
                )
              : t(
                  "Disabling a principal prevents its credentials from accessing protected resources.",
                  "停用主体后，其凭据将无法访问受保护资源。",
                )}
          </p>
        </WriteForm>
      )}
    </>
  );
}

function CreatePrincipal({ onClose }: { onClose: () => void }) {
  const { t } = useLocale();
  const [permissions, setPermissions] = useState<string[]>([]);
  return (
    <WriteForm
      title={t("Create tenant principal", "创建租户主体")}
      target={t("Selected tenant scope", "所选租户作用域")}
      path="/api/v1/principals"
      onClose={onClose}
      readBody={(form): CreatePrincipalInput => ({
        name: z.string().min(1).max(200).parse(field(form, "name")),
        tenantId: resourceIdSchema.parse(field(form, "tenantId")),
        namespaceId: nullableField(form, "namespaceId"),
        permissions,
      })}
    >
      <Field name="name" label={t("Principal name", "主体名称")} required />
      <ScopeFields namespaceOptional />
      <PermissionFields value={permissions} onChange={setPermissions} />
      <p className="footnote">
        {t(
          "The core validates whether you can delegate each permission. Platform bootstrap identities are not created here.",
          "核心会验证当前身份能否委派每项权限；平台 bootstrap 身份不在此创建。",
        )}
      </p>
    </WriteForm>
  );
}

function Credentials({ principalId }: { principalId: string }) {
  const { t } = useLocale();
  const { session } = useSession();
  const cursor = usePageCursor();
  const canManage = Boolean(
    session?.permissions.includes("manage_credentials"),
  );
  const query = useResource(
    `principal:${principalId}`,
    "/api/v1/credentials",
    pageSchema(credentialSchema),
    false,
    canManage,
    { principalId, cursor: cursor.cursor, limit: "32" },
  );
  const [create, setCreate] = useState(false);
  const [revoke, setRevoke] = useState<Credential | null>(null);
  if (!canManage)
    return (
      <Panel
        title={t("Current session credential", "当前会话凭据")}
        description={t(
          "Credential directory access requires manage_credentials. Your current session metadata remains visible.",
          "凭据目录需要 manage_credentials 权限，当前会话元数据仍可查看。",
        )}
      >
        <dl className="detail-grid">
          {[
            [t("Credential ID", "凭据 ID"), session?.credentialId],
            [
              t("Console session expires at", "Console 会话到期时间"),
              session?.expiresAt,
            ],
            [
              t("Credential expires at", "凭据到期时间"),
              session?.credentialExpiresAt,
            ],
          ].map(([name, value]) => (
            <div key={name}>
              <dt>{name}</dt>
              <dd>{value ?? t("Unknown", "未知")}</dd>
            </div>
          ))}
        </dl>
      </Panel>
    );
  return (
    <>
      <Panel
        title={t("Credentials", "凭据")}
        description={t(
          "Secrets are never returned by this list. A newly issued token is shown once in its operation response.",
          "列表不返回秘密，新签发的 token 仅在操作首次响应中显示一次。",
        )}
        action={
          <div className="inline-actions">
            <RefreshButton
              onClick={() => void query.refetch()}
              loading={query.isFetching}
            />
            {canManage && (
              <Button variant="outline" onClick={() => setCreate(true)}>
                {t("Issue token", "签发 token")}
              </Button>
            )}
          </div>
        }
      >
        {query.data && query.error && (
          <FailureNotice error={query.error} compact />
        )}
        <DataTable<Credential>
          caption={t("Credentials", "凭据")}
          data={query.data?.items ?? []}
          state={
            <ReadState
              error={query.error}
              loading={query.isPending}
              empty={Boolean(query.data)}
            />
          }
          columns={[
            {
              accessorKey: "id",
              header: t("Credential ID", "凭据 ID"),
              cell: ({ row }) => (
                <>
                  {row.original.id}
                  {row.original.id === session?.credentialId && (
                    <Badge variant="outline">
                      {t("Current session", "当前会话")}
                    </Badge>
                  )}
                </>
              ),
            },
            {
              accessorKey: "enabled",
              header: t("State", "状态"),
              cell: ({ row }) =>
                row.original.enabled
                  ? t("Enabled", "启用")
                  : t("Revoked", "已撤销"),
            },
            {
              accessorKey: "expiresAt",
              header: t("Expires at", "到期时间"),
              cell: ({ row }) => row.original.expiresAt ?? t("Unknown", "未知"),
            },
            {
              id: "revoke",
              header: t("Action", "操作"),
              cell: ({ row }) =>
                canManage && row.original.enabled ? (
                  <Button
                    variant="destructive"
                    size="sm"
                    onClick={() => setRevoke(row.original)}
                  >
                    {t("Revoke", "撤销")}
                  </Button>
                ) : null,
            },
          ]}
        />
        <div className="table-footer">
          <ObservationLabel observation={query.data} />
          <Pagination
            cursor={cursor}
            nextCursor={query.data?.nextCursor}
            busy={query.isFetching}
          />
        </div>
      </Panel>
      {create && (
        <WriteForm
          title={t("Issue token", "签发 token")}
          target={principalId}
          path="/api/v1/credentials"
          onClose={() => setCreate(false)}
          readBody={(form): CreateCredentialInput => {
            const value = field(form, "duration");
            if (!/^\d{1,12}$/.test(value) || BigInt(value) < 1n)
              throw new Error("Invalid credential lifetime");
            const multiplier =
              field(form, "unit") === "days"
                ? 86400000n
                : field(form, "unit") === "hours"
                  ? 3600000n
                  : 60000n;
            const lifetime = BigInt(value) * multiplier;
            if (lifetime > 18_446_744_073_709_551_615n)
              throw new Error("Credential lifetime exceeds protocol bounds");
            return { principalId, lifetimeMs: lifetime.toString() };
          }}
        >
          <Field
            name="duration"
            label={t("Validity duration", "有效时长")}
            value="24"
            type="number"
            required
          />
          <label className="form-field">
            <span>{t("Unit", "单位")}</span>
            <select name="unit" defaultValue="hours">
              <option value="minutes">{t("Minutes", "分钟")}</option>
              <option value="hours">{t("Hours", "小时")}</option>
              <option value="days">{t("Days", "天")}</option>
            </select>
          </label>
          <p className="footnote">
            {t(
              "The lifetime is fixed before the first submission. Core policy may apply a shorter maximum. Save the secret from the first result response.",
              "时长在首次提交前固定，核心策略可能限制最大有效期，请保存首次结果响应中的秘密。",
            )}
          </p>
        </WriteForm>
      )}
      {revoke && (
        <WriteForm
          title={t("Revoke credential", "撤销凭据")}
          target={revoke.id}
          path={`/api/v1/credentials/${revoke.id}`}
          method="DELETE"
          readBody={() => ({})}
          onClose={() => setRevoke(null)}
        >
          <p className="danger-note">
            {revoke.id === session?.credentialId
              ? t(
                  "This is your current login credential. Revoking it will invalidate this session.",
                  "这是当前登录凭据，撤销后会使当前会话失效。",
                )
              : t(
                  "Applications using this credential will lose access. Other credentials are unaffected.",
                  "使用此凭据的应用将失去访问权限，其他凭据不受影响。",
                )}
          </p>
        </WriteForm>
      )}
    </>
  );
}
