import { useId, useState } from "react";
import { useResource } from "@/api/query";
import {
  pageSchema,
  resourceIdSchema,
  resourceOptionSchema,
} from "@/api/types";
import { useSession } from "@/api/session";
import { useLocale } from "@/locales/locale";
import { Input } from "./ui/input";

export function ScopeFields({
  tenantId,
  namespaceId,
  namespace = true,
  optional = false,
  namespaceOptional = false,
  onChange,
}: {
  tenantId?: string | null;
  namespaceId?: string | null;
  namespace?: boolean;
  optional?: boolean;
  namespaceOptional?: boolean;
  onChange?: () => void;
}) {
  const { t } = useLocale();
  const { session } = useSession();
  const [tenant, setTenant] = useState(tenantId ?? session?.tenant ?? "");
  const [space, setSpace] = useState(namespaceId ?? "");
  const id = useId();
  const tenants = useResource(
    "tenants",
    "/api/v1/tenants",
    pageSchema(resourceOptionSchema),
    false,
    true,
    { limit: "32" },
  );
  const namespaces = useResource(
    `tenant:${tenant}`,
    "/api/v1/namespaces",
    pageSchema(resourceOptionSchema),
    false,
    resourceIdSchema.safeParse(tenant).success,
    { tenantId: tenant, limit: "32" },
  );
  return (
    <>
      <label className="form-field">
        <span>{t("Tenant UUID", "租户 UUID")}</span>
        <Input
          name="tenantId"
          aria-label={t("Tenant UUID", "租户 UUID")}
          list={`${id}-tenants`}
          value={tenant}
          onChange={(event) => {
            setTenant(event.target.value);
            setSpace("");
            onChange?.();
          }}
          required={!optional}
          autoComplete="off"
        />
        <datalist id={`${id}-tenants`}>
          {tenants.data?.items.map((item) => (
            <option key={item.id} value={item.id}>
              {item.name}
            </option>
          ))}
        </datalist>
      </label>
      {namespace && (
        <label className="form-field">
          <span>{t("Namespace UUID", "命名空间 UUID")}</span>
          <Input
            name="namespaceId"
            aria-label={t("Namespace UUID", "命名空间 UUID")}
            list={`${id}-namespaces`}
            value={space}
            onChange={(event) => {
              setSpace(event.target.value);
              onChange?.();
            }}
            required={!optional && !namespaceOptional}
            autoComplete="off"
          />
          <datalist id={`${id}-namespaces`}>
            {namespaces.data?.items.map((item) => (
              <option key={item.id} value={item.id}>
                {item.name}
              </option>
            ))}
          </datalist>
        </label>
      )}
      <p className="footnote">
        {t(
          "Choose an authorized resource suggestion or enter its UUID. Suggestions show a bounded page; a known UUID can always be entered.",
          "选择授权资源候选，或输入资源 UUID。候选只显示一页，也可以直接输入已知 UUID。",
        )}
      </p>
    </>
  );
}
