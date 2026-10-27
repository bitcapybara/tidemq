import { useEffect, useRef, useState, type ReactNode } from "react";
import { Link } from "react-router";
import { X } from "lucide-react";
import { useResource } from "@/api/query";
import { operationSchema } from "@/api/types";
import { useOperationRunner } from "@/api/operations";
import { useLocale } from "@/locales/locale";
import { Button } from "./ui/button";
import { Input } from "./ui/input";

export function Dialog({
  title,
  children,
  onClose,
}: {
  title: string;
  children: ReactNode;
  onClose: () => void;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  const { t } = useLocale();
  useEffect(() => {
    const dialog = ref.current;
    const opener = document.activeElement;
    dialog?.showModal();
    return () => {
      dialog?.close();
      if (opener instanceof HTMLElement && opener.isConnected) opener.focus();
    };
  }, []);
  return (
    <dialog
      ref={ref}
      className="action-dialog"
      onCancel={onClose}
      aria-label={title}
    >
      <header>
        <h2>{title}</h2>
        <Button
          type="button"
          variant="ghost"
          size="icon"
          onClick={onClose}
          aria-label={t("Close", "关闭")}
        >
          <X size={17} />
        </Button>
      </header>
      {children}
    </dialog>
  );
}

export function WriteForm({
  title,
  target,
  path,
  method = "POST",
  children,
  readBody,
  onClose,
  canSubmit = true,
}: {
  title: string;
  target: string;
  path: string;
  method?: "POST" | "PUT" | "PATCH" | "DELETE";
  children?: ReactNode;
  readBody: (form: FormData) => object;
  onClose: () => void;
  canSubmit?: boolean;
}) {
  const { t } = useLocale();
  const { run, pendingId } = useOperationRunner();
  const [error, setError] = useState(false);
  const submitted = useRef(false);
  const previous = useResource(
    `operation:${pendingId ?? "none"}`,
    `/api/v1/operations/${encodeURIComponent(pendingId ?? "none")}`,
    operationSchema,
    false,
    Boolean(pendingId),
  );
  const blocked =
    Boolean(pendingId) &&
    previous.data?.status !== "succeeded" &&
    previous.data?.status !== "rejected";
  return (
    <Dialog title={title} onClose={onClose}>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (blocked || !canSubmit || submitted.current) return;
          try {
            const body = readBody(new FormData(event.currentTarget));
            submitted.current = true;
            void run(path, method, body);
          } catch {
            setError(true);
          }
        }}
      >
        <p className="write-target">
          {t("Target", "操作目标")}: <strong>{target}</strong>
        </p>
        {children}
        {error && (
          <p role="alert" className="field-error">
            {t(
              "Check the entered values, UUIDs and encoding.",
              "请检查输入值、UUID 与编码格式。",
            )}
          </p>
        )}
        {blocked && (
          <p className="notice">
            <Link to={`/operations/${pendingId}`}>
              {t(
                "Recover the previous result before starting a new operation.",
                "请先查询原操作结果，再开始新操作。",
              )}
            </Link>
          </p>
        )}
        <footer>
          <Button variant="outline" type="button" onClick={onClose}>
            {t("Cancel", "取消")}
          </Button>
          <Button type="submit" disabled={blocked || !canSubmit}>
            {t("Confirm and submit", "确认并提交")}
          </Button>
        </footer>
      </form>
    </Dialog>
  );
}

export function Field({
  name,
  label,
  value,
  required = false,
  type = "text",
  placeholder,
}: {
  name: string;
  label: string;
  value?: string;
  required?: boolean;
  type?: string;
  placeholder?: string;
}) {
  return (
    <label className="form-field">
      <span>{label}</span>
      <Input
        name={name}
        defaultValue={value}
        required={required}
        type={type}
        placeholder={placeholder}
        autoComplete="off"
      />
    </label>
  );
}
export const field = (form: FormData, key: string) =>
  String(form.get(key) ?? "").trim();
export const nullableField = (form: FormData, key: string) =>
  field(form, key) || null;
