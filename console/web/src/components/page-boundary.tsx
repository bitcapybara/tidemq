import { Component, type ReactNode } from "react";
import { useLocale } from "@/locales/locale";
import { Button } from "./ui/button";

export class PageBoundary extends Component<
  { children: ReactNode },
  { failed: boolean }
> {
  state = { failed: false };
  static getDerivedStateFromError() {
    return { failed: true };
  }
  render() {
    return this.state.failed ? <PageLoadFailure /> : this.props.children;
  }
}

function PageLoadFailure() {
  const { t } = useLocale();
  return (
    <section
      className="panel"
      aria-label={t("Page load failed", "页面加载失败")}
    >
      <div className="panel-header">
        <h2>{t("This page could not be loaded", "此页面无法加载")}</h2>
      </div>
      <p className="panel-copy" role="alert">
        {t(
          "The Console may have been updated or a page failed to render. Reload to load its current assets. This does not change or resubmit a business operation; keep the original result link when recovering a write.",
          "Console 可能已更新，或页面无法呈现。请刷新加载当前资源；刷新不会变更或重提业务操作，恢复写操作时请保留原结果链接。",
        )}
      </p>
      <div className="panel-footer">
        <Button onClick={() => window.location.reload()}>
          {t("Reload page", "刷新页面")}
        </Button>
      </div>
    </section>
  );
}
