import { lazy, Suspense, useEffect, useRef, useState } from "react";
import {
  Activity,
  ChevronRight,
  CircleHelp,
  Globe2,
  LayoutDashboard,
  LogIn,
  Radio,
  Server,
  ShieldCheck,
  Waves,
  X,
} from "lucide-react";
import {
  BrowserRouter,
  Link,
  NavLink,
  Route,
  Routes,
  useLocation,
} from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { SessionProvider, useSession } from "@/api/session";
import { OperationProvider } from "@/api/operations";
import { PageBoundary } from "@/components/page-boundary";
import { LocaleProvider, useLocale } from "@/locales/locale";
import { FailureNotice, PageHeading, Panel } from "@/components/console";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { OperationLookup, OperationPage } from "@/features/operations";

const OverviewPage = lazy(() =>
  import("@/features/overview").then((module) => ({
    default: module.OverviewPage,
  })),
);
const TopicsPage = lazy(() =>
  import("@/features/topics").then((module) => ({
    default: module.TopicsPage,
  })),
);
const TopicPage = lazy(() =>
  import("@/features/topics").then((module) => ({ default: module.TopicPage })),
);
const ConsumersPage = lazy(() =>
  import("@/features/consumers").then((module) => ({
    default: module.ConsumersPage,
  })),
);
const ConsumerPage = lazy(() =>
  import("@/features/consumers").then((module) => ({
    default: module.ConsumerPage,
  })),
);
const IdentitiesPage = lazy(() =>
  import("@/features/identities").then((module) => ({
    default: module.IdentitiesPage,
  })),
);
const ClusterPage = lazy(() =>
  import("@/features/cluster").then((module) => ({
    default: module.ClusterPage,
  })),
);
const LoginPage = lazy(() =>
  import("@/features/login").then((module) => ({ default: module.LoginPage })),
);

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
});

function ConsoleLayout() {
  const { t, language, setLanguage } = useLocale();
  const { session, error, loading, epoch, refresh } = useSession();
  const location = useLocation();
  const main = useRef<HTMLElement>(null);
  const navigationToggle = useRef<HTMLButtonElement>(null);
  const [menuPath, setMenuPath] = useState<string | null>(null);
  const menuOpen = menuPath === location.pathname;
  useEffect(() => {
    main.current?.focus();
  }, [location.pathname]);
  useEffect(() => {
    const close = (event: KeyboardEvent) => {
      if (event.key === "Escape" && menuOpen) {
        setMenuPath(null);
        navigationToggle.current?.focus();
      }
    };
    window.addEventListener("keydown", close);
    return () => window.removeEventListener("keydown", close);
  }, [menuOpen]);
  const links = [
    { to: "/", icon: LayoutDashboard, label: t("Overview", "总览") },
    { to: "/topics", icon: Activity, label: "Topics" },
    { to: "/consumers", icon: Radio, label: t("Consumers", "消费者") },
    {
      to: "/identities",
      icon: ShieldCheck,
      label: t("Identity & access", "身份与权限"),
    },
    ...(!session || session.operator
      ? [{ to: "/cluster", icon: Server, label: t("Cluster", "集群") }]
      : []),
  ];
  const current =
    links.find((link) =>
      link.to === "/"
        ? location.pathname === "/"
        : location.pathname.startsWith(link.to),
    )?.label ?? t("Console", "控制台");
  return (
    <div className="console-layout">
      <a className="skip-link" href="#main-content">
        {t("Skip to content", "跳到内容")}
      </a>
      <aside
        id="console-navigation"
        className={`sidebar ${menuOpen ? "is-open" : ""}`}
      >
        <Button
          className="close-navigation"
          variant="ghost"
          size="icon"
          onClick={() => {
            setMenuPath(null);
            navigationToggle.current?.focus();
          }}
          aria-label={t("Close navigation", "关闭导航")}
        >
          <X size={17} />
        </Button>
        <Link to="/" className="brand" aria-label="TideMQ Console">
          <span>
            <Waves size={24} strokeWidth={2.5} />
          </span>
          <strong>TideMQ</strong>
          <Badge variant="outline">CONSOLE</Badge>
        </Link>
        <div className="cluster-identity">
          <div className="cluster-avatar">
            <Server size={17} />
          </div>
          <div>
            <strong>
              {session?.cluster.name ??
                t("Cluster not connected", "集群未连接")}
            </strong>
            <small>
              {session?.cluster.id ??
                t("Waiting for service connection", "等待服务连接")}
            </small>
          </div>
        </div>
        <p className="nav-group-label">{t("WORKSPACE", "工作空间")}</p>
        <nav aria-label={t("Main navigation", "主导航")}>
          {links.map(({ to, icon: Icon, label }) => (
            <NavLink key={to} to={to} end={to === "/"}>
              <Icon size={18} />
              <span>{label}</span>
            </NavLink>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <OperationLookup />
          <div className="scope-note">
            <ShieldCheck size={15} />
            <p>
              {session
                ? (session.tenant ??
                  t("Cluster operator scope", "集群运维范围"))
                : t(
                    "Access is verified by TideMQ.",
                    "访问权限由 TideMQ 核验。",
                  )}
            </p>
          </div>
          <div className="sidebar-version">TideMQ Console</div>
        </div>
      </aside>
      <div className="workspace">
        <header className="topbar">
          <div className="breadcrumbs">
            <Button
              ref={navigationToggle}
              variant="ghost"
              size="icon"
              className="mobile-menu"
              onClick={() => setMenuPath(menuOpen ? null : location.pathname)}
              aria-expanded={menuOpen}
              aria-controls="console-navigation"
              aria-label={t("Toggle navigation", "展开导航")}
            >
              <Waves size={18} />
            </Button>
            <span>TideMQ</span>
            <ChevronRight size={13} />
            <strong>{current}</strong>
          </div>
          <div className="topbar-actions">
            <span className="scope-chip">
              <span className="status-dot" />
              {session
                ? (session.tenant ?? t("Cluster scope", "集群范围"))
                : t("Not authenticated", "未登录")}
            </span>
            <label className="language-select">
              <Globe2 size={15} />
              <select
                value={language}
                aria-label={t("Language", "语言")}
                onChange={(event) =>
                  setLanguage(event.target.value === "zh-CN" ? "zh-CN" : "en")
                }
              >
                <option value="en">English</option>
                <option value="zh-CN">中文</option>
              </select>
            </label>
            <Button asChild size="sm" variant="outline">
              <Link
                to="/login"
                state={{ returnTo: location.pathname + location.search }}
              >
                <LogIn size={14} />
                {session?.principal.name ?? t("Sign in", "登录")}
              </Link>
            </Button>
          </div>
        </header>
        <main id="main-content" tabIndex={-1} ref={main}>
          {loading ? (
            <div className="connection-banner" role="status">
              <Activity size={15} />
              {t("Checking Console service…", "正在检查 Console 服务…")}
            </div>
          ) : (
            error &&
            error.kind !== "authentication" && (
              <div className="connection-banner">
                <FailureNotice error={error} compact />
                <button type="button" onClick={refresh}>
                  {t("Check again", "重新检查")}
                </button>
              </div>
            )
          )}
          <div key={epoch} className="page-content">
            <PageBoundary key={location.pathname}>
              <Suspense
                fallback={
                  <div className="empty-state" role="status">
                    {t("Loading page…", "正在加载页面…")}
                  </div>
                }
              >
                <Routes>
                  <Route path="/" element={<OverviewPage />} />
                  <Route path="/topics" element={<TopicsPage />} />
                  <Route path="/topics/:id" element={<TopicPage />} />
                  <Route path="/consumers" element={<ConsumersPage />} />
                  <Route path="/consumers/:id" element={<ConsumerPage />} />
                  <Route path="/identities" element={<IdentitiesPage />} />
                  <Route path="/cluster" element={<ClusterPage />} />
                  <Route path="/operations/:id" element={<OperationPage />} />
                  <Route path="/login" element={<LoginPage />} />
                  <Route
                    path="*"
                    element={
                      <>
                        <PageHeading
                          title={t("Page not found", "页面不存在")}
                          description={t(
                            "The address does not identify a Console page.",
                            "当前地址未对应 Console 页面。",
                          )}
                        />
                        <Panel
                          title={t("Return to your workspace", "返回工作空间")}
                        >
                          <p className="panel-copy">
                            <CircleHelp size={18} />
                            <Link to="/">{t("Open overview", "打开总览")}</Link>
                          </p>
                        </Panel>
                      </>
                    }
                  />
                </Routes>
              </Suspense>
            </PageBoundary>
          </div>
          <footer className="workspace-footer">
            <span>{t("Built for durable messages.", "面向持久消息。")}</span>
            <span>
              {t(
                "State is only as current as its observation.",
                "状态新鲜度以观测时间为准。",
              )}
            </span>
          </footer>
        </main>
      </div>
    </div>
  );
}

function OperationSession() {
  const { epoch } = useSession();
  return (
    <OperationProvider key={epoch}>
      <ConsoleLayout />
    </OperationProvider>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <LocaleProvider>
        <SessionProvider>
          <BrowserRouter>
            <OperationSession />
          </BrowserRouter>
        </SessionProvider>
      </LocaleProvider>
    </QueryClientProvider>
  );
}
