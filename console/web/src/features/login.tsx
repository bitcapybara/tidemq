import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { KeyRound, LockKeyhole, Waves } from "lucide-react";
import { useLocation, useNavigate } from "react-router";
import { useSession } from "@/api/session";
import {
  FailureNotice,
  PageHeading,
  RefreshButton,
} from "@/components/console";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useLocale } from "@/locales/locale";

export function LoginPage() {
  const { t } = useLocale();
  const session = useSession();
  const navigate = useNavigate();
  const location = useLocation();
  const schema = z.object({
    token: z
      .string()
      .min(1, t("Enter your personal token.", "请输入个人 token。"))
      .max(16384, t("The token is too long.", "token 过长。")),
  });
  const {
    register,
    handleSubmit,
    reset,
    formState: { errors, isSubmitting },
  } = useForm({ resolver: zodResolver(schema), defaultValues: { token: "" } });
  const unavailable = session.loading;
  const busy = session.authenticating || isSubmitting;
  const submit = handleSubmit(async ({ token }) => {
    reset();
    try {
      await session.login(token);
      const returnTo: unknown = location.state?.returnTo;
      navigate(
        typeof returnTo === "string" &&
          returnTo.startsWith("/") &&
          !returnTo.startsWith("//") &&
          !returnTo.includes("\\") &&
          !returnTo.startsWith("/login")
          ? returnTo
          : "/",
      );
    } catch {
      /* The provider preserves errors across identity cache resets. */
    }
  });
  return (
    <>
      <PageHeading
        title={t("Access your cluster", "访问集群")}
        description={t(
          "Use a personal TideMQ token. Your identity and access remain scoped to the cluster.",
          "使用个人 TideMQ token 登录，按身份与作用域访问集群。",
        )}
      />
      <div className="login-layout">
        <div className="login-intro">
          <div className="login-mark">
            <Waves size={42} strokeWidth={1.8} />
          </div>
          <h2>
            {t(
              "One place to understand your messages.",
              "在一个入口理解消息状态。",
            )}
          </h2>
          <p>
            {t(
              "Inspect bounded message ranges, follow consumer progress and manage resource access.",
              "有界排查消息，跟踪消费进度，管理资源授权。",
            )}
          </p>
          <div>
            <LockKeyhole size={17} />
            <span>
              {t(
                "Short-lived protected session · token stays in memory until submitted",
                "短期受保护会话 · 提交前 token 仅保存在内存中",
              )}
            </span>
          </div>
        </div>
        <section className="login-card" aria-label={t("Sign in", "登录")}>
          <div className="login-card-title">
            <KeyRound size={20} />
            <h2>
              {session.session
                ? t("Current identity", "当前身份")
                : t("Sign in with token", "使用 token 登录")}
            </h2>
          </div>
          {session.session ? (
            <>
              <dl className="detail-grid">
                <div>
                  <dt>{t("Principal", "主体")}</dt>
                  <dd>{session.session.principal.name}</dd>
                </div>
                <div>
                  <dt>{t("Cluster", "集群")}</dt>
                  <dd>{session.session.cluster.name}</dd>
                </div>
              </dl>
              <Button
                variant="outline"
                onClick={() => {
                  void session
                    .logout()
                    .then(() => navigate("/login"))
                    .catch(() => undefined);
                }}
              >
                {t("Sign out", "退出登录")}
              </Button>
            </>
          ) : (
            <form onSubmit={submit}>
              <label htmlFor="personal-token">
                {t("Personal token", "个人 token")}
              </label>
              <Input
                id="personal-token"
                type="password"
                {...register("token")}
                disabled={unavailable || busy}
                autoComplete="off"
                spellCheck={false}
                aria-invalid={Boolean(errors.token)}
                aria-describedby="token-hint token-error"
                placeholder={t("Paste your personal token", "粘贴个人 token")}
              />
              <small id="token-hint">
                {t(
                  "Sent only to this Console service over the same origin.",
                  "仅通过同源请求发送至当前 Console 服务。",
                )}
              </small>
              {errors.token && (
                <small id="token-error" role="alert" className="field-error">
                  {errors.token.message}
                </small>
              )}
              {session.loginFailure && (
                <FailureNotice
                  error={session.loginFailure}
                  compact
                  message={
                    session.loginFailure.kind === "authentication"
                      ? t(
                          "The token is invalid or expired, or its principal has been disabled. Check the credential and try again.",
                          "token 无效或已过期，或所属主体已停用，请检查凭据后重试。",
                        )
                      : undefined
                  }
                />
              )}
              {session.logoutFailure && (
                <>
                  <FailureNotice error={session.logoutFailure} compact />
                  <p className="footnote">
                    {t(
                      "Local protected data has been cleared. The service did not confirm session removal; retry sign out before leaving a shared device.",
                      "本地受保护数据已清除，但服务尚未确认移除会话；离开共享设备前请重试退出。",
                    )}
                  </p>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => void session.logout().catch(() => undefined)}
                  >
                    {t("Retry sign out", "重试退出")}
                  </Button>
                </>
              )}
              {unavailable && (
                <p className="login-unavailable" role="status">
                  {t(
                    "The session API is not connected. Sign-in is unavailable; no token will be submitted.",
                    "会话接口尚未接入，暂时无法登录，不会提交 token。",
                  )}
                </p>
              )}
              <Button
                type="submit"
                className="login-submit"
                disabled={unavailable || busy}
              >
                {busy ? t("Signing in…", "正在登录…") : t("Sign in", "登录")}
              </Button>
            </form>
          )}
          <div className="login-card-footer">
            <span>{t("Service connection", "服务连接")}</span>
            <RefreshButton
              onClick={session.refresh}
              loading={session.loading}
            />
          </div>
        </section>
      </div>
    </>
  );
}
