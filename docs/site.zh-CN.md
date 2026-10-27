# 文档站维护

站点复用 `docs/` 中的 Markdown，使用 VitePress 默认主题、中文导航和本地搜索。文档包与 Console 前端分别安装、构建；静态产物不需要 Node.js 服务。

文档工具精确锁定 VitePress `2.0.0-alpha.21`：稳定版 1.6.4 依赖的旧 Vite 链存在已知安全问题，当前采用官方的 2.0 预发布版本，不用跨主版本 override 拼接依赖。它仍是 alpha 工具链，升级须重新执行构建和预览；不影响 TideMQ 服务运行时。

## 本地构建

使用 Node.js 24 或更新版本，从仓库根目录执行：

```sh
npm --prefix docs ci
npm --prefix docs test
npm --prefix docs run docs:build
npm --prefix docs run docs:preview
```

构建产物为 `docs/.vitepress/dist`，预览地址为 `http://localhost:4173/tidemq/`；开发模式为 `npm --prefix docs run docs:dev`。base 固定 `/tidemq/`，用于本仓库的 GitHub 项目站点；自定义域名或其他仓库路径需要先修改配置并重新校验。

VitePress 构建保留默认的 Markdown 死链检查；随后 Node 检查所有生成页面中的本地 href/src、资源文件、项目路径与 fragment，中文编码锚点也参与校验。它不验证远程网站的可用性。不要启用 `ignoreDeadLinks` 来绕过错误。仓库源码或部署目录链接使用完整 GitHub URL，使 GitHub 阅读与静态站点访问均有效。

历史设计、研究与带日期的证据保留，通过折叠的“实现与历史资料”导航进入；它们不构成当前产品承诺。新产品说明应写入 guide / operations，对应更新文档索引和侧栏，不新增一套重复文档。

## GitHub Actions 与 Pages

[docs.yml](https://github.com/bitcapybara/tidemq/blob/main/.github/workflows/docs.yml)只在 docs、两份根 README 或该工作流变更时触发 PR / main 构建；也可手动运行。构建执行 npm ci、链接检查器自测、站点构建和链接检查，并上传 `github-pages` artifact，路径为 `docs/.vitepress/dist`。

默认只构建。发布需要操作者先完成审核、本地构建和 GitHub Pages 设置，再从 main 手动 dispatch，明确勾选 `publish`。deploy job 受 `github-pages` environment 保护，且只在手动发布 main 时运行；`pages: write` 和 `id-token: write` 只授予该 job，build 仅 `contents: read`。不使用 `pull_request_target`、分支推送或第三方部署 token。

仓库管理员在 Pages 的 Build and deployment 中选择 GitHub Actions，并核对 environment 的分支与审批规则。预计项目路径为 `https://bitcapybara.github.io/tidemq/`；以真正部署成功后的 `page_url` 为准。准备工作流或产物不表示远端已启用、发布或可访问。远端设置由仓库管理员单独配置。

实现依据：[VitePress 官方部署说明](https://vitepress.dev/guide/deploy#github-pages)、[官方配置参考](https://vitepress.dev/reference/site-config)、[GitHub 自定义 Pages 工作流](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages)。
