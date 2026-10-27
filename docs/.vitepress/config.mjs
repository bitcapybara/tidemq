import { defineConfig } from 'vitepress';

export default defineConfig({
  title: 'TideMQ',
  description: 'TideMQ 产品使用、部署与运维文档',
  lang: 'zh-CN',
  base: '/tidemq/',
  themeConfig: {
    nav: [
      { text: '使用文档', link: '/README.zh-CN' },
      { text: '快速开始', link: '/guide/quickstart.zh-CN' },
      { text: 'GitHub', link: 'https://github.com/bitcapybara/tidemq' },
    ],
    sidebar: [
      {
        text: '开始使用',
        items: [
          { text: '产品能力与限制', link: '/guide/overview.zh-CN' },
          { text: '架构与角色', link: '/guide/architecture.zh-CN' },
          { text: '快速开始', link: '/guide/quickstart.zh-CN' },
          { text: '部署单机与集群', link: '/guide/running.zh-CN' },
          { text: '配置参考', link: '/guide/configuration.zh-CN' },
          { text: 'Console', link: '/guide/console.zh-CN' },
        ],
      },
      {
        text: 'Rust 应用接入',
        items: [
          { text: 'SDK 与恢复', link: '/guide/rust-sdk.zh-CN' },
          { text: '生产消息', link: '/guide/production.zh-CN' },
          { text: '消费与 ACK', link: '/guide/consumption.zh-CN' },
        ],
      },
      {
        text: '运维',
        items: [
          { text: '安全与权限', link: '/operations/security.zh-CN' },
          { text: '指标', link: '/operations/metrics.zh-CN' },
          { text: '日志', link: '/operations/logging.zh-CN' },
          { text: '追踪', link: '/operations/tracing.zh-CN' },
          { text: '恢复', link: '/operations/recovery.zh-CN' },
          { text: '排障', link: '/operations/troubleshooting.zh-CN' },
          { text: '测试方法', link: '/testing.zh-CN' },
          { text: '文档站维护', link: '/site.zh-CN' },
        ],
      },
      {
        text: '实现与历史资料',
        collapsed: true,
        items: [
          { text: '实现设计', link: '/design/architecture.zh-CN' },
          { text: '历史架构草案', link: '/architecture.zh-CN' },
          { text: '历史实施记录', link: '/implementation-status.zh-CN' },
          { text: '历史要求覆盖', link: '/requirements-coverage.zh-CN' },
          { text: '证据索引', link: '/evidence/README.zh-CN' },
        ],
      },
    ],
    search: { provider: 'local' },
    outline: { label: '本页目录', level: [2, 3] },
    docFooter: { prev: '上一页', next: '下一页' },
    sidebarMenuLabel: '目录',
    returnToTopLabel: '返回顶部',
  },
});
