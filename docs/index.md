---
layout: home
hero:
  name: TideMQ
  text: 持久消息与独立存储
  tagline: 从第一条消息到集群部署、SDK 接入与故障恢复。
  actions:
    - theme: brand
      text: 快速开始
      link: /guide/quickstart.zh-CN
    - theme: alt
      text: 产品能力与限制
      link: /guide/overview.zh-CN
features:
  - title: 先确认持久性
    details: 区分生产提交、业务 ACK、对象发布与 WAL 回收，使用原操作身份恢复未知结果。
  - title: 按职责部署
    details: 单进程本地执行与内置 HTTPS Console；分离角色保留 RF1 / RF3 Raft 和独立 Console。
  - title: 面向应用与运维
    details: Rust SDK、Stream / Queue 订阅、作用域权限、观测、恢复和按现象排障。
---

[完整文档目录](README.zh-CN.md) · [English project README](https://github.com/bitcapybara/tidemq#readme)

当前文档对应 TideMQ `0.1.0` 开发版本。中文文档是用户主入口；[英文部署与配置手册](guide/deployment.md)提供同一版本的补充说明。

## 先选你的目标

| 你想做什么 | 从这里开始 |
| --- | --- |
| 先在一台电脑上试用 | [快速开始](guide/quickstart.zh-CN.md)：从源码启动 Combined，完成第一条消息 |
| 了解 Topic、Stream、Queue 和 ACK | [产品能力与限制](guide/overview.zh-CN.md) |
| 接入 Rust 应用 | [SDK 接入与恢复](guide/rust-sdk.zh-CN.md)，再看[生产](guide/production.zh-CN.md)和[消费](guide/consumption.zh-CN.md) |
| 部署多进程或 RF3 | [运行 TideMQ](guide/running.zh-CN.md) |
| 处理线上故障 | [按现象排障](operations/troubleshooting.zh-CN.md)，再进入侧栏中的专项手册 |

快速开始是源码验证路径，不代表生产部署或高可用承诺。生产环境请先阅读[产品边界](guide/overview.zh-CN.md)和[安全指南](operations/security.zh-CN.md)。
