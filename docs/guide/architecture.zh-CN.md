# 架构与角色

TideMQ 将入口连接、业务协调、复制 WAL、控制元数据和历史对象分开。一个 `tidemq` 二进制通过配置选择进程职责；`combined` 合并四个 MQ 角色，`console` 是独立的浏览器入口。

```text
Application SDK -> Gateway -> Broker -> Storage data Raft -> S3
                     |          |             |
                     +----------+---- Metadata Raft

Browser -> HTTPS -> Console -> authenticated SDK -> Gateway
```

## 每个角色负责什么

| 角色 | 持久权威与责任 | 部署关注点 |
| --- | --- | --- |
| Gateway | TLS / token 鉴权、公共 QUIC、缓存路由与转发；不以缓存结果替代提交权威 | UDP 入口、稳定域名和 QUIC 四层负载均衡；失败后 SDK 需要重新连接 |
| Broker | 持有服务所有权，协调生产、投递、缓存与准入 | 多个 Broker 可接管，但需完成数据 fence 和恢复才能激活新路由 |
| Storage | 数据 Raft：正文、offset、producer 去重结果、writer fence、attempt 与持久 ACK；执行归档和冷读 | 每个数据组独立 quorum、持久卷、容量和恢复检查点 |
| Metadata | 独立 Raft：租户、权限、Topic / Shard 布局、节点目录、所有权、manifest 和控制计划 | 不承载高频正文或逐条 ACK；增加备用 Metadata 不增加控制写吞吐 |
| Combined | 一个进程同时运行上述四种 MQ 职责，仍使用分离的元数据和数据组 | RF1 便于本地运行；RF3 Combined 与单机 RF1 的可用性不同 |
| Console | HTTP 静态资源、无状态加密会话、按登录用户权限调用 SDK | 不加入 peer / voter、不挂载 MQ 数据卷；需要 HTTPS 和副本共享会话密钥 |

MQ 角色的公共业务入口是 Gateway / Combined；内部角色不直接作为应用 endpoint。管理 HTTP 必须 loopback；浏览器访问 Console，不能直接暴露节点 `/diagnostics`。配置入口见[每角色配置](configuration.zh-CN.md)。

## 一条消息的完成点

1. 应用向逻辑 Topic 发送；SDK 保留 Producer 身份与原请求，Gateway 路由到 Broker。
2. 数据组验证授权、writer fence 与去重条件，分配 offset 并按 Raft 规则持久提交；成功回执才表示生产完成。
3. 订阅投递 attempt 先持久记录，再向应用提供正文。正文可来自热 WAL 或对象冷读。
4. 业务处理成功后，Stream 累计 ACK 或 Queue 单条 ACK 再次持久提交，才表示消费确认完成。
5. 后台工作独立完成对象登记、上传、manifest 发布与数据侧完成记录；满足全 voter 检查点和恢复保护后才回收本地正文。

因此网络 packet ACK、消息接收、Producer 成功、业务 ACK、对象发布和物理回收不能互相替代。响应丢失保持未知结果并恢复原操作，不能换身份重发或删除状态。

## Topic、Shard 与订阅

应用使用 `tenant/namespace/topic`。Topic 拆分、合并和权威转移由平台操作，普通 Producer / Consumer 不填写物理 epoch、generation 或复制 cut。MessageId 在这些操作中保持其出生身份。同一 Topic 的不同订阅具有独立进度；订阅无人在线仍保留状态和受保护积压。

逻辑积压、物理空间、共享回收滞后和进程内存是不同容量口径；不能相加，也不能用应用预算当作 RSS 上限。详见[配置](configuration.zh-CN.md)、[消费契约](consumption.zh-CN.md)和[内存排障](../operations/memory-troubleshooting.zh-CN.md)。

## 可用性与恢复

生产 RF3 需要跨真实故障域放置，并独立确认 Metadata 和数据组 quorum。Broker/Gateway 重启与 Raft 副本恢复是不同路径；前者还涉及 SDK 重连、所有权 fence 和成员恢复。对象存储不会自动取代丢失的 quorum 或完整 Raft 状态。

部署见[运行指南](running.zh-CN.md)，故障操作见[恢复](../operations/recovery.zh-CN.md)。状态机与协议细节继续保留在[实现设计](../design/architecture.zh-CN.md)，历史候选目标见[原架构草案](../architecture.zh-CN.md)。
