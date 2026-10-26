# 当前实现设计与提交权威

2026-10-19 用户确认开发计划。本文记录已接入运行时的权威边界；具体功能和验证范围见[实施状态](../implementation-status.zh-CN.md)。

## 仓库边界

四个 package：tidemq 是服务端，tidemq-protocol 提供 ID/有界 framing/公共与内部 schema，tidemq-transport 提供共享认证 QUIC/RPC/字节准入，tidemq-sdk 位于 sdk/rust。SDK 不依赖服务端、Raft、WAL、S3，也不选择进程 allocator 或安装全局 subscriber。根 manifest、lockfile、公共 schema/导出和核心观测由协调者维护。

## 持久权威与安全契约

1. 数据 Raft 组提交正文、offset、producer 去重原结果、writer fence 和发布意图；单消息 offset 与 Raft log index 不同。持久 quorum 后才能成功，失 quorum 不隐式改为 RF1。
2. 元数据 Raft 维护 Topic/拓扑/路由、授权 epoch、配置、清单和任务生命周期；高频正文/ACK 不进入全局组。新拥有者完成授权、数据组 fence 和恢复后才条件激活路由。
3. 订阅/分区的 cursor generation、attempt 和 ACK 由所属数据组持久提交。投递前持久记录 attempt，旧 token 不确认新尝试；seek 更换 generation。
4. 生产 ACK、消费持久 ACK、对象发布、WAL 回收四个完成点分别观测。所有消息数据边界使用排他上界，local_reclaim_end ≤ object_published_end ≤ committed_end；retained_floor 独立表示逻辑保留。
5. 发布遵循元数据登记保护→数据授权→清单条件发布→数据完成。取消先进入 CancelRequested，再取得不可逆 Aborted 证据；未知结果保留数据。删除等待全部引用、pin、快照/恢复依赖和未完任务解除，DELETING 禁止新增引用。
6. 控制有效性无法证明时停止新生产/投递、控制变更、清单发布和 GC 授权。有限在途收尾仍需权限、数据 quorum 和 deadline 有效。时钟/期限与重启规则必须验证，旧磁盘证明不能无限续权。

## 客户端与恢复

网络重连保留生产者/批次身份；相同身份内容冲突明确拒绝。首版不持久化 SDK outbox，不承诺 SDK 进程重启后的未决批次自动恢复。去重结果窗口过期返回过期/未知，不自动换身份重发。

SDK 公共网络调用默认使用配置超时；`RequestContext` 通过 `CancellationToken` 和绝对期限在多个调用之间共享生命周期。一次调用内的准入、连接、RPC、重试和退避共用原预算，客户端关闭取消在途请求；生产或注册中断保留原 PendingBatch/epoch。明确 Unknown 后继续查询/重试原身份。接口与作用域传播见[SDK 手册](../guide/rust-sdk.zh-CN.md)。

默认消费可并行完成，按 key 模式同 key 串行且失败阻塞，不默认自动跳过或死信后 ACK。外部副作用仍由业务幂等或 fencing 保护。稳定域名后使用 QUIC Gateway；Gateway 故障需要重新握手、鉴权与原身份恢复。

## 部署与验收

本地 RF1 不具备 HA；RF3 内网进程测试不等于三可用区故障。对象依赖使用 aws-sdk-s3，本地统一使用 RustFS，S3 HTTP/HTTPS 与业务 QUIC 分开。生产三区、性能对标及恢复目标必须单独实测；未经确认不创建付费资源。

`serve` 按本节点配置组装角色，内部只接受带 cluster/node/role/group 证据的 mTLS 请求；对外接受经过 scope 校验的语义请求，不能提交原始 Raft 命令。元数据和数据 leader 无须同机，Gateway/Broker/生命周期/成员迁移通过封闭内部协议访问权威。磁盘应用格式为元数据 version 2、数据 version 2；本开发分支没有历史格式自动迁移器，禁止为启动新版本而自动清空旧目录。

角色与部署见[运行时设计](runtime.zh-CN.md)，对象安全见[生命周期](object-lifecycle.zh-CN.md)，容量口径见[保留与配额](retention-quota.zh-CN.md)，迁移见[弹性设计](scaling.zh-CN.md)。

## 性能优化与回归约束

先减少已确认的应用层重复复制，保留小消息路径和完整内存计费；编码与共享缓冲区所有权见[QUIC 设计](quic-transport.zh-CN.md)。新序列化格式必须先验证读取、写入、校验、保留内存和版本兼容，不能以无校验的字段读取速度替换持久数据格式；候选结论见[格式调研](../research/zero-copy-formats.zh-CN.md)。

当前验收覆盖持久正确性、原 SDK 对象恢复与可查询观测链路；性能测试工具与基线已按用户要求删除，不以功能测试耗时推断性能目标。
