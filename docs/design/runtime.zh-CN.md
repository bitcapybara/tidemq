# 运行时装配与语义边界

运行时在一个进程拥有监听器、共享预算、统一指标 Registry 和后台任务。库不安装全局日志 subscriber、分配器或 exporter；进程入口选择这些设施。Combined 装配本地持久元数据/数据组、四个 MQ 职责及原生 HTTPS Console，不创建内部 Client、PeerPool、Raft 或 QUIC 监听。分离 RF1 / RF3 只创建各角色托管的真实组件；Storage 只有本组磁盘副本，Broker 不创建假的本地数据状态。

TOML 的角色边界在反序列化时校验，运行时继续使用同一份 `Config` / `RuntimeConfig`。外部监听位于 `[gateway]`，retention、归档与 S3 位于 `[storage]`；Combined 另需 `[console]` / `[console.session]`。Combined 的 `[runtime]` 只接受逻辑组、容量和业务策略；可信 peer/voter 拓扑、内部监听、bootstrap 与 TLS trust_roots 仅属于分离角色。`[authorization]` 提供首次初始化管理员 token 路径，业务凭据和授权在线持久管理，投递密钥由数据组持久保存。未知、旧字段及未启用角色的表均拒绝。完整字段见[配置指南](../guide/configuration.zh-CN.md)；调整配置须保留原身份与数据。

## 请求路径

1. Gateway 使用 TLS 和真实凭据检查外部操作，解析主题和分区，读取路由，将外部 `owner_epoch=0` 替换成可信路由 epoch。
2. Combined 在同进程传递可信 actor 和原业务身份，使用有界请求队列、共享准入和原 deadline；Console 直接调用本地业务接口。本地语义调用不经内部网络 envelope 编解码，该封装仅由 Remote 适配器使用。分离角色由 Gateway 转发给 owner Broker，再访问数据 Leader，每跳验证 mTLS 身份与 delegated principal/scope。
3. 数据权威重新检查原业务权限、新鲜控制 lease 和 fence；分离角色另检查实际稳定 membership。业务只通过封闭的语义请求构造状态机命令，客户不能直接提交命令。
4. Producer 的提交顺序只保持到本地有界队列或下游 RPC 成功入队，随后释放顺序令牌并独立等待持久 ACK。

分离角色的连接按 peer、身份、协议和流量类别共享。C 有核心 lane，P/D/R 为有限四 lane，B 为两 lane。P 按完整 producer 业务域散列；D/B 按 subscription/cursor/assignment 业务域分配。R 只用于真实非空 Raft 日志，不将 Gateway 业务伪装成 Raft。大快照和对象清单走 B；投票与 heartbeat 使用原生 Raft C 硬保留执行槽，避免语义控制等待 quorum 时占尽自身所需的复制控制能力。下文涉及远端 leader、voter 与成员迁移的步骤适用于分离部署；Combined 以实际本地提交位置和恢复健康为依据。

Combined 在打开 WAL 前校验 Console 素材、会话与 HTTPS 身份；整体 Ready 前拒绝新业务写入。必需 worker 意外退出或 panic、监听失效及确认的资源故障主动触发共享 Failed，迟到的 ready 不会覆盖 Failed / Draining。关闭保留有效 ACK / completion，所有 owned 任务、监听与存储共用一个五秒总期限。入口与观测见[Console 就绪](../guide/console.zh-CN.md#就绪和故障)。

## 新鲜权限与故障

Metadata Leader、数据 Leader 和 owner Broker 可以在不同节点。远端权威结果必须来自指定证书目标，绑定请求、operation、group、cluster、真实 quorum 位置与稳定 voter；TTL 从发请求之前开始计时。VerifiedGrant、ControlLease 和 cold 字节能力保持私有构造，不提供任意数字的公共工厂。

路由缓存只用于定位，不能当成权限。每分区 lease 使用独立 slot，避免一次元数据慢请求占据全局缓存锁。数据组配置依据元数据 quorum 新鲜查询缓存，实际 joint 配置或实际/声明 voter 不一致时拒绝新能力；完成稳定 membership 后按新 generation/voter 重新校准。初始配置的 voter 用于首次启动，standby 用于显式预置副本。

元数据控制中断时停止新写、新投递和控制变更；已取得的有限 finalize 能力按原期限完成。Cancellation 只影响指定 request 的等待，取消结果是 Unknown，不声称撤销已经进入 Raft 的业务。重试保留原业务键，新的 request ID 只标识尝试。

Metadata Leader 的 owned reconcile 任务使用受信只读 Probe 自动识别 owner Broker 失联，并复用已有 grant→fence→recover→activate 状态机执行 CAS 接管。探测证据、分页和每轮移交数量有固定上限，Leader/quorum 变化会清空失败证据；未知中间阶段继续原 grant。具体阈值、固定指标、排查步骤和双故障边界见 [Broker 自动故障接管](broker-failover.zh-CN.md)。

消费者的 B/D 分界 H 由 assignment 冻结。连续 enqueue 的 `received_before>=H` 只授权开始 D；ACK frontier 尚小于 H 时仍允许 B 的旧消息重投。D 严格从 H 开始。ACK hole ranges 仅帮助判断连续收到前缀，不能作为业务 ACK。

Read 的消息数和字节数都是上限。首条正文和冷页准备完成、首次 `RecordDelivery` 之前，运行时固定一次新投递工作片：截止时间取开始后 250ms 与剩余有效期一半的较早值。有效期取原请求 deadline、当前 writer lease、持有冷页的 pin 与全部控制 lease 的最早到期时间；工作片后续只能缩短，不能续期或重置。起始权限刷新提示按实际控制 TTL 和请求剩余时间的一半计算，支持合法短 TTL，不扩大授权期限。

已有明确持久交付前缀时，超过工作片或需要另一冷页便停止新投递，并经原 session、代次、ACK ranges 和冷页检查后返回部分批次。每个 token 仍须等待对应 `RecordDelivery` 持久成功；已开始写入的 Unknown 或错误仍使整个 Read 返回错误，不能伪装成部分成功。首条准备阶段受原期限约束；没有可用预算时返回不可用或期限错误，不反复返回空批。冷页正常返回后显式释放 pin，异常路径仍由既有 TTL/reaper 回收；接收屏障与 ACK 前沿语义保持一致。

一次 Read 至多持有一个计费热页，在页内通过 checked offset/get 共享消息正文，减少重复 quorum 热读。页耗尽时先释放旧页，再按原消息数和字节上限取得下一页，持续生产下仍可刷新已提交边界。每条消息继续独立持久化 `RecordDelivery` 并校验当前 fence、代次和保留边界；热正文回收冲突会清除缓存页，再进入原有冷读证明流程。页面许可跟随实际 owned 读取工作释放，取消调用方不会提前归还；新投递工作片仍只缩短。真实页复用、回收冲突和取消回归见[实施证据](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/docs/evidence/2026-10-21-live-read-hot-pages.zh-CN.md)。

热正文取得后，本地副本可能在 `RecordDelivery` 校验前被后台回收。状态机仍严格拒绝没有正文证据的签发；运行时只识别这一发生在投递状态变更前的 typed Producer `OffsetExpired`，并重新核对逻辑 `retained_floor`。消息仍在保留范围内时，已有持久前缀可经原尾部检查返回；首条则沿用同一 offset、attempt、operation、原 deadline 和只缩短的工作片，至多重取一次，通过既有 S3 冷页证明完成签发。真正的逻辑过期仍为终态 `OffsetExpired`，Unknown 仍使整个 Read 失败。该冲突用固定无标签的 `tidemq_runtime_read_hot_reclaim_conflicts_total` 记录，不逐条输出正文或 token。

## 后台对象与容量

后台先分页核对 Catalog 原对象 ID 和本地 pending 索引，再构造新归档。Published 的本地完成状态和 checkpoint 分开对账，checkpoint 失败不会误当成可回收。逻辑保留 floor 在数据组里同序提交，使用同锁的 revision、时间边界和所有有效订阅的保护下界；对象 manifest 退休必须从数据 quorum 读真实 floor，并取得当前全部 voter 的持久快照证据。

短期 pin 在正常冷读完成后显式释放；未释放 pin 由有界 reaper 发现，按域规则重新等待完整 TTL 后 CAS 释放。reaper 与归档不共享长时间执行槽。

容量采样区分逻辑未确认 backlog、本地物理文件和对象注册上界、共享对象回收滞后、OS RSS 以及独立 cgroup headroom。物理文件采样只读配置内有限副本目录，包含真实 WAL、snapshot 和原子替换文件长度；不把已保留的编码日志字节数当成物理大小。RSS 不与 allocator resident 或 cgroup current 相加。未知或陈旧样本暂停可选准入，Read/ACK/Control/GC 保留出口。高低水位避免临界值来回抖动。

管理 HTTP 使用独立 loopback 监听、四个并发槽、8 KiB 请求和 256 KiB 回复上限，拒绝带请求体和 Transfer-Encoding 的请求。统一 Registry 只导出一个 EOF；指标不以 topic、partition、node、operation 或任意外部字符串创建标签。

`/ownership/<topic UUID>/<partition u32>` 复用 Metadata 的按键诊断，返回单个分区的 `local_applied` owner、phase、epoch、route generation 和提交位置，不遍历快照，也不导出密钥、正文或投递 token。它与写响应共用两秒总期限；本地视图不提供线性一致性或写入授权证明。Docker 故障脚本用多数 Metadata 一致的诊断选择实际 Broker，再以原 SDK 的持久账本确认接管结果。


## 节点计划退出

受信 Metadata 运行时的 `drain_node` 先将节点的 `schedulable=false` 提交到元数据多数派，重启不会恢复为可调度。调用提供至多 64 个明确分区移交，逐个执行 prewarm、旧 owner 停新准入和 grant→fence→recover→activate。每次重试沿用已持久化业务身份；在途 ACK 的限时完成规则保持有效。

元数据 leader 的有限检查同时报告尚拥有的分区、数据组、未完成成员计划和 Metadata 角色阻塞。只有 durable blockers 清零才发送 Stop；目标节点自行经已认证语义 RPC 再次取得新鲜检查结果，不信任调用方传入的数字。之后进入统一的五秒关闭期限，停止接入并排空 owned 请求、连接及 WAL。数据成员仍在组内或 Metadata 角色未迁走时返回阻塞而不退出。Storage 的固定 standby 自动替换可推进数据组移出；当前 Metadata 成员退出保持显式阻塞，不能降 quorum 绕过。
