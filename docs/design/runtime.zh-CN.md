# 运行时装配与语义边界

运行时在一个进程拥有监听器、共享预算、统一指标 Registry 和后台任务。库不安装全局日志 subscriber、分配器或 exporter；进程入口选择这些设施。Combined 装配完整 RF1，其他角色只创建其托管的真实组件。Storage 只有本组磁盘副本，Broker 不创建假的本地数据状态。

## 请求路径

1. Gateway 使用 TLS 与 scoped 静态令牌检查外部操作，解析主题和分区，读取路由，将外部 `owner_epoch=0` 替换成可信路由 epoch。
2. Gateway 将同一业务身份转发给 owner Broker，Broker 转发给当前数据 Leader。内部每跳由 mTLS 叶证书指纹绑定 cluster、node、role、group 与流量类别，再按静态授权表解析 delegated principal/scope。
3. 数据 Leader 重新检查原业务权限、新鲜控制 lease、数据 fence 和实际稳定 membership。业务只通过封闭的语义请求构造状态机命令；客户不能反序列化并提交 Raft Command。
4. Producer 的提交顺序只保持到本地有界队列或下游 RPC 成功入队，随后释放顺序令牌并独立等待 ACK。同一 P lane 不在整个 quorum 等待期间被一个请求占据。

连接按 peer、身份、协议和流量类别共享。C 有核心 lane，P/D/R 为有限四 lane，B 为两 lane。P 按完整 producer 业务域散列；D/B 按 subscription/cursor/assignment 业务域分配。R 只用于真实非空 Raft 日志，不将 Gateway 业务伪装成 Raft。大快照和对象清单走 B；投票与 heartbeat 使用原生 Raft C 硬保留执行槽，避免语义控制等待 quorum 时占尽自身所需的复制控制能力。

## 新鲜权限与故障

Metadata Leader、数据 Leader 和 owner Broker 可以在不同节点。远端权威结果必须来自指定证书目标，绑定请求、operation、group、cluster、真实 quorum 位置与稳定 voter；TTL 从发请求之前开始计时。VerifiedGrant、ControlLease 和 cold 字节能力保持私有构造，不提供任意数字的公共工厂。

路由缓存只用于定位，不能当成权限。每分区 lease 使用独立 slot，避免一次元数据慢请求占据全局缓存锁。数据组配置依据元数据 quorum 新鲜查询缓存，实际 joint 配置或实际/声明 voter 不一致时拒绝新能力；完成稳定 membership 后按新 generation/voter 重新校准。初始配置的 voter 用于首次启动，standby 用于显式预置副本。

元数据控制中断时停止新写、新投递和控制变更；已取得的有限 finalize 能力按原期限完成。Cancellation 只影响指定 request 的等待，取消结果是 Unknown，不声称撤销已经进入 Raft 的业务。重试保留原业务键，新的 request ID 只标识尝试。

消费者的 B/D 分界 H 由 assignment 冻结。连续 enqueue 的 `received_before>=H` 只授权开始 D；ACK frontier 尚小于 H 时仍允许 B 的旧消息重投。D 严格从 H 开始。ACK hole ranges 仅帮助判断连续收到前缀，不能作为业务 ACK。

## 后台对象与容量

后台先分页核对 Catalog 原对象 ID 和本地 pending 索引，再构造新归档。Published 的本地完成状态和 checkpoint 分开对账，checkpoint 失败不会误当成可回收。逻辑保留 floor 在数据组里同序提交，使用同锁的 revision、时间边界和所有有效订阅的保护下界；对象 manifest 退休必须从数据 quorum 读真实 floor，并取得当前全部 voter 的持久快照证据。

短期 pin 在正常冷读完成后显式释放；未释放 pin 由有界 reaper 发现，按域规则重新等待完整 TTL 后 CAS 释放。reaper 与归档不共享长时间执行槽。

容量采样区分逻辑未确认 backlog、本地物理文件和对象注册上界、共享对象回收滞后、OS RSS 以及独立 cgroup headroom。物理文件采样只读配置内有限副本目录，包含真实 WAL、snapshot 和原子替换文件长度；不把已保留的编码日志字节数当成物理大小。RSS 不与 allocator resident 或 cgroup current 相加。未知或陈旧样本暂停可选准入，Read/ACK/Control/GC 保留出口。高低水位避免临界值来回抖动。

管理 HTTP 使用独立 loopback 监听、四个并发槽、8 KiB 请求和 256 KiB 回复上限，拒绝带请求体和 Transfer-Encoding 的请求。统一 Registry 只导出一个 EOF；指标不以 topic、partition、node、operation 或任意外部字符串创建标签。


## 节点计划退出

受信 Metadata 运行时的 `drain_node` 先将节点的 `schedulable=false` 提交到元数据多数派，重启不会恢复为可调度。调用提供至多 64 个明确分区移交，逐个执行 prewarm、旧 owner 停新准入和 grant→fence→recover→activate。每次重试沿用已持久化业务身份；在途 ACK 的限时完成规则保持有效。

元数据 leader 的有限检查同时报告尚拥有的分区、数据组、未完成成员计划和 Metadata 角色阻塞。只有 durable blockers 清零才发送 Stop；目标节点自行经已认证语义 RPC 再次取得新鲜检查结果，不信任调用方传入的数字。之后进入统一的五秒关闭期限，停止接入并排空 owned 请求、连接及 WAL。数据成员仍在组内或 Metadata 角色未迁走时返回阻塞而不退出。Storage 的固定 standby 自动替换可推进数据组移出；当前 Metadata 成员退出保持显式阻塞，不能降 quorum 绕过。
