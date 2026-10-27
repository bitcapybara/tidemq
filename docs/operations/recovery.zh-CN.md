# 重启、升级与恢复

恢复的目标是保留原身份、已提交数据和仍未知的操作结果。先记录源码/二进制版本、配置、cluster/node/group 身份、故障时间、相关 MessageId / operation ID 和运行节点的诊断；不要清空 WAL 或把 RF3 改成 RF1。

## 正常重启

1. 停止应用的新任务，等待业务处理，检查 SDK 的未决发送、ACK 和管理操作；恢复需要保留的原句柄。
2. 检查 close 报告。关闭不会替业务 ACK，也不删除持久订阅。
3. 向服务发送 SIGTERM / Ctrl-C，等待有界 drain 结束并保留退出日志。
4. 用原配置、节点身份、证书和完整数据目录重启；不要重新运行仅支持新目录的身份生成器。
5. 分别检查 health、readiness、Metadata / 数据 quorum，再验证原 Producer / 订阅的生产、接收和持久 ACK。

进程关闭使用共享的五秒总期限，超时或失败返回错误。端口仍占用时核实旧进程，不使用重复实例或端口复用掩盖问题。

## 按故障恢复

| 故障 | 操作 | 成功依据 |
| --- | --- | --- |
| Gateway 重启 / 入口中断 | 恢复 UDP 入口、TLS 与路由；由原 Client 重新连接 | 原 PendingSend / ACK 的结果、订阅继续和实际持久请求 |
| Broker 故障 | 检查所有权 Grant / Fence / Recover / Activate 及数据侧 fence | 新 owner 激活且旧 owner 不能继续写入；RPC 重连本身不是接管 |
| Storage / Metadata 单副本重启 | 恢复原目录与身份，检查真实复制和 catch-up | 相应组 quorum、提交与读取正常；进程存在不等于副本就绪 |
| 多数派丢失 | 优先恢复原副本与网络，保留原 voter 和数据 | quorum 恢复后继续原操作；S3 不能替代缺失的 Raft 多数派 |
| 磁盘满 / 容量采样未知 | 保存诊断，恢复实际空间或采样，检查回收依赖 | 水位与样本新鲜度恢复，已有 ACK / 控制 / GC 能继续 |
| S3 中断 / 对象任务未知 | 恢复同一桶/前缀的连通性，按原对象身份对账 | 上传、manifest 发布、数据完成及回收分别闭合 |
| SDK 进程退出且结果未知 | 用业务幂等账本和原消息/业务身份核实 | 不把新发送成功当成旧操作未执行的证据 |

详细命令及诊断字段见[存储](storage-troubleshooting.zh-CN.md)、[所有权](ownership-troubleshooting.zh-CN.md)、[QUIC](quic-troubleshooting.zh-CN.md)、[对象](object-storage-troubleshooting.zh-CN.md)和[订阅](subscription-troubleshooting.zh-CN.md)。

## 数据保全与升级

当前副本采用版本化分段 WAL。完整状态包括 `identity.bin`、`manifest.bin` 及其引用的段、检查点与具名快照；不能只复制最新段或单个 snapshot。复制仍在运行的目录不构成已验证的一致备份。离线保全完整副本目录、原配置、证书和匹配二进制，也不自动构成经过演练的集群级备份/恢复方案。

仓库没有旧实验格式的一键迁移或任意混合版本滚动升级承诺。旧 `raft.wal` / `snapshot.bin` 布局会被拒绝并保留。升级应先在隔离的恢复副本上验证目标二进制与格式，设计停机与回退步骤；不手工修改序列化数据、不删 manifest 引用文件、不用新空目录冒充旧节点。

对象不能按年龄或“已上传”直接删除。保留、manifest 引用、未决发布、pin 和恢复依赖均解除后才由生命周期流程删除；本地回收还依赖全 voter 检查点。详见[存储恢复契约](../design/storage-recovery.zh-CN.md)与[对象生命周期](object-lifecycle.zh-CN.md)。

## 计划替换与退出

更换节点采用在线登记、准备实际副本、`replace_data_replica` / `replace_metadata_replica`、检查原计划，然后 `begin_node_drain` → `node_drain_status` → `retire_node`。从存活 Gateway 操作；注册成功不等于数据副本已准备好，停止新调度也不等于已排空。退休身份不可复用，成功前不销毁卷。

管理超时沿原 PendingAdmin / operation ID 恢复，不创建冲突替代计划。参考[SDK 运维接口](../guide/rust-sdk.zh-CN.md)与[扩缩容](scaling.zh-CN.md)。在真实部署进行故障演练时，保存故障注入、已提交与 ACK 账本、复制状态和退出清理证据；单机进程崩溃测试不等于宿主掉电或真实三区验收。
