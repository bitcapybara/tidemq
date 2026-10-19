# 排障入口

先保存构建版本、allocator/feature、原配置、操作身份与故障时间，随后按运行节点的 `/healthz`、`/readyz`、`/diagnostics`、`/metrics` 对齐证据。管理入口仅 loopback；诊断是本进程已应用状态，不把 follower 本地视图当成线性权威。命令 `tidemq diagnose` 只描述它自身的新进程。

| 现象 | 先核对 | 对应手册与恢复方式 |
| --- | --- | --- |
| 启动失败/未 ready | 配置未知字段、身份目录、证书/DER pin、真实 quorum、容量 sample age | [运行时](runtime.zh-CN.md)与[存储](storage-troubleshooting.zh-CN.md)；保留原身份恢复，不删 WAL 或临时降 RF |
| 连接停顿/重连 | 当前 active connection、TLS、packet loss/PTO、flow blocked、liveness failure | [QUIC](quic-troubleshooting.zh-CN.md)；活性探测验证新原生包 ACK，不能代替业务提交 |
| Producer Unknown/Blocked | 原 PendingBatch 的 producer/epoch/sequence/digest，owned pending、commit/fsync | [生产](producer-troubleshooting.zh-CN.md)；先恢复原未决结果，再提交后续消息 |
| Fenced 或控制失效 | 元数据有效期、数据 fence、当前 owner/成员，是否正在 joint | [控制权](ownership-troubleshooting.zh-CN.md)；刷新权威和路由，旧 token 不改成新代次 |
| 消费/ACK 停顿 | 最早未确认 offset、holes、attempt、失败 key、H/连续接收屏障 | [订阅](subscription-troubleshooting.zh-CN.md)；收到正文不代表持久 ACK，业务失败不自动跳过 |
| QuotaExceeded/采样 Unknown | 逻辑积压、物理空间、共享回收滞后、RSS、cgroup、磁盘余量分别检查 | [保留与配额](../design/retention-quota.zh-CN.md)；恢复低水位和新鲜采样，保留已有 ACK/控制/GC 资源 |
| S3/回收停顿 | 原对象 ID、上传/发布/完成、pin/恢复保护、实际全 voter checkpoint | [S3](object-storage-troubleshooting.zh-CN.md)与[生命周期](object-lifecycle.zh-CN.md)；按原身份对账，不按对象年龄直接删除 |
| 迁移 Unknown/长期 repair | 原 plan、learner/joint/stable、源与目标新鲜容量、独立修复预算 | [扩缩容](scaling.zh-CN.md)；继续原计划，不重复加 learner 或丢弃预留 |
| 内存增长 | 业务许可、热正文、ACK 逻辑状态、allocator allocated/retained、RSS/cgroup 独立趋势 | [内存](memory-troubleshooting.zh-CN.md)；相同 binary/allocator 符号化，未知字段不填零 |
| 日志缺失/采集失败 | dropped/oversized/sink_errors、scrape up/耗时/样本上限 | [日志](logging.zh-CN.md)与[指标](metrics.zh-CN.md)；用有限临时过滤，不全局无限 DEBUG |
| trace 缺失/OTLP 失败 | 采样率、队列/字节许可、discarded/export_failures/timeouts、Collector accepted/refused | [追踪](tracing.zh-CN.md)；采样或导出失败不说明提交失败，不据此创建新的业务身份 |

短时详细关联使用启动参数 `--trace-seconds`，最多 300 秒后恢复过滤。受控服务 profile 使用同构建的 `--profile-output` 私有目录；native dump 超时后仍可能运行，原 owned 槽位释放前不得叠加新 dump。不要把消息正文、令牌、私钥、S3 凭据或投递签名根放进诊断包。

复现时明确区分网络接收、Producer 持久成功、消费接收屏障、业务持久 ACK、对象 Uploaded/Published/本地 Reclaimed。指标定位到某段后再启用对应模块的短日志和故障实验；最终以原业务身份恢复且全部已 ACK 数据仍可验证作为闭环。完整命令见[测试方法](../testing-benchmarks.zh-CN.md)，候选性能与恢复目标见[性能口径](../performance.zh-CN.md)。
