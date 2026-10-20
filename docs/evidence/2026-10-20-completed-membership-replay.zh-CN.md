# 已完成成员计划重放的并发确认

## CI 证据边界

CI run `39377051991` 的 allocator-system job `118477435548` 在 `fb12dc1` 的 `tests/runtime_queue.rs:1260` 失败。该行是首次迁移成功、已返回原计划之后的重复 `replace_replica` 确认，错误为 `Unavailable / Unknown / retryable`；不能归为首次追赶失败或上一轮磁盘不足。

保存的目录 `/tmp/tidemq-ci-system-39377051991` 只有其他失败场景的 12 个 subprocess 日志。显式替换场景的九个节点全部嵌入测试进程，没有该场景的服务日志。因此原 CI 内部失败阶段无法从旧工件确认。root 的 typed logger 也不能替代未安装 subscriber 的嵌入节点日志。

本机 allocator-system 原同名场景通过一次基线和随后 20 轮。该结果没有证明旧 CI 的真实内部原因。

## 定向红例

在真实 RF3、metadata/data leader 不同、迁移已经 complete 的 fixture 中，暂停另一个执行器的 Change→fresh View 响应，使其真实持有该组的变更锁。相同 canonical completed plan 再执行时，明确在 Change 返回 `Overloaded`，3.13 秒内失败；计划 operation ID 相同。该路径说明完成确认不应重新竞争变更阶段，但不能据此声称旧 CI 恰好命中了该竞争。

## 变更范围与约束

- 完成重放改为新鲜 metadata 原记录确认、当前 data leader 的真实 ObserveStable、再次 metadata 原记录确认；仍校验原 canonical ID、generation+1、target 和当前 complete。
- 复用私有三秒证明有效期，起点为 data 请求开始；实际源节点、voters、完整 plan、membership/applied log 和 term 均核验。没有增加有效期、重试、准入名额或复制预算。
- pending 的 Prepare→Change→Complete 保持原阶段。完成重放不写 metadata revision 或 membership log，Finish 继续有限清理原 lease。
- generation 前移、目标不符、退休记录、证明后 metadata 变化均拒绝。真实 proof 回复丢失仍为 Unknown，重新读取原 canonical plan 后才能再次确认。
- RPC Remote 错误添加固定阶段上下文并保留原 typed source；没有改变线缆失败分类。runtime 重放失败使用现有有界 HTTP helper 在两秒内采集至多五个相关节点的计划、成员/leader、metadata applied/vote 和 quota 摘要，没有正文、凭据或全节点日志。

## 验证

定向并发红例在只读路径实现后首轮转绿：两个真实 scaling_remote 场景共 4.99 秒。allocator-system 的 `--lib --test scaling_remote --test runtime_queue -- --test-threads=1` 为 104 passed、5 ignored、100.30 秒。包级 tests Clippy、workspace tests/examples Clippy，以及禁止 unwrap/expect/panic/未实现/越界索引的 workspace 严格生产 Clippy 均通过。原 CI 缺失的内部阶段仍保持未知，不将本机通过等同为 Linux CI 已通过。原 runtime 场景的完整 `[21,22,24]` 稳定成员、原 SDK Producer/Consumer、消息唯一 offset 和连续 ACK 前沿断言保留。
