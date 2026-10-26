# Live Read 页复用与一次原负载反馈

本记录对应页复用 `e3b110f`、取消生命周期注释 `e20b14c`、HotRead 元数据准入修正 `2cf302e` 合并后的干净源码 `fbe283da3bff9e6a520c10cc91a135786f8f3768`，tree 为 `9b994a94cca1668b33047ee0eca92aa6c1e63152`。单次 release 反馈通过，但不是完整性能验收，也不能证明已定位快照死锁。[小型原始摘要](live-read-hot-pages/20261021-fbe283d.json)

## 原失败与诊断边界

原 `80c7659` 完整 cohort 的 RF3 第二个候选 trial 在延迟阶段结束时完成 1024 次 commit、1000 次 ACK，Read 返回 deadline / Unknown。此前吞吐四分区均已完成 256 次 commit/ACK。原失败与更早 `4f79` 的吞吐 Unavailable 不是同一个现场，全部保留。原 80c 记录（相关性能资料已移除）

原现场检查没有证明快照与 append/apply 存在锁环。快照后应用与提交位置仍继续推进，checkpoint 本身约 65 ms；同相位成功基线也接近原 60 秒边界。这里修正的是实际 Read 路径上的重复工作，不能把时间相邻的快照日志改写成已经证实的根因。

## 最小产品改动与真实红绿

旧 Read 对每条消息调用 `Broker::hot_read(..., max_messages=1, ...)`，每次都会经过真实 quorum read barrier。改为复用现有有界 HotRead 页，并由原 Admission 持有额度；最多持有一页，替换前先释放。页耗尽后继续原循环刷新，保留持续生产时看到新提交的行为。每条消息仍使用原操作身份、代次及独立的 durable RecordDelivery，继续验证当前 fence、lease、retention 与冷热交付边界。NoEffect 回收后先丢弃热页，再按原身份进入既有冷读恢复。

真实三节点 UDP/mTLS/OpenRaft 回归先验证原消息身份和持久 ACK，再观察成功 HotRead 次数：8 条消息，旧代码为 9 次，最终复用版本为 2 次（一个数据页加原有空探测）。旧代码红、修改后绿，没有通过增加 Read 时间片、deadline、内存或 slot 配置获得结果。

页向量的元数据同时由共享 HotRead 接口正确计费。合法小正文请求实际分配 65,536 B Vec 元数据，原先只计费 513 B；修正后计费 66,048 B。默认四个并发页仍在原 4 MiB read lane 内，第五页拒绝及释放后恢复均已实测。正文 `Bytes` 继续共享，不重复计费。[准入原始证据](2026-10-21-hot-read-admission.zh-CN.md)

## 验证结果及独立失败

- 页复用实现阶段：回收、取消与元数据重放 10/10 通过，包含两个真实 RustFS 回收竞态、逻辑 retention floor 和两个 Unknown 前缀测试；SDK 全 targets 39/39 通过。
- 取消测试保留发送后的 `RequestUnknown { source: Cancelled }`。caller 取消不代表 owned 工作已经结束，页额度持续持有到真实工作和引用结束，再在原 deadline 内完成 drain，没有自动 ACK。
- 相关 runtime/ACK/retention 选择器实际为 37/38 通过。失败发生在 `tests/runtime_queue.rs:984` 的 Consumer::claim，返回 Unavailable/Unknown，尚未进入该 consumer 的 Read；由独立诊断线处理，原失败不能计为全套通过。
- 合并正确元数据计费后的 `fbe283d`：11/11 Broker、真实 Read、取消回归通过；严格 all-target Clippy 与 lib/bins 的 panic、unwrap、expect、indexing 审计全部通过。

## 唯一一次修改后 release 反馈

服务由受保护的原 build 函数实际重编译：先在私有缓存清理 workspace release 产物，再编译三个 workspace crate，耗时 1 分 40 秒。服务使用原 `allocator-mimalloc`、Rust 1.99.1，SHA256 为 `dd0efafbb811e2617a3fafd824be29d23ec1d72e8a46c2fb40144caa7752020e`。负载仍是原 `80c7659` SDK/load 二进制，SHA256 为 `7eecb0b5a9c0a0868668d071994fd32483ccca6be45c362cd9dc05ab54bceb73`，实际文件哈希已核验。

只执行一次独立修改前反馈和一次独立修改后反馈，均沿用原 RF3 拓扑、throughput → latency 顺序、预热、负载、原 60 秒阶段期限与预算。两次均保留全部结果，不计入接受 cohort，不追加同源试次挑选通过结果。

| 实际观测 | 独立修改前：80c | 独立修改后：fbe283d |
| --- | ---: | ---: |
| 吞吐四分区 commit/ACK | 每分区 256/256 | 每分区 256/256 |
| 吞吐生产最长完成时间 | 21.903 s | 22.813 s |
| 吞吐 ACK 最长完成时间 | 34.178 s | 33.618 s |
| 延迟阶段 commit/ACK | 1024/1024 | 1024/1024 |
| 延迟生产完成时间 | 47.024 s | 44.744 s |
| 延迟 ACK 完成时间 | 55.800 s | 53.538 s |
| 延迟成功 receive 调用次数 | 151 | 145 |

修改后 produce_unknown、ack_unknown、control_errors 均为零，三个服务均 exit 0、未强制退出。以上是每版本一个独立 trial 的实际观测：既包含生产吞吐阶段变慢的观测，也包含延迟阶段更快的观测，不能用于统计判断版本优劣、排除原失败或晋升基线。最终源码仍须重新执行原全套质量门禁与完整 32 试次。

## 原始证据

core worktree ignored 目录 `target/evidence/hot-page-e3b110f/` 保留安全日志、构建/load 身份、小型反馈摘要与 27 项 SHA256 清单。清单自身 SHA256 为 `4cdd8a7d50f645156c96d16280915b2367b1f61fb88241dc3595da6605f7daaa`。`summary.json` 和 README 不在该清单内，避免摘要与清单互相引用；仓库中的小型 JSON 引用这份清单。

完整大报告分别留在 `target/evidence/read-latency-80c7659/before-original-trial/`、`after-original-trial-fbe283d/`，没有改写。汇总包不复制凭据、私钥、配置或 WAL。本记录不包含后来 subscription_commit 后置错误链诊断改动，也不替代新冻结版本的验收。
