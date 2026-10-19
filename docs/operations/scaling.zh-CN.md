# 扩缩容和成员替换排障

本手册对应 `src/scaling` 的真实共识执行器、固定迁移槽和运行时 drain 接缝。不要直接修改 voter 配置、清空计划或删除仍有状态的节点目录。

## 存储节点替换

1. 保存当前 cluster、group、generation、旧 voter、目标 voter 和规范 operation ID。检查三个目标 zone、已注册 Storage 身份及目标 schedulable；向目标取得新鲜容量报告，包含复制量、已有预留和至少 1 GiB 额外余量。
2. 检查真实数据 leader 和 metadata leader，各自 quorum 健康；只变更一个 voter。运行时共享 node RepairBudget 和 PeerPool 预算，控制并发及 R/B 字节，不调整正常 C 心跳来“加快复制”。
3. 发起 `MembershipCoordinator::execute`。正常顺序为元数据 prepare、learner 追赶、joint、stable、真实线性化同锁证明、元数据 complete。查看 `membership_change(group)` 的原计划和 complete 状态。
4. 任何超时/连接中断都先保留 operation ID，查询原计划、实际 membership 和目标 applied 位置。若已 stable 而元数据仍旧配置，用原计划重取真实证明补 complete；若仍 joint，用同一 target 继续，不能改成第三套成员。
5. 完成 stable 和 metadata 后，重新取得数据组 authority 并对齐新 voter/generation。仅当所有组都不再引用退出节点、没有 pending 计划、对象恢复依赖通过当前完整稳定 voter 的快照屏障、安全排空全部本地任务时，才能关闭节点。

`schedulable=false` 只停止新分配。节点身份、Storage 角色、zone 和数据目录的保留是历史证据的一部分；不通过删除身份绕过成员变更或对象恢复保护。

## Broker / Gateway 缩容

先持久化停止新分配，然后按有限批次预热、停止该分区的新生产/投递准入、排空有限在途、执行固定旧 epoch/route 的移交。fence/恢复/激活间中断后恢复原 grant；更高 epoch 已取代时，旧操作只允许返回 Superseded。成功路由必须精确对应新 owner 和旧代次+1。

Gateway 需通知运行时执行受控连接关闭/重连，保留在途最终确认所需的限时能力。SDK 遇断连按原业务身份对账；Unknown 不能改新 producer、sequence 或 operation 重发。检查本地连接和在途许可实际归零，再退出进程。

`DrainPlan::inspect` 仍显示 owned_partitions、storage_groups 或 pending_membership_changes 时不得退出。Metadata 角色单独阻塞，须按元数据组自己的受信成员操作完成，不与无状态入口同时删除。

## 指标到阻塞原因

| 现象 | 检查 | 处理 |
| --- | --- | --- |
| Queue 延迟上升，正常 C/R 健康 | 节点修复并发、有限字节队列及原 deadline | 等待原计划释放名额或减少新计划；不叠加无界重试 |
| Learner 长尾，复制数据增长慢 | 目标磁盘/网络、pace 字节率、目标实际 applied、剩余容量、观测年龄 | 修复根因并刷新容量报告；保留同一计划和 repair lease |
| joint 中断 | 实际 joint 两套 voter、多数派可达性、leader | 恢复原计划 target；两套多数派不满足时保守停止变更 |
| 实际 stable，metadata 未完成 | 原 migration slot、generation、当前真实 quorum 证明 | 同一计划重新获取三秒有效证明并补 complete |
| Authority / Placement 拒绝 | 角色/zone、实际与声明成员、新目标 schedulable、五秒容量报告 | 先恢复合法条件，不能把 bool 或旧磁盘证明作为授权 |
| drain 无进展 | 持久分区/组/计划阻塞，本地连接和许可 | 逐项完成所有权移交/成员替换/受控重连，禁止强行清表 |
| ServiceRecovery 快但 ReplicaRepair 慢 | 分别查看两条时延，不混用口径 | 服务已恢复不表示三区完整冗余已恢复；继续有预算修复 |

看板为 `deploy/observability/dashboards/scaling.json`，告警为 `deploy/observability/alerts/scaling.yml`。Unknown 计数说明需要对账，不能据此断言数据未提交。重复 unknown 告警有持续窗口，避免每次可重试错误触发独立告警。

本地验证：`rtk cargo test -p tidemq --locked --test scaling_raft --test raft_quic --test application_failure`。测试使用隔离目录与真实 s2n-quic；`TIDEMQ_TEST_TRACE=1` 仅用于有界实验诊断，不作为生产全量日志默认值。物理三区、外部 UDP LB/CID 和正式恢复 SLO 在对应部署环境单独验收。
