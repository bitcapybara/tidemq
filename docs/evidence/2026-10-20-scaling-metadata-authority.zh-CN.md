# 扩缩容测试的 Metadata 换主回归

## 问题与修复范围

GitHub Actions 运行 `39402803302` 的 System 分配器任务在
`committed_joint_survives_leader_loss_and_finishes_only_the_original_plan`
等待协调器恢复时超时。该任务没有保存嵌入式 Raft 节点的阶段日志，不能据此认定原始 CI
失败的唯一触发条件。

在 `401c7c9` 上隔离当前 Metadata leader，并等待其余节点真正完成自动选举，可以稳定
暴露测试辅助函数的问题：它始终对 Metadata 节点 1 执行 `ensure_linearizable()`。
节点 1 已成为 follower 后返回结构化的 `ForwardToLeader`，而实际 Metadata leader 和
Data leader 的线性一致性屏障均成功。旧辅助函数仍等待至原有 12 秒上限。

本次只修改测试的协调器恢复过程。辅助函数分别查找真实 Metadata 和 Data leader，
要求两者通过线性一致性屏障，并使用对应节点创建协调器。没有修改生产选举、超时、
复制协议或成员变更逻辑。

## 保留与加强的断言

- 先提交 joint configuration，再停止原 Data leader，重启协调器继续执行原计划。
- 隔离原 Metadata leader，等待真实自动选举，不使用固定等待时间替代选举证明。
- 再隔离新 Metadata leader，形成实际多数派缺失；协调器就绪判断必须保持 pending。
- 恢复网络后仍须通过真实线性一致性屏障，只有 Data leader 被原计划退役时才重建适配器。
- 完成后等待全部三个 Metadata voter 应用完成记录，再检查原节点上的完成状态；Data
  成员集仍须精确为原计划中的 `{1, 2, 4}`。

## 验证

回归测试使用 Rust 1.99.1、nextest 0.9.148、`ci` profile 和 `allocator-system`，
未启用重试，也未放宽原有等待上限。

- 加强场景配合旧辅助函数：13.324 秒超时，阶段为 `after_metadata_isolation`。
- 在 `401c7c9` 生产代码上修正辅助函数：完整扩缩容测试 3/3 通过，指定场景固定 20 次
  压力运行 20/20 通过。
- 合入独立的 Control reply 修复 `95194d6` 并去除固定 Metadata 观察者假设后：完整
  扩缩容测试 3/3 通过，指定场景固定 20 次压力运行 20/20 通过。
- `cargo +1.99.1 fmt --all` 和工作区全部目标的严格 Clippy 通过，包含测试、示例和
  benchmark 目标，使用 `--all-targets --tests --no-deps --locked -- -D warnings`；
  System 分配器配置的全部目标也通过相同严格检查。

这组结果证明测试辅助函数存在独立的 Metadata 权威选择错误。原始 GitHub Actions
Linux/System 失败仍须由后续相同平台 CI 重新验证，不能用 macOS 的通过结果替代。
