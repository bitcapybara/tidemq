# 对象发布与 GC 排障

先通过 `Coordinator::diagnose(object_id, deadline)` 查询权威状态。查询需要元数据多数派；没有多数派时不要用磁盘旧状态代替删除授权。

| 阻塞原因 | 含义与处理 |
| --- | --- |
| PendingTask | Registered/Uploaded 尚未完成；按原 object ID 调用 reconcile，保留正文。 |
| CancelNeedsDataAbort | 元数据已阻止发布，但数据组尚未给出不可逆终止；恢复数据多数派后 cancel。 |
| LiveManifest | 对象仍有有效分区引用；先发布完整替代对象或由保留策略安全移除引用。 |
| ReadPin | 读取尚未 release；检查任务期限。通过 pending_pins 按组分页发现遗留 pin，再使用 expire_pin 完整等待 TTL。 |
| RecoverySnapshot | 旧快照/恢复仍依赖对象；恢复当前全部 voters，替代对象执行 checkpoint，逻辑保留回收执行 retire_before。 |
| Checkpoint | 发布完成尚未有安全快照屏障；禁止回收本地正文。 |
| DeletionRetry | DELETING 已阻止新引用；恢复 S3 后按原对象 collect。 |

不要从桶列表推导可读清单，也不要按对象年龄、任务超时、离线节点 TTL 或一次 HEAD 结果强制删除。对未知 PUT 保持原身份；对未知 DELETE 重试原对象，不重新注册 object ID。Deleted ledger 保留并定期重扫，以清理迟到的孤儿写入。

观察 `tidemq_lifecycle_operation_seconds`、`tidemq_lifecycle_active`、各阶段计数及 `tidemq_lifecycle_retries_total`。计数递增表示实际阶段请求完成，不表示整个对象链路完成。S3 阶段/完整流指标在对象存储看板；磁盘提交/快照错误在复制看板。ID 只用于有界诊断与采样日志，不添加到 Prometheus 标签。

初版每组最多 4,096 个永久对象身份、1,024 个读取 pin，每 pin 最多 64 个对象，pin TTL 最长 10 秒。容量耗尽时应限制新对象登记并扩容/升级退休边界方案，不删除终止记录腾空间。

本地真实验证命令（使用已验证 RustFS 1.0.3 可执行文件）：

```bash
TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/rustfs rtk cargo test -p tidemq --test lifecycle_persistence -- --ignored
TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/rustfs rtk cargo test -p tidemq --test runtime_queue actual_runtime_sdk_replays_s3_after_published_local_reclamation -- --ignored
rtk cargo test -p tidemq --test s3_boundaries
```

本地 RF1 重启与真实 QUIC/S3 冷读证明不覆盖机器掉电、真实三可用区、S3 提供商故障或独立操作系统进程的故障隔离。生产放行应附平台、构建版本、Raft 拓扑、S3 配置和故障时间线。
