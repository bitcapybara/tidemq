# 对象存储排查

对象存储中断不应阻塞已经满足 WAL quorum 的 producer ACK；它会增加未归档正文和本地磁盘占用，容量水位达到上限后由上层准入背压。`UPLOADED`、清单发布和本地回收必须分别观察。

1. **先确定阶段**：对照固定 `s3_` 指标。准入直方图升高或 admission_rejected 增加，先看归档并发、共享内存和限速；headers 升高看连接、凭证、后端响应和 SDK retries；body 升高看持续吞吐、慢后端及完整 deadline。HTTP 错误增加时从结构化错误链识别阶段，不在日志输出凭证或对象正文。
2. **上传未知**：保留已登记 Descriptor 和发布任务，调用 `verify_uploaded`。HEAD 一致但正文不一致仍视为损坏；不可只信 metadata、ETag 或一次超时。无法访问任一权威时保留对象和 WAL，不强推发布或删除。
3. **条件冲突或 checksum 错误**：核对 cluster/group/object 身份、清单描述符、对象实际长度和摘要。确认是否有外部客户端绕过 create-only 写入。停止该对象的发布与回收，保留健康副本及记录；修复流程应生成新的不可复用身份，不能原地覆盖历史对象。
4. **Range 错误**：416 表示请求范围与当前对象不一致；非 206、错误 Content-Range、截断正文、超长正文都不能作为历史消息交付。对比可信描述符的字节索引与 offset 范围，区分清单错误、后端对象替换和网络中断，不能调整 offset 跳过损坏消息。
5. **multipart 遗留**：取消正常路径会实际 abort，但进程终止、Create 响应丢失或 abort 网络失败仍可能遗留上传。恢复器读取最多 100 项的一页，持久保存下一页标记，对每项取得注册任务的终止或继续结论后，才调用 `abort_pending`。禁止仅按年龄批量清理；bucket listing 不能替代读取清单。
6. **删除后空间未下降**：区分逻辑 DELETE、所有清单/pin/恢复/快照引用解除和后端物理回收。检查 bucket 版本管理、Object Lock、后端保留和正在执行的 multipart。不能为了清空间解除仍有效的发布或恢复保护。

本地默认使用 RustFS 1.0.3，必须明确启用进程 strict、新桶继承，并逐桶核对现有覆盖设置。已有 `scripts/rustfs-precheck.sh` 负责 strict 与重启预检；SDK 验证命令如下，在本任务私有随机 loopback 端口和 `target/` 下启动实例，结束后清理进程、凭证和数据：

```sh
rtk cargo fmt --all
rtk cargo clippy -p tidemq --tests --no-deps -j 2
rtk cargo test -p tidemq --locked --test archive_format --test s3_boundaries -j 2
rtk proxy env TIDEMQ_TEST_RUSTFS_BIN=/absolute/path/to/verified/rustfs \
  cargo test -p tidemq --locked --test s3_rustfs -- --ignored --nocapture
```

显式 fixture 只接受 `rustfs 1.0.3`，使用已验证的官方资产；不要把下载地址替换成来历不明的二进制。成功摘要位于 `target/s3-sdk-evidence.json`，不含访问密钥。真实测试覆盖普通 PUT、create-only 重试、实际 PUT 200 响应被代理丢弃后的对账、HEAD、206/416、错误 checksum、缓存、超过 5 MiB 的 multipart part、条件 Complete 冲突、上传首个 part 后取消及丢弃句柄，并确认 ListParts/上传列表清空、DELETE。是否通过以报告和测试退出码为准，不能由预检 curl 的成功推断。

生产放行还需实际 AWS/S3 后端权限、三区、长时间积压、限速隔离、容量与断网恢复验证。首版指标尚未覆盖精确 SDK 原生分配量和每个 TCP/TLS 阶段，诊断时结合进程 RSS、分配器统计、磁盘及网络指标，不将这些口径直接相加。
