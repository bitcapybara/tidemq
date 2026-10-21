# Claim 已提交后 Unknown 的有界调查与错误链修复

本次确认并修复了 `subscription_commit` 提交后检查丢失错误源的问题。原先出现的 Claim 失败原因仍未知；已提交的 Claim 不能直接解释为成功回复丢失。本次没有修改 SDK、Claim 重试、租约期限、Read 缓存、性能策略或基线。

精确身份、原始事件与制品 SHA 见 [小型证据 JSON](wal-segments-claim/20261021-claim-postcommit-source.json)。原始失败、一次未复现探针和确定性回归分别保留。

## 原始失败与单次探针

原运行的编译基底是 `bb27a7fd74f11b3225ef8ed2e5a0e5adc12bf349`，仅有热读页复用及其两项测试的未提交改动；其补丁 SHA-256 为 `dfd0fd9697839bf2cf0e2dd775a025e44bc3d83a8d898039ccb75db6d21e3207`，后来提交为 `e3b110f5fe7e7236f6ba2e51202050fb58761f8e`。原 `runtime_queue` 二进制 SHA-256 为 `d5869f0f5fda275209fbf2210bee2da4d5e39c92c8a3913f45638fb463887351`。

原 nextest `fa3ad1fa-5776-4aa1-8055-fc8ba9fe46c1` 终态为 exit 100：38 项运行、37 项通过、1 项失败，耗时 107.316 秒。唯一失败为 `actual_runtime_sdk_automatically_compacts_partial_cold_objects_and_reclaims_sources`，4.834 秒后在 `Consumer::claim(...).await.unwrap()` 收到远端 `Unavailable(6) / Unknown(3) / retryable=true`，尚未执行该 consumer 的 `fill` 或 Read。

随后仅在该测试 setup 安装 WARN subscriber，依照原 Cargo fingerprint 使用精确的原依赖制品，独立编译测试入口并执行一次；结果为 1/1 通过、13.04 秒。唯一 WARN 属于另一条后台 `INTERNAL_COMMAND` 的 metadata quorum read，提示实际 voters 不匹配授权拓扑；没有原 Claim 的请求身份或错误链关联，因此不能将其当作原 Claim 的原因。未继续循环重跑该场景。

## 原现场的事后持久状态

先对已停止的原 fixture 全树做 SHA，再复制数据目录；只在 Linux 的另一份私有工作副本上使用已有 `DiskStore` 恢复接口读取。原件从未由 store 打开。原 16 个文件、14,323 字节在复制前、复制后、恢复检查后内容 SHA 完全一致；整个清单文件 SHA-256 为 `2b3baefd62e0d66d60a068ec5b25f04b2088001d6b41700a272b8f43469ff9f5`。

读取器使用已有 Linux `80c7659` rlib，产品改动只涉及 runtime 热读，不涉及存储格式；读取器经 rustfmt、独立 Clippy 和 rustc `-D warnings` 检查。仅在私有容器中运行，资源为 2 CPU、8 GiB RAM、16 GiB RAM+swap 总额、512 PID、无网络，诊断 profile 与正式覆盖率隔离。拓扑投影首个容器因把 0700 父目录整体挂载而 exit 1，保留原日志后改为直接只读挂载已授权数据副本，最终 exit 0；产品、原件和读取范围不变。

恢复结果如下，均为停止后状态：

| 项目 | 结果 |
| --- | --- |
| 原夹具 | RF1，Combined node 1，metadata/data voters 均为 `{1}` |
| data group 2 | term 1 / leader 1，commit 和 applied 均为 15，snapshot 为 13，无 purge，保留 16 条 entry |
| subscription | index 6 创建，index 15 Claim 已提交，cursor=1、assignment=1、ColdReplay |
| 消费状态 | `cold_before=2`，ACK/received/issued/next offset 均为 0，in-flight=0，无 RecordDelivery |
| metadata group 1 | applied=14、revision=13，实际 membership 与 group 2 voters 均为 `{1}` |
| 权威状态 | grant、route、data fence 的 operation 一致，owner=1、epoch=1，route/fence generation 均为 1 |

这排除了“该 Claim 从未持久化”的简单解释，也没有事后拓扑分叉证据。持久状态不保存原 process-local lease 的时间窗口、原 RPC 错误源或回复交付状态，不能据此推断失败瞬间原因。

## 能确认的错误映射缺口

Claim 经 `data_lease_for(minimum_remaining=ZERO)` 可复用当前仍有效的缓存租约。原 ControlPolicy TTL 为 3 秒，SDK 请求总期限为 10 秒；非 finalizing 的提交期限还会收紧到该租约的绝对到期时间。

`subscription_commit` 在真实 `client_write` 成功后会重新读取 fence 并检查原 lease。原实现把状态读取错误、fence 缺失及 lease/finalization 检查错误全部映射为没有源的 `Error::unknown()`。因此“已提交后租约过期”是合法 Unknown 路径，接近到期的缓存租约使其成为一个可证伪假设；它仍不是原失败的已证实原因。Raft 提交等待错误或收紧后的等待期限同样可能产生 Unknown，原日志没有足够信息加以区分。

本次只在这三处提交后路径保留 `Error::operation` 上下文：状态读取保留原 source，缺失 fence 提供有意义的 `NotFound` source，lease/finalization 检查保留 typed source。`Error::operation` 仍生成原 `Unavailable / Unknown / retryable=true`，已有 WARN 的字段和限流机制继续使用。全局 `wire::within`、提交前 `active_lease` 的 6/2 映射均未扩散修改。

## 真实提交后的红绿回归

回归基底为 `84ce35f6625a88fb21d0180c675039f54984b69c`。该版本与原 `e3b110f` 的完整原始 `subscription_commit` 源码片段 SHA 一致：`3f92ce445f581e83819e0605288f0c465cf0a27913d9c19f54a2fcdea6f3dc83`。

测试复用原 runtime fixture，Claim 本身通过进程内 `Service::handle`、真实 protobuf 与真实 Raft 执行，不宣称该次 Claim 经过原生 QUIC。仅有 cfg-test 边界在提交成功、后置检查前暂停；先读取真实 assignment 确认 session 与 assignment generation 已持久，再等待该请求实际使用的原租约到期并放行。没有增加 TTL、总期限、重试或生产测试开关。

| 阶段 | 真实终态 |
| --- | --- |
| 旧错误映射 | nextest `220511a8-f887-4d31-82d1-326473923d93`，exit 100，1/1 失败，4.449 秒；已提交与完整 Frame 比较先通过，唯一失败为 WARN 丢失实际 Expired 源 |
| 保留错误源后 | nextest `11e4a42e-808a-4ddd-b03b-94a6f4e890b1`，exit 0，2/2 通过，4.490 秒；包含真实 Expired 和另两类错误的完整 wire Frame 比较 |

绿色原始 WARN 链为 `Runtime operation failed: verify committed subscription writer: Control authorization has expired`。完整响应 Frame（包括 payload 字节）与旧 `Error::unknown()` 生成的 Frame 完全相同；补充测试同样核对状态读取失败、fence 缺失的 wire 结果。

已完成 `cargo fmt --all`、workspace/all-targets 严格 Clippy、生产 panic/unwrap/expect/indexing 审计和 `git diff --check`，均 exit 0。没有为本次诊断盲重跑原 38 项或修改原 Claim 夹具成功断言。后续完整验收应使用合并后的新冻结版本；旧 `80c7659` 覆盖率及单次性能反馈不能替代。

## 制品边界

原始日志、JUnit、红绿二进制身份、原源码补丁、安全状态投影与读取器保留在忽略目录 `target/claim-diagnosis-hot-page-20261021/`。可复核的安全归档 `claim-evidence-safe.tgz` SHA-256 为 `44be7de781aabfdfeb6231fa9be2acd8b2a8ace3b21aad79da4f4078ffcbd586`，不包含原 store、消息 body、token、签名密钥或 TLS 私钥。Linux 原始读取器输出另存于私有 `diagnosis-hot-page-claim-20261021` 和 `diagnosis-hot-page-claim-topology-20261021` 目录。

原 Claim 失败仍保留为原因未知的红证据。本修复解决的是可确定复现的诊断信息丢失；它不把已有 Unknown 改成成功，也不授权换新 Claim 或自动重试。
