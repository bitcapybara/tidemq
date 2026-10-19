# S3 与不可变 pack

本模块使用固定版本 `aws-sdk-s3 1.152.0`，不依赖 `object_store`。它提供已提交消息的后台编码、S3 上传、范围读取和缓存，不决定清单发布、对象 GC 或 WAL 回收。`Uploaded::Created` 只证明本次上传及 HEAD 校验完成；`ExistingVerified` 表示读取全部已有对象并验证身份和 BLAKE3 后，对账成功。这两个结果都不能直接推进 `object_published_end` 或 `local_reclaim_end`。

## 权威与接口

1. 调用方从数据组已提交状态取得同一 group 的分区、连续 offset 范围、正文及 `committed_end`。`Pack::build` 验证每个 offset 连续、`end <= committed_end`，拒绝跨组、重复分区和超限输入。`committed_end` 必须来自数据组权威读取，不能直接相信网络请求提供的数值。
2. 生命周期模块先登记任务与对象，取得有效上传授权，再调用 `S3Store::put_create_only` 或 `start_multipart`。T07 自身不伪造登记或发布状态。
3. 上传结果丢失时调用 `verify_uploaded`：HEAD 对比固定身份、长度及内容摘要元数据，再读取全部对象校验 BLAKE3。超时或无法证明一致时保留未知状态，不覆盖原 key。
4. T08 提交清单引用并持久记录发布完成后，读取方持有效清单视图中的 `Descriptor` 调用 `ColdReader::read_partition`。桶列表不构成读取清单。
5. `delete`、`abort_pending` 由生命周期权威决定何时调用；本模块不根据对象年龄、超时、未列出引用或一次 HEAD 失败自行删除。

`Descriptor`、`PartitionIndex` 和 `PackIdentity` 可序列化，供生命周期状态持久化。Descriptor 包含 cluster、非零 group、不可复用 object UUID、总长度、整对象 BLAKE3，以及每个分区的 topic UUID、partition ID、排他 offset 范围、字节范围和分区块 BLAKE3。实现不依赖全局元数据类型，后续由 T08 组合这份描述符及其代次、授权和任务状态。

## 格式 v1

所有整数采用大端编码。无压缩，不按 key 丢弃历史记录。一个 pack 可合装同组多个低流量分区；大流量分区可单独生成对象。每个分区在一个对象内只占一个连续块。

| 部分 | 布局 | 字节数 |
| --- | --- | --- |
| Header | `TIDEPK01`、cluster UUID、group u64、object UUID、前 48 字节的 BLAKE3 | 80 |
| Record | topic UUID、partition u32、offset u64、key 长度 u32、body 长度 u32、record BLAKE3、key、body | 68 + payload |
| Index entry | topic UUID、partition u32、offset start/end u64、byte start/end u64、分区块 BLAKE3 | 84 |
| Footer | index start/length u64、entry count u32、index BLAKE3、`TIDEEND1`、前 60 字节的 BLAKE3 | 92 |

Record 摘要的域为 `tidemq-pack-record-v1\0`，覆盖 topic、partition、offset、两个长度和正文。Index 覆盖所有记录块且不能有空洞、重叠、空范围或越界。完整验证还检查 header/footer、整对象摘要及描述符与对象内索引完全一致。范围读取先验证可信索引给出的块摘要，再逐条验证身份、连续 offset、长度和 record 摘要；正文返回 `ArchivedRecord` 只读视图，offset 公共字段与 key()/body() 切片共享底层预算所有者；视图共享一次解码的索引预算所有者，克隆不会提前释放预算，也不为每条 key/body 另建分配。同一缓存块并发读取的每份索引分别准入，避免把共享正文预算重复当成每次解码的额度。

默认限制：对象 64 MiB、分区 1,024、记录 100,000、每条 key + body 256 KiB。构建前计算并检查完整大小，预留进程共享 bulk 内存预算和 fallible Vec 容量；大对象编码应运行在后台 blocking worker，不能放在 producer ACK 路径。对象 key 固定为 `{prefix}/{cluster_uuid}/packs/g{group}/{object_uuid}-{blake3}.pack`；调用方不得复用 object UUID，改变摘要也不代表允许重新使用已登记身份。

## S3 行为与资源限制

`S3Config` 显式提供 endpoint、region、bucket、prefix、cluster 和静态 `Credentials`。不读取默认凭证链或实例元数据；Debug 不输出凭证或 endpoint。生产使用 HTTPS；HTTP 只接受显式启用的数字 loopback 地址与非零端口。bucket 使用固定 DNS 名称子集，prefix 限定 ASCII 安全字符，再自动加入 cluster 隔离路径。

SDK 默认请求 checksum 计算和响应 checksum 校验保持开启，ETag 只用于 multipart 协议，不作为 BLAKE3。普通 PUT 与 multipart Complete 都使用 `If-None-Match: *`。条件冲突时必须验证已有正文；同 key 不同内容返回冲突或完整性错误。普通 PUT 发生不确定网络错误时尝试在原期限内对账；不能完成对账则返回 `Unknown`，不把超时当作未写入。

默认同时准入 4 个操作，读范围最多 64 MiB，速率 64 MiB/s，完整操作期限 30 秒，multipart part 为 8 MiB。SDK 最多 3 次尝试，完整期限同时覆盖重试、限速等待、响应头和整个响应体；body 按 chunk 读取并在每个 chunk 前检查同一个 deadline 和剩余长度，不使用无界 `collect`。Range 必须返回 HTTP 206、精确 Content-Range、长度、身份元数据；完整对象范围额外检查整对象摘要。越界请求在发请求前拒绝，后端 416 单独归类。

操作准入同时取得 count permit 与共享字节 permit，包含受限的工作缓冲估计；正文操作使用 bulk，HEAD/DELETE 的 64 KiB 小包使用普通 control，不占 core/handshake 保留，也不等待正文的字节限速时间线。所有请求仍受同一 count 上限，避免无限 metadata 请求；GC 公平调度由上层完成。请求内存按两倍对象/读取长度加 64 KiB 预留；原始输入、已构建 pack、返回 `Bytes`、缓存持有者须分别纳入共享预算。此口径不等于 RSS 或 SDK/TLS/内核精确分配量，容量配置还需保留原生库、socket、线程栈和分配器 retained 空间。速率允许一个已准入请求或 part 的有限突发；多个操作共享限速时间线，multipart 在相邻 part 之间等待，不能创建无限 part task。

`Upload` 是 owned worker 的句柄；调用方取消、丢弃句柄或上传错误后，worker 保留准入直到 abort 完成。abort 有额外最多 5 秒清理期限，随后用最多 1 个 part 的 ListParts 检查；仍有 part 时最多再 abort，两次补偿仍不能确认时保留未知状态。Create 响应丢失时可能拿不到 upload ID，进程被杀也可能遗留 multipart；恢复器用 `pending_uploads` 每页最多 100 项和显式 continuation 对账，不能无限扫描。Complete 与取消竞态仍可能已经生成对象，返回未知结果后必须按注册身份重新确认，不能声明取消等于没有对象。

ColdReader 将大块 checksum/record 解码放入有界 owned spawn_blocking，同一 S3Store 的所有 ColdReader 共享 worker slot，上限等于 S3 max_in_flight，取消调用方仍保留 worker slot、正文及索引预算直至计算完成。缓存 miss 只解码一次，通过后才缓存；完整 S3 正文校验按最多 64 KiB 片段增量哈希并主动让出 executor。每个分区连续块使用一次 Range 请求，缓存同时限制字节和条目、先校验后缓存、按 FIFO 淘汰。缓存 key 绑定对象摘要和字节范围，读者持有克隆时淘汰不会虚假释放内存预算。首版不做猜测性预取，跨对象合并、回放公平调度与 pin 有效性由上层调度及生命周期模块负责。

首版要求专用普通 S3 bucket；桶版本管理、Object Lock 或外部写入策略造成的额外物理保留须单独管理，DELETE 的逻辑不可见不能直接报告为物理空间已释放。只允许生命周期服务写入和删除该 cluster prefix，避免外部工具绕过不可变写入契约。

## 指标与验证

`S3Store::metrics()` 返回固定 series 的 Collector，由调用方注册到同一 Registry 的 `s3` prefix。覆盖 SDK attempts/retries、HTTP 错误、准入拒绝、缓存命中/未命中，以及准入、range headers、body、解码队列、解码计算、完整操作直方图；没有 bucket、key、topic、token 或凭证动态标签。range headers 包含 SDK 重试时间，body 为后续完整流读取（含有界拷贝、增量 checksum 与 executor 让出等待）；两者不能冒充精确网络 RTT。

`tests/archive_format.rs` 验证格式、损坏、已提交与同组边界及克隆预算；`tests/s3_boundaries.rs` 用实际 SDK 访问私有 HTTP fixture，验证伪造 200、checksum、慢 headers/body、并发准入及指标边界。`tests/s3_rustfs.rs` 为显式运行的真实 RustFS 集成，成功后写入不含凭证的 `target/s3-sdk-evidence.json`。本机结果不证明云上三区、硬件断电或长期耐久性。

API 来源：[crates.io 版本元数据](https://crates.io/crates/aws-sdk-s3/1.152.0)、[版本化 Client 文档](https://docs.rs/aws-sdk-s3/1.152.0/aws_sdk_s3/struct.Client.html)、[官方上游](https://github.com/awslabs/aws-sdk-rust)。构建器和 ByteStream API 同时对照了本机相同锁定版本源码。

## 本机验证记录（2026-10-19）

核心代码为 `65952f4`，RustFS fixture 随本记录提交。宿主为 macOS 26.6.2（25G83）、Darwin arm64，编译器 `rustc 1.96.0 (ac68faa20 2026-05-25)`；`Cargo.lock` SHA256 为 `16b590675af611bd8e371a9773b5b128379df3bad027e6c384f514ceca44bf86`，本任务没有修改依赖清单或锁文件。

| 验证 | 实际结果 |
| --- | --- |
| fmt + `clippy -p tidemq --tests --no-deps -j 2` | 干净，无 allow 豁免生产代码 |
| `cargo test -p tidemq --locked -j 2` | 69 passed、1 ignored，15 suites，21.46 秒；ignored 为显式 RustFS fixture |
| 显式 `s3_rustfs` fixture | 1 passed，4.26 秒，真实 AWS SDK 与私有 RustFS 实例 |
| PUT 成功响应丢失 | 私有 TCP 代理观察到 RustFS 的 PUT 200 后丢弃响应；SDK 重试和正文对账返回 ExistingVerified，历史内容一致 |
| multipart 取消 | 约 12 MiB pack，8 MiB 首 part 确认存在后取消；abort + ListParts、上传列表为空、目标 HEAD 404 |
| 取消句柄与内存 | Drop 后 owned worker 完成清理，字节许可恢复；同一缓存块每次解码单独准入，克隆视图保持许可 |
| 隔离边界 | 伪造 200、慢 headers/body、坏 checksum、错误 cluster/object cache 身份、数据内存耗尽后 metadata HEAD、跨 reader 共享 decoder 槽均通过 |

本机使用先前已固定的[官方 macOS arm64 1.0.3 资产](https://github.com/rustfs/rustfs/releases/download/1.0.3/rustfs-macos-aarch64-v1.0.3.zip)，归档 SHA256 再次核对为 `9ec6f2ad89df88db9b8889f59e37bb27f6b67c61ee8a97968dd90aaa2ac4dd30`。所有测试实例仅使用随机 loopback 端口和任务 target 子目录，退出后清除自身进程、凭证和数据；不接触用户已有对象服务。

尚未完成：真实 AWS 云端或三区验证、硬件断电、Linux 运行、长期容量与吞吐压测；这些本机证据不能替代 T08 生命周期对账与 T10 真实服务链路的交叉验证。
