# 本地 RustFS 兼容预检

本地对象服务固定 RustFS 1.0.3；所有 TideMQ 对象数据仍走 S3 API。当前提供并实跑了 macOS arm64 原生隔离预检，不要求安装 Docker，不全局安装二进制，不接入现有桶或真实凭证。Linux/容器环境以及正式 aws-sdk-s3 集成另行验收。

## 执行

在仓库根目录运行：

```sh
scripts/rustfs-precheck.sh
```

需要 Bash、curl 的 `--aws-sigv4`、unzip、shasum、file、lsof、openssl、cmp 和 awk。脚本只支持 `Darwin arm64`，其他平台明确失败。首次约下载 91 MB 至 `target/tools/rustfs/1.0.3`，先核对发行包 SHA256，再执行 `file`、`--version` 和 `server --help`；不修改系统安装或全局 Gatekeeper。

| 项目 | 固定值 |
| --- | --- |
| 发行包 | `rustfs-macos-aarch64-v1.0.3.zip` |
| SHA256 | `9ec6f2ad89df88db9b8889f59e37bb27f6b67c61ee8a97968dd90aaa2ac4dd30` |
| 实跑版本 / 上游 commit | `1.0.3` / `331fb896285a7686f927baf8d9d4dc2d3d3a10a9` |
| API 监听 | `127.0.0.1`，在 39000—39999 选择空闲端口并确认由本次 PID 持有 |
| 控制台 / 观测导出 | 禁用控制台，不配置 OTLP endpoint |
| 数据 / 凭证 / 日志 | `target/rustfs-precheck/run.*` 独立目录，权限受 `umask 077` 约束 |
| 进程持久设置 | `RUSTFS_DURABILITY_MODE=strict`、`RUSTFS_NEW_BUCKET_DURABILITY_MODE=inherit` |
| S3 访问 | path style、`us-east-1`、SigV4，curl 从私有文件读取临时 fixture 凭证 |

成功时 stdout 输出不含秘密的 JSON，另保存至 `target/rustfs-precheck/last-report.json`。缓存的发行包可复用；每次重新解包核验过的包。脚本启动时清空继承环境，避免误用外部 KMS、凭证、endpoint 或 relaxed 持久选项。

## 实际检查顺序

1. 用随机测试身份和独立数据目录启动服务，等待 `/health/ready`；确认监听属于自己启动的 PID。
2. 建桶，读取 admin durability 接口，核验新桶无 override，继承显式 strict 进程设置；随后将该 fixture 桶显式设为 strict。
3. PUT 固定对象，GET 比较字节；Range `0-6` 返回 206 且字节正确；HEAD 核对长度；越界 Range 返回 416。
4. 仅修改 fixture 桶为 relaxed，期间不写入对象；SIGKILL 自己的服务进程，以相同数据目录、凭证和 strict 进程设置重启。
5. 读取已有桶的 override，确认 relaxed 仍被保留，再显式纠正为 strict 并回读。仅修改进程环境不会迁移已有桶的覆盖值。
6. GET 强杀前已确认的对象并比较完整字节，删除对象并确认 404，再删除 fixture 桶。
7. trap 停止本次 PID，并删除本次 `run.*` 数据、日志和凭证目录；不查杀系统其他 RustFS 进程，不删除其他数据目录。

该脚本的 HTTP 和 `UNSIGNED-PAYLOAD` 仅用于 loopback fixture 的 curl 兼容预检。正式实现将使用 aws-sdk-s3 的默认完整性校验，按部署配置 TLS；本脚本不证明 checksum trailer、multipart complete/abort、条件操作、流中断或 SDK 全操作期限行为。

## 持久性与排查边界

2026-10-19 的 macOS arm64 实跑通过了上述操作。进程强杀后对象存在，只证明本机这次进程故障场景，不证明宿主机掉电、磁盘故障、RF3 或三可用区持久性，也不构成 TideMQ 的生产 ACK 或 WAL 回收依据。

全局 strict、已有桶 override 和新桶默认是三个不同配置。实际部署逐桶查询 `/rustfs/admin/v3/bucket-durability/<bucket>`；`mode: null` 表示继承，其他值覆盖全局模式。预检故意验证已有 relaxed override 不会因重启被抹掉，随后只纠正自己的 fixture 桶。不要将此脚本改为批量修改真实桶。

脚本出错返回非零并清理 fixture。常见原因依次检查下载/摘要、平台/CLI、空闲端口、`/health/ready`、HTTP 状态与 durability 回读。不要用禁用签名、改为 none/relaxed 或降低内容校验绕过失败。检查仍需保留证据时，在独立测试目录手动重现并先脱敏；不要输出访问密钥、secret 或用户对象正文。

来源：[官方 macOS 安装](https://docs.rustfs.com/en/installation/macos)、[1.0.3 发布](https://github.com/rustfs/rustfs/releases/tag/1.0.3)、[固定版本 durability 契约](https://github.com/rustfs/rustfs/blob/1.0.3/docs/operations/durability-modes.md)、[健康检查](https://docs.rustfs.com/en/operations/status-check)。实际参数以下载包的 `server --help` 为准；该版本 `--console-enable` 是 flag，本 fixture 不传该 flag 并显式设置环境值为 false。
