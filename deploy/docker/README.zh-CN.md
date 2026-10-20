# Docker 隔离验证

本目录用于在已有业务容器的 Linux Docker 主机上，先验证 RF1，再验证 RF3 的 10 个角色。所有命令必须使用新目录和生成的独立 Compose 项目名；结束 RF1 后再启动 RF3。构建、启动和故障注入应由同一个操作者协调。

项目只有一个私有 bridge 网络。`network` 容器持有网络 namespace，TideMQ、RustFS 和验证客户端通过 `network_mode: service:network` 共用该项目的 loopback。每个角色仍有独立 PID namespace、cgroup 和数据卷，管理 HTTP 和对象存储 HTTP 保持 loopback 策略。没有 host 网络、host 端口、特权容器或 Docker socket 挂载。[Compose 网络与资源选项](https://docs.docker.com/reference/compose-file/services/)说明了这些设置的语义。

这套拓扑验证容器进程故障、持久目录和真实 QUIC/S3 链路；共享 namespace、内核和主机，不能证明跨 AZ 或独立节点网络故障域。RF3 配置中的 zone 名称只用于逻辑拓扑。开发证书由原 `deploy/local/generate.sh` 签发，有效期 7 天。

## 构建契约

需要 Linux x86_64、Docker Engine、支持这些 Compose 字段的 Compose（本次静态验证使用 5.6.0）、Bash、OpenSSL 和 coreutils。Docker 主机应支持 cgroup v2 和内存、swap、PID 限额。不要向主机安装 Rust 工具链。

`Dockerfile.builder` 只准备 Rust 1.96.0、rustfmt、clippy、CMake、clang、pkg-config、OpenSSL 等构建依赖，不执行 Cargo 编译。`Dockerfile` 只复制预编译的 `bin/tidemq` 和 `bin/deploy_validation`，安装运行时库及 curl/OpenSSL。两者均要求显式传入已验证的基础镜像 digest，没有浮动默认值。

以下两个基础镜像已由本次验证操作者在目标机实际拉取并核对：

```bash
RUST_IMAGE=rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc
DEBIAN_IMAGE=debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
REPO=/absolute/clean/checkout
STAGE=/absolute/new-build-directory
BUILDER_IMAGE=tidemq-validation-builder:rust-1.96.0
TIDEMQ_IMAGE=tidemq-validation:source-commit
mkdir -m 0700 "$STAGE"
docker build -f "$REPO/deploy/docker/Dockerfile.builder" \
  --build-arg RUST_IMAGE="$RUST_IMAGE" -t "$BUILDER_IMAGE" "$REPO/deploy/docker"
```

在受限 builder 容器中编译。只读挂载确定的源码快照，输出写入独立 staging 目录；默认系统分配器与仓库默认构建一致。实际验证应记录源码 SHA、Cargo.lock SHA、镜像 ID 和二进制 SHA，不能把不同快照的结果混用。

```bash
docker run --rm --cpus 4 --memory 8g --memory-swap 8g --pids-limit 512 \
  -e CARGO_BUILD_JOBS=2 -e CARGO_TARGET_DIR=/stage/target \
  --mount "type=bind,source=$REPO,target=/work,readonly" \
  --mount "type=bind,source=$STAGE,target=/stage" \
  "$BUILDER_IMAGE" /bin/bash -c \
  'cargo build --locked --release -p tidemq --bin tidemq --example deploy_validation -j2 &&
   install -Dm0755 /stage/target/release/tidemq /stage/runtime/bin/tidemq &&
   install -Dm0755 /stage/target/release/examples/deploy_validation /stage/runtime/bin/deploy_validation'

docker build -f "$REPO/deploy/docker/Dockerfile" \
  --build-arg DEBIAN_IMAGE="$DEBIAN_IMAGE" -t "$TIDEMQ_IMAGE" "$STAGE/runtime"
docker image inspect --format '{{.Id}}' "$BUILDER_IMAGE" "$TIDEMQ_IMAGE"
sha256sum "$REPO/Cargo.lock" "$STAGE/runtime/bin/tidemq" "$STAGE/runtime/bin/deploy_validation"
```

Runtime 构建 context 只有 `runtime/bin/{tidemq,deploy_validation}`，无需把整个仓库或生成的密钥交给 Docker build。容器 UID/GID 为 `10001:10001`。镜像创建该用户拥有、模式为 `0700` 的 `/var/lib/tidemq` 和 `objects` 子目录；空 named volume 的初始化依赖 Docker 的[卷内容复制语义](https://docs.docker.com/engine/storage/volumes/)，运行前仍须实际核对卷的 owner 和可写性。

### Linux Rust 回归容器

回归测试与只编译二进制的容器有两个额外挂载要求：RustFS fixture 写入当前源码目录下的 `target/s3-rustfs`，不跟随 `CARGO_TARGET_DIR`；Rustdoc 会在临时目录生成并执行测试程序。Docker 的 `/tmp` tmpfs 默认带 `noexec`，因此必须为构建测试提供独立、可执行的 `TMPDIR`。运行集群的 `/tmp` 仍保持 `noexec`。

为已冻结的提交创建独立测试快照，并将同一个自有输出目录挂载到 `/stage` 和 `/work/target`。以下命令只改变新建的 staging 目录；不要把已有业务目录或密钥目录作为测试输出。

```bash
TEST_SOURCE="$STAGE/test-source"
TEST_OUTPUT="$STAGE/test-output"
test ! -e "$TEST_SOURCE" && test ! -e "$TEST_OUTPUT"
mkdir -m 0700 "$TEST_SOURCE" "$TEST_OUTPUT"
git -C "$REPO" archive --format=tar HEAD | tar -xf - -C "$TEST_SOURCE"
mkdir -m 0700 "$TEST_SOURCE/target" "$TEST_OUTPUT/doctest-tmp"
chown -R 10001:10001 "$TEST_SOURCE" "$TEST_OUTPUT"

docker run --rm --user 10001:10001 --cpus 4 --memory 8g --memory-swap 8g \
  --pids-limit 512 --read-only --cap-drop ALL --security-opt no-new-privileges:true \
  --tmpfs /tmp:rw,size=1g,uid=10001,gid=10001,mode=1777 \
  --mount "type=bind,source=$TEST_SOURCE,target=/work,readonly" \
  --mount "type=bind,source=$TEST_OUTPUT,target=/stage" \
  --mount "type=bind,source=$TEST_OUTPUT,target=/work/target" \
  --mount "type=bind,source=$RUSTFS_BINARY,target=/tools/rustfs,readonly" \
  -e CARGO_HOME=/stage/cargo -e CARGO_TARGET_DIR=/stage/target \
  -e TMPDIR=/stage/doctest-tmp -e TIDEMQ_TEST_RUSTFS_BIN=/tools/rustfs \
  "$BUILDER_IMAGE" /bin/bash -euc \
  'cargo fmt --all -- --check
   cargo clippy --workspace --tests --examples --no-deps --release --locked -j2 -- -D warnings
   cargo test --workspace --release --locked -j2 -- --test-threads=1
   cargo test -p tidemq --example deploy_validation --release --locked -j2
   cargo test -p tidemq --test s3_rustfs --test lifecycle_compaction --test lifecycle_persistence --release --locked -j2 -- --ignored --test-threads=1
   cargo test -p tidemq --test runtime_queue actual_runtime_sdk --release --locked -j2 -- --ignored --test-threads=1'
```

先设置下面说明的 `RUSTFS_BINARY`。新测试输出没有 Cargo 缓存，首次执行需要下载依赖；已有专用缓存可以单独挂载并设置 `CARGO_HOME`，但不能把宿主的其他项目缓存或凭据整体带入容器。上面的普通测试不自动运行 ignored，后两条只运行真实 RustFS 的对应测试；需要官方 Collector 的 ignored 另按[测试方法](../../docs/testing-benchmarks.zh-CN.md)执行。每条命令须保存真实退出码和日志；fixture 准备失败或临时目录执行失败不能算业务验证通过。

RustFS 使用官方 1.0.3 的 Linux x86_64 musl 可执行文件，只读 bind 到同一个 runtime 镜像，不依赖未经核实的 RustFS 镜像。此次操作者已验证的发布物为：

| 对象 | SHA-256 |
| --- | --- |
| `linux-x86_64-musl-v1.0.3.zip`，101926479 字节 | `29a083f156fe1a246f0336afe16ff77fffe587628e97339448d8f4fba38dba54` |
| 解压后的 `rustfs` | `c14fd1216f99efba9cac4bb469a5fa31ea9e461b82b79a28b69280e057613c93` |

从[官方 1.0.3 release](https://github.com/rustfs/rustfs/releases/tag/1.0.3)核对下载资产和校验来源。传入可执行文件的绝对路径；`--version` 的实际输出为多行，首行为 `rustfs 1.0.3`，不要要求整个 stdout 只有一行。

## 生成与启动

为每种拓扑创建新的状态目录，不能覆盖已有输出。父目录应预先存在且私有。`prepare.sh` 调用现有 local generator，保留节点 ID、端口、拓扑和身份；只分发文件、改写容器内路径并添加 loopback RustFS 配置。

```bash
STATE=/absolute/new-rf1-state
RUSTFS_BINARY=/absolute/verified/rustfs
export TIDEMQ_IMAGE RUSTFS_BINARY
bash "$REPO/deploy/docker/prepare.sh" rf1 "$STATE"
# For RF3 use a different new directory; 1g is the bounded RustFS maximum.
# bash "$REPO/deploy/docker/prepare.sh" rf3 "$STATE" 1g
chown -R 10001:10001 "$STATE/run" "$STATE/client" "$STATE/rustfs" "$STATE/scripts"

TASK_PROJECT=$(awk -F= '$1 == "COMPOSE_PROJECT_NAME" {print $2}' "$STATE/project.env")
if [[ ! $TASK_PROJECT =~ ^tidemq-validation-(rf1|rf3)-[0-9a-f]{16}$ ]]; then
  printf 'Invalid validation project name\n' >&2
  exit 2
fi
dc() { COMPOSE_PROFILES= docker compose --project-name "$TASK_PROJECT" --env-file "$STATE/project.env" -f "$STATE/compose.yaml" "$@"; }
dc --profile tools config --quiet
dc up -d network rustfs bucket
```

生成的 `project.env` 只含随机且独立的 `COMPOSE_PROJECT_NAME=tidemq-validation-<rf1|rf3>-<hex>`。上面的函数仍显式传 `--project-name`，因为[调用 shell 中的同名变量优先于 env 文件](https://docs.docker.com/compose/how-tos/environment-variables/envvars/)，不能让继承的环境将操作指向其他项目。`TIDEMQ_IMAGE` 和 `RUSTFS_BINARY` 必须由调用环境设置，前者使用已经构建并核对的本地镜像，配置中的 `pull_policy: never` 防止意外拉取替代品。相对 bind 路径以生成的 `compose.yaml` 所在目录为基准。

RustFS 使用 `127.0.0.1:19000` 和独立的 `rustfs-data` 卷。它通过健康检查后，`bucket` 执行一次有界初始化：先 HEAD，404 时 PUT 创建私有桶；其他异常立即失败，节点依赖其成功退出。请求通过 stdin 将凭据交给 curl 的 SigV4 配置，连接超时 1 秒、单请求超时 5 秒，响应丢到 `/dev/null`，不打印凭据或对象内容。64 KiB 的 curl `max-filesize` 检查还受所安装 curl 对未知长度响应的支持限制，不应将它理解为通用流量限额。无自动循环重试。可用以下命令在 120 秒内检查初始化结果：

```bash
deadline=$((SECONDS + 120))
while :; do
  bucket_id=$(dc ps -a -q bucket)
  if [ -n "$bucket_id" ]; then
    bucket_status=$(docker inspect --format '{{.State.Status}}:{{.State.ExitCode}}' "$bucket_id")
    if [ "$bucket_status" = exited:0 ]; then break; fi
    if [[ $bucket_status == exited:* || $bucket_status == dead:* ]]; then
      dc logs --tail 40 bucket
      exit 1
    fi
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then dc ps -a; exit 1; fi
  sleep 1
done
dc up -d
```

RF1 节点为 `node-1`；RF3 为 Metadata `11/12/13`、Storage `21/22/23`、Broker `31/32`、Gateway `41/42`。逐节点核对卷权限，并通过 holder 内部的 loopback 进行有界就绪检查，不发布管理端口：

```bash
node_ids=1
# For RF3: node_ids='11 12 13 21 22 23 31 32 41 42'
deadline=$((SECONDS + 120))
for id in $node_ids; do
  dc exec -T "node-$id" /bin/sh -c \
    'test "$(id -u):$(id -g)" = 10001:10001 && test -w /var/lib/tidemq && test "$(stat -c "%u:%g:%a" /var/lib/tidemq)" = 10001:10001:700'
  until dc exec -T network curl --fail --silent --output /dev/null --max-time 2 \
    "http://127.0.0.1:$((8400 + id * 10))/readyz"; do
    if [ "$SECONDS" -ge "$deadline" ]; then dc ps -a; exit 1; fi
    sleep 1
  done
done
dc exec -T rustfs /bin/sh -c 'test -w /var/lib/tidemq/objects && test "$(stat -c "%u:%g:%a" /var/lib/tidemq/objects)" = 10001:10001:700'
```

`init/` 保留完整初始化材料和 CA 私钥，绝不挂载给角色。`run/node-ID/` 只有该节点自己的私钥/证书、所有节点的 public DER、CA、共享 access/delivery 材料及其配置；只有 Combined/Storage 额外获得 S3 凭据。`client/` 严格只有 `ca.pem` 和 `access.token`。目录模式 `0700`，文件 `0600`；只对前述四个运行目录变更 owner，保留 `init/` 的初始化 owner。

## SDK 验证与诊断

`validator` 属于显式 `tools` profile，挂载 `/run/client` 为只读，并独享 `validation-data:/var/lib/tidemq`。RF1 的网关地址为 `127.0.0.1:7410`，RF3 的 Gateway 41 为 `127.0.0.1:7810`。server name 必须匹配生成的 SAN，通过独立 TLS 参数传入，无需 DNS 或 `extra_hosts`；Docker 的 [container 网络模式不支持 add-host](https://docs.docker.com/engine/network/#container-networks)，不能在共享 namespace 服务上配置它。每次使用尚不存在的 case 子目录：

```bash
dc --profile tools run --rm --no-deps validator \
  --endpoint 127.0.0.1:7410 --server-name node-1.local \
  --ca /run/client/ca.pem --token-file /run/client/access.token \
  --output-dir /var/lib/tidemq/rf1-case-001 \
  --messages 64 --message-bytes 1024 --replay-settle-ms 5000
```

使用 Linux Bash harness 保留容器和可复核证据，接口为 `scripts/docker-validate.sh STATE CASE`。需要目标机已有的 Docker、jq 和 GNU coreutils；同一项目逐项执行，不能同时运行多个故障场景。环境仍只需前面设置的 `TIDEMQ_IMAGE`、`RUSTFS_BINARY`。脚本严格验证 `project.env` 的唯一规范行，始终显式传项目名，并在每次 kill/pause/update/start 或写阶段标记前核验精确容器 ID、Compose 项目/服务 label 和实际 Image ID。每个 validator 在启动前固定唯一 [Compose run name 与 case label](https://docs.docker.com/reference/cli/docker/compose/run/)；先确认该精确名字不存在，再创建。若 CLI 超时但 daemon 可能已经创建容器，trap 最多用 5 秒按该精确名字找回 ID，并额外核对 Name/case label 后处理，绝不泛扫其他 driver；无法确认会在失败报告中保留名字、label 和 `unresolved` 状态。

```bash
bash "$REPO/scripts/docker-validate-selftest.sh"
bash "$REPO/scripts/docker-validate.sh" "$STATE" smoke
bash "$REPO/scripts/docker-validate.sh" "$STATE" replay
# RF1 only:
bash "$REPO/scripts/docker-validate.sh" "$STATE" oom
# RF3 only, after switching STATE to the separately prepared RF3 project:
# bash "$REPO/scripts/docker-validate.sh" "$STATE" storage-kill
# bash "$REPO/scripts/docker-validate.sh" "$STATE" broker-kill
# bash "$REPO/scripts/docker-validate.sh" "$STATE" metadata-quorum
# bash "$REPO/scripts/docker-validate.sh" "$STATE" storage-quorum
# bash "$REPO/scripts/docker-validate.sh" "$STATE" gateway-kill
```

| CASE | 验证内容 |
| --- | --- |
| `smoke` | 64 条 × 1024 字节，完整唯一收发和持久 ACK |
| `replay` | 64 条 × 64 KiB，先生产、等待 5000 ms，再从原 H 消费 |
| `storage-kill` | 21/22/23 的 group 2 leader/term 至少 2 份一致；只杀实际选择的 leader；离线期间新增 8 条提交且持久 ACK，剩余副本确认另一 leader 和更高 term |
| `broker-kill` | 至少 2/3 Metadata 返回相同 `active` owner/epoch/route generation；只杀该 Broker；离线期间新增 8 条提交且持久 ACK，再确认另一 owner 和更高 epoch |
| `metadata-quorum` | pause 11/12 并等待 4 秒租约失效；验证原 PendingBatch 没有成功回执，恢复 quorum 后完成 |
| `storage-quorum` | pause Storage 21/22 并等待 4 秒；验证原 PendingBatch 没有成功回执，恢复原两个副本后重试并完成全部 80 条持久 ACK |
| `gateway-kill` | 杀连接入口 41，验证未完成的原 PendingBatch，启动原容器恢复；只证明 Gateway 冷重启 |
| `oom` | RF1 完成 warmup 后，将专用 node-1 的 memory/swap 从 384 MiB 强制降到 Docker 允许的最小 6 MiB；必须观察 `OOMKilled=true`、退出 137，再恢复原限额与原容器，完成原 PendingBatch；daemon 拒绝调整或未发生 OOM 都判失败 |

故障场景按固定小文件 `waiting-fault.json` → `fault-injected` → `fault-observed.json` → `recovered` → `report.json` 协调，最终完整持久 ACK 范围为 80 条。driver output 子目录必须由 driver 自己创建；host 不预建它。每个阶段标记和退出等待最多 120 秒，Docker 命令在独立进程组内有硬超时，并夹在该阶段期限内，避免 Compose plugin 子进程越过 CLI 期限。Metadata/Broker 的诊断响应是 `local_applied`，2/3 诊断一致仍不是线性一致读证明；脚本保存原始响应及所选 tuple。Storage 诊断同样是进程内有界采样，故障前后都需一致证据，不能把单次旧采样当成准确 leader。

每次在 `$STATE/evidence/<case>.<random>/` 保存 driver report、终态/exit、各角色状态与挂载、受限日志、metrics、diagnostics、故障标记和选择依据。OOM 还保存角色容器自身的 `memory.max/current/events`、实际降低后的 HostConfig/State、原限额和恢复后的 cgroup 文件，不修改宿主全局 cgroup；这是受控调低限额后的 OOM 恢复验证，不能代表默认限额下的自然负载压力结果。每次 telemetry 快照最多 30 秒；单份日志/HTTP 输出最多 256 KiB、JSON report 最多 64 KiB、阶段标记最多 4 KiB。故障中不可访问的节点会留下错误证据；故障前后正常节点的必需采集失败会让该 case 失败。

失败 trap 只恢复本次记录且再次核验身份的原容器，恢复原内存/swap 并 unpause，不创建替代节点；它在共享 5 秒预算内 TERM/必要时 KILL 本次 validator，记录真实终态。所有容器和卷都保留，脚本不执行清理。只有 driver 退出 0、report 为 passed、64/80 条唯一收发与持久 ACK 全部相符时才报告通过。无 Docker self-test 覆盖解析、拒绝错误身份、未知启动找回、标记限额、Storage/Broker 一致性、离线持久进度、恢复及错误退出/report，并用 fake Docker/plugin/sleeper 验证整个进程组超时退出；Linux 必须通过 GNU timeout 检查，缺少 GNU coreutils 的 macOS 会明确跳过这一项。它不替代真实 Docker/OOM/故障验证。

诊断通过项目内 holder 执行，例如 `dc exec -T network curl --fail --silent --max-time 2 http://127.0.0.1:8410/diagnostics` 或 `/metrics`。RF3 管理端口按 `8400 + node_id * 10` 计算。`dc logs --tail 100 node-1` 只读取该项目的有界 Docker 日志；不将 SDK token、S3 密钥或完整配置输出到日志。默认未启用 OTLP。

## 资源、持久化与清理

所有服务使用 UID/GID `10001:10001`、只读根文件系统、`cap_drop: ALL`、`no-new-privileges`、独立 cgroup、128 PID、16 MiB `/tmp` tmpfs、Docker `local` 日志 `10 MiB × 3`、`restart: no`、6 秒 stop grace。`memswap_limit` 等于 `mem_limit`，不给额外 swap；tmpfs 使用量同样受该容器的内存限额约束。

| 服务 | CPU 上限 | 内存上限 |
| --- | --- | --- |
| 每个 TideMQ 角色 | 0.5 | 384 MiB |
| RustFS | 1 | 默认 512 MiB，可显式选 1 GiB |
| namespace holder | 0.05 | 16 MiB |
| 一次性 bucket 初始化 | 0.1 | 32 MiB |
| validator | 0.5 | 192 MiB |

RF3 十个角色加全部辅助服务，即使把未运行的 bucket 和工具客户端同时计算在内，默认合计 4592 MiB；RustFS 1 GiB 时合计 5104 MiB，低于 5 GiB。资源限额用于安全隔离，可能触发真实 OOM，应记录 OOM/exit 状态，不能静默提高或移除限额。构建容器独立限制为 4 CPU / 8 GiB / 512 PID / Cargo jobs 2，不属于运行集群预算。

每个 `data-ID`、`rustfs-data` 和 `validation-data` 都是当前 Compose 项目拥有的独立 named volume。`down` 保留卷，使用原状态目录和配置重新 `up` 可恢复该身份及其数据；不要重新生成身份后复用旧卷。holder 重建后须协调重建共享 namespace 的容器。磁盘占用和保留策略仍需要操作者监控，CPU/内存限额不等于磁盘配额。

结束当前拓扑的验证时，用同一个 `STATE`、环境和 `dc` 函数操作：

```bash
dc down --timeout 6 --remove-orphans
# Only after saving evidence and intentionally discarding this project's data:
dc --profile tools down --timeout 6 --volumes --remove-orphans
```

这些命令只作用于生成的随机项目；不要使用全局 prune、按模糊容器名 kill、清理其他项目卷或重启 Docker daemon。源码、镜像、状态目录及构建缓存的清理分别按记录的精确路径/ID 操作。

## 本地静态自检

```bash
bash -n "$REPO/deploy/docker/prepare.sh" "$REPO/deploy/docker/init-bucket.sh" "$REPO/deploy/docker/selfcheck.sh"
bash "$REPO/deploy/docker/selfcheck.sh"
# A standalone Compose CLI and an existing TideMQ binary are optional inputs:
# COMPOSE_BIN=/absolute/docker-compose TIDEMQ_BIN=/absolute/tidemq bash "$REPO/deploy/docker/selfcheck.sh"
```

自检在专属临时目录生成 RF1/RF3，检查凭据边界、权限、配置路径、拒绝覆盖、节点/服务/卷数量、namespace 及内存/swap 上限，并执行 Compose `config`；提供 `TIDEMQ_BIN` 时还验证全部 11 份节点配置。它不会运行容器。真实镜像运行、named-volume owner、RustFS 桶、RF1/RF3 SDK 与容器故障恢复须在目标 Docker 主机单独验证，结果由对应证据文档记录。
