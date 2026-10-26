# 第二个 Linux runner

只新增一个仓库级 runner，名称 `tidemq-ci-container-u126-8`、标签 `tidemq-ci-container`。原生 `tidemq-dev-u126-8` 保留，专门运行故障/部署和报告合并。容器由同一 `tidemq-runner` VM 的 Docker Compose 管理，配置见 [compose.yaml](compose.yaml)，镜像构建见 [Dockerfile](Dockerfile)。不增加 Kubernetes runner controller 或额外 VM。

## 资源和路径

runner VM 为 8 vCPU / 16 GiB，IP 和 64 GiB 数据盘保持不变。原生 runner 的 systemd drop-in 为 `CPUQuota=400%`、`MemoryMax=6G`；容器为 4 CPU、8 GiB RAM、无额外 swap、1024 PID。两条 Cargo 流程各 `CARGO_BUILD_JOBS=2`，nextest 仍逐项执行；这是 job 并行，不改变测试内部断言与重试策略。

| VM 路径 | 容器内路径 / 用途 |
|---|---|
| `/srv/tidemq/container-runner/state` | `/state:ro`，独立注册凭据；仅首次注册临时以 rw 挂载 |
| `/srv/tidemq/container-runner/diag` | `/diag`，独立 runner 诊断和清理锁 |
| `/srv/tidemq/container-runner/work` | `/srv/tidemq/work`，独立 checkout，保持两边源码绝对路径相同 |
| `/srv/tidemq/container-runner/target` | `/srv/tidemq/cargo-target`，独立编译、profile 和临时报告目录 |
| `/srv/tidemq/container-runner/tmp` | `/srv/tidemq/tmp`，独立测试临时文件 |
| `/srv/tidemq/rust/cargo` | `/opt/tidemq-rust/cargo`，共享下载缓存及 Cargo 的全局缓存数据库/锁；其中 bin 只读 |
| `/srv/tidemq/rust/rustup` | `/opt/tidemq-rust/rustup:ro`，固定 Rust 1.99.1 工具链 |
| 原生 runner 的 bin / externals、固定 nextest / llvm-cov | 只读工具，不包含原生 runner 的注册状态 |
| `/srv/tidemq/maintenance/cargo-cache.lock` | 同路径文件挂载，协调 Cargo 流程与缓存回收 |

容器不发布端口，不挂载 Docker socket、libvirt、kubeconfig、SSH key 或原生 runner 的 `.credentials`。根文件系统只读，runner 的工作安装副本位于有界 HOME tmpfs；重启从镜像重建，注册状态从 `/state` 读取。日志、checkout、target 和 tmp 位于持久数据盘，不依赖容器可写层。`RUNNER_MANUALLY_TRAP_SIG=1` 使用上游 `run.sh` 自带的信号转发，把停止信号传给 Listener 并释放 GitHub 会话；以 GitHub online 状态和 `Listening for Jobs` 日志确认恢复，不能只看容器 running。

两个 runner 共享下载缓存可避免重复下载；各自 target 仍需编译。独立构建可以同时进行，Cargo 自身的包锁保护下载/解压。[with-cargo-cache.sh](../../../scripts/ci/with-cargo-cache.sh) 在整个流程持有共享 flock，清理取得非阻塞独占锁后才删除 registry/git。手动 Cargo 命令也必须通过该入口；不可绕过锁清理共享目录。

每边 target 空闲保留上限 12 GiB、7 天，共享 registry/git 各 2 GiB；数据盘至少保留 12 GiB 可用量。大于额度的 target 在制品上传后的钩子清空。原生小时定时器还回收空闲容器的取消任务残留，规则见 [资源清理](../runner-maintenance.zh-CN.md)。Docker 日志限制 10 MiB × 3；近期诊断日志保留，软额度 128 MiB。

## 安装和重建

脚本在宿主 `ssh dev` 的受跟踪源码副本中执行，且两个 runner 都必须空闲。VM 首次扩容需先停原生 runner、关闭 VM，再通过 libvirt 设置 8 vCPU / 16 GiB 后启动；新建环境直接使用 [inventory.json](../inventory.json) 的资源值。不要在有任务时关闭 VM。

```bash
cd /root/tidemq-lab-source
bash scripts/lab/deploy-runner.sh maintenance
bash scripts/lab/deploy-container-runner.sh build
```

`build` 使用 inventory 固定 digest 的 Ubuntu base 和已校验的原生 runner 2.338.1 公共安装文件。只把 config/run 脚本打入镜像，bin/externals 与 Rust 工具只读挂载；不复制已注册 runner 目录。基础系统包使用 Ubuntu 签名校验的 HTTPS 镜像源。冷重建需可访问固定镜像及包源，APT 包不承诺逐字节重建。

首次注册使用仓库短期 registration token。操作端通过已授权的 `gh api --method POST repos/bitcapybara/tidemq/actions/runners/registration-token --jq .token` 重定向到权限 600 的临时文件，再安全复制到宿主私有路径；不把 token 打印、写入仓库或传入 CI 变量日志。然后在宿主执行：

```bash
bash scripts/lab/deploy-container-runner.sh register /var/lib/tidemq-lab/private/container-runner/registration.token
bash scripts/lab/deploy-container-runner.sh start
```

注册脚本仅在注册阶段挂载 state 为 rw，结束删除 token；常驻容器以 ro 使用独立状态。已有 `/state/.runner` 时拒绝重复注册。若 GitHub 已添加 runner、但本地注册失败，先核对并删除该次创建的孤立注册项，再使用新 token 重试，避免重复 runner。

日常修改镜像/钩子/额度后执行 `build`、`start`，不要重复 register。更新共用 Rust 工具链或原生 runner bin/externals 前，先暂停任务并在 VM 执行 `docker stop tidemq-ci-runner`，完成更新后重新 build/start。runner 自动更新关闭；升级版本要同步镜像版本、校验配置和本脚本的固定版本检查。

## 检查与 CI

```bash
cd /root/tidemq-lab-source
source scripts/lab/guest.sh
lab_ssh tidemq-runner sudo docker ps --filter name=tidemq-ci-runner
lab_ssh tidemq-runner sudo docker logs --tail 40 tidemq-ci-runner
lab_ssh tidemq-runner sudo systemctl show actions.runner.bitcapybara-tidemq.tidemq-dev-u126-8.service -p CPUQuotaPerSecUSec -p MemoryMax
lab_ssh tidemq-runner sudo systemctl start tidemq-runner-maintenance.service
```

GitHub 的 Linux Validate 指定容器标签，Faults 指定 `tidemq-dev`，两者没有依赖关系；macOS 继续使用 hosted runner。Coverage 等待两边完成，下载 `coverage-inputs-native` / `coverage-inputs-lab`，校验同一提交、run、工具和工作区路径，合并已有二进制/profile，再导出报告及比较受保护基线，不重跑 Rust 测试。输入制品保留 3 天，最终覆盖率/VM 报告 30 天。完整流程和当前端到端缺口见 [测试指南](../../../docs/testing.zh-CN.md)。

两个执行器属于同一可信仓库、同一 actions UID 和同一 VM，共享可写依赖缓存，不构成恶意代码隔离边界。fork PR 保持 GitHub-hosted 路径；容器只接受仓库内可信任务。
