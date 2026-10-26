# Runner 资源清理与重建

清理只覆盖 `tidemq-runner` 上可重建的构建物料，以及带有明确所有权的临时 CI 资源。入口是 [runner-maintenance.sh](../../scripts/lab/runner-maintenance.sh)，保留额度在 [runner-maintenance.env](runner-maintenance.env)。常驻 MQ、公共 RustFS、观测数据、VM 磁盘、工具链、服务配置及凭据不属于缓存。

## 执行时机与互斥

安装程序把受跟踪的脚本及配置复制到 root 管理的 `/usr/local/lib/tidemq-lab/`，并在 runner `.env` 注册任务开始、完成两个钩子。完成钩子在制品上传及 action post steps 之后执行，依据 [GitHub 官方钩子协议](https://docs.github.com/en/actions/how-tos/manage-runners/self-hosted-runners/run-scripts)。脚本在 checkout 之外，删除旧 checkout 不影响自身执行；更新 `.env` 后安装程序重启空闲 runner。

`tidemq-runner-maintenance.timer` 每小时运行一次，并在启动后补查。检测到 `Runner.Worker` 时跳过；任务开始钩子与定时器取得同一个本地 flock，所以即使定时检查与新任务同时发生，任务步骤也会等待清理完成。清理还取得现有宿主全局租约，避开手动部署和故障测试。手动运行仍必须使用该租约。

每次执行限制 10 分钟，钩子和 systemd 外层限制 12 分钟；超时或磁盘预算不足会明确失败。任务取消或异常退出留下的资源由后续钩子/定时器重试。定时服务失败可通过 journal 和现有磁盘水位告警排查。

## 范围与额度

| 物料 | 策略 |
|---|---|
| `/srv/tidemq/work/` 的仓库 checkout | 每次任务前后删除；保留 runner 的 `_` 前缀工作目录 |
| `_actions` 下载缓存 | 最长 7 天，超过 1 GiB 清空；开始钩子保留已下载的当前任务 action |
| `_temp` | 删除超过 1 天的入口，不删除当前钩子仍可能打开的 file commands |
| `/srv/tidemq/cargo-target` | 最长 7 天，总额 18 GiB；超额清空并按需编译 |
| Cargo registry / git | 各最长 7 天、2 GiB；不删除 `cargo/bin`、rustup 或固定工具 |
| `/srv/tidemq/tmp` | 最长 1 天，2 GiB；在全局租约保护下清理 |
| Runner `_diag` | 删除超过 7 天的文件；超过 128 MiB 时删除超过 1 天的文件，保留近期及当前日志 |
| Runner Docker | 清理停止的容器、未使用镜像和构建缓存；检查 VM role、挂载点和 Docker data-root，固定连接本机 socket；有运行中容器时拒绝清理 |
| 临时 Kubernetes / fixture / loadgen 数据 | 使用下述所有权检查与共用回收脚本 |

数据盘可用量低于 12 GiB 或根盘低于 2 GiB 时，再清理可重建的 Cargo / 临时缓存。仍未达到额度则拒绝继续任务，保留运行数据供排查。额度是空闲时的保留限制；单个运行中任务仍受 64 GiB 数据盘及 CI 超时约束，不能把它当成无限临时空间。近期 runner 日志和 file commands 可能暂时超过软额度，磁盘检查不会为腾空间删除它们。

LLVM profile 与编译产物在单元、集成、故障钩子、外部依赖测试和最终报告导出之间一直保留；完成钩子在上传后运行。独立 coverage job 仍只下载、比较报告，没有重编译或重跑 Rust 测试。

Docker 删除包括 runtime base 的本地副本。[ensure-runtime-base.sh](../../scripts/lab/ensure-runtime-base.sh) 按 inventory 的官方完整 digest 拉取并核验；已有镜像身份错误会失败，不静默接受。冷重建需要访问固定镜像及依赖的下载源。

## CI 遗留资源回收

[cleanup-ci.sh](../../scripts/lab/cleanup-ci.sh) 同时用于 CI 的 EXIT trap 和空闲回收，必须持有宿主租约。只接受 `ci-数字-数字` 身份；namespace 必须带 `tidemq.lab/ci=true` 标签。CI 开始即写私有 `.tidemq-ci` 标记，即使部署尚未创建 namespace，也能在下次回收时发现。

先恢复允许故障的 VM，再检查 namespace 和所有 PV 的 claim namespace、固定名字及精确本地路径。确认 namespace 删除完成后，才删除该 run 的 6 个 PV、worker 数据、fixture 进程/单元、loadgen 目录和私有构建。任何不匹配或 API 故障都会保留重试材料并返回失败。`tidemq-dev`、无标签的手工 namespace 和公共服务不在允许范围内。

正常 CI 会先收集业务报告和故障日志，再调用清理。GitHub 上传失败时应及时从任务日志排查；本地 checkout 会按上述规则回收，不作为长期证据库。长期需要保留的结果应合并为脱敏验收记录，原始过程通过固定 Git 提交和 GitHub 制品定位。

## 安装、查看与手动执行

宿主运维入口：

```bash
cd /root/tidemq-lab-source
bash scripts/lab/deploy-runner.sh maintenance
source scripts/lab/guest.sh
lab_ssh tidemq-runner sudo systemctl list-timers tidemq-runner-maintenance.timer
lab_ssh tidemq-runner sudo journalctl -u tidemq-runner-maintenance.service -n 80 --no-pager
lab_ssh tidemq-runner sudo systemctl start tidemq-runner-maintenance.service
```

安装时 runner 必须空闲；`all` / `runtime` 部署也自动安装同一机制。修改额度或脚本后重新执行 `maintenance`，不能只修改 Git 而遗漏已安装副本。安装程序不会注册新的 runner，也不更换现有凭据。

## 重建及保留资料

当前项目保留完整资源 inventory、固定二进制 URL/SHA、镜像 digest、工具构建 Dockerfile、部署脚本、CI 脚本及验收归档索引。根盘/数据盘里的以下副本可删：旧 checkout、Cargo target、下载缓存、runner/runtime 构建 bundle、镜像 tar、公共镜像构建副本及历史升级目录中的二进制。旧迁移删除脚本已移除，避免历史授权被再次应用于其他项目。

工具重建在宿主执行：

```bash
cd /root/tidemq-lab-source
bash scripts/lab/download-tools.sh
bash scripts/lab/prepare-runner-assets.sh
bash scripts/lab/deploy-runner.sh runtime
```

`runtime` 在缺少工具或工具链 bundle 时也会调用相应准备脚本。注册及私有访问材料仍独立管理；更新工具不能重置 workload 身份。日常 Rust CI 会自动从 checkout、Cargo.lock 和固定工具重建，不需要恢复旧 target。

公共镜像可在持有全局租约的 runner 中，使用新的源码 checkout 和 `build-image.sh NEW_PRIVATE_BUILD` 重建。恢复当前验收版须选用 `acceptance.json` 的源码提交及锁定工具；验证生成的二进制/镜像身份并记录新结果，再按常驻 `OnDelete` 逐个更新策略部署。重建可能产生新的 manifest，不凭提交号声称 digest 必然相同。已有 worker 上的运行镜像不因 runner 清理而删除。

必须备份并保留 `/var/lib/tidemq-lab/private/{tls,credentials,workloads}`、受限 SSH 密钥、kubeconfig、VM 定义和磁盘，以及各服务的持久数据。公用 workload 的完整身份在 `private/workloads/tidemq-dev/workload`；runner 的 `public/tidemq-dev/workload` 及 loadgen `/srv/tidemq/public-dev` 也保留。它们不能由重新生成 CA、cluster ID 或 token 代替。宿主其他项目、OS 工具和用户明确保留的 YuXiangagent 任务不参与自动清理。

历史一次性证据的完整原件仍在[固定 Git 归档](../../docs/evidence/README.zh-CN.md)，当前只保留[合并验收](evidence/2026-10-25/README.zh-CN.md)及最新清理验收。GitHub 测试制品保留 14 天、覆盖率/VM 制品 30 天；长期证据不依赖这些制品永远在线。
