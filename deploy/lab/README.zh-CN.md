# TideMQ dev VM 实验室运维清单

本目录是已经选择的方案 A（KVM/libvirt + Ubuntu VM + kubeadm）的实施配置。机器为 `ssh dev`，物理地址 `192.168.126.8`。资源真值在 [inventory.json](inventory.json)，下载来源和校验值在 [binaries.json](binaries.json)。原来的开发计划不承担部署清单职责。

常驻部署身份与首次完整验收在 [evidence/2026-10-25/](evidence/2026-10-25/README.zh-CN.md)，新增容器 runner、并行 CI、API 覆盖率与最新清理验收在 [runner-maintenance.json](evidence/runner-maintenance.json)，时间使用 UTC。常驻 `tidemq-dev` 已部署；CI 使用独立的临时 namespace 和 RustFS 实例。

实验室模拟独立 Linux 内核、节点磁盘、网络、Raft 副本和依赖故障。11 台 VM 共享一台物理机和一块 NVMe；软件拓扑测试不能证明物理可用区隔离、真实跨区延迟、宿主机断电或整块 NVMe 损坏时的行为。

## 1. 物理资源与 VM

宿主机：Ubuntu 24.04，Xeon E5-2696 v4，22 核 / 44 线程，约 62 GiB RAM，468 GiB 根文件系统，KVM/QEMU 8.2.2、libvirt 10.0.0。VM 合计 36 vCPU、46 GiB RAM、250 GiB 虚拟磁盘上限；数据盘使用预分配 raw，根盘使用基于锁定 Ubuntu cloud image 的 qcow2。数据盘 `cache=none`，保留正常 flush 语义。VM 不使用内存气球。

Worker 和 observe 系统盘为 16 GiB，其余 VM 为 8 GiB（实际根文件系统约 6.8 GiB）。构建、测试数据和观测数据放到独立数据盘。

| VM | 管理 IP | 数据 IP | vCPU | RAM | 数据盘 / 挂载点 | 故障域 |
|---|---|---|---:|---:|---|---|
| tidemq-cp-a | 192.168.240.11 | 192.168.241.11 | 2 | 2 GiB | 无，etcd 在根盘 | zone-1 |
| tidemq-cp-b | 192.168.240.12 | 192.168.241.12 | 2 | 2 GiB | 无，etcd 在根盘 | zone-2 |
| tidemq-cp-c | 192.168.240.13 | 192.168.241.13 | 2 | 2 GiB | 无，etcd 在根盘 | zone-0 |
| tidemq-worker-a | 192.168.240.21 | 192.168.241.21 | 4 | 4 GiB | WAL 8 GiB + metadata 2 GiB | zone-1 |
| tidemq-worker-b | 192.168.240.22 | 192.168.241.22 | 4 | 4 GiB | WAL 8 GiB + metadata 2 GiB | zone-2 |
| tidemq-worker-c | 192.168.240.23 | 192.168.241.23 | 4 | 4 GiB | WAL 8 GiB + metadata 2 GiB | zone-0 |
| tidemq-storage | 192.168.240.31 | 192.168.241.31 | 2 | 2 GiB | 8 GiB，`/srv/tidemq` | infra |
| tidemq-observe | 192.168.240.32 | 192.168.241.32 | 4 | 6 GiB | 16 GiB，`/srv/tidemq` | infra |
| tidemq-runner | 192.168.240.33 | 192.168.241.33 | 8 | 16 GiB | 64 GiB，`/srv/tidemq` | infra |
| tidemq-fixtures | 192.168.240.34 | 192.168.241.34 | 2 | 2 GiB | 8 GiB，`/srv/tidemq` | infra |
| tidemq-loadgen | 192.168.240.35 | 192.168.241.35 | 2 | 2 GiB | 4 GiB，`/srv/tidemq` | infra |

Worker 挂载点为 `/var/lib/tidemq/storage` 和 `/var/lib/tidemq/metadata`。每个 namespace 使用自己的子目录。Local PV 采用 `Retain`，CI 在成功删除 namespace 后，显式删除该次运行的 PV 和目录。PV 声明容量不是 ext4 子目录硬配额；磁盘压力测试须独占实验室，不能把 PVC 的 `1Gi` 当作硬隔离。

## 2. 网络与 Kubernetes

| 项目 | 配置 |
|---|---|
| 管理网络 | `tidemq-management` / `virbr-tmq-mgmt`，192.168.240.0/24，NAT，网关 192.168.240.1 |
| 数据网络 | `tidemq-data` / `virbr-tmq-data`，192.168.241.0/24，隔离二层网络，无默认路由 |
| Kubernetes | kubeadm v1.36.5，3 control-plane + 3 worker，stacked etcd |
| API VIP | 192.168.241.10:6443，kube-vip v1.3.1 |
| Pod / Service CIDR | 10.244.0.0/16 / 10.96.0.0/16 |
| CNI | Cilium 1.20.4，VXLAN，data0，保留 kube-proxy，关闭不需要的 L7/Envoy |
| UDP LoadBalancer | MetalLB 0.16.1，L2，192.168.241.200–209，data0 |
| TideMQ 公共 / CI VIP | 192.168.241.200:7443 / 192.168.241.201:7443，UDP |
| 存储 | `tidemq-local`，no-provisioner，本地盘，WaitForFirstConsumer |
| Kubernetes 状态指标 | kube-state-metrics v2.20.0，只读最小资源权限，NodePort 30080 |

每个 RF3 实例包含 3 metadata、3 storage、2 broker、2 gateway，共 10 个角色 Pod。metadata 11/12/13 与 storage 21/22/23 分别跨三个 worker；broker 31/32 和 gateway 41/42 在 worker-a/b。每个角色使用独立 StatefulSet、稳定 UDP Service IP 和独立证书。容器监听通配地址，广播可达的稳定 Service 地址；管理接口只监听 Pod loopback。

MQ 入口采用 `externalTrafficPolicy: Local`，由有本地 gateway 的节点接流，避免入口再转到另一个节点的 UDP backend。[MetalLB 流量策略](https://metallb.io/usage/#traffic-policies)说明了 L2 模式的取舍；此模式的入口流量集中在当前公告节点，不能将两个 gateway 的 CPU 简单相加作为入口容量。

常驻实例来自完整 CI 通过的干净提交 `c386434bbf6d478e96d68a53d7040323396b1648`，镜像 `localhost/tidemq-lab:public-c386434bbf6d`，manifest digest `sha256:ed44f2266704e3613adc8e82f850d7a6e8babd7973346ab483baefbb5a3eab5c`。已创建 `validation` tenant 及其 `default` namespace，供 SDK 验收使用。初次部署清单见 [public-cluster-resources.json](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/deploy/lab/evidence/2026-10-25/public-cluster-resources.json)，升级后的实际镜像与节点见 [final-state.json](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/deploy/lab/evidence/2026-10-25/final-state.json)。本次保留原有 cluster ID、CA、token、Service IP 和卷绑定，10 个角色逐个重建并等待 Ready。公网关的 Service IP 为 `10.96.1.178`，UDP NodePort `31226`，健康检查 TCP NodePort `32278`；这些 NodePort 没有作为外部入口发布。

公共镜像使用绑定提交的独立 `public-COMMIT` 标签，CI 的内容哈希标签可以复用，不再改写公共引用。[public-image-identity.json](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/deploy/lab/evidence/2026-10-25/public-image-identity.json) 分别记录 Docker/manifest 身份与运行时 config digest；后者为 `sha256:9dffd22cafa91b655b32cd15762e684829e2c86fe22d1f55c082ca60d278b841`。CRI 状态可能返回同一 digest 已有的标签名，因此升级检查 Pod spec 的公共标签、实际 imageID 和 Ready 状态，不以状态中的标签字符串代替镜像身份。

每个 namespace 独立创建 QUIC CA、节点证书、访问 token 和 cluster ID。配置由 [workload.mjs](../../scripts/lab/workload.mjs) 生成，沿用仓库已有的 RF3 配置生成器。私钥及含 Secret 的资源文件只保存在私有目录，禁止作为 GitHub artifact 上传。

节点启动时通过 systemd 将 kubelet 排在 containerd 之后。故障恢复检查最多等待 110 秒，逐个访问 10 个角色的应用 `/readyz`；Node Ready 可能是重启前的旧状态，不能单独证明服务恢复。该基础设施等待耗时单独保存，之后才通知 SDK 开始原请求恢复。SDK 每次请求仍为 10 秒，原 Pending 恢复窗口仍为 60 秒，整个验证实例仍受原有总期限约束；接收暂态错误只重试原 Consumer，不创建替代消费者。

## 3. 外部设备可访问入口

在局域网 / 已有 VPN 的 DNS 或设备 hosts 文件中，将以下名称映射到 **192.168.126.8**。当前放行 `192.168.126.0/24` 和配置时观测到的客户端 `192.168.127.73/32`；新增 VPN 网段应修改 inventory 并重跑 `host-ingress.sh`。

```text
192.168.126.8 s3.lab.tidemq.test rustfs.lab.tidemq.test grafana.lab.tidemq.test metrics.lab.tidemq.test otlp.lab.tidemq.test mq.lab.tidemq.test
```

| 地址 | 用途 / 认证 |
|---|---|
| `https://s3.lab.tidemq.test:18443` | 公共 RustFS S3 API，独立 bucket 范围凭据 |
| `https://rustfs.lab.tidemq.test:18445/rustfs/console/index.html` | RustFS 管理页面，根路径会跳转到此处，仅运维使用 root 管理凭据 |
| `https://grafana.lab.tidemq.test:18444` | Grafana 页面，admin + 私有密码 |
| `https://metrics.lab.tidemq.test:18444` | Prometheus 查询 / 页面，reader + 私有密码，只放行 GET |
| 同一 metrics 入口的 `/loki/api/v1/...` | Loki 只读查询 |
| 同一 metrics 入口的 `/tempo/api/...` | Tempo 只读查询 |
| 同一 metrics 入口的 `/alertmanager/` | Alertmanager 只读页面 / 查询 |
| `https://otlp.lab.tidemq.test:4318/v1/{traces,metrics,logs}` | OTLP/HTTP，必须携带实验室 CA 签发的客户端证书 |
| `mq.lab.tidemq.test:17443` | TideMQ 公共 QUIC/UDP，独立 QUIC CA 和 token |

服务 TLS CA 为 [ca.pem](ca.pem)，它是公开证书。外部设备应显式信任该证书，不关闭 TLS 校验。QUIC 工作负载使用各自独立的 CA，不能用此服务 CA 替代。

外部设备可运行 [external-collector.yaml](external-collector.yaml)：设置 `OTEL_QUEUE_DIRECTORY`、`OTEL_DEVICE_ID`、`OTEL_CA_FILE`、`OTEL_CLIENT_CERT`、`OTEL_CLIENT_KEY`，用锁定的 contrib Collector 启动。应用把 traces/metrics/OTLP logs 发给本机 `http://127.0.0.1:4318`。stdout / 文件日志应按设备 OS 增加 journald 或 filelog receiver，仍由 OS 轮转；不要同时采集同一份日志。每台设备应分配独立客户端证书，示例 `private/tls/external.{pem,key}` 仅用于初次联通验证。

宿主机只用专属 iptables 链做受限 DNAT/SNAT，由 `tidemq-lab-ingress.service` 持久化；没有将整个 VM 子网或 Kubernetes API 暴露到局域网。新增允许源地址需要同步防火墙。VM 的 SSH 22、Kubernetes API 6443、内部监控端口不作公共入口。

## 4. 公共服务与 CI 依赖

公共 RustFS 1.0.3 运行在 storage VM，原生 systemd 服务 `tidemq-rustfs`，严格持久化模式。`tidemq-dev` 和外部设备分别使用 `tidemq-dev` / `tidemq-devices` bucket 和不同账户；CI 不持有公共 RustFS root 凭据。

RustFS 按[官方观测协议](https://docs.rustfs.com/en/operations/observability)向本机 `127.0.0.1:4318` 推送 metrics/traces，再由带持久队列的 agent 通过 mTLS 转发。公共服务名 `rustfs-public`，fixture 服务名 `rustfs-fixture-a/b`；metrics 每 15 秒发送、trace 采样 10%。日志只保留 stdout/journald 采集路径，关闭 RustFS 的 OTLP logs 导出，不再另外配置日志文件。公共 RustFS 的 readiness 和 telemetry 缺失均有告警；测试 fixture 的正常删除不会触发公共服务告警。

每次 CI 在 fixtures VM 创建两个独立 RustFS 实例 a/b，各自有账户、进程、数据目录和恢复计时器：

| 项目 | a | b |
|---|---|---|
| TLS S3 入口 | 192.168.241.34:36443 | 192.168.241.34:36444 |
| 本地 RustFS 监听 | 127.0.0.1:19000 | 127.0.0.1:19010 |
| 数据 / 凭据 | `/srv/tidemq/fixtures/ci-RUN-ATTEMPT/a` | 同路径 `/b` |
| bucket | tidemq-ci | tidemq-ci |

CI 使用 `ci-RUN-ATTEMPT` namespace，运行结束删除依赖实例和数据。故障先由 guest systemd 安排自动恢复，再停止 / 暂停指定实例。a 发生故障时，必须验证 b 健康；公共 RustFS 和观测后端不参与这类故障注入。

同一 fixtures VM 上的 a/b 用于实例故障；fixtures VM 整机故障属于共同故障域。测试分布式对象存储的节点容错时，应分配多台 storage fixture VM，不能把同机两个进程当作独立存储节点。

## 5. log / tracing / metrics

TideMQ 输出有界 JSON 到 stdout。Kubernetes 由 CRI/kubelet 保存并轮转本地日志，系统服务使用持久 journald（128 MiB，最长 7 天），每台 VM 的原生 OTel agent 采集 journald、CRI 文件和主机指标。无需应用再同步写一套无限增长的日志文件。

Kubernetes 节点通过最小只读 ACL 允许 `otelcol` 访问 CRI 日志，新 Pod / 新轮转文件继承权限。filelog 从文件开头发现新文件，使用持久 checkpoint 避免重启后重复读取，并采集当前及未压缩轮转文件；已压缩的历史文件仍由 kubelet 本地保留策略管理。已验证 agent 停止期间发生轮转后，重启可以回补未压缩文件中的两条测试日志。超过本地保留范围或有界发送队列的观测数据仍可能丢失。

宿主机也运行相同的 agent（实例 `tidemq-host`），覆盖物理 CPU、NVMe、内存和磁盘水位；自监控仅绑定 VM 管理桥 192.168.240.1:8888。所有 agent 合计 12 个，其中 11 个位于 VM。

应用 Pod 内的 sidecar 抓取 loopback OpenMetrics 和 OTLP traces，再通过 mTLS 发送到中央 Collector。原生 VM agent 提供 file_storage checkpoint 和持久发送队列；中央 Collector 也有持久队列。Pod sidecar 的队列有界但为内存队列，Pod 丢失时未导出的 telemetry 可能丢失，业务消息持久性不依赖 telemetry 队列。

实验室 trace 采纳上限为每进程 16 spans/s，队列 128，批次 16，导出间隔 100 ms、超时 500 ms。批次容量覆盖采纳预算，避免健康状态下因为固定批次过小而持续丢弃。采样与丢弃是不同指标；export failure / discarded span 的告警保留。

| 组件 | 版本 | 本地监听 | 持久化与保留 |
|---|---|---|---|
| OTel contrib agent / central | 0.163.2 | agent 127.0.0.1:4318；自监控管理 IP:8888；central 127.0.0.1:24318 / 8889 | agent `/var/lib/otelcol`；central `/srv/tidemq/collector-queue` |
| Prometheus | 3.15.1 | 127.0.0.1:9090 | 14 天 / 2 GB |
| Loki | 3.7.9 | 127.0.0.1:3100，gRPC 3101 | 本地 TSDB/filesystem，7 天 |
| Tempo | 3.1.1 | HTTP 3200，OTLP 14318，gRPC 9095，均 loopback | 单体、本地块存储，72 小时 |
| Grafana | 13.2.4 | 127.0.0.1:3000 | `/srv/tidemq/grafana` |
| Alertmanager | 0.34.1 | 127.0.0.1:9093 | `/srv/tidemq/alertmanager` |
| alert-audit | 仓库脚本 | 127.0.0.1:19094 | 有界 current / previous JSONL，各 8 MiB |

配置在 [observe/](observe)。Grafana 自动配置 Prometheus/Loki/Tempo 数据源和实验室 dashboard。日志、trace 与业务指标使用 service/instance/node/namespace 关联；CI namespace 保存在 structured metadata / resource attributes 中，不创建按消息 ID、Topic 名或任意 payload 增长的索引标签。

直接打开 `https://grafana.lab.tidemq.test:18444/d/tidemq-lab` 查看 agent、后端、对象存储和按 namespace 过滤的应用状态。业务面板的 UID 分别是 `tidemq-runtime`、`tidemq-producer`、`tidemq-lifecycle`、`tidemq-scaling`；在同一 Grafana 的 `/d/UID` 路径访问。RustFS fixture 的 metrics/traces 也带有 `k8s.namespace.name=ci-RUN-ATTEMPT` 和对应的 `service.instance.id`，便于回查同一次故障。

部署时复用仓库 `deploy/observability/alerts/` 与 `dashboards/` 中的 runtime、producer、object lifecycle、scaling 告警与面板；不复制维护第二套业务指标规则。

告警覆盖后端不可用、VM agent 不可用、导出队列压力、磁盘剩余量。Alertmanager 将 firing/resolved 发到本机有界审计 webhook，便于验证完整告警链路；没有自动向人员发送消息。接入人员通知渠道需要另行配置和授权。

常驻 MQ 还检查 10 个应用实例的 telemetry 是否齐全。OTLP 推送完全停止时，`up == 0` 本身不能发现缺失序列，因此另有 absence 检查；临时 CI namespace 的删除不会触发这条常驻服务告警。

本地开发允许默认 nop tracing、stdout logging 和按需 metrics；CI 要求三类信号实际存储并可查询。正式生产应保持相同协议与字段，并采用独立故障域、HA 后端、持久存储、访问控制和经过容量评估的保留策略。本实验室的单体后端是测试实现，不是生产 HA 配额。

## 6. Runner 和 CI

GitHub 仓库为 `bitcapybara/tidemq`。runner 名称 `tidemq-dev-u126-8`，ID 21，标签 `self-hosted, Linux, X64, tidemq-dev`，运行在独立 runner VM 的 actions 用户下。CI 的 Docker 位于 runner VM，不挂载宿主 Docker / libvirt socket。observe VM 用 Docker Compose 运行观测后端，宿主 Docker 仅用于准备固定工具链 bundle。

新增且只新增一个容器 runner：`tidemq-ci-container-u126-8`，ID 23，专用标签 `tidemq-ci-container`，容器名 `tidemq-ci-runner`。它运行在同一 runner VM 中，复用同一 Linux 内核的 Cargo 下载缓存和文件锁。VM 扩为 8 vCPU / 16 GiB；原生服务限制 4 CPU / 6 GiB，容器限制 4 CPU / 8 GiB，预留 2 GiB 给 OS。容器只执行普通检查/测试，未挂载 Docker socket、kubeconfig、实验室 SSH key 或原生注册凭据，也不发布端口。配置、注册、重建及共享缓存约束见 [容器 runner 运维](container-runner/README.zh-CN.md)。

| 路径 | 内容 |
|---|---|
| `/srv/tidemq/actions-runner` | Actions runner 2.338.1 与 systemd 服务 |
| `/srv/tidemq/action-archives` | 两个 runner 共用的只读官方 Action 缓存；固定提交、SHA-256、离线安装及额度见[容器 runner 运维](container-runner/README.zh-CN.md) |
| `/srv/tidemq/work` | checkout、有限测试结果 |
| `/srv/tidemq/tools` | 验证 SHA 的 RustFS、Collector 测试二进制、kubectl、Helm、nextest、llvm-cov |
| `/srv/tidemq/rust` | Rust 1.99.1 工具链与 Cargo cache，`/opt/tidemq-rust` 软链接 |
| `/srv/tidemq/maintenance` | 自动清理的本地互斥锁；已安装脚本在 `/usr/local/lib/tidemq-lab` |
| `/srv/tidemq/cargo-target` | 可重建的 debug / coverage 编译缓存 |
| `/srv/tidemq/tmp` | 测试临时目录 |
| `/srv/tidemq/container-runner/{state,diag,work,target,tmp}` | 容器独立注册状态、诊断、checkout、编译缓存、临时目录；内部 work/target/tmp 映射为与原生相同的绝对路径 |
| `/home/actions/.config/tidemq-lab` | 私有 kubeconfig、受限 CI SSH key、telemetry 客户端证书与运行材料 |

Docker data-root 和 containerd 镜像存储也放在 runner 数据盘。`prepare-runner-image-store.sh` 在无运行中构建容器时迁移并通过 bind mount 持久化，避免根盘被镜像解包填满。

任务开始/完成钩子和每小时空闲定时器自动回收 checkout、超额缓存、停止的容器、未使用镜像和取消任务遗留资源。保留额度、所有权检查、互斥、安装与冷重建方法见 [Runner 资源清理](runner-maintenance.zh-CN.md)。原公共构建副本不再长期保存，服务身份和持久数据独立保留。

Linux 校验、集成测试、覆盖率与 Kubernetes / VM 故障均使用 self-hosted runner。macOS 保留 GitHub-hosted 平台校验。复杂步骤位于 `scripts/ci/`，workflow 只保留调度、checkout、调用脚本和上传结果。

当前 Linux Validate（静态检查、单元、默认 SDK、普通集成）、Faults（故障钩子、真实外部服务、K8s黑盒/VM故障）和Coverage顺序使用现有原生 `tidemq-dev` executor，避免长期容器runner的共享cgroup占用干扰功能测试。容器部署和真实容器限额负测保留，长期runner配置未改。两个测试job从开始即使用LLVM插桩，分别打包二进制/profile并上传，再接续下一job。coverage job校验同一源码/CI run/工具/工作区路径及每个输入的SHA256，再合并五阶段JUnit、两个job的原生profile和全部远程实例profile，导出LLVM JSON、LCOV、HTML、原始profile归档并比较基线，不重新编译或重跑Rust测试。K8s的10个角色实例和loadgen客户端均使用插桩二进制；measurement绑定源码、镜像、二进制哈希、角色/Pod/worker/imageID和行为断言；缺少任一实例则失败。公共常驻服务与手动普通验收仍使用release构建。资源边界、独立8GiB对照及最终CI验收限制见[测试指南](../../docs/testing.zh-CN.md)。

单元测试运行 `--lib --bins --examples`，主动移除外部服务环境变量。外部进程 / 网络依赖测试放在 `tests/`，由独立 integration 阶段运行；内建本地 mock 不要求预装外部服务。ignored 的真实 RustFS / Collector 测试在 Linux 上显式执行，缺少固定二进制会失败。

RF3 集群测试由 loadgen VM 运行真实 SDK，runner 仅控制和收集证据；覆盖发送/接收/ACK、延后读取、单 worker 断电、数据网断链、双 worker 丢失仲裁及原 Pending/Consumer 恢复、control-plane 故障、对象存储实例隔离、runner 连接消失后独立恢复计时器，以及三类观测入库查询。

按用户要求，性能 workflow、负载工具、比较器、配置、基线和专项报告已删除。GitHub 上默认分支残留的 Performance 工作流已设为 disabled_manually，防止旧入口误触发。本次只以有限 SDK 流量验收部署与持久正确性，不执行性能测试。

Rust workflow 使用 `tidemq-dev` concurrency group，不取消运行中的故障清理。故障/部署流程取得宿主 flock 租约；Validate普通测试与只读报告合并不占用故障租约。所有手动Cargo命令须经 `scripts/ci/with-cargo-cache.sh` 持有共享缓存锁；涉及共享实验室服务的手动测试和部署还必须经 `scripts/lab/with-lease.sh`。root运维使用同一 `/var/lock/tidemq-lab.lock` 的 `flock -n`。故障指令白名单只允许cp、worker、fixtures、loadgen，禁止操作storage、observe、runner；持续时间5–300秒，宿主先注册systemd恢复计时器再执行故障。

此仓库为公共仓库：fork PR 的单元测试走 GitHub-hosted runner；self-hosted jobs 不接受 fork 代码。GitHub outside contributor 审批策略设为 `all_external_contributors`，期望状态见 [github-policy.json](github-policy.json)。workflow 不能替代 GitHub 管理面的审批设置。

CI 的 VM 故障作用于共享的三个 worker，因此同时位于这些 worker 的 `tidemq-dev` 也会受故障影响；公共 RustFS 和观测 VM 保持独立。需要公共 MQ 连续服务时，应为 CI 增配独立 worker 集群。

## 7. 常用操作

宿主机上的配置副本：`/root/tidemq-lab-source`；状态：`/var/lib/tidemq-lab`。以下运维脚本在宿主 Linux Bash 运行；本地 macOS Bash 3.2 不支持 `coproc`，不要用它执行 runner 租约脚本。

```bash
ssh dev
cd /root/tidemq-lab-source
source scripts/lab/guest.sh
virsh list --all
lab_ssh tidemq-cp-a sudo kubectl --kubeconfig=/etc/kubernetes/admin.conf get nodes -o wide
lab_ssh tidemq-cp-a sudo kubectl --kubeconfig=/etc/kubernetes/admin.conf get pods -A
lab_ssh tidemq-observe sudo docker compose -f /opt/tidemq-lab/observe/compose.yaml ps
lab_ssh tidemq-storage sudo systemctl status tidemq-rustfs --no-pager
```

手动触发 GitHub 校验，并查看该次 revision 的结果：

```bash
gh workflow run rust.yml --repo bitcapybara/tidemq --ref feat/yyx/kvm-ci-lab-main
gh run list --repo bitcapybara/tidemq --workflow rust.yml --branch feat/yyx/kvm-ci-lab-main
gh run watch RUN_ID --repo bitcapybara/tidemq --exit-status
```

Runner 操作须以 actions 用户加载 `/etc/profile.d/tidemq-runner.sh`，设置 `TIDEMQ_LAB_HOME=/home/actions/.config/tidemq-lab`。部署接口：

该 profile 同时将 `TMPDIR` 指向 `/srv/tidemq/tmp`。手动 Cargo / Node 集成测试也应加载它，不能让大测试目录落在 runner 的小根盘 `/tmp`。

```bash
bash scripts/lab/with-lease.sh bash scripts/ci/lab.sh
# Operator deployment, using a fresh private output directory:
bash scripts/lab/build-image.sh PRIVATE_BUILD
bash scripts/lab/deploy-workload.sh tidemq-dev PRIVATE_BUILD \
  https://s3.lab.tidemq.test:18443 PRIVATE_OBJECT_CREDENTIALS PRIVATE_OUTPUT
```

`deploy-workload.sh` 会导入固定镜像、创建资源并等待全部角色 Ready。它要求新 namespace / 输出目录；重部署已有持久实例应先保存数据和身份材料，再按变更策略操作，不能直接删除公共 namespace 作为日常升级方式。

`build-image.sh` 现在同时从同一源码和前端锁文件构建 Console 静态资源，镜像内路径为 `/opt/tidemq/console`。打包需要支持的 Node.js 和 npm；runner 只有 Node 24 二进制而没有 npm 时，脚本从官方 registry 临时下载固定 npm 包，先核验完整 SHA512，再执行，不改变 runner 的全局安装。普通 Cargo 构建及 Rust 工具链镜像继续不依赖 Node/npm。全部静态文件（含 Vite manifest 与源码树哈希）纳入 runtime 校验清单和镜像标签；`source-patch-sha256.txt` 还组合前端完整源码树哈希，覆盖尚未提交的前端文件。Console 以 `role = "console"` 独立部署，提供登录、授权/Topic/消息管理及消费和组件状态；HTTPS、共享密钥和探针配置见 [Console 部署与验证](../console/README.zh-CN.md)。现有十角色 workload 不自动新增 Console 实例，lab 主机的 loopback 观测端点也不会因此变为 Pod 可访问；本文件的历史验收不代表这次 Console 版本已经在平台验收。

角色 StatefulSet 使用 `OnDelete`。变更配置后，须在独占租约下逐个删除对应 Pod，再等待新 Pod 创建且 Ready 后处理下一个；同一 metadata/storage quorum 同时最多停一个副本。`kubectl rollout restart/status` 不能代替该策略下的实际 Pod 重建。

公共镜像的源码、manifest/config digest、二进制身份和验收结果保存在 [合并验收](evidence/2026-10-25/README.zh-CN.md) 中；镜像 tar 和旧构建目录可清理，由固定源码及工具重建。当前三个 worker 的运行镜像与独立公共标签保留。升级时仅采用 CI 通过且 Rust 补丁为空的构建，先导入全部 worker 并验证 digest，再逐个更新角色；资源身份、卷绑定与私有材料必须沿用已有备份。

公共服务私密材料在宿主 `/var/lib/tidemq-lab/private/credentials/`：`kubernetes.toml`、`devices.toml`、`grafana-password`、`metrics-password`。TLS 客户端材料在 `private/tls/`；发给外部设备时只提供对应客户端证书、私钥和 CA，不提供 CA 签名私钥。QUIC 客户端只取该 workload 的 `ca.pem` 和 `access.token`，不取节点私钥。

常驻 MQ 的完整身份备份在宿主 `/var/lib/tidemq-lab/private/workloads/tidemq-dev/`，客户端只需其中 `workload/material/ca.pem` 与 `access.token`。loadgen VM 已有 `/srv/tidemq/public-dev/{tidemq,deploy_validation,ca.pem,access.token}`。例如，以私有客户端目录保存这两个文件后，在本机执行：

```bash
cargo build --locked --bin tidemq --example deploy_validation
target/debug/examples/deploy_validation --endpoint 192.168.126.8:17443 \
  --ca PRIVATE_CLIENT_DIRECTORY/ca.pem \
  --token-file PRIVATE_CLIENT_DIRECTORY/access.token \
  --messages 64 --output-dir NEW_PRIVATE_RESULT_DIRECTORY
```

结果目录必须尚不存在，父目录由使用者创建。升级修复版本后，loadgen VM 与这台 Mac 的局域网/VPN 入口均完成 64 条消息的发送、接收、持久 ACK 和客户端关闭，记录已并入 [部署验收](evidence/2026-10-25/acceptance.json)。最新 Mac 客户端按 `87b3442` 构建，又完成 16 条消息及 9 类 API 契约验证；公共 10 个实例的指标、日志和 trace 可经外部入口查询，详见 [最新验收](evidence/runner-maintenance.json)。公共镜像仍为 `c386434`；它与 `87b3442` 的生产源码 `src/`、`crates/`、`sdk/rust/src/` 无差异。

服务入口 TLS 叶证书有效期 365 天，本批 storage 证书在 2027-10-25 UTC 到期。工作负载 QUIC 节点叶证书有效期 30 天，本批常驻实例在 **2026-11-24 16:01:30 UTC** 到期；CI 每次新建独立证书。当前未部署证书自动续期，常驻实例持续使用前需安排证书轮换；不得通过重新生成 cluster ID、token 或删除数据来代替轮换。可在宿主用 `openssl x509 -in /var/lib/tidemq-lab/private/workloads/tidemq-dev/workload/material/node-11.pem -noout -enddate` 检查日期。

恢复 / 初始化脚本的顺序为：`provision-host.sh` → `prepare-nodes.sh` → `bootstrap-kubernetes.sh` → `install-kubernetes-addons.sh` → `prepare-infra.sh` → `create-certificates.sh` → `deploy-agents.sh` → `deploy-backends.sh` → `prepare-fixture-host.sh` → `install-host-control.sh` → `prepare-runner-assets.sh` → `deploy-runner.sh` → `register-runner.sh`。先阅读各脚本参数；不要为修复单个组件重跑整套初始化。首次准备工具先执行 `download-tools.sh`。注册 token 是短期凭据，禁止写进仓库。

`prepare-runner-assets.sh` 使用 inventory 中固定 digest 的官方 Ubuntu / Rust 镜像，复用 `deploy/docker/Dockerfile.builder` 生成工具链 bundle 和 runtime base tar，并记录镜像 ID 和 SHA-256。`deploy-runner.sh runtime` 更新 runner 工具及清理机制，`maintenance` 只安装清理机制，`access` 只更新受限访问材料。不要向其他设备分发整个 bundle 或 runner 私有目录。

## 8. 旧环境清理

用户授权删除旧服务、数据卷、源码、配置和历史测试报告。已删除旧 TDengine / IDMP / Kafka / Explorer 等宿主 Docker 资源和旧测试目录；旧应用的系统级 service/target、enablement symlink、stressctl、kache 用户服务和 TDengine 本机残留也纳入清理。清理脚本使用明确允许名单，不删除 OS、SSH、libvirt、新实验室及其恢复单元。

另已删除 4 个部署前遗留的 bash/rustc core dump，约 3.9 GiB。用户明确确认保留 YuXiangagent 的 3 条 root 定时任务及相关文件；它们是本次清理的保留项。

一次性的 `cleanup-legacy-host.sh` 和 `cleanup-legacy-units.sh` 已从当前目录删除，实现和原始执行证据仍在 [固定 Git 归档](../../docs/evidence/README.zh-CN.md) 中；历史授权不适用于后来创建的其他项目。日常 CI 只调用上述范围明确的资源回收机制。

Git 只跟踪非敏感资源配置、公开 CA、脚本和脱敏验证证据。VM 磁盘、Docker tar、kubeconfig、Secret YAML/JSON、token、客户端私钥、CA 私钥、bucket 密钥、runner 配置凭据及其 bundle 不得提交。
