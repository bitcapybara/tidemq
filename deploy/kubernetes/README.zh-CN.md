# Kubernetes 分离 RF3 部署与运维

[English](../../docs/guide/deployment.md#containers-and-kubernetes)

本目录部署真实分离角色 RF3：3 个 Metadata、3 个 Storage、2 个 Broker 和 2 个 Gateway。Metadata Group 1 的 voter 为 11/12/13，Data Group 2 的 voter 为 21/22/23；Gateway 为 41/42，Broker 为 31/32。Combined 是单进程本地执行版本，不参与该拓扑，也不能作为 peer 或 voter。

独立 Console 使用同一二进制和镜像，以原生 HTTPS 提供页面和 API；见[Console 部署](../console/README.zh-CN.md)。它无 MQ 数据卷和平台写权限。

## 准备与创建

需要 Bash、OpenSSL、kubectl、可动态分配 RWO PVC 的 StorageClass，以及包含同版本二进制与静态资源的固定镜像。构建方法见[Docker](../docker/README.zh-CN.md)。生成器要求显式 context、新 namespace 和新私有输出目录，会在所选集群直接创建资源，不替换已有部署。

```sh
bash deploy/kubernetes/selfcheck.sh
bash deploy/kubernetes/prepare.sh CONTEXT tidemq-functional IMMUTABLE_IMAGE STORAGE_CLASS /absolute/new-private-directory
kubectl --context CONTEXT -n tidemq-functional wait --for=condition=Ready pods --all --timeout=180s
```

每个角色按 `node_id % 3` 对应 `zone-0/1/2` 调度；Metadata 和 Storage 各覆盖三个 zone。将 Kubernetes 节点的 `topology.kubernetes.io/zone` 标签对应到真实故障域。仓库 [kind.yaml](kind.yaml) 提供三个带标签的 worker，仅用于验证调度、PVC 和 Pod 重建，不能证明物理跨主机或跨 AZ 容灾。

## 服务、证书和权限

| 对象 | 配置与边界 |
| --- | --- |
| `mq` Service | UDP 7443，只选择 Ready Gateway Pod；内部角色不作为 SDK 入口 |
| 每节点 Service | UDP 7444、稳定 ClusterIP，允许未 Ready 节点通信；完整 peer 清单包含十个真实分离身份 |
| 管理 | Pod 内 `127.0.0.1:9419`；exec readiness 请求 `/readyz`，不暴露管理 Service |
| TLS | 节点证书包含本节点名称与实际 Service IP；Gateway 证书同时覆盖 mq 名称与 IP |
| Secret | 每节点只含自己的叶私钥、必要公开证书与 CA；只有 Metadata 初次初始化需要 bootstrap token |
| 私有文件 | init container 复制至有界内存卷，目录0700、文件0600，owner 10001；CA签名私钥仅保留在本地输出目录 |

SDK 在集群内访问 `mq` 的 ClusterIP:7443。外部接入须提供受控的 UDP 四层入口，并重新签发覆盖该入口的 SAN；TCP `kubectl port-forward` 不支持验证 QUIC。Service IP 已写入 peer 地址和证书，不能随意删除重建。

开发证书有效七天。生产使用组织签发与轮换流程，不将生成的 CA 签名私钥、bootstrap token 或输出目录提交到仓库。Unix 私钥权限和 UID 应在目标平台实际验证。

## 卷与可用性

六个 Metadata/Storage 实例各有独立 8Gi RWO PVC，保留策略为 Retain；Broker/Gateway 使用有界128Mi ephemeral data volume。StatefulSet 使用 OnDelete，避免配置修改时同时重建所有副本。

四个角色各有独立 PDB：Metadata 和 Storage 的 `minAvailable=2`，Broker 和 Gateway 的 `minAvailable=1`。单一总 Pod 数不能保证两个 Raft Group 的 quorum。PDB 只约束部分自愿中断，不能取代持久退休证明，也不能防止所有直接删除或强杀。示例不提供 HPA、自动删除卷或关停钩子强制替换。

## 更换和退休节点

使用[扩缩容手册](../../docs/operations/scaling.zh-CN.md)中的在线管理协议。新增候选必须是对应的 `metadata` 或 `storage` 角色，使用新的节点 ID、独立 Service、证书与持久卷；不可复用已退休身份。

Storage 候选先配置该组的当前真实 voters，并把自身放入 `standby`，保持 `bootstrap=false`；先登记并启动真实非投票副本，再执行数据副本替换。Metadata 候选按 Metadata 替换流程准备。两种角色分别完成各自的 learner/joint/stable 和持久快照检查，不能一次用同一个 Combined 节点替换两种职责。

先停止原节点的新放置，按真实阻塞项分别处理副本/owner，再执行 drain、查询 drain-status，最后 retire。仅当原操作已完成且 `retired=true`、实际成员和所有恢复依赖已确认，才将对应 StatefulSet 缩为零。中断或 Unknown 沿原持久计划恢复，不通过修改 bootstrap voters、清空 WAL 或重新生成集群解决。保留原 PVC 和匹配二进制用于恢复与审计。

## 验证与资源边界

本目录自检使用伪 kubectl 验证生成内容、真实开发证书、十角色成员、六个持久卷、四个 PDB 和禁止多节点 Combined；设置 `TIDEMQ_BIN` 时还逐份执行纯配置验证。它不启动 Kubernetes 或验证业务链路。

目标平台须分别保留源码/镜像/二进制身份，确认所有 Pod readiness、真实 quorum、SDK 收发与持久 ACK、拒绝授权、重启恢复、成员替换和清理结果。可在具有可写临时目录的 Pod 内使用镜像的 `deploy_validation`，连接真实 Gateway；管理员 token 从 Metadata 的受保护文件或专用运维凭据提供，不把 token 分发给所有角色。

本例未配置 S3。需要归档与冷读时，只在 Storage 配置 `[storage.objects]`，所有副本访问同一个受控对象存储；Pod 本地目录不是跨节点共享对象存储。示例资源值仅用于功能部署，不构成容量承诺。历史 RF3 Combined 证据保留原样，不能作为当前分离拓扑的通过证据。

整体销毁仅用于专属实验环境。先保存恢复材料与验证证据，再由实验所有者按实际环境删除资源；脚本不提供自动强删路径。
