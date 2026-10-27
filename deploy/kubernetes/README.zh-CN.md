# Kubernetes 人工部署与退休

Console 使用同一二进制和镜像，以 `role = "console"` 启动独立无状态 Deployment，提供登录、授权/Topic/消息管理及消费和组件状态；配置、HTTPS Ingress、跨副本会话密钥和固定探针示例见 [Console 部署与验证](../console/README.zh-CN.md)。Console 不加入 MQ 注册表或 Raft 成员，不需要本文件 MQ StatefulSet 的 PVC 或平台写权限；配置生成和 HTTP 测试不代替目标集群的真实部署验收。

这是使用稳定节点身份和 PVC 的 RF3 Combined 示例，包含三个活动实例和一个尚未启动的替换实例。节点 1/2/3 分别要求 Kubernetes 节点标签 `topology.kubernetes.io/zone=zone-1/zone-2/zone-0`，候选节点 4 位于 `zone-0`。真实部署应将标签映射到实际故障域；同机 Kind 的标签仅验证调度规则。分角色部署使用相同管理协议，不能通过增加 Metadata 副本提高数据写入容量。

需要 Bash、OpenSSL、kubectl、jq、可动态分配 RWO PVC 的 StorageClass，以及按源码提交构建的 TideMQ 镜像。镜像沿用 [Docker 构建](../docker/README.zh-CN.md)，使用固定 tag 或 digest；不能使用 `latest`。脚本要求显式 context，并且拒绝已有 namespace 和输出目录。它会在所选集群创建资源，不替换已有部署。

```bash
bash deploy/kubernetes/selfcheck.sh
bash deploy/kubernetes/prepare.sh CONTEXT tidemq-functional IMAGE STORAGE_CLASS /absolute/new-private-directory
kubectl --context CONTEXT -n tidemq-functional wait --for=create --for=condition=Ready pod/node-1-0 --timeout=180s
kubectl --context CONTEXT -n tidemq-functional wait --for=create --for=condition=Ready pod/node-2-0 --timeout=180s
kubectl --context CONTEXT -n tidemq-functional wait --for=create --for=condition=Ready pod/node-3-0 --timeout=180s
```

脚本创建每节点的固定内部 UDP Service，允许未 Ready 的 learner 通信；`mq` UDP Service 仅选择 Ready 实例。证书同时包含实际 Service IP 与节点名称，Service 不可随意删除重建。SDK 从集群内连接 `mq` 的 ClusterIP 和 7443 端口。外部访问需要支持 UDP 的受控 LoadBalancer/网关及其证书 SAN，不能用仅支持 TCP 的 `kubectl port-forward` 验证 QUIC。[Service 协议](https://kubernetes.io/docs/concepts/services-networking/service/)与 [Kind 使用说明](https://kind.sigs.k8s.io/docs/user/quick-start/)提供平台要求。

管理 HTTP 仍绑定 Pod 内 loopback；readiness 使用 exec probe。Secret 的根所有权与服务器的严格私钥权限校验通过 init container 衔接：复制到有限内存卷，设为 UID 10001、目录 0700、文件 0600。只将叶私钥和必要公开材料放入各节点 Secret，CA 私钥留在本地私有输出目录。候选节点不带 bootstrap token；首次初始化后凭据及撤销记录由 Metadata Raft 持久化。示例证书有效期七天，生产应使用组织的证书签发与轮换流程。

PVC 采用 Retain，StatefulSet 采用 OnDelete；PDB 保持至少三个 Ready 实例。PDB 和 readiness 都不能替代 TideMQ 的持久退休证明，也不会阻止所有直接删除或强杀。示例不配置 HPA、不自动删除卷，不将关停钩子当作必能完成的成员替换流程。

## 新增替换实例

先从一个活动 Pod 使用管理 CLI 注册候选身份，再启动它。下面的 pin 从公开 DER 计算，内部 IP 取生成目录的 `material/address-4`。管理命令的详细结果/Unknown 处理见[扩缩容手册](../../docs/operations/scaling.zh-CN.md)。

候选配置将节点 4 列入数据组 2 的 `standby`，因此预先打开该组的非投票副本。它不进入初始 voter 集合，不参与 bootstrap，不持有 root token。节点进程 Ready 不能代替目标已实际托管待替换组的检查；缺少 standby 配置时，成员替换保持未决并报告目标未准备，不降低 RF 或跳过 learner 复制。

```bash
kubectl --context CONTEXT -n tidemq-functional exec node-1-0 -- \
  /usr/local/bin/tidemq admin --endpoint 127.0.0.1:7443 --ca /private/ca.pem --token-file /private/bootstrap.token \
  register-node --node-id 4 --role combined --zone zone-0 --internal-endpoint NODE_4_SERVICE_IP:7444 \
  --certificate-name node-4.local --certificate-der /private/node-4.der
kubectl --context CONTEXT -n tidemq-functional scale statefulset/node-4 --replicas=1
kubectl --context CONTEXT -n tidemq-functional wait --for=create --for=condition=Ready pod/node-4-0 --timeout=180s
```

## 排空原节点

在仍活动的 Pod 中以同一组 CLI 连接参数依次执行：

```text
set-schedulable --node-id 3 --enabled false
replace-data-replica --group-id 2 --removed 3 --added 4
replace-metadata-replica --removed 3 --added 4
drain-node --node-id 3
drain-status --node-id 3
retire-node --node-id 3
```

先检查组 2 的实际 stable voter 和 Metadata voter 都为 1/2/4，反复查询 drain blocker。仅在 `retire-node` 返回已完成且 `retired=true` 后，才能将 `statefulset/node-3` 缩为零。退休身份不可重用；该节点的保留 PVC 不用于启动一个新身份。中断或 Unknown 沿原持久计划继续，不通过修改 TOML 中的 bootstrap voter、清空目录或重建集群恢复。

节点 3 的数据组已通过原生 learner/joint/stable 和 checkpoint 替换，不要求跨组移动历史。如果要让一个数据组彻底停止服务，应逐 shard 使用 AuthorityTransfer，等待目标 checkpoint、读取/恢复和 GC 依赖完成；仅仅 Merge 或移除 Broker owner 不允许删除数据组。

## 验证与存储范围

在活动 Pod 内可用镜像携带的 `deploy_validation` 执行少量真实 SDK 收发，`--output-dir` 使用 PVC 下新建目录。保留报告中的消息 ID、持久 ACK、SDK close 以及镜像/二进制身份；在替换前、退休后和 Pod 重启后分别执行。

首次使用验证程序前创建 `validation/default` Namespace。以下仅在全新集群执行一次，后续验证复用其持久记录：

```bash
admin() {
  kubectl --context CONTEXT -n tidemq-functional exec node-1-0 -- \
    /usr/local/bin/tidemq admin --endpoint 127.0.0.1:7443 --ca /private/ca.pem --token-file /private/bootstrap.token "$@"
}
tenant_id=$(admin create-tenant validation | jq -er '.result.id')
admin create-namespace "$tenant_id" default
```

本示例未配置 S3，功能验证依赖持久热数据和 Raft 副本。生产需在 `[storage.objects]` 配置所有副本可访问的对象存储、凭据和保留策略，不能把每 Pod 本地目录当作跨节点共享对象存储。S3 冷读、不可变历史和 GC 另由真实对象存储集成测试覆盖。示例资源数值是功能验证边界，未经压测或参数校准。

Kind 宿主机须为各 worker 留足 inotify 实例额度；kube-proxy 报 `fsnotify watcher init: too many open files` 时，先按 [Kind 已知问题](https://kind.sigs.k8s.io/docs/user/known-issues/#pod-errors-due-to-too-many-open-files)检查宿主限制和 kube-proxy 就绪，再检查 UDP Service，不能把缺少代理规则当作应用故障。临时实验调整应记录原值，结束后恢复。

Kind 适合验证多个 Kubernetes worker、PVC、Pod 重建和管理流程；不构成物理跨节点/AZ、外部 UDP LB 或性能验收。在用集群按上述协议退休单个节点；整体销毁专属功能实验集群会删除测试数据，须先保留所需证据，再由实验所有者显式删除 namespace 和专属集群。脚本不提供自动强删路径。

仓库的 `kind.yaml` 提供三个带 zone 标签的 worker。先按 Kind 官方发布页校验工具及 node image，再用 `kind create cluster --name tidemq-functional --config deploy/kubernetes/kind.yaml --kubeconfig /absolute/private/kubeconfig` 创建专属实验集群，加载已构建镜像，并设置该 `KUBECONFIG`。随后以 `kind-tidemq-functional` context、`standard` StorageClass 调用本目录生成器；不要将已有业务集群当作实验目标。
