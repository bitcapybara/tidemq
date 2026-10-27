# Console 部署与验证

Console 使用同一 `tidemq` 二进制和镜像，以 `role = "console"` 启动独立无状态进程。镜像内的前端静态资源由 Rust HTTP 服务提供，浏览器经统一 HTTPS 入口访问，运行时不需要 Node.js。

当前实现覆盖个人 token 登录、按作用域管理用户/应用身份、凭据和授权、Topic 创建/删除、手动发送消息，以及消费者成员/租约/消费进度、生产者登记/租约、节点与组件状态。消息以 Topic → Shard → offset 有界浏览，支持消息 ID、key 和属性筛选。消息正文需要独立的内容检查权限；Produce、Consume 和普通 Inspect 不自动授予正文访问。租户查看本租户授权数据，跨租户集群观测与外部详情链接限定运维权限。

业务写入结果和恢复记录保存在 TideMQ 核心，Console 副本之间共享会话密钥即可切换，不依赖本地会话、持久卷或粘性路由。结果未知时查询原操作，不换一个操作 ID 自动重发；凭据明文仅在成功创建时展示一次，后续恢复只返回结果及“明文不可恢复”的事实。结构化日志和现有 Loki 链路提供操作审计，采集故障期间不承诺每条审计日志无损。

样例面向隔离开发环境和与本代码配套的新集群。本次核心授权增加持久字段，尚未验证已有数据格式或混合版本滚动升级；不要据此直接升级现有持久集群。这里不提供迁移层，也不会更新现有 lab 集群。配置生成、单元测试或本地 HTTP 联调均不等于实际 Docker/Kubernetes 环境已验收。

Console 使用现有集群入口，不加入 MQ 节点、peer 或 voter，不挂载 MQ 数据目录。扩缩容、排空、调度和其他集群运维操作仍使用现有工具。本目录只生成新的本地文件，`prepare.mjs` 不调用 Docker、kubectl 或修改已有集群。

## 镜像和配置

按 [Docker 构建](../docker/README.zh-CN.md) 打包；runtime context 必须同时包含 `bin/` 与由 `scripts/build-console.sh` 新生成的 `console/`。Lab 的 `scripts/lab/build-image.sh` 已同步执行此步骤，并将全部静态资源及源码树哈希纳入镜像身份。使用同一冻结源码构建二进制和静态资源，普通 Cargo 构建不需要 Node.js。

运行配置使用 `tidemq serve --config /config/console.toml`，静态路径为 `/opt/tidemq/console`。公共 HTTP 监听 8080，仅经已有 HTTPS 入口访问；管理监听固定为进程内 `127.0.0.1:9420`，不发布 Service 或宿主端口。

会话密钥由部署人员提供：恰好 32 个随机原始字节，所有 Console 实例使用同一个文件内容。Unix 允许 owner 读写和可选的只读服务组，禁止其他用户及组写入/执行；常用模式为 `0400/0600/0440/0640`。服务 UID/GID 为 `10001:10001`，密钥必须能由该用户读取。TLS 信任根仅包含集群 CA；部署配置及挂载 Secret 不包含节点私钥、CA 签名私钥或个人 token。用户仅在 HTTPS 登录页面输入自己的 token，凭据由服务端按用户身份用于 SDK 请求，不存入浏览器 localStorage。

### 无状态会话与密钥轮换

会话在 `__Host-tidemq-session` 中加密保存，带 `Secure`、`HttpOnly`、`Path=/` 和 `SameSite=Lax`。默认最长 30 分钟，并受核心返回的真实凭据到期时间限制；读取请求不会自动延长会话。每次业务请求仍由核心按该用户的 token 核权。写入还要求部署配置中的精确 HTTPS Origin 和会话 CSRF token，因此代理必须保留浏览器的 Origin，公开域名应与 `publicOrigin` 一致。

退出登录清除该浏览器会话；需要撤销凭据时使用凭据管理。所有 Console 副本只连接其配置中的同一个固定集群地址；集群地址、证书名、公开域名或会话密钥发生变更时应重新核对部署配置。

所有副本必须使用相同的当前密钥和最多两个旧验证密钥。Kubernetes settings 的 `previousSessionKeyNames` 引用同一个 `privateSecret` 中额外的文件名；Docker settings 的 `previousSessionKeyFiles` 引用已有的绝对文件路径，生成器以只读方式挂到固定的 `/private/session-previous-N.key`。旧密钥和当前密钥都必须是不同的 32 字节随机内容，不能用同一内容的副本充数。

轮换分三步，每步更新配置并按既有部署流程重启全部 Console 副本：先让所有副本保留当前密钥 A，并将新密钥 B 加入旧验证列表；再将 B 设为当前，A 放入旧验证列表；最后等待最长会话期限和仍有效的页面游标过期后，移除 A。这样部署期间任意副本都能验证另一副本签发的会话。配置和密钥仅在进程启动时加载；紧急移除旧密钥会让仍依赖它的会话失效并要求重新登录。不要在多个无关集群之间共享会话密钥。

### 固定的只读观测源

settings 可选 `observability`，同时用于 Compose 和 Kubernetes；不填写时不会探测外部依赖。示例中的地址必须替换成 Console 网络可以访问的实际只读健康入口和用户浏览器可以访问的 HTTPS 详情页：

```json
{
  "observability": {
    "components": [
      {"id": "prometheus", "name": "Prometheus", "probeUrl": "http://prometheus:9090/-/ready", "detailUrl": "https://metrics.example.test/graph"},
      {"id": "object-storage", "name": "RustFS object storage", "probeUrl": "http://rustfs:9000/health/ready"}
    ],
    "alertsUrl": "http://prometheus:9090/api/v1/alerts",
    "alertsDetailUrl": "https://metrics.example.test/alerts"
  }
}
```

完整可复制的 [observability.example.json](observability.example.json) 覆盖以下实际组件路径。把其中的 `observability` 合并到平台 settings，并将示例 DNS 替换为真实固定入口；需要 Console 导出 trace 时同时合并并替换其中的 `tracingEndpoint`。agent 按实例各列一项，最多 32 个组件，足以覆盖当前 lab 的 12 个 agent 和观测后端。核心角色和 Console ID 保留，不可覆盖。

| 组件 | 只读健康入口 | 依据 |
|---|---|---|
| RustFS 1.0.3 | `:9000/health/ready` | [fixture 实际等待路径](../../scripts/lab/fixture-runtime.sh) |
| Central OTel Collector 0.163.2 | `:13134/` | [已有 health_check 配置](../lab/observe/collector.yaml) |
| 各主机 OTel agent 0.163.2 | `:13133/` | [已有 agent 配置](../lab/agent.yaml)、[安装后实际校验](../../scripts/lab/install-agent.sh) |
| Prometheus 3.15.1 | `:9090/-/ready` | [Prometheus 管理接口](https://prometheus.io/docs/prometheus/latest/management_api/) |
| Loki 3.7.9 | `:3100/ready` | [Loki readiness](https://grafana.com/docs/loki/latest/reference/loki-http-api/#readiness-probe) |
| Tempo 3.1.1 | `:3200/ready` | [Tempo readiness](https://grafana.com/docs/tempo/latest/api_docs/#readiness-probe) |
| Grafana 13.2.4 | `:3000/api/health` | [Grafana health](https://grafana.com/docs/grafana/latest/developer-resources/api-reference/http-api/api-legacy/other/#returns-health-information-about-grafana) |
| Alertmanager 0.34.1 | `:9093/-/ready` | [Alertmanager readiness](https://prometheus.io/docs/alerting/latest/management_api/) |

当前 lab 的这些监听多数只绑定各自进程所在主机的 loopback；独立 Console 容器中的 `127.0.0.1` 无法访问它们。示例 DNS 不代表现有 lab 已提供这些地址。只有在部署方已有受限只读路由、同网络 namespace 或受控健康代理时才能填写可达入口；本次配置生成器不会开放管理监听、改防火墙或把所有主机管理端口暴露到网络。未接入的实例保留未知状态，不把一台 agent 的成功当成全体 agent 健康。

探测没有个人 token、Cookie、任意 URL 参数或重定向；禁止配置带凭据的 URL。每次探测最多两秒、响应体最多 64 KiB，共享并发最多四个，等待容量最多两秒。内部 HTTP 入口应由网络策略限制访问，HTTPS 使用系统受信任证书；这里不绕过证书验证，也不为探测配置业务凭据。

HTTP 成功仅证明所配置的健康入口返回成功。核心 MQ 组件同时读取有界注册表和受鉴权的节点探测：区分注册、存活、真实 readiness、采样时间、退役、进程 instance/zone/version，以及逐组 leader/voters/线性 quorum 证明。只接受领导者本人返回的 quorum 证明，follower 的未知不会被当成 quorum 失败；缺少 fresh readiness 或 leader 证明时保持未知。存储组热数据使用领导者样本，不累加同组副本。日志丢弃和 sink 错误计数原样保留用于排查，不新增阈值告警规则，也不推测期望副本数。Console 只报告当前响应进程的 readiness，不猜测其他副本数。

未配置、超时、权限拒绝、响应超限或不支持的返回格式都会显示未知或部分结果；告警源失败不会被当成“零告警”。告警仅支持 Prometheus `/api/v1/alerts` 的 firing/pending，最多 128 条，详情跳转只使用配置中的链接。既有告警规则仍来自 `deploy/observability/alerts/`，本目录不复制维护另一套阈值。

### Console trace 导出

Compose 和 Kubernetes settings 均支持可选的顶层 `tracingEndpoint`，用于把 Console 自身及其 SDK 调用的 trace 发往已有 OTel Collector。未填写时不启用导出。两个平台 JSON 示例中的 `192.0.2.10` 是文档地址，必须替换为 Console 网络中真实可达的采集入口；不需要导出时删除该字段：

```json
{"tracingEndpoint": "https://192.0.2.10:4318/v1/traces"}
```

端点规则沿用 [TracingConfig](../../src/observability/tracing/mod.rs)：最多 512 字节，host 必须为 IPv4/IPv6 字面地址，路径固定为 `/v1/traces`，禁止凭据、query 和 fragment。远端使用 HTTPS 和系统信任的证书；HTTP 仅允许 `127.0.0.0/8` 或 `::1`。探测用的 DNS 健康地址不能直接作为 trace 导出地址。容器中的 loopback 仍属于 Console 自身的网络 namespace，只有已有同网络采集组件时才适用；生成器不会开放采集端口或创建新代理。

生成的 `[tracing]` 使用核心的默认有界预算：本地采样参数 `10/1000`，每秒最多 128 spans，队列最多 256，批量最多 32，导出间隔和超时均为 500 ms。导出失败遵循现有有界丢弃和诊断机制；个人 token、消息正文和搜索参数不作为配置或导出字段。部署 settings 只接收固定端点，不接收任意请求 headers、查询语言或转发目标。

## Kubernetes

示例需要现有 namespace、集群 Gateway、NGINX Ingress Controller，以及组织提供的 HTTPS 证书。镜像使用完整 digest；已由 lab 生成的 `localhost/tidemq-lab:<content-hash>` 也可用于已导入镜像的实验环境。先复制并修改 [kubernetes.example.json](kubernetes.example.json)：镜像、HTTPS origin、固定集群地址、证书匹配的 server name、namespace、两个 Secret 名及现有 NGINX IngressClass。

共享 Secret `privateSecret` 必须预先包含 `ca.pem` 和 `session.key`；`tlsSecret` 为 HTTPS 入口的 `kubernetes.io/tls` Secret。下列命令是操作者步骤，生成器不会执行。所有路径应来自本次部署的私有目录，TLS 证书应匹配公开域名：

```sh
kubectl --context CONTEXT -n NAMESPACE create secret generic tidemq-console-private \
  --from-file=ca.pem=/absolute/private/ca.pem \
  --from-file=session.key=/absolute/private/session.key
kubectl --context CONTEXT -n NAMESPACE create secret tls tidemq-console-tls \
  --cert=/absolute/private/tls.crt --key=/absolute/private/tls.key
node deploy/console/prepare.mjs kubernetes /absolute/settings.json /absolute/new-console-kubernetes
kubectl --context CONTEXT -n NAMESPACE apply --dry-run=server -f /absolute/new-console-kubernetes/resources.json
kubectl --context CONTEXT -n NAMESPACE apply -f /absolute/new-console-kubernetes/resources.json
```

生成内容包括 ConfigMap、两个副本的无状态 Deployment、ClusterIP Service 和 TLS Ingress。非 root、只读根文件系统、丢弃 capabilities、禁止提权；没有 PVC、平台访问 token、RoleBinding 或平台写权限。Secret 采用 `0440`、`fsGroup: 10001`，避免 root 所有的 `0400` 文件让非 root 用户无法启动。[Kubernetes 安全上下文](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/)说明组访问语义。

startup/readiness/liveness 均通过容器内 exec curl 检查 loopback 管理监听；不会让 kubelet 从 Pod 外访问 loopback。readiness 只表示本地配置、密钥和静态资源可用，上游 MQ/DNS 不可达仍保留页面和本地健康入口；需要核心重新核权或读取的数据会明确返回上游不可用，不伪造空列表或健康状态。Ingress 仅导出 8080，NGINX 注解要求 HTTPS 重定向；组织采用其他 Controller 时应调整其 HTTPS 策略并重新验证。[Ingress TLS](https://kubernetes.io/docs/concepts/services-networking/ingress/#tls)的证书和域名由实际平台负责。

## Docker Compose

示例创建独立的 Console 服务，通过宿主 loopback 端口连接既有 NGINX HTTPS 入口。Console 的容器网络必须能够解析并访问所填 Gateway 地址；已有隔离验证拓扑的 `127.0.0.1` 仅属于其共享网络 namespace，不能直接填入独立 Console 容器。

复制并修改 [docker.example.json](docker.example.json)，填写实际镜像、HTTPS origin、可从 Console 网络访问的 Gateway DNS/IP、服务端证书名、可用宿主端口和现有 CA/会话密钥绝对路径。文件必须由 UID 10001 或只读组 10001 可读；生成配置不包含密钥内容。输出目录必须尚不存在：

```sh
node deploy/console/prepare.mjs docker /absolute/settings.json /absolute/new-console-compose
docker compose --project-name tidemq-console -f /absolute/new-console-compose/compose.json config --quiet
docker compose --project-name tidemq-console -f /absolute/new-console-compose/compose.json up -d
```

生成的 `nginx.conf` 沿用现有宿主反向代理模式，转发 Host、声明 HTTPS，并关闭包含 URL 的 access log。操作者将证书/私钥路径改为现有 TLS 材料，纳入自己的 NGINX 配置，先 `nginx -t` 再按既有流程 reload；生成器不安装或重载宿主服务。无需新增代理镜像。

Console 仅发布 `127.0.0.1:<hostPort>:8080`，不将明文入口绑定全部网卡。进程使用 `10001:10001`、只读根文件系统、有限 CPU/内存/PID、只读文件挂载，不挂载持久卷或 Docker socket。当前样例不配置实例亲和，也不把连接池或本地缓存作为恢复依据。不要跨多个无关集群复用同一个 Console 配置。

仓库现有 RF1/RF3 隔离 Compose 使用 `network` holder 共享 loopback。若需要同项目交付，在**新建项目首次启动前**给 Docker settings 添加 `"networkService": "network"`，并将 `clusterAddress` 设为相应的 `127.0.0.1:7410`（RF1）或 `127.0.0.1:7810`（RF3），`clusterServerName` 选择所用 Gateway 证书包含的名称。生成文件此时是 overlay：Console 使用 `network_mode: service:network`，宿主 loopback 端口由 holder 发布。先按原部署文档生成新的 `STATE`，再将原 Compose 放在首位合并：

该 overlay 适用于仓库 RF1/RF3 基准：holder 已显式声明 `networks: [private]`，Compose 合并后保留该 internal 网络，并追加独立的 `console-ingress` 非 internal bridge，使 Docker 能实际发布 `127.0.0.1` 上的页面端口；Docker 29 的纯 internal 网络不会落实该端口发布。不要假设 overlay 会保留基准的隐式 `default` 网络；其他部署须先在基准中显式声明 holder 的网络，再首次部署。此时整个共享网络 namespace 允许 egress，不能再按只有 internal 网络的隔离边界描述。MQ 和管理监听仍绑定各自原有的 loopback，只新增 Console 的页面端口，不发布 MQ 管理端口或挂载 Docker socket。

```sh
docker compose --project-name NEW_PROJECT --env-file /absolute/new-state/project.env \
  -f /absolute/new-state/compose.yaml -f /absolute/new-console-overlay/compose.json config --quiet
```

后续启动同样始终使用这两个文件和同一个项目名。不要给已运行项目直接套用此 overlay；新增 holder 端口可能重建网络 namespace，影响共享它的 MQ 容器。没有 `networkService` 时生成独立 Console 服务，不修改 holder 或原 MQ 配置。

## 检查与实际验收

```sh
bash -n scripts/build-console.sh scripts/lab/build-image.sh
node --test scripts/console-package.test.mjs deploy/console/prepare.test.mjs
# With a newly built Console-capable binary, validate the generated TOML as well:
TIDEMQ_BIN=/absolute/tidemq node --test deploy/console/prepare.test.mjs
# Isolated real-MQ, two-Console HTTP integration tests:
cargo test -p tidemq --test runtime_queue scalable::console_http::
```

静态测试检查不可覆盖输出、锁文件/构建失败、私密文件排除、哈希变化、npm 校验先于执行，以及部署的只读/无持久化/Secret/loopback 探测边界；不运行 Docker 或 Kubernetes。HTTP 集成测试在临时隔离集群中检查跨 Console 会话、Origin/CSRF、权限撤销、Topic/消息/身份/凭据/授权、原操作回执与只读消费进度；测试结束关闭实例，不访问已有 lab 集群。

目标平台验收仍须记录源码/镜像/静态资源身份，检查 UID 与 Secret 访问，验证真实 HTTPS、证书、重定向和页面深链接，再验证业务流程、跨副本恢复及已有日志/metrics/tracing 采集链路。具体通过状态以该次运行证据为准。示例内存请求/限额尚无真实并发负载验收依据；消费者删除/位置重置、节点扩缩容/排空/调度等操作继续使用 CLI。
