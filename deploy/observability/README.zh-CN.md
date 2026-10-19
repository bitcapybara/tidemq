# 本地可选观测组件

先按[快速开始](../../docs/guide/quickstart.zh-CN.md)启动宿主上的 TideMQ RF1。业务进程不依赖这些组件；停止监控不改变生产、持久 ACK、对象发布和 GC 的权威事实。

```sh
docker compose -f deploy/observability/compose.yaml --profile observability config --quiet
docker compose -f deploy/observability/compose.yaml --profile observability up -d
docker compose -f deploy/observability/compose.yaml --profile observability ps
```

示例固定 OpenTelemetry Collector core 0.162.0、Prometheus 3.15.0、Grafana OSS 13.2.4，首次执行 up 会拉取这些镜像。配置使用 host networking，让容器通过 `127.0.0.1:8410` 抓取已有管理面，不需要将管理监听改为公网地址。需要 Linux Docker Engine，或显式启用 host networking 的 Docker Desktop 4.34 及以上版本，具体限制见[Docker 官方说明](https://docs.docker.com/engine/network/drivers/host/)。host 模式没有独立网络命名空间；不要增加公网监听或假定 ports 映射会隔离它。

TideMQ 默认关闭 OTLP。需要验证导出时，按[追踪手册](../../docs/operations/tracing.zh-CN.md)在节点配置中开启 `[tracing]`，发送至 `http://127.0.0.1:4318/v1/traces`。Collector 使用[官方 core 镜像](https://opentelemetry.io/docs/collector/install/docker/)；仓库配置默认 `nop`，接收后丢弃，不提供 trace 搜索或持久存储。接后端前须另行设置认证、有限队列/重试和存储保留。

| 组件 | 本地入口与行为 | 独立资源限额 |
| --- | --- | --- |
| Collector | OTLP HTTP `127.0.0.1:4318`；自指标 `127.0.0.1:18888` 由 Prometheus 单独抓取；64 KiB 请求体、单 batch 最多 128 spans、默认无网络出口/重试/持久队列 | 1 CPU、192 MiB 容器内存，Go 软限制 96 MiB；memory limiter 128 MiB/spike 32 MiB，临时目录 tmpfs 16 MiB |
| Prometheus | `http://127.0.0.1:19090`；默认每 15 秒抓取 RF1，3 秒超时、4 MiB/20,000 样本上限；4 个并发查询、每次最多 200,000 样本和 5 秒 | 1 CPU、768 MiB 容器内存，Go 软限制 192 MiB；数据 tmpfs 512 MiB、临时目录 16 MiB |
| Grafana | `http://127.0.0.1:13000`；预置四个看板及本地只读数据源；仅匿名 Viewer，禁用初始管理员、basic auth 与登录表单 | 1 CPU、256 MiB 容器内存，Go 软限制 128 MiB；状态 tmpfs 64 MiB、临时目录 16 MiB |

各组件最多 128 个进程/线程、5 秒停止宽限，默认不自动重启；根文件系统只读。容器 local 日志驱动每文件 10 MiB、保留 3 份，另有运行时元数据/压缩余量。镜像缓存和 Docker 自身数据仍需要宿主磁盘预算，不能把日志配置当作 Docker 整体配额。tmpfs 计入容器内存，Go 软限制不是进程 RSS 硬上限；到达容器或文件系统限额时，观测可能拒绝、丢样或退出，不改变宿主业务存储。

Prometheus 同时设置一小时和 64 MiB 的历史 block 保留。WAL、当前 head 和压缩临时空间不包含在 64 MiB 内，按实际占用另计；最终 tmpfs 是硬边界。Collector 自指标抓取另限 1 MiB/2,000 样本，所有抓取均限制标签数和标签长度。所有观测状态都是临时的，容器重建后丢失。这个本地示例没有长期 trace/日志存储、告警通知发送器或生产 SSO。公开部署必须另行配置受保护入口、认证、持久卷/文件系统配额、保留和备份，不能直接将匿名入口改绑公网。

Grafana 默认使用仓库 JSON 看板，数据源名字为 TideMQ Prometheus；面板变量可选该唯一数据源。维护配置遵循[Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/)，镜像版本参考[官方发布记录](https://github.com/grafana/grafana/releases/tag/v13.2.4)。只读 Viewer 不保存 UI 修改；修改仓库文件后重新创建容器。

停止并删除这组临时容器：

```sh
docker compose -f deploy/observability/compose.yaml --profile observability down
```

验证证据：官方 Compose 5.6.0 CLI 已执行 config 校验；原生 Prometheus 3.15.0 已校验配置、14 条规则，以及 readiness/capacity、trace loss/Collector downtime 两组告警触发/恢复测试。2026-10-20 使用官方原生 Collector core 0.162.0、Grafana 13.2.4、Prometheus 3.15.0 和真实 RF1 进程完成链路验证：四个看板共 29 个面板通过 provisioning 加载，均不可编辑；Grafana 数据源实际查询到 `up{job="tidemq"}=1` 和 `up{job="tidemq-collector"}=1`，匿名管理员接口返回 403。四个自有子进程正常退出，随后五个 TCP 与两个 UDP 监听端口均可重新绑定。此项验证覆盖服务端 API 与真实数据源，未验证浏览器画面；Collector 的真实 span 接收另见[OTLP 证据](../../docs/evidence/2026-10-20-otlp.zh-CN.md)。当前机器未安装 Docker，未运行这组容器，不声称镜像运行、容器资源限额或宿主网络模式已经验收。
