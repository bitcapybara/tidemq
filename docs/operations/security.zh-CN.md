# 安全与权限

TideMQ 的公共业务入口是带 TLS 的 QUIC Gateway，内部节点使用绑定 cluster/node/role/group 的 mTLS 身份。部署方负责私有网络、证书签发和轮换、Secret 分发、HTTPS 入口与存储权限。

## 网络边界

| 入口 | 允许访问者 | 部署要求 |
| --- | --- | --- |
| Gateway / Combined 公共 QUIC | 持有有效作用域凭据的 SDK / Console | 开放实际 UDP 端口；负载均衡支持 QUIC，服务名与证书匹配 |
| 内部 QUIC | 注册且证书 pin 匹配的集群节点 | 限制节点网络；不能接受客户端自行声明的节点身份 |
| 管理 HTTP | 本机运维、同网络 namespace 中的采集器，或受保护代理 | 必须 loopback；不把 diagnostics 直接暴露到公网或浏览器 |
| Console HTTP | 经过 HTTPS 反向代理的浏览器 | 精确 HTTPS Origin、Secure Cookie 与 CSRF；管理端口仍为 loopback |
| S3 endpoint | 配置对象访问权限的数据节点 | 生产使用 HTTPS、专用桶/前缀和最小权限凭据 |

普通 HTTP 反向代理不能直接转发 QUIC；容器内 `127.0.0.1` 也不指向宿主或其他 Pod。网络调整后同时验证允许与拒绝路径，不以进程启动代替 TLS/权限验证。

## 身份与权限

仅全新 Metadata / Combined 的首次初始化读取 `authorization.bootstrap_admin_token_file`，将管理员凭据校验记录持久提交到 Metadata。生成器的 `access.token` 供隔离本地演示；它不是生产应用凭据清单。其他 MQ 角色配置公共 `[authorization]` 表，不分发静态业务 token 列表或共享 ACK 签名文件。

管理员通过 SDK / Console 在线管理 Tenant、Namespace、Principal、Credential 和作用域权限。Namespace 名称只解析资源，不授予权限。为每个应用签发独立、有限权限和有效期的凭据；禁用主体、撤销凭据及到期都会约束新准入。权限变更和结果未知沿原 PendingAdmin 恢复，不用重复签发代替核实原结果。

内容检查权限独立于普通浏览与 Produce / Consume；运维跨租户观测和集群写操作也有自己的授权边界。凭据明文只交付一次；收到 `CredentialSecretUnavailable` 时根据原 Credential ID 撤销，再签发新值。

## 秘密与日志

- 节点只取得自己需要的证书、私钥和信任根；SDK 只需要 CA 证书，不需要 CA 私钥。
- token、私钥和 S3 凭据使用私有文件或部署 Secret；不得写入源码、镜像、网页 localStorage、命令输出或诊断包。
- S3 使用显式凭据文件，不读取机器上的隐式默认凭据链。凭据文件常用 `0600`，并确保服务 UID 可以读取。
- Console 会话密钥为 32 个原始随机字节，副本共享；按[部署说明](https://github.com/bitcapybara/tidemq/blob/main/deploy/console/README.zh-CN.md)逐步轮换，不跨无关集群复用。
- 日志、trace 和支持包只保留有界操作身份与错误链，避免消息正文/key、token 或签名材料；日志丢弃与 sink 错误需独立监控。

本地生成证书七天过期，不用于生产。证书 pin 绑定节点身份，轮换或替换遵循注册与成员协议，不能只替换磁盘文件并假设旧身份自动可信。

## 上线前验证

按实际网络分别验证最小权限应用能够 Produce / Consume / ACK、越权管理和内容访问被拒绝、撤销/到期拒绝新准入、loopback 管理端口未被发布，以及 Console 的 HTTPS / Origin / CSRF 与非 root Secret 可读性。功能 fixture 中的拒绝测试仅证明相应版本与范围，部署环境仍需实测；入口见[测试方法](../testing.zh-CN.md)。

配置字段见[配置参考](../guide/configuration.zh-CN.md)，已有部署脚本见[Docker](https://github.com/bitcapybara/tidemq/blob/main/deploy/docker/README.zh-CN.md)、[Kubernetes](https://github.com/bitcapybara/tidemq/blob/main/deploy/kubernetes/README.zh-CN.md)和[Console](https://github.com/bitcapybara/tidemq/blob/main/deploy/console/README.zh-CN.md)。
