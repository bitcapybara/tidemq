# 排障入口

当前基础阶段可执行 validate-config、diagnose、metrics 和 allocator-probe。先记录版本/配置路径/构建 feature 和命令退出状态；diagnose 只描述当前进程，不会连接远端节点。

1. 配置失败：检查未知字段、RF、地址隔离、帧/批次/内存预算。错误分类来自类型和稳定代码，不依赖匹配错误文本。
2. 日志缺失：分别读取队列丢弃、超大事件和 sink 错误；检查过滤器和输出管道。不要先全局无限 DEBUG。
3. 内存增长：按[内存排障](memory-troubleshooting.zh-CN.md)对齐业务容量、allocator 与 OS/cgroup 的范围，复现相同构建。
4. 消息/复制/S3 故障：在运行实现交付前没有可运行的服务诊断。后续每模块需补故障注入→指标→关联日志/trace→只读查询→恢复证据。

未来运行诊断默认有权限、范围、分页、字节和期限限制；临时采样自动停止。native dump 不一定能由 Tokio timeout 中止，不能在超时后叠加启动新 dump。profile 必须匹配 binary/build ID、符号、allocator 和采样配置。
