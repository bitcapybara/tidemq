# VM 实验室部署验收

本批实施发生在 2026-10-25 至 26 日，具体时间以 JSON 中 UTC 时间为准。资源、端口和日常操作见[实验室清单](../../README.zh-CN.md)。

[acceptance.json](acceptance.json) 合并最终部署身份、11 台 VM / 6 个 Kubernetes 节点 / 10 个常驻角色、完整 CI、覆盖率复用、8 个 VM 业务场景、watchdog、外部 SDK、三类观测、S3 权限和后端重启后的历史数据查询结果。它记录提交 `c386434bbf6d478e96d68a53d7040323396b1648` 的验收，不代表之后任何提交自动通过。完整 [GitHub CI 40266082534](https://github.com/bitcapybara/tidemq/actions/runs/40266082534) 已通过。

本次整理将 70 份中间/重复文件合并为一份结果，初始快照、失败现场、复现修正、原始 JUnit/LLVM 哈希和每步证据可从 [Git 历史原目录](https://github.com/bitcapybara/tidemq/blob/e0be78cfc00c81f7d36caaf3f7835b9c45e963b6/deploy/lab/evidence/2026-10-25/) 读取。Git 历史没有重写；该链接固定到整理前已推送的提交。恢复单个文件：

```bash
git show e0be78cfc00c81f7d36caaf3f7835b9c45e963b6:deploy/lab/evidence/2026-10-25/github-vm-validation-passed.json
```

此处镜像身份中的 `hostArchive` 是验收时路径；构建副本和 tar 后续可以清理，重建方法以当前实验室文档为准。CA、token、私钥、VM 磁盘和含 Secret 的资源仍只保留在私有运行目录。性能测试未执行。
