---
name: operon
description: 使用 Operon CLI 在 Obsidian 中创建和维护项目、任务、父子关系与进度，并核验 Kanban、Table、Tasks 共用同一组任务。适用于 Operon 项目跟踪及其持续同步。
---

# Operon 项目跟踪

优先用 `operon` CLI 读写任务。先查看当前版本的 help、live Catalog 和 schema；本文记录 CLI 1.2.0、Operon 4.3.0、Obsidian 1.14.5 的实际验证经验，不把历史版本的能力当作当前事实。

## 连接和发现

```sh
operon version --json
operon profile list --json
operon doctor --live --json
operon catalog --consistency live-verified --json
```

已有正确 profile 时复用它；多 vault 时显式传 `--profile <alias>`。尚未配置时，使用用户指定或实际打开的 vault：

```sh
operon setup --vault '<absolute-vault-path>' --name '<profile-alias>' --live --json
```

确认 Runtime 可读写、vault identity 匹配且索引已 settled。连接失败时先检查 Obsidian、Operon 插件和 `doctor --live`，不要修改插件私有数据来模拟在线状态。遵守当前环境的命令代理规则，例如要求 RTK 时使用 `rtk proxy operon ...`。

## 日常同步

1. 从项目清单和最新测试、review、CI 证据确认真实进展；先查询现有任务，按稳定 Operon ID 更新，避免按标题重复创建。
2. 保持业务编号，例如 `T05`、`T05.P3`，并保存编号到 `operonId`、实际文件路径、父任务 ID 的映射。映射和操作收据放工作区证据目录，不把个人 vault 路径、任务数据或日志提交到产品仓库。
3. 任务正文保存范围、实施计划、验收标准和证据入口；`note` 保存简短的最新进展、提交和剩余工作。`note` 是单行字段，不用它存整份计划。
4. 状态使用 live Catalog 的准确 status ID，通过语义 transition/complete/reopen 接口修改。`progress`、完成子项计数和时间戳由 Runtime 维护，不能手填百分比。
5. 新增子任务时设置真实父子关系，并添加与项目视图相同的标签。依赖关系按实际前置工作保存，不把普通参考关系都设成阻塞依赖。
6. 本地验证、独立 review、候选 CI、main CI 和部署分别记录；某个切片通过不能使整个产品任务自动完成。
7. 每次有任务增补、实质进展或状态变化，同步项目清单与 Operon，再读回核验。三个视图引用同一索引，不分别维护三份静态清单。

创建、更新和状态变更的可复制例子见 [CLI 操作配方](references/cli-recipes.md)。只读取当前操作需要的 schema，避免整份 schema 或 minified 插件源码进入上下文。

## 预览、应用和恢复

- 对 agent 使用的 typed `--input`，CLI 返回的是预览计划，不等于已经创建或更新。检查 JSON 的顶层 `ok` 和结果状态；仅退出码为 0 不足以证明成功。
- 检查计划的实际目标、字段、父子关系、风险与 `createEffects`，保存 `client.planRef`。只应用已审阅、未变化且未过期的计划。
- 要求确认时，通过 `operon plan show <planRef> --json` 读取 `.result.plan.confirmationToken`，将该 token 传入 `plan apply --confirm`。它不是 `receiptTargetDigest` 或某个任务的 `targetDigest`。
- 用户已明确授权的项目维护可以按该授权应用计划；CLI 的技术确认流程本身不意味着必须再次询问用户。超出既有授权的内容仍须先确认。
- 批量跨文件创建可能分组提交，不保证全局原子性。若结果不确定或部分提交，读原计划、收据和实际任务，使用 `plan recover` 恢复同一计划；不要更换幂等键并重新创建整批任务。
- 计划过期且确认未应用时，重新读当前状态再生成新预览。不要盲目重放旧计划，也不要手改 sealed plan。
- 成功后用 `live-verified` 查询核对任务 ID 集合、数量、字段、父子关系、无重复和 `truncated=false`；查看实际查询时间，不能只引用旧的预览结果。
- `ok=true` 仍可能伴随 `result.status=failed`。本次实际遇到 `stale-context`：只有确认 `mutationMayHaveApplied=false`、没有已提交分组后，才重新读状态并生成新预览。UI、父子任务或设置变化都可能使上下文过期，审阅后应及时应用；未知或部分生效仍走同计划恢复。
- 同一 vault 的相互影响写入按“读取 → 预览 → 审阅 → 应用 → 读回”逐项完成，避免先收集多份计划再依次应用。状态及 Note 需要同时更新时，可使用下方配方中的语义 transition 附带 allowlisted changes。

## Kanban、Table、Tasks

CLI 能管理任务，不代表能管理所有视图。先检查当前 `manifest` 和 `capabilities`；已验证的 1.2.0 没有原生视图预设管理命令，不编造 `operon kanban create` 等接口。

需要建立或调整三个视图时，阅读 [原生视图与显示边界](references/views.md)。一次性视图配置可以使用 Obsidian UI；已有视图后的日常状态、Note、父子关系及新增任务继续优先通过 CLI 同步。

## 持续维护本 skill

后续每次使用遇到新能力、命令变化或已复现的问题，先实操验证，再就地更新对应配方或边界；记录适用版本及验证方式。删除已过时的说法，不叠加互相冲突的历史步骤。只写已证实的行为，不把一次失败或猜测提升为通用规则。

涉及命令例子的修改，用只读操作或已授权的真实同步验证；需要试建数据时使用隔离的临时项目并完成清理。skill 自身通过独立 review 后，随项目正常 CI 和合入流程交付。
