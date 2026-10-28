# 原生视图与显示边界

以下为 Operon 4.3.0 的实际行为。优先重新检查 CLI 能力；能力仍缺失时用 Obsidian UI 配置视图，不直接写插件私有 `data.json`，也不注入未公开的插件内部方法。

## 同源配置

1. 建立项目专用 saved filter，例如标签包含 `TideMQ`，确认所有现有任务都匹配，后续新增任务沿用该标签。保存稳定 filter ID。
2. **Kanban**：建立独立预设并绑定该 filter，选择任务实际使用的 pipeline。加入收藏，保留可快速打开的原生看板入口；不要覆盖其他项目的默认预设。
3. **Table**：从该 filter 的操作菜单中选择新增 Table，使原生 `.table` 文件绑定同一 filter。显示 Task、Note、status、priority、Subtask Progress 和 parentTask；不需要的日期字段可以隐藏。
4. **Tasks**：选择同一个 saved filter，或者在项目笔记中嵌入原生 `operon` 任务列表。不要再创建一套普通 Markdown 复选框来代表同样的工作。
5. 用 CLI 更新一个已授权任务的真实 Note/状态，再核验 live 查询和三个视图展示同一变化。正文中的建立时快照必须标明其时间，最新进展以当前字段为准。

已有专用 Table 时复用其文件或预设，不反复点击“新增 Table”。`Default table` 是通用默认表，项目专用表绑定项目筛选器；两个表可以读取同一组任务，这不表示任务被复制。项目入口只链接专用表，多余打开的标签页可以关闭，不能据名称删除用户已有的默认表。

Table 预设由公开的 `.table` 文档保存。需要按文件修改时，基于 UI 实际生成的文件保留其 format、version、id 和 filterSetId，只改已知显示字段，并核验插件实际重载。不要凭空编造格式或随意复制预设 ID。

## 支持的 Markdown 嵌入

任务列表：

````markdown
```operon
filterId: "<actual-saved-filter-id>"
```
````

Table：

````markdown
```operon-table
presetId: "<actual-native-table-preset-id>"
rows: 20
width: 100
```
````

ID 必须取自已保存的实际视图。当前版本没有已验证的 Kanban Markdown 嵌入语法，不编造 `operon-kanban` code block；使用保存的原生 Kanban 预设。入口说明笔记不能冒充交互看板。

## 为什么出现很多 0

Project pipeline 有多个状态，默认优先级泳道又包含多个级别。状态和优先级交叉展开后，空列、空泳道及空单元格都会显示任务数量 `0`，不是“项目完成度为 0”。

本次 TideMQ 看板采用 **No swimlane**，取消优先级网格，并开启 **Collapse empty columns**。任务的优先级字段仍保留。插件折叠空状态后仍保留窄列的 `0` 计数；不能把“折叠”描述为完全隐藏，也不要为了去掉这些计数而删除全局流程状态。

有实际子任务的卡片可显示 Runtime 计算的进度；该比例是子项计数，并非投入工时或全项目 CI 验收百分比。任务正文和 Note 应解释切片验收范围。
