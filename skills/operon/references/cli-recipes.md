# CLI 操作配方

以下字段形状已在 CLI 1.2.0 / Operon 4.3.0 实测。实际 pipeline、status、priority、template ID 从当前 Catalog 读取，示例 ID 不能直接当作其他 vault 的事实。

## 查找已有任务

把查询保存为 `query.json`，为每次请求生成新的 request ID：

```json
{
  "contractVersion": 1,
  "requestId": "query-project-20261028-001",
  "kind": "task-query",
  "filters": {"text": "Example project"},
  "include": ["notes"],
  "consistency": "live-verified",
  "limit": 100
}
```

```sh
operon query --profile '<profile-alias>' --input query.json --json
operon schema get task-query-request --json
operon relationships --help
```

文本查询适合发现；任务 ID 映射和精确读回用于防止同名混淆。结果被截断时按当前 schema 分页，不把第一页当全量。

`include: ["notes"]` 的读回字段为 `tasks[*].note`；默认精简查询可能不包含 Note，不能据字段缺失判定更新丢失。原生视图还可用 `filter-query` 和 `task-filter-query-request` 按 saved filter ID 精确核对同一任务集合。

## 创建项目及子任务

使用 `operon schema get mutation-intent --json` 查看 CLI intent；`mutation-preview-request` 是较低层的 Runtime 请求，包含另一套必需字段，不要混用。

`create.json` 的最小父子示例：

```json
{
  "contractVersion": 1,
  "kind": "mutation-intent",
  "idempotencyKey": "example-project-create-20261028-001",
  "reason": "Create the project and task requested by the user",
  "spec": {
    "operation": "create",
    "items": [
      {
        "itemRef": "project",
        "description": "Example project",
        "target": {"representation": "file", "mode": "configured-default"},
        "statusId": "st_project_in_progress",
        "priorityId": "pr_a",
        "tags": ["ExampleProject"],
        "fields": [{"kind": "text", "field": "taskType", "value": "Project"}],
        "bodyMarkdown": "# Example project\n\nScope, plan, acceptance criteria and evidence links."
      },
      {
        "itemRef": "task-1",
        "description": "T01 Verify the implementation",
        "target": {"representation": "file", "mode": "configured-default"},
        "statusId": "st_project_in_progress",
        "tags": ["ExampleProject"],
        "parent": {"kind": "created", "itemRef": "project"},
        "fields": [{"kind": "text", "field": "note", "value": "Implementation ready; CI pending"}],
        "bodyMarkdown": "# T01 Verify the implementation\n\nRun the required checks and retain their exact revision and result."
      }
    ]
  }
}
```

已有父任务时改用 `parent: {"kind":"existing","operonId":"<actual-parent-id>"}`。同批 `itemRef` 不是已创建的 Operon ID；应用后的 `createEffects` 才包含实际 ID 和生成路径。

File Task 的 `exact-path` 文件名必须与描述的规范化文件名匹配；不能任意指定 `T01.md` 而描述为另一个长标题。没有必须指定路径的需求时，使用 `configured-default`，让插件生成文件名，再保存实际路径。本次批量 46 个文件任务及 45 条父子关系已创建并用 live 查询完整读回；这不是所有版本的批量上限，先看 Catalog/schema 的当前限制。

```sh
operon task create --profile '<profile-alias>' --input create.json --json > preview.json
operon plan show '<planRef-from-preview>' --json > reviewed-plan.json
operon plan apply '<same-planRef>' --confirm '<confirmationToken-if-required>' --json
```

无需确认的计划省略 `--confirm`。预览中实测有效期为 5 分钟，以当前计划的 `expiresAt` 为准。若 create 结果不确定，按主 skill 的同计划恢复流程处理，不重建整批数据。

CLI 1.2.0 的 `plan apply` 不接受 `--profile`；在任务预览命令中选择 profile，然后原样使用返回的 `planRef`。不要给所有子命令统一追加相同选项。本次把 `--profile` 传给 `plan apply` 时，CLI 在本地返回 `invalid-request / unknown-flag`；修正选项后沿用同一个计划应用，并用原 profile 的 live 查询核验。

## 更新已有 File Task 的最新进展

`target` 中的 locator 来自实际读回，不自行拼写。把下列内容保存为 `update.json`：

```json
{
  "contractVersion": 1,
  "kind": "mutation-intent",
  "idempotencyKey": "example-note-update-20261028-001",
  "reason": "Record the verified project progress",
  "target": {
    "operonId": "<actual-task-id>",
    "locator": {"representation": "file", "filePath": "Operon/Tasks/<actual-file-name>.md"}
  },
  "spec": {
    "operation": "update",
    "changes": [{"field": "note", "valueType": "text", "value": "Targeted checks passed; full CI pending"}]
  }
}
```

```sh
operon task update --profile '<profile-alias>' --input update.json --json
```

审阅、应用和读回方式与创建一致。不要将 create 的 `{kind,field,value}` 字段项格式用于 update；update 使用 `{field,valueType,value}`。`compact-lines` 的批量 update 在本次版本仅接受同一 inline source 的原子更新，不能用它更新多个独立 File Task；按当前支持能力逐项或使用合法的 typed 操作，不伪造批量成功。

状态、父子关系、依赖不是普通 Note 更新。使用 `task transition --help`、`task complete --help`、`task update --help` 及 `mutation-preview-request` 中相应 spec，分别生成语义操作；例如普通字段更新不要混入 `parentTask` 关系替换。

状态和 Note 可以在一个语义 transition 中提交。复用上例真实 `target`，将 `spec` 改为：

```json
{
  "operation": "transition",
  "targetStatusId": "st_project_finished",
  "expectedStatusId": "st_project_in_progress",
  "changes": [{"field": "note", "valueType": "text", "value": "Implementation and independent review passed; main delivery is tracked separately"}]
}
```

用 `operon task transition --input transition.json --json` 预览，按同一计划应用并精确读回状态。上例已在 T14 实操，`postflight.status=verified` 且 `task get` 读回 `workflow.status.id=st_project_finished`、`checkbox=done`；目标和预期状态仍须取自当前任务及 Catalog。

## 应用结果不确定

```sh
operon plan show '<original-planRef>' --json
operon plan recover --help
operon task get --help
```

先核对该计划已提交的组和已存在的目标；只按当前 `plan recover` 的合约恢复同一计划。不要在未知是否生效时生成新幂等键重试创建。
