# Default Memory v3 迁移任务合同

- 状态：implementation candidate
- 日期：2026-08-15
- 关联条款：MEM-005、MEM-008、PLG-001～PLG-004、PLG-008～PLG-010、PLG-014
- 上游：[0036](../decisions/0036-plugin-composition-keeps-promotion-owner.md)、[Cordis 能力等价](cordis-plugin-capability-parity.md)、[context-prepared 与 Memory capability](plugin-context-prepared-memory-capability-task-contract.md)、[DashboardContext](plugin-v3-dashboard-context-task-contract.md)、[持久化状态地图](persistence-state-map.md)

## 1. 目标

把内置 `default_memory` inspector 从 v2 phase/EventBus/Dashboard ABI 迁移到 v3：

```text
┌───────────────────── Core publication / promotion ─────────────────────┐
│ MEMORY_RUNTIME                  stable/candidate generation + data_root │
└──────────────┬───────────────────────────────────────┬─────────────────┘
               ▼                                       ▼
┌──────────────────────────┐              ┌──────────────────────────────┐
│ is_active(ServiceView)   │              │ DashboardContext             │
│ active := engine=default │              │ exact Root runtime data_root │
└──────────────┬───────────┘              └──────────────┬───────────────┘
               │                                         │
       ┌───────┴────────┐                                ▼
       ▼                ▼                    ┌──────────────────────────┐
turn.context_prepared  tool.result           │ plugin-owned JSONL reader│
       │                │                    └──────────────────────────┘
       └───────┬────────┘
               ▼
  data_root/recall_inspector.jsonl
```

迁移后插件自己实现记录格式、过滤和 Dashboard reader；Core 只提供 typed event、只读
Memory runtime 描述、generation 数据根、Dashboard 接入点、Effect 清理与晋升。

## 2. Change intent

```yaml
change_type: migration
semantic_delta: compatible
capability_owner: mixed
consumer_scope:
  - default_memory builtin plugin
  - composition plugin api v3
  - static Skill and Dashboard publication
runtime_patch: required
runtime_patch_reason: "Core owns generation publication and must omit inactive v3 static contributions; the plugin owns whether its selected Memory capability makes it active."
authoritative_state_owner: "Default Memory owns recall inspector JSONL; Core owns candidate/formal data-root allocation and stable/latest publication."
client_only_alternative: "Not applicable; lifecycle listeners, Skill publication and Dashboard routes are server runtime capabilities."
```

## 3. v3 行为合同

- v3 namespace 可选导出同步 `is_active(services: ServiceView) -> bool`。Core 在装配前冻结
  与 Root provider 同源的 typed service view，并把结果冻结到 exact Fiber/RuntimeSnapshot；
  original、candidate clone 和 formal rebuild 不共享模块全局。没有导出时默认 active；非
  callable、awaitable 或非 bool 结果 fail-loud。
- static predicate 为 false 时 Core 不执行该插件的 `apply`，也不把其 Skill、Dashboard 或
  generation 投入静态投影。`default_memory` 通过 `ServiceView.get(MEMORY_RUNTIME)` 判断；
  Core 没有 Memory runtime 或 engine 不是 `default` 时，整个 Root 仍可 ready。
- engine 是 `default` 时，插件串行观察 `turn.context_prepared`，最终观察 `tool.result`；只在
  source 为 `passive` 且 tool 名为 `recall_memory` 时记录 settled result，保持 v2 passive-turn
  fanout 边界；proactive/subagent 不新增诊断写入。两条路径沿用现有 JSONL schema、turn
  ID、trace 归一化和错误传播。
- Dashboard 使用 exact composition Root 的 `PluginRuntime.workspace/data_dir` 构造
  `DashboardContext`，与同 generation listener 读取同一 JSONL；不得再通过可变
  `generation.validation_workspace` 猜测 candidate。HTTP route、分页、过滤、404 和损坏
  JSONL fail-loud 行为不变。
- listener、Dashboard module、route 和 closeable 都由 generation/Fiber scope 持有；candidate
  discard 与旧 generation drain 后不得残留。

## 4. 数据迁移与回滚

旧路径 `<workspace>/observe/recall_inspector.jsonl` 是追加式诊断证据；新路径是
`<workspace>/plugin-data/default_memory-builtin/recall_inspector.jsonl`。本 PR 不改写任何
既有 JSONL 字节，也不删除旧名字：

1. 新路径已存在而旧路径不存在时，直接使用新路径。
2. 旧路径存在而新路径不存在时，正式 v3 apply 用 `link(2)` 原子建立新名字；两个路径指向
   同一 inode。若跨文件系统或无权限则 fail-loud，不退化为可能分叉的 copy。
3. 两个路径都存在时必须是同一文件，否则 fail-loud，要求维护者先对账。
4. candidate 只操作 validation workspace/data_root 的副本，不读取或链接正式旧路径。
5. 回滚到 v2 后，v2 继续向旧名字追加；因为同 inode，新路径保持同步。删除旧名字留给最终
   v2 物理清理 PR，并需要独立备份与字节/inode 验证。

状态只追加 JSONL 行，不原位更新、不逻辑失效、不物理减少。恢复点是
`backup/default-memory-v3-before-20260815`；失败 oracle 同时核对旧字节、新路径和正式
workspace write set。

## 5. v2 删除点

本 PR 直接删除 Default Memory 自身的 v2 `Plugin` 继承、`ContextPrepareRecordModule`、
`@on_tool_result` 和三参数 Dashboard ABI；不删除 Core 的通用 v2 host。Core 侧既有
`V2_REMOVAL(...)` marker 继续保留，直到其余插件完成迁移。

最终 v2 清理还需：

1. 删除 legacy phase/EventBus/ToolHook/Dashboard host 及 `PluginContext` 公共 ABI；
2. 删除 `observe/recall_inspector.jsonl` 的旧名字，但保留 plugin-data 内同一文件；
3. 删除只验证 Default Memory v2 module/decorator 的测试与兼容扫描。

## 6. 验证

- active `default` engine：正式 snapshot 的 topology 含两个 listener；真实 lifecycle 与
  `ToolExecutor` 各追加一条等价 JSONL，Dashboard 读取同一数据。
- inactive `akasha` engine：无 listener、无 Skill catalog/name claim、无 Skill link、无
  Dashboard binding、无写入；另一个 active 插件可声明同名 Skill。
- candidate：事件写入仅落 validation data_root；discard 正式字节不变；promotion 后 formal
  Root/Dashboard 才使用正式 data root；candidate Dashboard 可立即读取 candidate listener
  写入，default→default installed promotion 保留 Skill 和 active generation 投影。
- legacy migration：旧字节不变、两个名字 inode 相同；v2 风格旧路径后续追加可从新路径
  读到；冲突双文件与 hard-link 失败 fail-loud。
- 相关 Default Memory、composition loader/lifecycle/Tool、Skill link、Dashboard、hot reload
  回归、两套 Pyright、compileall、`git diff --check` 与公开 change-impact Gate 通过。
