# 插件 context-prepared 与 Memory runtime capability 任务合同

- 状态：implementation candidate
- 日期：2026-08-15
- 关联条款：PLG-001～PLG-004、PLG-008、PLG-014、MEM-001～MEM-009
- 上游：[0036](../decisions/0036-plugin-composition-keeps-promotion-owner.md)、[插件 lifecycle 接入点任务合同](plugin-lifecycle-seam-task-contract.md)

## 1. 目标

为 Default Memory v3 迁移补两个最小接入点：

1. typed serial event `turn.context_prepared`，观察现有 before-turn 依赖图与 slot export 处理后的 `BeforeTurnCtx`；
2. Core-owned 只读 service `core.memory.runtime`，只公开当前 Memory engine 的稳定名称，不公开查询、写入、数据库或任意 engine 对象。

```text
ContextStore.prepare
        │
        ▼
 BeforeTurnCtx ─ existing legacy DAG ─ collect exports
        │
        ▼
 turn.context_prepared ─ return

Core MemoryEngine.describe()
        │ freeze name
        ▼
 core.memory.runtime ── optional ctx.get() ── plugin-owned behavior
```

本 PR 不迁移 Default Memory，不移动 `observe/recall_inspector.jsonl`，不修改 Memory 数据库、查询、写入或 Dashboard，不新增通用 repository/SQL/HTTP capability。

## 2. Change intent

```yaml
change_type: feature
semantic_delta: compatible
capability_owner: core
consumer_scope:
  - composition plugin API v3
  - before-turn lifecycle
runtime_patch: required
runtime_patch_reason: "Core owns the selected Memory runtime and the exact point where prepared context becomes observable."
authoritative_state_owner: "Memory engine remains Core-owned; plugins receive only a frozen descriptive view. BeforeTurnCtx remains the existing lifecycle fact."
client_only_alternative: "Not applicable; this is server runtime composition."
```

## 3. 合同与顺序

- `CONTEXT_PREPARED_EVENT` 使用 `SerialEventKey[BeforeTurnCtx, object]`；listener 按 generation 注册顺序逐个等待。
- EventBus 与现有 legacy module 的相对顺序、slot、requires/produces 不变；早期 legacy module 仍可在 EventBus 前运行。composition seam 只新增对 `before_turn.collect_exports` 的依赖，并在 `before_turn.return` 之前执行。
- listener 返回 `Bail` 时以 `LIFECYCLE_BAIL_NOT_ALLOWED` fail-loud；异常立即传播，保持旧 phase module 的失败语义。
- 没有 bound RuntimeSnapshot 或 composition Root 时保持 no-op。
- `MemoryRuntimeInfo` 是 frozen value，仅包含 `name`；`MEMORY_RUNTIME` 是可选 ServiceKey。没有 Memory engine 时 Core 不注册 provider，插件通过 `ctx.get()` 得到 `None`。
- provider 由 Root Fiber 的 Effect 拥有，先于插件 mount；Manager 首次建立 v3 Root 时冻结描述值，candidate/formal Root 分别取得同内容的独立 provider，Root dispose 后注销。
- 插件不得通过此 service 读取/修改 Memory 数据、持有 engine、执行 SQL 或绕过现有 Memory owner。

## 4. 受保护状态与副作用

- 不写正式或候选 workspace，不触碰 `MEMORY.md`、`PENDING.md`、`memory2.db`、`akasha.db` 或 `observe/recall_inspector.jsonl`。
- 不改变 stable/latest、promotion、lease、generation、Root 隔离或 Memory engine 选择。
- 不新增 Job、Channel、MCP、phase contribution adapter 或通用外部能力。
- 回滚点：`backup/plugin-context-prepared-capability-before-20260815`。

## 5. 验证

- lifecycle oracle 保持既有 legacy DAG，并固定 `collect exports → composition → return`、payload identity、Bail/异常与 no-root no-op。
- Manager 真链路证明 provider 在 v3 `apply` 前可选可见、只公开名称、进入 topology，并随 Root 清理。
- candidate/formal rebuild 的 topology identity 与 provider 内容一致；stable Health/Incident 和正式 workspace 不受影响。
- 运行 lifecycle、composition loader/kernel、hot reload、Memory contract 回归、两套 Pyright、compileall、`git diff --check` 与公开 change-impact Gate。
