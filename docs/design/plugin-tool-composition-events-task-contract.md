# 插件 Tool 组合事件任务合同（R10）

- 状态：implemented / reviewed；公开 Change Gate 待提交后执行
- 日期：2026-08-15
- 实现基线：`ea4357832729eb64e0807b6a7d1ce96449289f70`
- 关联条款：PLG-001～PLG-004、PLG-006、PLG-008、PLG-013～PLG-014、ERR-001
- 上游：[Transform/Observe 事件](plugin-transform-observe-task-contract.md)
- 对照：`deepseek-harness@47f943859bef60e4160492346772ded9b24f765a` 的 `tools/pre-execute`、`tools/result`

## 1. 目标

为真实 Tool consumer 提供三个领域 seam，不复制 DSH 完整 waterfall：

```text
ToolInput
   │
   ├─ tool.input.prepare ───────────────> 只变换 arguments
   │
   ├─ V2_REMOVAL: legacy pre hooks ─────> 旧改参 / allow / deny
   │
   ├─ tool.execution.authorize ─────────> 最终参数上的 pass / Bail(reason)
   │
   └─ invoker ──> V2_REMOVAL: legacy post ──> immutable tool.result
```

最终所有 ToolHook consumer 迁完后物理删除两段 legacy 夹层，只剩：

```text
input.prepare → execution.authorize → invoker → immutable result
```

## 2. Payload 与领域合同

- `ToolInput` 冻结 call id、tool name、source、session/channel/chat、request text、batch 和 index。支持入口是 `input.with_arguments(new_arguments)`；直接构造或 `dataclasses.replace()` 也会重新 canonicalize 参数，不能绕过递归冻结。换掉 call identity 会 fail-loud `TOOL_INPUT_IDENTITY_CHANGED`。
- arguments 在插件侧是递归冻结的 JSON view；`dict/list` 分别投影为只读 mapping/tuple。Core 在调用 legacy hook 或 invoker 前重新恢复成普通 `dict/list`，不改变工具 schema 输入类型。
- `TOOL_INPUT_PREPARE` 使用稳定 payload contract `akashic.tool-input.v1`，进入 composition topology identity。零 listener 原样通过，多 listener 依注册顺序 fold。
- `TOOL_EXECUTION_AUTHORIZE` 使用带稳定 Bail contract `akashic.tool-deny-reason.v1` 的 `serial`；`None` 表示 pass，`Bail(str reason)` 表示 deny。dispatch 在仍持有 listener owner 时校验 reason 类型，非法值记录该 owner 的 `serial_failure`。deny 是正常 settled result，不进入 invoker。
- `ToolResult` 冻结最终参数、status、字符串结果和 extra messages。`TOOL_RESULT` 使用 `observe`，普通 observer 失败只记 Incident，不得改变 settled `ToolExecutionResult`。
- prepare/authorize 普通异常使本次工具结果成为 `error`，不进入 invoker；composition listener 已按 owner 记录 `transform_failure` 或 `serial_failure` Incident。caller cancellation 和进程级终止继续传播。
- `preflight()` 执行 prepare、legacy pre 与 authorize，但不调用 invoker，也不发布 `tool.result`，避免把 admission probe 伪装成真实工具执行。

## 3. 迁移期固定顺序

```text
1. v3 tool.input.prepare
2. legacy pre hooks
3. v3 tool.execution.authorize
4. invoker
5. legacy post hooks
6. v3 immutable tool.result
```

Shell Restore / Shell Safety 四种组合必须等价：

| Restore | Safety | 实际顺序 |
|---|---|---|
| v2 | v2 | legacy restore → legacy safety |
| v3 | v2 | v3 prepare → legacy safety |
| v2 | v3 | legacy restore → v3 authorize |
| v3 | v3 | v3 prepare → v3 authorize |

legacy pre 已 deny 时不再运行 v3 authorize；无论 prepare error、legacy deny、authorize deny、invoker error 或 success，真实 `execute()` 都只发布一次最终 `tool.result`。legacy post 完成后才发布 result。

## 4. Ownership 与 v2 删除

- 事件 key 与 immutable payload 由 Tool 领域拥有，定义在 `agent.tools.events`；composition kernel 只提供 dispatch 与 Fiber/Effect 回收。
- `ToolExecutor` 只从当前 generation snapshot 取得同一棵 `composition_root` 和 legacy hook catalog，不能跨 snapshot 混用。
- `V2_REMOVAL(tool-hooks)` 覆盖 `ToolHook`、HookContext/HookOutcome/HookTraceItem、`ToolExecutionResult` 的 legacy traces、snapshot `tool_hooks` catalog、Manager metadata/contribution 收集和 ToolExecutor 两段夹层。
- 最终物理删除 PR 还必须迁移或删除 ToolHook 注入链：`agent/tools/spawn.py`、`agent/background/subagent_manager.py`、`agent/background/subagent_profiles.py`、`agent/looping/core.py`、`agent/core/passive_turn.py`、`agent/subagent.py` 以及 proactive/drift runtime/factory；同时处理 passive/subagent 的 loop-guard trace 持久化，不能只删除接口定义。
- 删除前必须迁移 Shell Restore、Shell Safety、Tool Loop Guard，以及仍消费 post hook 的插件；按四矩阵回放证明 invoker 参数、deny reason、invoked、result、legacy trace 与 Incident trace 等价。
- 本 PR 只增加 Core seam，不修改任何外部 canonical plugin source，不删除 legacy。

## 5. 验证

- Oracle：四种 Restore/Safety 组合；prepare fold 与 identity fencing；`dataclasses.replace`/raw alias 不能绕过冻结；authorize deny/error/非法 Bail 的 owner Incident；legacy 非 JSON rewrite 只形成一次 settled error；legacy post 在 result 之前；result observer failure 不改变 settled output；递归参数不可变；preflight 不发布 result。
- Targeted：ToolExecutor + composition events/kernel；cumulative：plugin/tool/runtime inspection 回归、Basedpyright、compileall、`git diff --check`、公开 Change Gate。
- 禁止副作用：正式 workspace/plugin-data、manifest/cache、真实工具、渠道或外部 API。
- 回滚点：Git tag `backup/plugin-tool-events-r10-before-20260815`。

## 6. 后续插件栈

1. Shell Restore → `TOOL_INPUT_PREPARE`。
2. Shell Safety、Tool Loop Guard → `TOOL_EXECUTION_AUTHORIZE`。
3. Default Memory 等 final consumer → `TOOL_RESULT`。
4. exact-commit 组合 Gate 覆盖混合迁移矩阵；最后单独 PR 物理删除 legacy ToolHook。

## 7. 本 PR 证据

- 定向：ToolExecutor、composition events/kernel，`96 passed`。
- 累计：plugin/tool/runtime inspection/default memory，`578 passed`。
- Basedpyright：`0 errors / 0 warnings`；compileall 与 `git diff --check` 通过。
- Terra xhigh 独立只读复审：两轮发现的 ToolInput canonicalization 与 owner-specific Bail Incident 已修复，最终无 P0/P1。
- 未触达正式 workspace、plugin-data、渠道、真实工具或外部 API；公开 Change Gate 在冻结 commit 后执行并回填 PR。
