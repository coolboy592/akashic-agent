# 插件 Transform 与 Observe 事件任务合同（R4）

- 状态：implemented / reviewed
- 日期：2026-08-15
- 实现基线：`4973b1e2473d8c220f970c178f75fff911db77ca`
- 关联条款：PLG-001～PLG-004、PLG-008～PLG-010、PLG-014、ERR-001
- 上游：[R3b Health/Incident/Validation](plugin-composition-health-incident-task-contract.md)
- 参考：`deepseek-harness@47f943859bef60e4160492346772ded9b24f765a` 的 Cordis event 与 `packages/core/tools/src/index.ts::notifyResult`

## 1. 目标

在既有 `emit / serial / parallel / executor` 之外补两个窄语义，不引入通用 waterfall：

```text
TransformEventKey
original ──listener 1──> value 1 ──listener 2──> value 2 ──> caller
           sync/async       必须同类型       注册顺序固定

ObserveEventKey
frozen settled fact ──┬──> observer 1 ──failure──> Incident
                      ├──> observer 2 ───────────> complete
                      └──> observer 3 ───────────> complete
                                     最终事实不被改写
```

## 2. Transform 合同

- `TransformEventKey(name, payload_type, payload_contract)` 用真实 type 做 Root 内运行时校验，用显式稳定 token 进入 topology identity。candidate clone 与 formal import 使用同一个 token；同名事件不能用不同 mode、type 或 token 注册。
- `await ctx.transform(key, original)` 在零 listener 时返回 original 本身。
- listener 按注册顺序执行，可同步或异步；每一个必须返回 `payload_type` 实例，返回 `None`、`Bail` 或其他类型都 fail-loud。返回原对象表示显式 pass；Core 不做 deep copy。
- listener exception 记录所属 Fiber 的 `transform_failure` Incident，并立即传播；后续 listener 不执行。
- Transform 不拥有 authorize/deny 语义；Tool 的 prepare、authorize 和 legacy 夹心顺序由后续独立 PR 接入。

## 3. Observe 合同

- `await ctx.observe(key, payload)` 先按注册顺序调用全部 sync/async observer，再等待所有 awaitable settle。
- 单个 observer 的普通 sync throw、async rejection 或 self-cancellation 记录所属 Fiber 的 `observer_failure` Incident；不传播、不停止其他 observer、不产生返回值。同步 `KeyboardInterrupt`、`SystemExit` 等进程级终止会关闭此前返回但尚未启动的 awaitable 后传播；关闭失败记 `observer_cleanup_failure` Incident，但不能覆盖原终止异常或阻断后续清理。异步进程级终止会取消并排空其余已启动 observer 后传播。
- 调用方自身 cancellation 仍取消并排空尚未完成的 observer，然后传播 cancellation；不能把真正的 turn cancellation 伪装成 Incident。
- Observe 不承诺冻结任意 Python 对象；领域 owner 必须在 dispatch 前提供 immutable/frozen payload。R10 的 `tool.result` 将遵守这一边界。

## 4. 范围与验证

- 只改 `agent/plugin_composition/**`、事件测试与本文；不接 legacy EventBus、ToolExecutor、Phase、正式 workspace 或外部插件。
- oracle：transform 零 listener、同步/异步链、错误类型、异常 Incident、冻结 listener list；observe 先调用完整 callback 列表再 settle、全 listener、失败隔离、caller cancellation；event contract conflict 与 topology descriptor。
- targeted：composition events/kernel；cumulative：全部 plugin 相关回归、Basedpyright、compileall、`git diff --check`、公开 Change Gate。
- 停止条件：observer failure 改写 caller result、transform 隐式接受 `None/Bail`、同名不同 payload contract 共存、cancellation 留下 task。
- 回滚点：Git tag `backup/plugin-transform-observe-r4-before-20260815`。

## 5. v2 清理关联

R4 不新增 v2 compatibility。后续 R10 用新 key 接入 ToolExecutor 时保留明确迁移夹心；所有 legacy hook consumer 迁移完成后，物理删除 v2 ToolHook 与夹心顺序，不保留 deprecated alias。

## 6. 验证证据

- 定向：`75 passed`（composition events + kernel）。
- 累计：`552 passed`（plugin、tool executor、runtime inspection 相关回归）。
- 静态：Basedpyright `0 errors, 0 warnings`；`compileall` 与 `git diff --check` 通过。
- 只读复审：Terra 最终结论无 P0/P1；覆盖 callback/settle 顺序、进程级终止、caller cancellation、Incident 归属和自定义 awaitable cleanup。
