# 插件组合 Health、Incident 与 Validation 任务合同（R3b）

- 状态：implemented / reviewed
- 日期：2026-08-15
- 实现基线：`3f60bd45`
- 关联条款：PLG-001～PLG-004、PLG-008～PLG-010、PLG-014、ERR-001
- 上游：[R3a identity/revision](plugin-composition-revision-task-contract.md)、[R2b candidate Root 隔离](plugin-candidate-root-isolation-task-contract.md)

## 1. 三类事实

```text
Validation（可晋升证明）
├── immutable topology hash
├── sealed composition revision
├── current required Health
├── write / external-effect observations
└── sealed incident sequence

Health（当前可恢复）              Incident（已发生诊断）
├── Fiber state/error              ├── Root-local monotonic sequence
├── required dependency            ├── owner / kind / message / error type
├── explicit ctx.health()           ├── stable recent buffer: 128
└── ctx.spawn task failure          └── candidate attempt cap: 1024
```

历史错误不再永久污染 readiness。candidate promotion 读取 candidate Root 自己的当前 Health；stable Root 的 Health/Incident 不跨 Root 影响候选。

## 2. 插件合同

- `await ctx.health(name, required=True)` 返回 Effect-owned `HealthHandle`。`degrade(reason)` 只改变当前 Health；`recover()` 恢复，不自动生成 Incident。
- `ctx.report_incident(kind, message)` 显式追加结构化 Incident，不隐式降级 Health。
- `ctx.spawn(coroutine, name=...)` 未捕获异常由 Core 记录 `task_failure` Incident，并降级所属 Fiber 的 task health；Fiber restart 卸载旧 task failure，重新 apply 成功后恢复当前 Health，Incident 保留。
- optional Health/Fiber 失败可观察但不阻止 readiness；required Fiber pending/failed、required Health degraded、candidate incident overflow 或 external observation 阻止 publication。
- Health handle 随 Fiber unload 自动注销；注销后的 handle 操作 fail-loud。
- `FiberHandle` 与 `HealthHandle` 都继承 Context 的同步 worker 边界；保存 handle 不能绕过 `ExecutorService` 的线程隔离。

## 3. Incident 与候选封存

- stable Root 只保存最近 128 条，丢弃最老诊断但 sequence 继续单调；buffer 轮转不改变当前 Health。
- candidate Root 保留本 attempt 最多 1024 条。超过上限设置 `incident_overflowed=True`，publication fail-loud，不能用截断后的 sequence range 伪装完整验证。
- candidate seal 记录 incident sequence。seal 后新增 Incident 即使未改变 Health，也使原 validation identity 失效并要求 fresh validation。
- installed candidate 在暂停 admission 并排空 lease 后，Core 先封存 exact validation Root，再立即 dispose 其 task/Effect，之后才允许进入 formal runtime rebuild。由此 candidate Health 不会被新 Root 清零，也没有“检查后仍继续运行”的 await 空窗。
- formal rebuild 仍独立检查新 Root 的当前 Health。唯一例外是 v3 composition fingerprint 未变、且重用 exact old stable Root 的 v2-only/payload-only publication：它携带 candidate Root 的验证证明，但不把旧 stable Root 的 Health 当成候选准入事实。
- candidate attempt 的 Incident 随隔离 Root 结束；本轮只把 sealed validation identity 带入 publication，不把 attempt-local 诊断复制进 formal Root。
- Incident 目前只在 Root 内存和 receipt 中，不新增 durable store；Runtime Inspection 投影由后续独立 PR 接入。

## 4. 验证与边界

- explicit required Health degrade/recover 不增 Incident；历史 Incident 不阻止已恢复 Root compile。
- optional Health/Fiber failure 不污染 required readiness。
- task failure → degraded + Incident；restart success → recovered，Incident retained。
- stable buffer bounded；candidate overflow fail-loud；seal 后 Incident 使 promotion 失败。
- manager 级 oracle 覆盖：candidate required Health degrade 后 promotion 拒绝，recover 后同一 candidate 可重试；candidate Incident overflow 拒绝；stable required Health degrade 不阻止健康的 v2-only candidate，晋升后继续重用同一 stable Root。
- targeted：composition kernel/events、loader/hot reload；Basedpyright、compileall、`git diff --check`。
- cumulative：R2 publication、R3a revision 与公开 Change Gate。
- 本地证据：全部 plugin 相关回归 `436 passed`；composition/loader/experiment targeted `86 passed`；Basedpyright `0 errors`；compileall 与 `git diff --check` 通过。
- Terra xhigh 两轮只读复审最终无 P0/P1；其 candidate formal rebuild、stable Health 隔离、external-effect exemption 与 SyncTask handle mutant 均已固化为 oracle。
- 停止条件：Incident 永久毒化 Health、stable Health 阻止 isolated candidate、overflow 静默丢失、task failure 无当前降级、recover 删除历史 Incident。
- 回滚点：Git tag `backup/plugin-composition-health-incidents-r3b-before-20260815`。

## 5. v2 清理关联

R3b 不新增 v2 compatibility。legacy lifecycle/job/proactive 的错误模型仍由原 manager 路径拥有；这些路径迁移为 v3 capability/Fiber 后，随 v2 contribution compiler 一起物理删除。
