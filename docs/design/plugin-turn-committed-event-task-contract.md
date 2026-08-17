# Turn committed typed event 任务合同

- 状态：accepted / implemented
- 日期：2026-08-17
- 目标分支：`codex/plugin-v3-mobile-ui-query`
- 恢复点：`backup/c02-turn-event-pre-20260817`
- 上游：[0036](../decisions/0036-plugin-composition-keeps-promotion-owner.md)、[插件事件与同步执行能力合同](plugin-event-executor-task-contract.md)

## Goal

让 v3 插件从当前 Turn 冻结的 generation Root 监听已经构造完成的 `TurnCommitted` 事实。Core 只提供 phase-owned typed event；插件自己决定怎样投影、排队和持久化。现有 v2 EventBus、phase slot、SessionDB、发送与晋升行为保持不变。

```text
Session commit
      │
      ▼
┌─────────────────────┐
│ legacy EventBus     │  v2 observer 保持原顺序
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ turn.after_turn.    │  sync emit / request-bound Root
│ committed           │
└──────────┬──────────┘
           ▼
     plugin-owned logic
```

## Ownership and event contract

- `after_turn` phase 拥有 `turn.after_turn.committed` 的发生位置：旧 `TurnCommitted` fanout 完成之后、budget 日志与 `AfterTurnCtx` fanout 之前。
- payload 是同一个 `TurnCommitted` 对象，不复制字段或引入第二套 DTO。
- dispatch mode 是 `emit`：同步 listener 按 generation 内注册顺序运行，异常立即传播；异步 listener 在注册边界 fail-loud。
- 当前 request 没有 composition Root 时保持 no-op；继承到错误 task 或已释放 lease 的绑定 fail-loud。事件只从 request-bound snapshot 取得 Root，不读取全局最新 generation。
- listener 是所属 Fiber 的 Effect，reload、依赖消失和 dispose 后不残留。
- Core 不新增 Observe 领域 Service、priority、waterfall 或领域数据库接口。
- Proactive Feedback 使用独立的 Core-owned `ProactiveFeedbackCommitted` frozen DTO 与
  `ObserveEventKey("proactive.feedback.committed")`；producer 只在自己的 feedback DB commit 后
  `ctx.observe` 该 DTO。该 seam 只传递稳定 id、score/reason 与 bounded previews，不修改
  `TurnCommitted.extra`，Core 也不读取插件 DB，Emotion 不 import Proactive Feedback。

## Change and persistence

```yaml
change_type: feature
semantic_delta: compatible
capability_owner: core
consumer_scope:
  - v3 plugins observing committed passive turns
  - Observe
  - Proactive Feedback
runtime_patch: required
runtime_patch_reason: >-
  Turn commit position and request-bound generation identity are Core-owned facts;
  a plugin cannot reconstruct them from a global service safely.
authoritative_state_owner: >-
  Core owns TurnCommitted and snapshot binding; each plugin owns its derived state.
protected_state:
  - legacy EventBus handler order and v2 plugin behavior
  - phase slots, SessionDB write set and outbound dispatch
  - stable/latest promotion and snapshot lease ownership
  - formal workspace and plugin-data
allowed_effects:
  - generation-local listener registration and dispatch
  - temporary composition roots in tests
forbidden_effects:
  - formal runtime switch or hua-home writes
  - workspace, plugin-data, SessionDB or manifest writes
  - channel messages and external API calls
rollback: Revert this commit or return to backup/c02-turn-event-pre-20260817.
```

本变更不增加、更新、逻辑失效或物理减少权威持久记录，只在现有 Turn commit 路径增加 generation-local 内存 dispatch。

## Verification

- legacy EventBus 先完成，composition listener 随后取得同一个 payload；
- listener 失败立即传播，未绑定 composition Root 时旧路径保持 no-op；
- fresh interpreter 证明公开 leaf contract 不加载 `after_turn` phase runtime；
- wrong-task 与 inactive lease 继续由 lifecycle snapshot owner fail-loud；
- lifecycle、plugin generation、hot reload 与集中 Gate 保持通过。
