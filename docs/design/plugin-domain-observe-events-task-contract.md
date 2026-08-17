# Core 领域 Observe 事件任务合同

- 状态：implemented / focused-tested
- 日期：2026-08-17
- 目标分支：`codex/plugin-v3-mobile-ui-query`
- 恢复点：`backup/observe-events-pre-20260817`
- 上游：[插件 Transform 与 Observe 事件任务合同](plugin-transform-observe-task-contract.md)
- 关联：[Turn committed typed event 合同](plugin-turn-committed-event-task-contract.md)

## 1. 目标

为已经存在的三个领域事实提供 Core-owned、request/generation-bound 的
`ObserveEventKey`，让 v3 插件可以监听同一对象，同时保持 v2 EventBus 的既有
消费顺序和 payload。Core 不复制领域 DTO，也不替插件拥有数据状态。

```text
settled domain fact
        │
        ▼
┌────────────────────────┐
│ legacy EventBus fanout │  先完成既有 handler
└────────────┬───────────┘
             ▼
┌───────────────────────────────┐
│ request-bound CompositionRoot │  ObserveEventKey
└────────────┬──────────────────┘
             ▼
        plugin observer
```

| 事实 | Key | Core owner | 当前生产入口 |
|---|---|---|---|
| `ProactiveFinished` | `proactive.finished` | `agent/turn_events/observe.py` | `EventBus.enqueue` → `fanout` |
| `RetrievalCompleted` | `memory.retrieval.completed` | `agent/turn_events/observe.py` | `DefaultMemoryRetrievalPipeline.retrieve` |
| `MemoryWritten` | `memory.written` | `agent/turn_events/observe.py` | `EventBus.fanout`（Memory2 supersede） |

payload 继续使用 `bus.events_lifecycle.ProactiveFinished`、
`core.memory.events.RetrievalCompleted` 和 `core.memory.events.MemoryWritten`；
桥接时传递原对象，不重新拼装事件。

## 2. 调度与 generation 边界

- `EventBus.fanout` 先等待已有 v2 handlers，再调用
  `observe_composition_domain_event`。无 v2 handler 时不能提前返回，仍要运行
  composition observer。
- EventBus 已绑定 request lease 时，桥接读取同一个
  `get_lifecycle_runtime_snapshot()`；candidate、stable 和旧 generation 不从
  全局 latest 重新选择。
- Retrieval pipeline 接收可选 EventBus。正式 wiring 传入主 EventBus，从而保留
  legacy fanout；没有 EventBus 的单元调用直接使用当前绑定的 composition Root。
- `TurnCommitted` 继续由 after-turn phase 自己在 legacy fanout 后发出，不能在
  这个通用 bridge 中重复发送。

## 3. Retrieval 事实

`DefaultMemoryRetrievalPipeline` 在 MemoryEngine 成功返回后从同一
`MemoryQueryResult` 形成 `RetrievalCompleted`：

- `rewritten_query`、`aux_queries`、`hyde_hypotheses` 和 `route_decision` 只从
  engine result 的 trace/raw 读取；缺失时使用原始请求或空集合；
- 每个 `MemoryRecord` 转成现有 `RetrievalHitSummary`，保留 id、kind、score、
  summary、injected、confidence/forced signals 和 metadata；
- engine 普通异常先发布带 `error` 的 settled event，再重新抛出原异常；取消
  不伪造完成事件，继续传播 cancellation；
- 没有 MemoryEngine 的旧无记忆路径保持空结果和 no-op，不写事件或持久状态。

## 4. 失败、清理与持久化边界

- 没有 composition Root 时保持旧路径 no-op；错误 task、释放 lease 或错误
  binding 由 lifecycle snapshot owner fail-loud。
- 普通 observer failure 仍由 `EventRegistry.observe` 记录所属 Fiber Incident
  并隔离；调用方取消、进程级异常和 bridge/lease 错误不能被吞掉。
- 三个 bridge 只在内存中 dispatch。candidate 不写正式 workspace、plugin-data、
  SessionDB、memory DB 或外部渠道；插件自身的派生写入仍必须使用 Core 分配的
  candidate data root，并由插件合同验证。
- 本变更没有删除、更新或迁移权威持久记录。旧 EventBus handler、队列 lease、
  Root Effect/listener 的清理语义保持不变。

## 5. 验收与 mutant

定向测试位于 `tests/test_plugin_composition_lifecycle.py`，覆盖：

- 三类事件的 legacy-before-composition 顺序和原对象 identity；
- 没有 legacy handler 时 composition observer 不被 early-return mutant 跳过；
- 两个绑定 Root 间的 candidate/generation 选择与 wrong-task fail-loud；
- Retrieval payload 字段、engine failure event 和原异常传播；
- leaf contract 在 fresh interpreter 中不加载 phase runtime。

验证命令：

```bash
./.venv/bin/python -m pytest -q tests/test_plugin_composition_lifecycle.py
./.venv/bin/python -m compileall -q agent/turn_events/observe.py \
  agent/lifecycle/composition.py bus/event_bus.py \
  agent/retrieval/default_pipeline.py agent/looping/core.py bootstrap/tools.py
```

回滚：恢复到 `backup/observe-events-pre-20260817` 或回退本次 Core 事件桥接变更。
