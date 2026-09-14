# 章节证据包：统一 ReAct 执行骨架

> 本包是章节事实底稿：数据、机制、踩坑、源码锚点与禁止推导。写作与验收以 `../00-README.md` 为准，通用主张边界见 `../01-fact-boundaries.md`；本包不是封闭事实清单。

## 1. 历史版本边界

- 正文使用 `d565ca44356b5d52fe8d00f8cb008c14fff56c0b`。
- 对外入口是 `agent/looping/core.py::AgentLoop`，其中 `_process` 负责一次输入处理，`_react` 进入具体被动执行。
- 核心执行在 `agent/core/passive_turn.py` 中组装请求状态、工具可见集合、上下文压缩、provider 调用、工具执行和终态输出。
- `bootstrap/tools.py::CoreRuntime` 在该基线仍暴露旧 `loop` 与 `session_manager`，供历史 runner 使用。
- 不要将当前 Message runtime 的类名、路径和状态结构写入正文。

## 2. 正常请求的执行参考

```text
接收输入并进入 session lane
  → 读取会话历史与本次运行约束
  → 组织稳定指令、历史/摘要、动态记忆和可见工具 Schema
  → 调用模型
      ├─ 返回工具调用：校验可见性与权限 → 执行 → 保存结果 → 下一轮判断
      └─ 返回有效正文且无工具调用：形成终态回答
  → 记录使用过的工具、token/缓存统计和最终输出
  → 根据渠道边界处理发送或内部返回
```

需要明确：保存的是用户输入、模型输出片段、工具调用参数和工具结果等可观察事实，不是模型完整内部思维链。

## 3. 关键设计判断

### 模型负责建议，宿主负责效果

- 模型只能调用当前可见的工具。
- 工具执行前还需要参数准备、权限检查和禁用列表检查。
- 工具结果以对应调用身份回填，供下一轮模型判断。
- 这样可以区分“模型想做什么”和“程序实际执行了什么”。

### session lane 保证同一会话的顺序边界

历史基线通过 `SessionLaneRegistry` 管理同一 session 的处理顺序，避免两个输入无约束地同时修改同一段对话状态。代价是慢工具可能阻塞同一 lane，需要通过中断和状态记录处理，而不是盲目并发。

### 循环上限不能切断已产生的工具调用

运行时从配置读取 `max_iterations`；达到上限后不再发起新的模型判断，但工具调用和结果边界必须保持完整。配置值只是成本/复杂度保护，不是效果最佳参数。

### 被动回答与主动任务复用执行能力但不复用授权

历史 `process_direct_message` 可承载内部/评测输入，Wake 也使用受限 scoped ReAct；但主动任务拥有独立工具集合和结构化终态，普通回答正文不能自动获得发送权限。

## 4. 踩坑候选

### 工具调用边界在容量重试中被破坏

- 表象：重试后的上下文可能只保留 tool result 或只保留 assistant tool call。
- 根因：按消息条数裁剪，没有把调用与全部结果视为闭合批次。
- 修复：容量重试和压缩都以完整逻辑交互/工具批次为边界。
- 证据提交：`31c129bd`、`c3e83d59`。

### 用户中断不等于外部效果被回滚

- 表象：新输入到来后旧模型请求或工具仍可能完成。
- 根因：取消协程只能停止本地等待，不能证明 provider 或外部系统没有产生效果。
- 修复判断：旧任务停止继续决策；已开始的工具结果仍需结算。不要承诺任意工具 exactly-once。
- 历史锚点：`agent/looping/interrupt.py::ActiveTurnState`、`AgentLoop._run_inbound_turn`。

### 生成完成不等于送达成功

- 历史 runtime 对渠道发送和 durable delivery 有独立边界。
- 发送未知时不能重新调用模型生成一个新答案来掩盖投递问题。
- 只说明状态分离，不声称所有渠道都能自动恢复。

## 5. 与四条简历能力的接口

- 记忆：在请求前提供自动材料，在执行中提供 `recall_memory` 等补搜工具。
- 工具/上下文：决定哪些 Schema 和消息进入每次 provider 请求。
- 主动交互：复用判断和工具执行，但增加准入、调查预算和终态发送授权。
- 端到端评测：让真实 Agent loop 在隔离环境中完成任务，并由外部 Verifier 判定。

## 6. 结果边界

可以说：统一了模型判断、工具执行和观察回填的执行骨架，并为恢复、归因和能力组合提供可观察边界。

不能说：任意外部工具都恰好执行一次；取消能够撤销已经发生的副作用；保存了完整思维链；默认循环次数是经过效果最优实验得到的。

## 7. 源码与提交锚点

- `agent/looping/core.py::AgentLoop/_process/_react/process_direct_message`
- `agent/looping/session_lane.py::SessionLaneRegistry`
- `agent/looping/interrupt.py::ActiveTurnState`
- `agent/core/passive_turn.py`
- `bootstrap/tools.py::CoreRuntime/build_core_runtime`
- `31c129bd`：保留 context retry 中的工具调用边界
- `c3e83d59`：按完整逻辑交互处理 Akasha 与压缩
