# Stage 4 研究与自查记录

## 研究对象

- 主仓库源码基线：`F:/agent/akashic-agent-d565`，HEAD `d565ca44356b5d52fe8d00f8cb008c14fff56c0b`。未使用当前主工作区迁移实现，也未运行测试或新评测。
- 工具与执行：`agent/tools/registry.py`、`agent/tools/tool_search.py`、`agent/core/passive_turn.py`、`agent/core/runtime_support.py`、`agent/tools/executor.py`、`agent/mcp/client.py`、`agent/plugins/mcp_generation_host.py`、`agent/plugins/composition_generation_host.py`。
- 上下文：`plugins/compaction/engine.py`、`plugins/compaction/plugin.py`、`plugins/compaction/runtime.py`、`session/manager.py`、`agent/context.py`、`agent/prompting/assembler.py`、`tests/test_context_compaction_contract.py`、`tests/test_session_compaction_runtime.py`。
- 相关运行材料和边界：`docs/design/runtime-model-registry-and-onboarding.md`、`docs/refactor/clean-code-ledger.md`、`docs/benchmark` 未作为新实验运行。
- Observe 研究入口：`drafts/stage4-execution/observe-research/`。当前内容对应 canonical Observe `main` commit `8913be0e0cda9b7a71682b48f8b304ee334d628c`，manifest `version=2.0.0`、`api_version=3`；历史 artifact `1.2.0` 对应 `4d85b9dc64ef0d8d96c5a635586ca17dd94b59cd`。兼容性按服务键和 Message runtime 核对，不按 API 标签或日期推断。

## 核心事实核对

- 工具集合明确分为 `registered`、`visible`、`executable`；目录搜索是元数据/关键词搜索，不是向量检索。
- 初始工具来自 always-on、显式 preload 和 session LRU；`max_tool_schemas`、`max_unlocked`、`preloadable`、`requires_turn_search` 分别约束容量、单次解锁、跨轮预加载和当轮执行授权。
- 搜索命中的 `matched` 不等于解锁；真实 identity 进入下一次 provider Schema 的 `unlocked` 才可调用，超额项为 `capacity_limited`。旧稿中的 `max_tool_schemas=2` 回归场景保留。
- compaction gate 估算完整 `messages + tools`；历史参数为软阈值 `0.74` 和近期约 `20,000` token。摘要按完整历史单元生成/增量更新，摘要 metadata 有 generation、parent generation、source ref；原始历史保留但工具结果长度不应写成无损永久保存。
- 摘要使用 session-selected/default model 绑定，`tools=[]`、关闭 reasoning；输入 chunk 按完整 unit 预算，超窗时二分缩小；业务 `ContextLengthError` 最多一次压缩重试。摘要正确性与协议闭合、容量恢复是不同问题。
- Observe 从 Message `model.facts.call_record_id` 回读 Models usage，Turn 聚合所有成功且 exact 的调用；命中率是 `sum(cached_input_tokens)/sum(input_tokens)`，按 token 加权。已有 usage 对账不是对照实验。

## 未决问题和正文限定

- Observe 研究副本对“input 已知但 cached_input_tokens 缺失”可能按 0 计入命中率分母，这是统计口径风险；正文已明确，未修改插件。
- Observe 2.0.0 依赖后续 Message runtime，不能直接说与 d565 兼容；正文区分了 1.2.0 历史 artifact、2.0.0 当前研究副本和 d565 基线。
- Observe 的 `wake` 映射代表 Wake-derived 主动链路，不等于 Wake 内部 content/drift/alert 的细分；正文没有扩大该结论。
- 没有把 token/cache 记录写成节省或提升，也没有把 4 Turn 对账、看板 delta、被动/主动曲线写成 control/treatment 实验。
- 没有新增测试、评测、数据库或插件代码；没有修改旧 approved、正式指导书、简历或业务代码。
