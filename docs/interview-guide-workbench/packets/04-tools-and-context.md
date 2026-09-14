# 章节证据包：工具发现与上下文治理

> 本包是章节事实底稿：数据、机制、踩坑、源码锚点与禁止推导。写作与验收以 `../00-README.md` 为准，通用主张边界见 `../01-fact-boundaries.md`；本包不是封闭事实清单。

## 1. 对应简历

> 工具发现与上下文管理：统一注册本地与 MCP 工具，通过目录检索、当轮解锁及常用工具预加载按需暴露 Schema；分层编排稳定指令与动态记忆，按模型窗口水位压缩历史并保留完整交互单元，支持工具扩展与长对话执行，并记录输入 token、缓存命中率用于开销分析。

## 2. 解释工具流程时需要区分的三个集合

1. **registered：** 宿主已经加载并能根据真实 identity 找到的工具。
2. **visible：** 当前 provider 请求中实际携带 Schema、模型可以选择的工具。
3. **executable：** 在当前任务、权限和状态下真正允许产生效果的工具。

registered 不等于 visible；visible 也不等于一定可以越过执行权限。

这是准确解释的依据，不要求题目或正文先背诵三个术语再进入技术内容。

## 3. 工具按需暴露链路

```text
统一注册本地/MCP 工具
  → 形成未加载工具目录
  → 初始请求只带 always-on + LRU/显式预加载工具
  → 受 provider.max_tool_schemas 限制生成可见投影
  → 模型调用 tool_search(query)
  → 目录命中绑定真实工具 identity
  → grant_current_turn_search 在当前 attempt 授权
  → 后续实际模型请求纳入容量内的命中工具 Schema
  → 模型可以调用已进入可见集合的工具
```

关键实现事实：

- `ToolRegistry.get_deferred_names` 用全量注册集合减去 meta 工具和当前可见集合构造目录。
- `ToolSearchTool` 支持 `select:工具名` 和关键词搜索。
- `max_unlocked` 保证新解锁工具不会突破 provider Schema 上限。
- `ToolDiscoveryState.get_preloaded_ordered` 提供会话级 LRU 顺序。
- `preloadable=False` 的工具不能因为近期使用而自动预加载。
- 搜索是基于工具元数据/关键词的目录搜索，不要称为向量语义检索。

## 4. 上下文组织与压缩链路

```text
稳定指令
  + 已提交历史/已有摘要
  + 当前请求锚点
  + 动态记忆和其他运行材料
  + 当前可见工具 Schema
        ↓
估算完整 provider payload
        ↓ 超过 0.74 × context window
选择完整逻辑交互单元，将较旧部分生成/更新摘要
        ↓
保留最近约 20,000 token，重新组装并校验
```

关键事实：

- 历史实现常量为 `SOFT_LIMIT_RATIO = 0.74`、`KEEP_RECENT_TOKENS = 20_000`。
- 水位估算包含消息和工具 Schema，不只计算纯文本历史。
- 已提交工具批次必须在 assistant tool call 与全部 result 闭合后记录。
- 压缩选择完整 `CommittedContextUnit`，不从工具调用中间切开。
- 摘要有 generation、parent generation 和 source ref；摘要是投影，原始历史仍由会话存储保留。
- provider 仍报容量错误时，重试必须有上限，不能把所有错误都当成 context overflow。
- 动态 Akasha 记忆放在稳定历史之后，以保留更长的稳定 Prompt 前缀。

## 5. 关键取舍

### 全量 Schema vs 目录加当轮解锁

全量 Schema 不需要搜索，但工具增长会持续挤占上下文并触碰 provider 上限。目录模式节省每轮暴露量，却多出一次发现步骤，并要求搜索结果与真实工具身份可靠绑定。

### 每轮只带最少工具 vs LRU 预加载

完全不预加载会让高频工具重复搜索；LRU 降低常用工具摩擦，但会占用 Schema 槽位，因此仍需 provider 上限和 non-preloadable 约束。

### 滑动窗口 vs 模型摘要与近期保留

窗口裁剪实现简单、能保留近期原始消息，但窗口外的信息不再直接可见。模型摘要可保留部分较早信息，代价是一次摘要调用和信息失真风险。本项目已有材料描述的是较旧完整交互生成/更新模型摘要、近期内容保留、原始记录可回查；不能仅用“不拆工具调用链”替代对压缩技术本身的解释。

### 删除旧消息 vs 摘要投影

删除实现简单，却损害恢复和原文取证；独立摘要保留原始事实，但需要维护摘要代际、范围和验证。

### 固定条数裁剪 vs 逻辑交互单元

固定条数便宜，却可能拆开 tool call/result；逻辑单元实现复杂，但保持 provider 协议和恢复语义。

## 6. 踩坑候选

### 搜索结果只有文字，没有可执行 Schema

- 表象：模型看到某工具名称后立即调用，却被判定为 Schema 不可见。
- 根因：搜索结果只是自由文本提示，没有进入受 provider 上限约束的真实工具投影。
- 修复：结果绑定真实 identity，通过当前 attempt 授权，并在下一轮可见集合中为新工具释放 Schema 槽位。
- 提交：`981b51b1`。

### 上下文裁剪破坏原始历史

- 表象：为了缩短请求，原消息本身被修改/丢失，后续无法回查。
- 修复：压缩模型视图而不是删除事实来源。
- 提交：`2c3a8f28`。

### 容量重试切断工具协议边界

- 修复演进：`31c129bd`、`064602b2`、`c3e83d59`。
- 重点讲清“为什么必须等整个工具批次闭合”，不要堆提交号。

### 摘要重复当前任务或突破自身输出预算

- 摘要生成也受输入/输出窗口约束；重复当前任务会浪费空间并引入冲突。
- 提交：`e17cb95f`。

### 动态记忆破坏稳定缓存前缀

- 根因是每轮变化的 Akasha 内容出现在稳定历史之前。
- 修复为调整 Prompt 分层顺序。
- 提交：`403e6924`。

## 7. 已有数据与边界

局部微基准：

- 2,000 docs / 4 keywords 下，`_score` 中位数 `5.000025 → 4.898831 ms`（约 `-2.02%`）。
- `_explain` 中位数 `3.551934 → 2.672931 µs`（约 `-24.75%`）。
- 完整 search 微基准受调度噪声影响，没有稳定收益。

可以说系统记录输入 token 与缓存命中信息用于开销分析；不能给出没有记录支持的端到端节省率或缓存提升比例。记录功能、运行统计、优化效果对比应分别说明。

用户补充的 [observe](https://github.com/akashic-plugins/observe) 是必须考虑的相关实现入口。公开 README 描述了缓存效率视图、近期 KV Cache 命中率、主动/被动链路差异和 Turn 明细。

### Observe 观测插件核对结论（2026-09-13 执行期核实）

- 研究副本为 canonical main commit `8913be0e0cda9b7a71682b48f8b304ee334d628c`（manifest `2.0.0`、`api_version=3`）；历史 artifact `1.2.0` 对应 `4d85b9dc64ef0d8d96c5a635586ca17dd94b59cd`。
- Observe `2.0.0` 依赖后续 Message runtime、`models.calls.v1`/`models.call-history.v1` 与 Turn projection，与 d565 基线不直接兼容，不能倒灌为基线实现；`1.2.0` 只作旧 lifecycle 的运行记录线索。
- 命中率口径为 token 加权：`sum(cached_input_tokens)/sum(input_tokens)`，分组与时间桶内先求 token 总量再算比例，不是逐 Turn 比例平均；Turn 聚合只计成功且 usage 为 exact 的调用。
- 口径风险：input 已知而 cached_input_tokens 缺失时，缺失 hit 可能按 0 计入分母，报表存在显示偏低的风险；`wake` 映射代表 Wake-derived 主动链路整体，不区分 content/drift/alert owner。
- 4 个 terminal Turn 对账（input 合计 49,653、cache hit 合计 14,976）证明 usage 在多个存储间一致；对账不是对照实验，不能证明命中率提升。

## 8. 需核实的统计与实验方向

先查主仓库与 `observe` 已有记录，确定哪些只是尚未整理、哪些确需对比实验；不要再把缓存观测笼统列成待实现功能。

- 全量 Schema 与按需暴露在相同任务集上的输入 token、工具成功率和总轮数。
- 有无压缩时的长对话完成率、摘要遗漏率和原文补搜次数。
- Prompt 分层调整前后的可比缓存命中和成本；单次命中率或已有看板不能直接证明分层调整带来提升。

## 9. 源码与文档锚点

- `agent/tools/registry.py`
- `agent/tools/tool_search.py`
- `agent/tools/search_backend.py`
- `agent/core/passive_turn.py::_initial_visible_tools`
- `plugins/compaction/engine.py`
- `plugins/compaction/runtime.py`
- `docs/refactor/clean-code-ledger.md`
- [observe README](https://github.com/akashic-plugins/observe#readme)：缓存观测入口；版本核对结论见本包 §7
- 提交：`981b51b1`、`2c3a8f28`、`31c129bd`、`064602b2`、`c3e83d59`、`e17cb95f`、`403e6924`

## 10. 禁止推导

- 不得把关键词目录搜索称为 embedding 语义搜索。
- 不得说解锁永久修改全局权限。
- 不得把记录 token/cache 指标写成已经取得节省效果。
- 不得把 0.74 和 20,000 描述为适合所有模型的最优参数。
