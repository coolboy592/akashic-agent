# 阶段 4：工具发现与上下文管理

## 从“注册了工具”到“本轮能调用”

简历里“统一注册本地与 MCP 工具、按需暴露 Schema”解决的不是工具如何写，而是工具规模增长后，怎样把能力放进每次模型请求。这里至少有三个不同集合：`registered` 是宿主已经加载、能按真实 identity 找到定义和执行入口的工具；`visible` 是当前 provider 请求实际携带 Schema、模型可以提出调用的工具；`executable` 则还要满足当前任务、权限、参数和运行状态检查。注册不代表本轮可见，可见也不等于执行一定放行。

本地工具在启动 wiring 时注册到共享 `ToolRegistry`。MCP 工具的过程更完整：generation 启动时与 server 完成 `initialize`、`notifications/initialized` 和 `tools/list`，校验名称、描述、object input schema、重复名以及恢复后的契约是否漂移；就绪后把路由包装成普通工具注册到当前 generation 的 registry。正式 generation 暴露 server 返回的工具，candidate generation 则受声明的 allowlist 约束。MCP 工具的真正执行仍经过工具准入、registry、generation 绑定的路由、MCP client，最后才是 JSON-RPC `tools/call`。因此“支持 MCP”在这里包括发现、契约校验、路由绑定和执行链，不只是把远程名称列出来。

## 目录、预加载与当轮解锁

完整 registry 之上维护的是工具目录，而不是另一份可执行接口。目录记录名称、说明、来源和预加载等元数据，搜索使用关键词或 `select:工具名` 精确选择，不能称为 embedding 语义检索。`get_deferred_names` 会从注册集合中排除 meta 工具和当前可见集合，形成未加载目录；被 provider 容量裁掉的工具也可以在目录中被找到。

初始请求通常只放三类工具：必须保持可用的 `always_on` 工具（包括 `tool_search`）、显式要求预加载的工具，以及会话级 LRU 中近期使用且允许预加载的工具。历史实现默认每个 session 最多保留 5 个 LRU 项；它只记成功搜索解锁或成功执行的、可 `preloadable` 的普通工具，排除 `tool_search` 和不可预加载工具。LRU 是减少高频工具重复搜索的体验优化，不是全局权限或永久解锁。

随后按 `provider.max_tool_schemas` 生成 visible projection。没有上限时可视为无限；有正整数上限时按投影顺序裁剪。通常 `always_on` 和最近 LRU 优先，但 always-on 自身也可能因上限过小而被裁掉，不能笼统地说所有常驻工具永远可见。若搜索功能关闭而 Schema 本身超过 provider 上限，基线实现直接报配置错误，不替系统静默裁剪。

模型发现工具时调用 `tool_search`。搜索返回的 `matched` 只是给模型看的命中信息，真正决定后续请求的是绑定到 registry 的 identity 以及 `unlocked` 集合。系统在当前 session、turn、attempt 的 search scope 中登记授权；有 `requires_turn_search` 的工具必须在同一 attempt 重新搜索，光知道名称、被 scoped preload 或出现在历史摘要里都不够。一次搜索最多解锁 `max_schemas - 1` 个工具，为 `tool_search` 留出槽位；剩余命中标为 `capacity_limited`。

解锁不会只修改搜索文本。下一次 ReAct provider 请求重建 Schema 投影，必要时淘汰旧的 visible 项，为本次真实解锁的工具释放容量。只有当新工具的真实 Schema、必填字段和参数约束进入这个请求，模型才拥有可靠地产生结构化调用的条件。调用提出后仍依次经过 visible 检查、grant、输入准备、执行授权、真实调用和结果观察；工具结果回填后才继续下一轮。

这个状态闭环解释了一个实际的修复案例。旧逻辑曾让搜索结果写出工具名称，模型看起来已经“找到”工具，但下一次投影仍优先保留旧的 always-on 工具，导致 provider 收到的 Schema 没有新工具。修复不是让模型多读一段文字，而是让搜索命中绑定真实 identity，记录当轮解锁，重建投影时给新解锁项显式优先级，并把容量不足的命中标成 `capacity_limited`。已有回归场景设置 `max_tool_schemas=2`：初始请求是 `tool_search + always_a`，选择 `selected_tool` 与 `overflow_tool` 后，下一请求变成 `tool_search + selected_tool`；前者执行一次，后者不执行且留在容量受限集合中。这个验证证明的是 Schema 投影和执行边界正确，不是工具搜索带来了任务成功率提升。

## 按需暴露的成本边界

全量 Schema 的优点是请求少一个发现步骤，工具少、定义短且变化不大时最简单；代价是每轮都支付所有描述和参数的上下文成本，工具增长还可能碰到 provider 的 Schema 数量或请求大小限制。目录加当轮解锁把初始 payload 控制在较小范围，但会增加目录信息、一次搜索调用、下一轮请求以及发现状态管理的成本。工具很多并且任务只用其中一小部分时，这个交换通常更有价值；工具集很小或每轮都需要大多数工具时，全量暴露可能更合理。没有同一任务集的对比，不能把“按需”直接写成 token 节省率。

每轮只放最少工具也不是总是最优：命中质量差会增加搜索轮次，频繁使用的工具还会反复发现。LRU 预加载是在两者之间利用会话局部性的方案，但它占用 Schema 槽位，所以仍受容量和 `preloadable` 约束。另一个独立边界是执行授权：降低可见工具数量可以控制模型选择空间，却不能替代权限、风险和副作用检查。

## 一次请求如何容纳历史和工具

工具投影和历史压缩最后汇合在 provider payload。基线请求按稳定 system 指令、历史投影、context frame、当前 user 的结构组装；动态 Akasha 记忆和其他运行材料放在稳定历史之后，避免每轮变化的内容打散更长的稳定 Prompt 前缀。当前可见工具 Schema 也属于完整请求，而非历史之外的“免费配置”。因此水位估算必须覆盖消息、摘要、稳定指令、动态材料和工具 Schema。

正式 compaction gate 使用当前 bound model 的 `estimate_context_tokens(messages, tools)`：历史实现中的不同 driver 采用不同的字符近似系数，一类按 JSON 字符数约除以 3，另一类约除以 4；provider 返回精确 input usage 后，同一 Schema 集合的后续轮次可以用精确基数加新增内容估算。另一个 `/3` 的消息生命周期指标不包含 Schema，不能与压缩 gate 混为一谈。`SOFT_LIMIT_RATIO = 0.74` 是基线采用的软触发参数，未知 context window 时可以估算但不主动压缩，也不进行 overflow retry；它不是所有模型的最优比例。

## 模型摘要与完整交互单元

本项目采用的是“较旧历史生成或更新模型摘要，加上近期完整内容”的组合，不是单纯滑动窗口。固定条数裁剪既不能反映消息和工具结果的真实 token 大小，也可能把 assistant 的一批 tool call 与对应 result 拆开。基线将连续的逻辑交互聚合成 `SessionHistoryUnit`（在压缩接口中作为 `CommittedContextUnit` 使用），包含来源序号、消息 ID、渲染消息和数据库引用；tool-chain 会重新展开为 assistant tool calls、全部 tool results 和必要的后续 assistant 消息。只有批次闭合后才可提交。仍在运行的 shell 也会限制压缩切点不能越过其起始单元。

生成压缩投影时，generation 0 先从最近历史向前挑选完整 unit，保留尾部至少约 `KEEP_RECENT_TOKENS = 20_000`，并至少留下一个 unit；正式持久摘要至少需要两个候选单元。更旧的 SessionDB 记录不因这次投影消失。当前 turn 的闭合批次可以形成临时摘要，但未闭合的 pending/current query 保留；如果旧历史压缩后连完整当前任务都已低于边界，也不额外生成临时摘要。

摘要调用先使用当前 session 选择的模型，再使用冻结 execution 的 default model；不会因为重复 binding 无限重试。请求不带工具（`tools=[]`），关闭 reasoning，且不设置输出上限字段（基线用 `max_output_tokens=0` 表示不发送该字段）。这意味着“没有独立固定输出预算”本身是一个实现局限，摘要输入仍需先在完整 unit 边界内做预算控制：系统用二分寻找能容纳的最大连续前缀，provider 报超窗时对当前 chunk 对半缩小，绝不拆单个 unit。后续 chunk 以已有摘要作为更新基底，而不是每次从所有历史重新生成。

摘要提示词不是泛泛要求“总结一下”。它要求固定的九个 Markdown 标题，并保留路径、命令、错误、数值、外部效果和 active execution；返回标题必须精确匹配。摘要元数据记录 `generation`、`parent generation` 和 `source ref`，说明版本演进及覆盖的原始范围。这样能把摘要当作面向模型的投影，同时让系统知道该回查哪里；但它不能保证语义无损。协议完整只说明 provider 看到合法的消息序列，不说明摘要没有漏掉任务约束、精确参数或事实关系；多次更新还可能累积失真。

因此压缩后的请求会重新组装并检查边界、Schema 容量和必要的当前锚点。若业务 provider 明确报告 `ContextLengthError`，基线最多进行一次 `force=True/context_overflow` 压缩后重试，第二次错误原样传播；普通 Schema capacity、认证、限流、无效参数和服务错误不进入这条路径。重试解决的是容量或估算误差，不是自动修复摘要语义。

摘要替换的是 provider 的模型视图，不是 SessionDB 的事实来源。原始 user/assistant 消息和执行记录可按 source ref、message ID 回查，`search_messages` 返回预览和 source ref，`fetch_messages` 才取原文及邻近上下文。不过应准确表述为“结构和来源可回查”，而不是承诺所有工具结果字节永久无损：历史投影和 tool-chain 入库路径对单个结果有长度限制。原始记录、摘要遗漏和 provider 容量错误是三类不同问题，分别需要回查、语义校验和有限重试处理。

## 观测开销，而不是把观测当成优化证明

简历所说的输入 token 和缓存命中记录，需要依靠 provider usage 的真实来源。历史运行时的模型调用账本在请求前建立 call record，成功时写入 provider usage，失败或取消时保留失败状态而不把未知 usage 写成零。多个模型调用 driver 将 provider 的 input/output、cached input、cache write 和 reasoning 字段归一化为 `ModelUsage`，并标记整体 coverage。这个调用记录再通过模型输出中的 `model.facts.call_record_id` 关联到闭合 Message Turn。

本次核对的 Observe 研究副本是 canonical `main` 当前内容，对应 commit `8913be0e0cda9b7a71682b48f8b304ee334d628c`，manifest 版本为 `2.0.0`、API version 为 3。它从 Message 前缀投影闭合 Turn，收集每个 Output 的 call ID，再从 `models.call-history.v1` 读取账本；一个 Turn 的 input、cached input 和 output 分别对所有成功且 usage 为 exact 的调用求和。Turn 按 Message source 映射为 `agent`、`proactive`（`wake`）或 `drift`，并写入 Observe SQLite。`turns` 是明细真相表，`model_calls` 是调用镜像，`kv_cache_totals` 是可由 turns 重建的派生投影；cursor、receipt 和唯一 identity 用于幂等重扫，闭合前的 open Turn 不推进 cursor。

Dashboard 的缓存命中率是 token 加权而不是逐 Turn 比例的平均：

```text
cache_hit_rate = sum(cached_input_tokens) / sum(input_tokens)
```

分组时，被动统计取 `source = agent`，主动统计取 `source in {proactive, drift}`，时间序列也在桶内先求 token 总量再计算比例。分母为零显示未知而不是 0%。移动端近期指标则从最近的被动 Turn 明细重新聚合，不等同于历史累计总览。Observe 还展示近期 Turn、迭代次数、输入/输出 token、主动/被动趋势和运行健康状态；这说明采集、存储和展示链路已经存在。

这里有两个必须保留的边界。第一，Core 的 `coverage=exact` 主要说明 input/output request coverage，并不单独保证 `cached_input_tokens` 存在；当前 Observe 研究副本在 input 已知而 cache hit 缺失时可能将缺失 hit 转为 0，进入 tracked 分母，因而有显示为 0% 的风险。更严谨的报表应要求 cache 字段本身可用，或单独展示 cache coverage。第二，`wake` 是 Wake-derived 的主动集合，Wake 内部的 content、drift、alert owner 在当前生产写入中统一使用 Message source `wake`，所以不能从该看板进一步声称已经区分三种 owner，也不能把 RAG 的 explicit/passive caller 维度和 Turn 的主动/被动维度混为一谈。

这份 Observe 2.0.0 不能直接倒灌为 d565 基线的实现。它依赖后续 Message runtime、`models.calls.v1`、`models.call-history.v1`、Turn projection 等服务，当前 CI 固定到后续 Core ref；直接安装在 d565 原始运行时并不兼容。历史 Observe 1.2.0 artifact `4d85b9dc64ef0d8d96c5a635586ca17dd94b59cd` 可以作为旧 lifecycle 下的运行记录线索，但不能和 2.0.0 的 Message 投影接口混写。已有材料中的 4 个 terminal Turn 对账（input 总计 49,653、cache hit 总计 14,976）证明 usage 在多个存储间一致，不是上下文分层或压缩优化的 control/treatment 实验；Dashboard 的 bucket delta、主动/被动两条曲线和单次看板同样不能证明命中率提升。

## 如何评价这套设计

这套设计把两个增长问题放在同一个请求边界里处理。工具侧保留完整注册能力，却只把符合任务、当轮搜索和容量约束的 Schema 暴露给模型；历史侧保留可回查来源，却用摘要和近期完整单元控制模型视图。它的主要收益是边界和恢复语义清楚：搜索成功必须落到下一次真实 Schema，工具批次不能被截断，provider 容量错误不会无限重试，动态记忆也不会无意打散稳定前缀。

代价同样明确：目录搜索增加轮次和 token，LRU 需要维护局部状态，Schema 投影增加跨请求一致性，摘要调用有额外模型成本并可能遗漏或累积失真，完整 payload 估算只是 provider-specific heuristic。小工具集可以选择全量 Schema；短对话可能不需要摘要；强一致事实仍需从 source ref 回查。当前已有的是实现、回归合同、usage 对账和观测看板，尚未完成在同一任务集上比较全量 Schema 与按需暴露、不同预加载策略、有无压缩或不同分层顺序的对照实验，因此不应把这些机制写成已经取得某个 token、缓存、延迟或任务成功率提升。
