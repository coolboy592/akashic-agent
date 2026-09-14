# 章节证据包：主动交互策略

> 本包是章节事实底稿：数据、机制、踩坑、源码锚点与禁止推导。写作与验收以 `../00-README.md` 为准，通用主张边界见 `../01-fact-boundaries.md`；本包不是封闭事实清单。

## 1. 对应简历

> 主动交互策略：将推送决策拆为基于用户记忆的兴趣初筛与工具辅助的证据核验，在限定调查步数内决定发送或跳过；结合事件版本去重、兴趣衰减和唤醒阈值控制触发，支持不适合打扰时静默结束。

## 2. 已有机制与设计动机

### 背景与卡点

- 新内容不等于值得打扰。
- 仅靠标题或单次模型判断容易产生无证据推荐。
- 同一主题跨批次、跨会话可能重复出现。
- provider 超时或渠道回执未知时，简单重试可能造成重复发送。

### 判断

把主动链路拆成三个不同成本和权限的阶段：

1. 程序化准入：事件身份、revision、兴趣质量、衰减与阈值决定是否值得启动模型。
2. 模型两阶段决策：一步初筛，再在固定预算内使用工具调查证据。
3. 结构化终态：只有显式 share/skip 决定才能改变事件与发送状态；普通 final text 没有发送权。

## 3. Content 主动链路

```text
EventMail 接收不可变 revision
  → 以 source_id/item_id/revision 去重
  → 新 revision 首次计算兴趣分并进入衰减池
  → 池质量严格超过阈值且当前批次有新 revision，才具备 Content 唤醒依据
  → screen：一步、选择 1～8 条候选，只做兴趣初筛并记录待确认问题
  → investigate：最多 20 个 ReAct 输出，可 recall/fetch 核验证据
  → share：引用 1～5 个真实事件项，等待真实 delivery
     或 skip：零发送并结束本次选择
  → delivered 后才结算；unknown 保留不确定状态
```

历史参数边界：

- Content 质量半衰期约 36 小时。
- `WAKE_ADMISSION_FLOOR = 0.02`。
- pool threshold 为 1.0，必须严格超过阈值。
- screen 一步，最多选择 1～8 条候选。
- investigate 最多 20 个 ReAct 输出。
- share 最多引用 1～5 个事件项。

这些是历史策略参数，不是用户效果最优实验结论。

## 4. 状态与身份设计

- EventMail 使用 `(kind, source_id, item_id, revision)` 唯一约束；精确重复复用，内容冲突 fail-loud。
- 新 revision 可以成为新候选；旧 revision 不能仅因定时器再次扫描而恢复质量。
- snapshot/state version 用于并发转换约束。
- EventMail Content 选择状态负责候选生命周期：`pending/deferred` 候选可被领取为 `selected`；`share` 将合法引用推进到 `ready_for_delivery`，`skip` 则释放本次选择并保持零发送。
- durable delivery 使用独立状态链表达真实投递：`prepared/provider_started/delivered/projected/settled`，并保留 `rejected/uncertain` 分支。
- `delivery_unknown` 是 Wake 尚未确认 `settled` 时的上层结果，不等同于 durable delivery 的 `uncertain` 状态；两者都不能被当成安全可重发依据。
- Content、Alert、Context 共享不可变信封思想，但生命周期不同；Drift 另有 proposal/selection/delivery 状态。

## 5. 关键取舍

### 单次模型判断 vs 程序准入加两阶段模型

单次判断延迟低，但容易把“看起来相关”直接变成打扰。分层方案让便宜、确定的身份/衰减先过滤，再把昂贵工具调查留给少数候选；代价是链路更长、状态更多。

还应讨论初筛错过有价值内容、两次模型阶段增加调用成本、调查预算不足等代价，而不是预设两阶段必然更好。“两个模型阶段”不自动意味着使用两个不同的模型，具体配置需核对。

### 标题去重 vs 事件版本身份

标题去重实现简单，却会错杀真正更新或漏掉文本变体。`source/item/revision` 保留生产者身份和版本，代价是内容源必须提供稳定 identity。

### 自动重试发送 vs 保留 unknown

自动重试追求可用性，但回执丢失时可能重复打扰。unknown 状态更保守，需要后续核对或渠道幂等能力。

### 共用普通 ReAct vs 独立发送工具

调查可以复用 ReAct 和记忆/抓取工具；发送必须是 Wake 私有终态工具，避免普通正文越权。

## 6. 踩坑候选

### 幻觉式主动推荐

- 表象：模型依据薄弱候选直接生成看似合理的推荐。
- 根因：兴趣判断和证据核验被合并成一次自由生成。
- 修复：一步初筛后进入受限调查阶段，允许 recall/fetch，终态引用真实事件项。

### 跨会话重复话题

- 表象：相似主题在不同会话反复推送。
- 根因：只看当前候选，没有把最近已送达主动消息作为决策上下文。
- 修复：注入受限的最近主动历史，同时保留事件 identity/revision 去重。
- 注意：最近上下文辅助模型判断，不应夸大为硬编码主题去重算法。

### 普通 final text 越过发送授权

- 表象：模型返回一段正文就被当成“应该发送”。
- 根因：生成结果和领域终态没有分开。
- 修复：只接受显式结构化 content decision；普通正文不具备发送权。
- 提交：`deb23b54`、`d1b2282c`。

### 旧内容反复唤醒

- 根因：重复检查时重新计算或恢复静态兴趣，没有把一次评分与时间衰减分开。
- 修复：新 revision 一次评分、确定性衰减池、旧内容不能自行重触发。
- 提交：`a8d3e4b7`、`47e0d7cc`。

### UTC 与本地时间混用

- 表象：主动消息使用错误的当地时间口吻，衰减/时机判断也可能偏移。
- 修复：显式注入北京时间锚点并统一时间语义。
- 提交：`4e645e4a`。

## 7. 结果与边界

可以说：实现了从不可变事件、程序化准入、两阶段调查到明确发送/跳过终态的闭环，并能记录静默、失败和送达不确定状态。

不能说：主动推送准确率、采纳率或误打扰率已达到某个水平；20 步和 36 小时是效果最优参数；所有渠道都支持安全重试。

## 8. 需核实的统计与实验方向

先查相关插件、已有遥测和发送记录，再区分已有记录待整理、需要新增统计或需要用户反馈实验；不将这里全部标成尚未实现的功能，也不因写作自动运行实验。

- 新候选数、进入池比例、过阈值比例、screen 通过率、最终 share 率。
- revision 去重数、旧内容拦截数、跨会话重复拦截数。
- delivered、unknown、failed 比例。
- 用户采纳率、负反馈率和误打扰率。

## 9. 源码与提交锚点

- `plugins/eventmail/store.py`
- `plugins/eventmail/plugin.py`
- `plugins/wake/pool.py`
- `plugins/wake/selection.py`
- `plugins/wake/plugin.py`
- `plugins/wake/state.py`
- `plugins/drift/store.py`
- 提交：`deb23b54`、`47e0d7cc`、`d1b2282c`、`a8d3e4b7`、`4e645e4a`
