# 章节证据包：Terminal-Bench 端到端验证闭环

> 本包是章节事实底稿：数据、机制、踩坑、源码锚点与禁止推导。写作与验收以 `../00-README.md` 为准，通用主张边界见 `../01-fact-boundaries.md`；本包不是封闭事实清单。

## 1. 对应简历

> Agent 端到端评测：接入 Terminal-Bench 2.1，构建真实任务运行、工具执行与官方 Verifier 评分闭环；实现逐题容器隔离、断点续跑、执行轨迹留存与失败归因，完成 DeepSeek V4 Flash 89 题全量评测，最终通过 59/89（66.3%），支持区分 Agent、Provider 与 Verifier 故障。

## 2. 评测对象

核心不是评估 DeepSeek V4 Flash 的聊天回答质量，而是验证：给定一个真实终端任务，Akashic Agent 是否能在隔离环境内理解任务、调用工具修改目标状态，并通过官方 Verifier。

评测集未来可以替换，闭环中的四个能力仍然成立：

1. 隔离并可复现地运行真实 Agent。
2. 使用任务官方判定，而不是模型自评。
3. 保存执行和环境证据。
4. 对失败阶段进行归因。

## 3. 单题执行闭环

```text
读取 Terminal-Bench 任务和官方时限
  → 为该题创建独立 Trial / Docker project / workspace
  → 校验挂载、网络、volume、在线进程和凭据边界
  → 启动 Akashic runtime 与真实 Agent turn
  → 模型通过真实工具完成终端操作
  → 清理 Agent 生命周期并冻结候选状态
  → 准备且运行官方 Verifier
  → 保存 reward、trace、资源证据、artifact digest 和 final manifest
  → 将完整 outcome 追加到 campaign WAL
  → 从 accepted 事件原子生成结果投影
```

关键实现边界：

- 每题有独立 Trial 和 Docker project，并通过 `validate_isolation` 拒绝越界挂载、Docker socket 和非允许 volume。
- Agent 原始时限只终止 Agent；清理完成后仍应交给官方 Verifier 判定候选。
- Verifier 依赖准备必须证明没有改变 `/app` 候选。
- trial “生命周期完成”与 reward `1` 是两个不同维度；失败题也可以拥有完整证据。
- campaign WAL 是 append-only 恢复依据；`accepted-results.json` 是投影，不是原始事实源。
- accepted 只接受完整 trace、外部 Verifier、容器终态和 owner 一致的结果。

## 4. 正式结果口径

运行配置：

- Terminal-Bench 2.1 固定 89 题。
- DeepSeek V4 Flash。
- `reasoning_effort=max`。

最终报告：

| 类别 | 数量 | 是否计入 59 个通过 |
|---|---:|---|
| 官方 Verifier reward `1` | 59 | 是 |
| 有效 reward `0`，含真实 Agent timeout | 26 | 否 |
| Provider fallback | 3 | 否，单独归因 |
| Verifier 依赖下载未进入测试正文 | 1 | 否，基础设施问题 |

固定集合最终结果为 `59/89 = 66.3%`。

历史 High 轨道 `63/89 = 70.8%` 只作内部核查线索，不要求正文保留对照列。不能与最终轨道拼接或替代简历数字；直接比较需要先核对模型配置、轨道、任务与运行条件，不能据数字差异推断“审计导致降分”。面试官没有默认获知该历史成绩，不设置“为何不用 63/89”的独立核心题。

## 5. 关键取舍

### 文本 Judge vs 官方 Verifier

文本 Judge 便于通用问答，但终端任务的真实结果存在于文件、进程、配置或测试状态。官方 Verifier 更贴近任务完成定义；代价是需要管理依赖、时限和 Verifier 自身故障。

### 共享环境 vs 逐题隔离

共享环境成本低，但前一题的文件、进程、端口和凭据会污染后一题。逐题隔离提高运行成本，却让失败和产物有明确 owner。

### 只保存最终分数 vs 保存冷证据

只有 reward 无法定位失败。保存 trace、stdout、资源快照和 digest 增加存储与实现复杂度，但能够区分模型执行失败和基础设施失败。

### 覆盖结果文件 vs WAL 加投影

直接覆盖简单，但中断时容易丢失已完成题目或混合多个 campaign。append-only WAL 保存事实，再生成 accepted 投影，恢复语义更清楚。

## 6. 踩坑候选

### Provider fallback 被记为 reward 0

- 表象：结果文件中出现 0 分，看起来像 Agent 完成了任务但做错。
- 根因：runtime fallback 被当作 completed model turn，accepted projection 没有先检查 provider 终态。
- 修复：读取真实 turn/trace 分类 Provider 失败，不把 fallback 正文当成有效 Agent 结果。
- 实际识别出 3 题。

### Verifier 依赖下载吞掉计时

- 表象：任务没有真正进入测试正文就耗尽时限。
- 根因：依赖准备和官方 Verifier 执行共用计时/环境边界。
- 修复：在官方计时前准备依赖，冻结并对比候选 digest，证明准备阶段没有修改答案。
- 实际识别出 1 题基础设施失败。

### timeout 与子进程生命周期

- 表象：Agent 超时后残留进程可能与下一题重叠，或将 cleanup 时间误算成 Agent 工作。
- 修复：显式区分 Agent deadline、cleanup reserve、进程/compose project owner，并在下一题前验证在线进程边界。
- `path-tracing-reverse` 与 `torch-tensor-parallelism` 保留为真实 Agent timeout，不为了提高分数继续重跑。

### 多 campaign 恢复被误当成独立结果

- 根因：把多个中断/恢复目录直接汇总，会重复计算或制造“多次评测”。
- 修复：固定 task-set identity，以 WAL 中 accepted outcome 生成唯一投影。

### 凭据与 provider 配置不一致

- 表象可能与模型失败相同。
- 修复方向是让凭据注入、provider 终态和 trace 都成为可审计证据，而不是根据最终文本猜测。

## 7. 结果与复盘

需要解释 66.3% 测量的是什么：哪些任务真正完成、哪些是 Agent 失败、哪些没有形成有效模型回合或有效 Verifier。进一步用有记录的任务说明执行与失败机制，不能只围绕数字筛选过程展开。

复盘重点：闭环评测的价值在于让后续优化针对正确层级。如果把 Provider fallback 当作 Agent 策略失败，会把工程资源投入错误方向。

这份评测支持对特定模型、运行时与任务集合的端到端表现作解释，不直接证明长期记忆、主动推荐或线上用户效果。

## 8. 源码与文档锚点

- `benchmark/harbor_v4flash/agent.py`
- `benchmark/harbor_v4flash/controller.py`
- `benchmark/harbor_v4flash/isolation.py`
- `benchmark/harbor_v4flash/runtime_driver.py`
- `benchmark/harbor_v4flash/result_projection.py`
- `benchmark/harbor_v4flash/resource_evidence.py`
- `docs/benchmark/terminalbench-2.1-run-audit-2026-08-05.md`
- `docs/benchmark/terminalbench-2.1-case-results-2026-08-05.csv`
- `docs/benchmark/v4flash-harness-experiment-ledger.md`
- `docs/benchmark/v4flash-terminalbench-89-case-diagnostics.md`

## 9. 禁止推导

- 不得称 3 个 fallback 和 1 个 Verifier 故障为 Agent 答错。
- 不得把历史 63/89 与最终 59/89 组合。
- 不得声称 66.3% 是模型通用能力或线上业务效果。
- 不得将一次 campaign 的恢复片段称为多次独立实验。
