# 能力章：Agent 端到端评测

> 对应简历条目：「接入 Terminal-Bench 2.1，构建真实任务运行、工具执行与官方 Verifier 评分闭环；实现逐题容器隔离、断点续跑、执行轨迹留存与失败归因，完成 DeepSeek V4 Flash 89 题全量评测，最终通过 59/89（66.3%），支持区分 Agent、Provider 与 Verifier 故障。」
>
> 事实底稿：`packets/06-terminalbench.md`；主张边界：`01-fact-boundaries.md`。文中实现细节均已对基线源码副本（`d565ca44356b5d52fe8d00f8cb008c14fff56c0b`，`F:/agent/akashic-agent-d565`）核实。

## 一、口述层

> 段首加粗标签与括注字数仅为排练导航，口述时不读。全文第一人称，合计约 1,590 字（含英文词），对应 3～6 分钟。

**【背景与问题】**（约 250 字）

项目推进到一定阶段，必须回答一个问题：Akashic Agent 在真实任务上到底表现如何。当时手上只有功能演示和单元测试，它们能证明单个工具、单条链路正确，回答不了"给一个真实终端任务，Agent 能不能完成"。终端任务的特殊性在于，结果不在模型的回答文本里，而在环境的最终状态里：文件改没改对、进程起没起来、测试过没通过。这让两个日常习惯都失效了——相信模型说"已完成"不可信，只看一个最终分数也不够。一次失败可能来自 Agent 策略错误，可能来自 provider 根本没有形成有效模型回合，也可能来自验证基础设施自身故障。这三类问题对应完全不同的修复方向，混在一起，优化资源就会投错层级。

**【我的判断】**（约 300 字）

判断有两个。第一，评测对象必须是"隔离环境中的真实执行，加任务官方的外部判定"。对比过文本 Judge：让另一个模型给输出打分，便宜、快，适合评回答质量，但只能看表面合理性，检查不了容器里留下了什么。所以我选了 Terminal-Bench——公开的终端任务评测基准，每道题提供真实任务描述、独立容器环境和官方判分脚本；这份脚本就是官方 Verifier：任务作者预写的判定程序，在环境里把"任务完成"翻译成可执行的检查——文件内容、进程状态、测试是否通过——输出通过与否。判定因此外部、独立、可复现，不随提示词漂移；代价是要管理容器生命周期、验证依赖，以及 Verifier 自身也可能失败。第二，失败必须分层归因：接受结果前先确认 provider 形成有效回合、候选状态冻结、Verifier 进入测试正文，三层都成立，零分才算 Agent 失败。放弃的选项也明确：共享环境省成本但跨题污染；只存分数无法归因；中断后全量重跑不可信也不可扩展。

**【具体行动】**（约 440 字）

我接入 Terminal-Bench 2.1 的单步任务契约，固定 89 题集合，围绕单题闭环搭建运行框架。每道题先创建独立的 Trial、Docker project 和工作区，再由 `validate_isolation`——启动前校验容器隔离边界的函数——把关：它拒绝越出本题目录的宿主机挂载、Docker socket、白名单外的卷和对外发布的端口，校验通过才启动运行时，让真实 Agent 用真实工具执行任务。Agent 有自己的时限，但时限只终止 Agent：先清理它的进程生命周期，确认没有残留进程继续改动环境，再冻结候选状态。官方验证前有一道准备工序：Verifier 的依赖在官方计时之外预先装好，并在验证前后对候选目录做摘要对比，证明准备阶段没有改动待测答案，然后才运行官方 Verifier。每题的完整结果——reward、执行轨迹、资源证据、候选摘要——以只追加的方式写入 campaign 事件日志，这是恢复的事实源；对外展示的结果文件从日志里被接受的事件原子生成，可随时重建。89 题长跑难免中断，断点续跑就靠这份日志：恢复时先校验任务集合、源码摘要和运行协议与中断前一致，已完成的题直接恢复，不再重跑。重试也有边界：只有 provider 限流和瞬时故障走退避重试，Agent 失败保留为有效失败，不为分数反复重跑。

**【结果与复盘】**（约 310 字）

正式口径是：Terminal-Bench 2.1 固定 89 题，被测模型 DeepSeek V4 Flash，推理配置 `reasoning_effort=max`——推理投入的最高档位参数。最终官方 Verifier 判定通过 59 题，通过率 59 除以 89，即 66.3%。这个数字测的是什么必须说清：它是 Akashic Agent——统一 ReAct 内核加真实工具执行——在隔离环境里理解任务、改动环境并通过官方判定的端到端通过率；不是模型通用能力分数，也不是线上效果，只在同一口径内可比。构成比表面分数更有信息量：89 题中，59 题通过；26 题是有效失败，Verifier 判零分，其中含真实的 Agent 超时，只有这些进入 Agent 失败分析；3 题是 Provider fallback，没有形成可评价的有效模型回合；1 题是 Verifier 基础设施失败，依赖下载耗尽时限、未进入测试正文。四类合计正好 89。把 30 个未通过拆成 26、3、1，后续优化才知道每一分该找谁负责。

**【设计取舍、踩过的坑、怎么修的】**（约 350 字）

最有讲述价值的坑是 Provider fallback 被记成零分。现象：结果文件里出现零分，表面看是 Agent 执行了任务但做错了，最初的归因也朝这个方向走，去查提示词和工具调用。核对真实回合与轨迹才发现，其中一些题根本没有形成有效模型回合，是运行时的 fallback 文本被下游当成了已完成的模型回合；我的误判在于默认"运行时返回了内容"等于"provider 有效执行了"，而结果管道在接收前没有检查 provider 的真实终态。修复是调换因果顺序：接受 Agent 结果前，先读真实回合和轨迹确认 provider 终态有效，fallback 单独归为 Provider 故障，重新分类后识别出 3 题。这次修复没有改变总题数，改变的是失败的含义——Agent 失败统计不再混入无效回合，策略分析才有正确输入；不修的话，会拿供应链问题当策略问题去调提示词。第二个坑在同一条边界上：一道题的 Verifier 依赖下载与官方计时共用边界，任务没进测试正文就耗尽时限。修复是把依赖准备移到官方计时之前，并用候选摘要的冻结对比证明准备没改答案；这一题归为基础设施失败，不算 Agent 做错。

## 二、支撑层

### 2.1 关键机制详解

#### Terminal-Bench 与官方 Verifier：是什么、为什么用

- **Terminal-Bench 是什么**：公开的终端任务评测基准。每道题由三部分定义——自然语言任务描述（要求完成的真实终端操作）、容器化初始环境（含待修改的工作区）、官方判定脚本。本项目固定 Terminal-Bench 2.1 的 89 题，并只使用单步任务契约：一条任务指令、一轮执行、一次判定；多步任务会被本轨道的运行框架显式拒绝。
- **官方 Verifier 是什么**：即上述官方判定脚本，任务自带的外部判定程序。Agent 结束、候选状态冻结后，它在任务环境内运行，把"任务完成"落实为可执行的检查——文件内容、进程与端口状态、依赖版本、测试是否通过——并输出 reward（`1` 计通过，`0` 计未通过）。判定依据是环境的最终状态，不是模型的回答文本或自我报告。
- **为什么要用**：其一，任务真实——评测对象是终端操作本身，正对 Akashic Agent 的目标场景，不是评聊天回答质量；其二，判定独立——脚本由任务作者预先写好、在 Agent 之外运行，不随被测模型、提示词或评审模型漂移，避免"模型评模型"；其三，结果可比、可复现——同一外部标准下，不同实现与不同改动的通过率才有比较意义。
- **作用与边界**：Terminal-Bench 提供标准化的任务集合、环境定义与完成判定标准，回答"评什么"；本项目的闭环负责"怎么评"——隔离运行、时限管理、候选冻结、证据留存与失败归因。评测集未来可以替换，闭环的四个能力（隔离复现运行、官方判定、证据保存、失败归因）仍然成立。Verifier 只是闭环里的判定环节，自身也可能因依赖或环境问题失败，因此验证器自己也进入归因链。

#### 单题执行闭环

```text
读取任务与官方时限
  → 创建独立 Trial / Docker project / workspace
  → 校验挂载、网络、volume、在线进程与凭据边界
  → 启动运行时与真实 Agent turn
  → 模型通过真实工具完成终端操作
  → 清理 Agent 生命周期并冻结候选状态
  → 官方计时外准备 Verifier 依赖，运行官方 Verifier
  → 保存 reward、trace、资源证据、候选摘要与 manifest
  → 完整 outcome 追加进 campaign WAL
  → 从 accepted 事件原子生成结果投影
```

trial "生命周期完成"与 reward `1`（官方 Verifier 判定通过的分值）是两个维度：失败题也可以拥有完整、可信的证据链；反过来，一条结果记录存在，也不说明它是有效的 Agent 成败。

#### 逐题隔离与边界校验

- `validate_isolation`（启动前校验容器隔离边界的函数）：要求所有容器属于同一 benchmark project；bind 挂载只能来自本 trial 目录；named volume 必须精确匹配白名单且只读；禁止挂载 Docker socket；禁止向主机发布端口；触及受保护路径直接拒绝。
- `BENCHMARK_PREFIX`（benchmark 专属的命名前缀常量）：所有 compose project、网络与清理操作只认这个 owner 前缀，防止误删其他 Docker 资源；清理前还要重新核对容器身份与停止状态。
- `reserve_compose_network`（为单个 trial 原子预留独立子网的函数）：在 `BENCHMARK_NETWORK_POOL`（预留给 benchmark 的独立 IPv4 网段常量）内按 /28 切分子网，Docker 负责原子判定重叠。
- `online_process_snapshot` 与 `validate_online_processes_unchanged`（记录并比对正式工作区相关进程身份的函数对）：按 pid、启动 tick 与命令行做只读快照，campaign 前后必须一致，防止残留进程跨题污染。
- `require_storage_capacity`（磁盘容量门）：artifact、临时目录与 Docker 根三个文件系统的剩余容量任一低于阈值即停止创建新容器，不做全局自动清理。

#### 时限、清理与候选冻结

- Agent 的原始时限只终止 Agent；清理完成后才把候选交给 Verifier。Agent 超时前若已形成正确状态，Verifier 仍可判过——超时是可评价的有效结果，不是被丢弃的异常。
- `_capture_candidate_digest`（验证启动前冻结候选状态的钩子）：对任务工作树做内容摘要，连同根路径写入 `candidate-identity.json`（候选身份文件）；Verifier 看到的候选与 Agent 冻结的候选是否同一份，靠它回答。
- `_prepare_verifier_runtime`（官方计时外准备验证环境的协程）：对齐验证器运行时（如 uv 版本）、定位官方测试脚本，证据写入 `verifier-bootstrap.json`（验证器引导证据文件）。
- `_SerializedVerifierTrial`（把官方 Verifier 收进进程级调度信道的 Trial 子类）：provider 无效终态时写 `verifier-skipped.json`（显式跳过官方验证的记录文件）而不运行 Verifier；官方 Verifier 执行以 `_VERIFIER_CONCURRENCY = 1`（进程内单并发常量）串行入场，依赖准备另有独立并发上限。

#### Provider 终态分类（失败归因的第一层）

- `_turn_was_empty_provider_response`（识别 fallback 终态的判定函数）：只认运行时自有 fallback 标记——回合状态为 completed、唯一 assistant 消息内容等于 `_EMPTY_PROVIDER_REPLY`（运行时表示 provider 未流出任何回复的固定占位常量）、无任何工具调用——不按普通模型正文猜测失败。
- `_turn_was_rate_limited` / `_turn_was_transient_provider_failure` / `_turn_was_account_limited`（三个 provider 故障判定函数）：分别识别 429 限流家族、结构化 5xx 与响应体中断、账户额度终态；账户额度不当作瞬时 429 重试。
- 归因顺序固定：先 provider 终态，再候选冻结与验证是否有效，最后才读 reward。三层都有效时的零分才是 Agent 失败。本次 89 题中据此分出 3 个 Provider fallback、1 个 Verifier 基础设施失败、26 个有效 Agent 失败。

#### WAL 与结果投影

- `_append_campaign_event`（向 campaign 事件日志追加事件的函数）：单行 JSON 追加后立即 `fsync` 刷盘；`events.jsonl` 即 campaign WAL 文件，记录 attempt_started、accepted、attempt_failed、retry_scheduled、campaign_resumed 等事件。
- `_accepted_campaign_outcomes`（从 WAL 恢复已接受结果的函数）：逐行解析，行损坏或对同一 task 重复接受都直接报错，不静默跳过——恢复语义宁可失败也不含糊。
- `_write_campaign_results`（生成结果投影的函数）：按任务集合顺序从 accepted 事件生成 `accepted-results.json`（对外结果投影文件，含 passed / total / pass_rate）；写入经 `atomic_json`（临时文件加原子改名的原子写函数）完成。WAL 是事实源，投影可随时重建；一次 campaign 的中断与恢复属于同一次评测。

#### 断点续跑与任务集身份

- `manifest.json`（campaign 清单文件）：固化任务集合、源码摘要、并发上限、重试协议与调度表；恢复时五项任一变化即拒绝续跑。
- `_task_set_identity`（固化任务集合身份的函数）：任务集合以数据集身份入册，防止把部分数据投影成全量结果。
- `_seed_campaign_outcomes`（核验旧 campaign 结果作种子的函数）：要求种子覆盖完全相同的任务集合；对每个种子结果回看权威回合与驱动终态重新分类，provider 基础设施失败被排除并逐条记录理由，防止旧口径污染新结果。

#### 并发、调度与重试边界

- `MAX_CAMPAIGN_CONCURRENCY = 4`（受控轨道的并发硬上限常量）：信号量是唯一并发 owner，每个 task 仍持有独立 Trial 与 Docker project。
- `find_open_concurrency_gate`（并发授权门）：只有当前源码刚完成过隔离验证通过的 smoke，才允许打开四并发。
- 调度策略为 LPT（longest processing time first，长任务优先的贪心调度），减少尾部等待。
- 重试只覆盖 `provider_rate_limited` 与 `provider_transient`（两类可退避重试的 provider 故障分类），带退避与次数上限；Agent 失败与控制器失败不重试——`path-tracing-reverse` 与 `torch-tensor-parallelism` 两题保留为真实 Agent timeout，不为提高分数继续重跑。

#### 证据留存

- 执行证据：`agent/trace.jsonl`（逐事件执行轨迹文件）、turn 终态与驱动终态记录、stdout；它们是归因时"谁真正执行了什么"的依据，不声称覆盖模型内部思维。
- 资源证据：`resource_probe_command`（生成只读 cgroup v2 内存探针命令的函数）只读白名单文件，产出 `container-resource.json`（资源证据文件）；required 文件缺失时探针非零退出，收集失败记为显式 unknown，不把未知伪装成无 OOM。
- 完整性证据：`artifact_digests`（对证据目录散列的函数）显式排除自引用 manifest；`source_tree_digest`（源码内容摘要函数）与 `create_source_bundle`（导出可恢复源码历史的 Git bundle 的函数）固定源码身份，保证一次 campaign 内结果可归到同一份代码。
- accepted 的一致性条件：完整 trace、有效外部 Verifier、容器终态、owner 一致，四者齐备的结果才进入投影。

#### 通用概念对照

- Terminal-Bench（终端任务端到端评测集与运行框架）→ [官方仓库 laude-institute/terminal-bench](https://github.com/laude-institute/terminal-bench)
- ReAct（被测 Agent 的执行骨架）→ [learnagent.wiki「ReAct（推理与行动协同）」](https://learnagent.wiki/agent)
- Write-ahead logging / WAL（先追加日志再派生状态的恢复技术）→ [PostgreSQL 官方文档](https://www.postgresql.org/docs/current/wal-intro.html)
- Docker Compose（多容器 project 的定义与编排工具）→ [Docker 官方文档](https://docs.docker.com/compose/)
- cgroup v2（Linux 内核资源隔离与统计接口，资源证据的来源）→ [内核官方文档](https://docs.kernel.org/admin-guide/cgroup-v2.html)

### 2.2 简历效果 ↔ 实现技术对照表

| 简历主张 | 实现技术 |
| --- | --- |
| 接入 Terminal-Bench 2.1 | 基于 Terminal-Bench 单步任务契约接入（`_SerializedVerifierTrial` 包装官方 Trial）；89 题以 `_task_set_identity` 固定集合身份入册 `manifest.json` |
| 构建真实任务运行、工具执行与官方 Verifier 评分闭环 | 单题闭环：独立环境启动真实 Agent turn → 清理冻结候选 → 官方计时外 `_prepare_verifier_runtime` → 摘要对比 → 官方 Verifier 出 reward |
| 实现逐题容器隔离 | 每题独立 Trial / Docker project / workspace / 子网（`reserve_compose_network`）；`validate_isolation` 拒绝越界挂载、Docker socket、白名单外 volume 与主机端口；`online_process_snapshot` 前后比对进程边界 |
| 断点续跑 | `_append_campaign_event` 逐条 fsync 追加 `events.jsonl`；恢复校验 manifest 五项一致；`_accepted_campaign_outcomes` 恢复已完成题，重复接受与损坏行报错；`accepted-results.json` 投影按 accepted 事件原子重建 |
| 执行轨迹留存 | `agent/trace.jsonl`、turn / 驱动终态、stdout、`container-resource.json`（cgroup v2 白名单探针）、`candidate-identity.json`、`verifier-bootstrap.json`、`artifact_digests` 与源码摘要 |
| 与失败归因 | 三层顺序：`_turn_was_empty_provider_response` 等 provider 终态判定 → 候选冻结与验证有效性（`verifier-skipped.json` 显式跳过）→ reward；26 / 3 / 1 分别落层 |
| 完成 DeepSeek V4 Flash 89 题全量评测，最终通过 59/89（66.3%） | `MAX_CAMPAIGN_CONCURRENCY = 4` + smoke 并发门 + LPT 调度 + 限流退避重试完成全量；`accepted-results.json` 的 score 即该口径（官方 Verifier reward `1` 计通过） |
| 支持区分 Agent、Provider 与 Verifier 故障 | provider 四个判定函数分类供应链故障；Verifier 依赖失败按"未进入测试正文"单列；三层有效后的 reward `0` 才计入 Agent 失败 |

### 2.3 与主流替代方案对比

**判定方式：模型自评 / 文本 LLM Judge / 任务自带测试·官方 Verifier（本项目）/ 人工评估**

- 模型自评（把被测 Agent 的自我总结当结论）：零额外成本、随跑随得；偏差也最直接——它看到的是自己的操作上下文，不是独立可信的终态，命令返回 0 不等于改对了文件。对方更优的场景：只要粗粒度过程信号、不在乎判定可信度的快速冒烟。
- 文本 LLM Judge（另一个模型按题目与输出文本打分，开放式问答评测的主流形态）：通用、可批量、评分标准可调；它评的是"回答像不像正确的"，对文件权限、进程生命周期、端口状态这类环境事实天然失明，且打分自身随模型与提示漂移，需要校准集维护。对方更优的场景：没有可执行判定器的开放式生成任务（写作、问答、对话质量）。
- 任务自带测试·官方 Verifier（本项目）：判定直接落在任务的完成定义上，跨 Agent 可比，无"模型评模型"的漂移；代价是要管理容器、验证依赖、时限，以及 Verifier 自身的可靠性——本次就有 1 题因 Verifier 依赖下载未进入测试正文而单列。使用外部判定器不等于验证阶段不会失败，验证器自己也进归因链。
- 人工评估：判定上限最高、能处理模糊标准；不可扩展、不可精确重复，不适合 89 题级别的回归基线。
- 关系：不是互斥选项——本项目把官方 Verifier 作为唯一通过标准，trace 与文本只作诊断输入；LLM Judge 可以作为叠加的诊断信号，但不在正式口径内。

**隔离粒度：共享环境 / 快照复用 / 逐题独立 Trial + 容器（本项目）/ 托管沙箱池**

- 共享环境（全部题目在同一容器或虚拟机内顺序执行）：启动成本最低、依赖只装一次；前一题的文件、进程、端口与凭据会污染后一题，高分可能来自残留状态，失败无法归属。对方更优的场景：题目之间显式无干扰的一次性演示。
- 快照复用（每题从干净快照启动，宿主与网络共享）：折中形态，比共享干净、比逐题新建快；快照恢复自身有时延与失败模式，宿主层资源（磁盘、端口、进程表）仍然共享。
- 逐题独立 Trial + Docker project + 独立子网（本项目）：输入、执行、产物都有唯一 owner，失败可复现、证据可归属，是"分数含义成立"的前提；代价是容器创建、依赖准备与存储成本随题数线性增长，必须配套并发门与容量门（`MAX_CAMPAIGN_CONCURRENCY`、`require_storage_capacity`）。
- 托管沙箱池（云端沙箱与托管 runner 一类）：弹性与运维外移，适合大规模持续评测；代价是成本模型、网络与凭据边界受平台约束，且链路越长，基础设施故障的归因越难落到自己手里。
- 关系：本项目的隔离校验（挂载 / volume / 端口白名单、进程边界比对）与运行位置正交——迁移到托管沙箱后这些校验仍然必要。

**结果记录与恢复：覆盖写单文件 / 每题独立文件 / 中心数据库 / append-only WAL + 投影（本项目）**

- 覆盖写单文件：实现最简；中断即半写入，多次运行互相覆盖，"已完成"与"进行中"无法区分。对方更优的场景：单题、单次、无恢复需求的冒烟脚本。
- 每题独立结果文件 + 事后汇总：部分写入的风险小了；但汇总脚本自身易错，且没有统一的"接受"语义——什么样的结果算数没有判定标准。
- 中心数据库（SQLite 或服务端）：事务与查询能力强；引入运维依赖，而证据主体（trace、容器产物）本来就在文件系统，两处状态需要同步一致。
- append-only WAL + 投影（本项目）：事件只追加、逐条 fsync，天然记录"进行到哪一步"；结果文件从 accepted 事件原子生成、可随时重建；重复接受与损坏行在恢复时直接报错。代价是多一层投影一致性维护，读结果的人要理解"事实在日志、视图可重建"。这与数据库领域的 write-ahead logging 是同一思想：先记事实，再派生视图。

**失败归因粒度：单一 pass/fail / 逐题日志人工归因 / 三层结构化归因（本项目）**

- 单一 pass/fail（多数公开排行榜的口径）：汇报最简单、可比性最好；一个零分背后可能是策略、供应链或基础设施，对改进没有指向性。对方更优的场景：只做横向排名、不驱动内部迭代的场合。
- 逐题保留日志、人工归因（不少框架与复现报告的实际做法）：信息都在，但归因结论不进结果管道，换人、换次运行口径就漂移，无法作为结构化统计。
- 三层结构化归因（本项目）：provider 终态 → 候选冻结与验证有效性 → reward，在结果接收路径里强制分层，本次 30 个未通过被拆成 26 个有效 Agent 失败、3 个 Provider fallback、1 个 Verifier 基础设施失败。代价：必须保存冷证据并校验一致性（trace、终态、摘要、owner），且分类逻辑自身要经得起复查——错误分类比没有分类更误导，fallback 被记零分即是例证。

### 2.4 高概率追问与要点

1. **Terminal-Bench 和官方 Verifier 分别是什么？为什么不用模型打分？** 要点：Terminal-Bench 是终端任务评测基准，每题由任务描述、容器环境、官方判定脚本三部分构成；官方 Verifier 就是那份判定脚本，在环境里运行检查、以环境事实为判据输出 reward；模型自评与 LLM Judge 只能对文本表态，检查不了最终状态；选它的理由是任务真实、判定独立、结果可比可复现，代价是验证基础设施要自己运维并纳入归因。
2. **66.3% 到底测的是什么？能比较什么？** 要点：固定 Terminal-Bench 2.1 89 题、DeepSeek V4 Flash、推理投入最高档配置 `reasoning_effort=max`，官方 Verifier 判定通过（reward `1`）计入通过；测的是本项目 Agent 端到端完成任务的比例，不是模型通用能力或线上效果；同一口径内的改动可以比，跨任务集、跨配置的数字不可直接比。
3. **为什么不重跑那 3 个 fallback 和 1 个 Verifier 失败，把分数做高？** 要点：campaign 内只有 provider 限流与瞬时故障走退避重试；Agent 失败（含真实 timeout）保留原结果不刷分；fallback 与基础设施失败的价值在归因正确，不在数字好看——重跑刷分会让口径失去可比性。
4. **Agent 超时了为什么还要跑 Verifier？超时算谁失败？** 要点：时限只终止 Agent 工作阶段；清理冻结后候选仍可能已满足任务条件，Verifier 判过就有效；判不过则计入 26 个有效 Agent 失败——超时是有效失败，不是被丢弃的异常。
5. **怎么保证 Verifier 自己的准备不会改掉候选答案？** 要点：依赖准备在官方计时之外完成（`_prepare_verifier_runtime`）；验证前冻结候选摘要（`candidate-identity.json`），前后对比证明 `/app` 未被准备阶段改动；provider 无效终态时显式写 `verifier-skipped.json`，不跑无效验证。
6. **断点续跑怎么防止重复计数或"多次评测"假象？** 要点：事实只追加进 `events.jsonl`；恢复校验 manifest 五项（任务集合、源码摘要、数据集身份、并发、重试协议）一致；同一 task 重复接受直接报错；投影由 accepted 事件唯一生成——一次 campaign 的恢复仍是同一次评测。
7. **逐题隔离的成本怎么控？** 要点：四并发硬上限 + smoke 并发门 + LPT 长任务优先调度；三个文件系统容量门；容器与网络清理只认 benchmark owner 前缀并核对身份。成本换的是分数含义与归因能力。
8. **三类故障具体靠什么证据区分？** 要点：Provider——回合终态与 fallback 标记（`_turn_was_empty_provider_response` 等四个判定函数）；Verifier——是否进入有效测试正文、依赖准备证据与候选摘要对比；Agent——前两层有效后的官方 reward。全部可回查 trace 与终态记录，不靠最终文本猜。
9. **这个成绩和 SWE-bench 或其他 Agent 的数字能比吗？** 要点：不能直接比——任务域（通用终端操作 vs 代码修复）、被测 Agent、模型与配置、判定器都不同；可比的是同一评测口径内的前后改动。
10. **单次运行的 66.3% 有多少噪声？** 要点：当前口径是固定集合上的单次 campaign，没有重复运行的方差数据，这是结果边界的一部分；provider 侧的限流与瞬时故障本身也是被测链路的真实组成，已在重试与归因中处理，但不等于量化了方差。
11. **评测集换掉之后，这套东西还剩什么价值？** 要点：四个与评测集无关的能力——隔离可复现地运行真实 Agent、以任务官方判定而非自评作标准、保存执行与环境证据、对失败阶段归因；换任务集只需替换任务定义与判定器接入。
