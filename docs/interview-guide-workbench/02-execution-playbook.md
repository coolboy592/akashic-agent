# 章节编写执行手册（ZCode 新会话用法）

> 配合 `00-README.md` 使用：写作规则以 00-README 为准，本手册只回答"怎么开新对话、贴什么、怎么验收、怎么返修"。适用范围：`chapter-tools-context` 至 `appendix-comparisons` 共 6 份文档，每份文档一个新 ZCode 会话，能力章先行、总述与附录最后。

## 0. 执行哲学（先读这段）

本手册的标准**约束产物，不约束思考**。执行 Agent 是有判断的写作者，不是模板填充器：

- **产物规格刚性**：五段模板、两层结构、符号与链接规则、数字口径——这是防"深浅漂移"的根基，必须执行；
- **思考路径弹性**：研究顺序、叙事重点、结构细节由执行 Agent 自主决定；各 prompt 给出的"参考线索"只是起点，可以推翻；
- **批判与交互保留**：对规则、黄金样例、事实包的质疑不要求咽下去，分级处理——事实冲突先问再动笔，规则与风格异议随稿上交（见各 prompt 的"交互分级"）；
- **用户侧闭环**：验收时逐条处理随稿议题（§4 第 11 条）——异议成立就改规则，不成立就说明原因。否则 Agent 会学会沉默，批判通道名存实亡。

## 1. 总流程（每份文档走同一个循环）

1. 在 `F:\agent` 工作目录打开新 ZCode 会话；
2. 原样粘贴 §3 中对应文档的启动 prompt（无需附加其他背景）；
3. Agent 读规则与事实包 → 自主研究、取舍、写稿 → 交稿时提交合规自查 + 批判性自查与议题（含异议与建议清单）；
4. 你按 §4 验收要点读稿、逐条裁决随稿议题、试讲一遍口述层；
5. 全部通过 → 贴 §6 定稿指令，结束会话；有问题 → 用 §5 返修模板定点反馈（会话还开着就在原会话说；已关闭则开新会话贴返修模板）。

会话中途断掉：开新会话贴同一启动 prompt，加一句"草稿已在 drafts/chapters/chapter-X.md，先读现有内容继续完成"。

## 2. 编写顺序与依赖

| 顺序 | 文档 | 事实包 | 可选素材（archive，不继承结构） |
| --- | --- | --- | --- |
| 1 | `chapter-tools-context.md`（打样章：素材最全，先验证规格） | `packets/04` | `04-tools-context.candidate.md`（深稿）、`04-tools-context.approved.md`（旧稿） |
| 2 | `chapter-memory.md` | `packets/03` | `02-memory.approved.md` |
| 3 | `chapter-terminalbench.md` | `packets/06` | `03-terminalbench.approved.md` |
| 4 | `chapter-proactive.md` | `packets/05` | `05-proactive.approved.md` |
| 5 | `chapter-00-positioning.md`（总述，从四章成稿提炼） | `packets/01`、`packets/02` | `01-positioning.approved.md` + 四个已验收章节 |
| 6 | `appendix-comparisons.md`（汇总各章对比） | `01-fact-boundaries.md` §6 | 六份成稿的支撑层板块 |

后一站开工前，前一站必须已验收——总述和附录直接取材于能力章成稿，顺序不可倒置。

## 3. 启动 prompt（每条独立可粘贴；规则不内联，由 Agent 自行读文件，保持单一事实源）

各条 prompt 中的「参考线索」是我方预判的叙事重点，仅作起点——执行 Agent 可质疑、替换、补充，交稿时说明实际选定的重点及理由。

### 3.1 chapter-tools-context（第 1 站）

```text
为 Akashic Agent 面试指导书写「工具发现与上下文管理」章。

必读（按顺序，写作规则与事实以这些文件为准）：
1. F:/agent/akashic-agent/docs/interview-guide-workbench/00-README.md —— 产物定义、五段模板、黄金样例与硬性标准
2. F:/agent/akashic-agent/docs/interview-guide-workbench/01-fact-boundaries.md —— 术语、数字口径、禁用结论
3. F:/agent/akashic-agent/docs/interview-guide-workbench/packets/04-tools-and-context.md —— 本章事实底稿（含 Observe 核对结论）
4. F:/agent/agent简历.md —— 本章对应「工具发现与上下文管理」条目
可选素材（只取事实，不继承结构与文风）：
F:/agent/akashic-agent/docs/interview-guide-workbench/drafts/archive/04-tools-context.candidate.md
F:/agent/akashic-agent/docs/interview-guide-workbench/drafts/archive/04-tools-context.approved.md

产出：F:/agent/akashic-agent/docs/interview-guide-workbench/drafts/chapters/chapter-tools-context.md
结构：口述层（五段模板，第一人称，约 1000～1600 字）＋ 支撑层（关键机制详解 / 简历效果↔实现技术对照表 / 与主流替代方案对比 / 高概率追问与要点）。

参考线索（仅作起点，可质疑、替换或补充，交稿时说明你实际选定的重点及理由）：registered/visible/executable 三集合的区分；"搜索解锁却没进 Schema"踩坑闭环；压缩以完整交互单元为边界；稳定前缀与动态记忆分层。

交稿时在回复中提交两部分自查：
A 合规自查：五段是否齐全；口述层字数与代码符号数（≤3 且首现括注作用）；支撑层每个符号是否随文解释；链接是否只出现在支撑层；所引数字与限定语是否与 01-fact-boundaries、packet 一致。
B 批判性自查与议题（不阻塞成稿）：你认为本章最薄弱的 1～2 处及原因；你对五段模板、黄金样例、硬性标准或事实包内容的异议与建议（不要自行改动，列出交我裁决）；需要我提供输入的开放问题。

边界：只写上述产出文件。不自行修改 00-README、01-fact-boundaries、packets、archive、简历与业务代码；发现它们的问题通过 B 部分随稿提出，由我决定是否变更。不运行评测。packet 事实不足时可读 F:/agent/akashic-agent-d565 对应源码核实，并在自查中记录所读文件。

交互分级：与简历主张冲突或无法确定的关键事实——先停下来问我再动笔；对规则、样例或结构有异议或更好的判断——随稿提交，不阻塞；研究顺序、表达方式等执行细节——自主决定。对任务理解有歧义先问。
```

### 3.2 chapter-memory（第 2 站）

```text
为 Akashic Agent 面试指导书写「长期记忆与证据检索」章。

必读（按顺序，写作规则与事实以这些文件为准）：
1. F:/agent/akashic-agent/docs/interview-guide-workbench/00-README.md
2. F:/agent/akashic-agent/docs/interview-guide-workbench/01-fact-boundaries.md
3. F:/agent/akashic-agent/docs/interview-guide-workbench/packets/03-memory.md
4. F:/agent/agent简历.md —— 本章对应「长期记忆与证据检索」条目
可选素材（只取事实，不继承结构与文风）：
F:/agent/akashic-agent/docs/interview-guide-workbench/drafts/archive/02-memory.approved.md

产出：F:/agent/akashic-agent/docs/interview-guide-workbench/drafts/chapters/chapter-memory.md
结构：口述层（五段模板，第一人称，约 1000～1600 字）＋ 支撑层（四板块，同 00-README 定义）。

参考线索（仅作起点，可质疑、替换或补充，交稿时说明你实际选定的重点及理由）：三路信号各解决什么（稠密语义 / BM25 词法 / 图关联补全，非固定加权融合）；"自动注入—按需补搜—原文取证"三层链路；查询与学习分离；替代方案对比要落到向量＋BM25＋RRF 与重排的具体形态。

交稿时在回复中提交两部分自查：
A 合规自查：五段是否齐全；口述层字数与代码符号数（≤3 且首现括注作用）；支撑层每个符号是否随文解释；链接是否只出现在支撑层；所引数字与限定语（隔离重放口径）是否与 01-fact-boundaries、packet 一致。
B 批判性自查与议题（不阻塞成稿）：你认为本章最薄弱的 1～2 处及原因；你对五段模板、黄金样例、硬性标准或事实包内容的异议与建议（不要自行改动，列出交我裁决）；需要我提供输入的开放问题。

边界：只写上述产出文件。不自行修改 00-README、01-fact-boundaries、packets、archive、简历与业务代码；发现它们的问题通过 B 部分随稿提出，由我决定是否变更。不运行评测。packet 事实不足时可读 F:/agent/akashic-agent-d565 对应源码核实，并在自查中记录所读文件。

交互分级：与简历主张冲突或无法确定的关键事实——先停下来问我再动笔；对规则、样例或结构有异议或更好的判断——随稿提交，不阻塞；研究顺序、表达方式等执行细节——自主决定。对任务理解有歧义先问。
```

### 3.3 chapter-terminalbench（第 3 站）

```text
为 Akashic Agent 面试指导书写「Agent 端到端评测」章。

必读（按顺序，写作规则与事实以这些文件为准）：
1. F:/agent/akashic-agent/docs/interview-guide-workbench/00-README.md
2. F:/agent/akashic-agent/docs/interview-guide-workbench/01-fact-boundaries.md
3. F:/agent/akashic-agent/docs/interview-guide-workbench/packets/06-terminalbench.md
4. F:/agent/agent简历.md —— 本章对应「Agent 端到端评测」条目
可选素材（只取事实，不继承结构与文风）：
F:/agent/akashic-agent/docs/interview-guide-workbench/drafts/archive/03-terminalbench.approved.md

产出：F:/agent/akashic-agent/docs/interview-guide-workbench/drafts/chapters/chapter-terminalbench.md
结构：口述层（五段模板，第一人称，约 1000～1600 字）＋ 支撑层（四板块，同 00-README 定义）。

参考线索（仅作起点，可质疑、替换或补充，交稿时说明你实际选定的重点及理由）：59/89 = 66.3% 测的是什么（口径与构成）；三类故障归因（Agent 失败 / Provider fallback / Verifier 基础设施失败）怎么识别；逐题隔离、断点续跑、轨迹留存机制；本章有真实数字，口述层第 4 段应正面使用并说明口径。

交稿时在回复中提交两部分自查：
A 合规自查：五段是否齐全；口述层字数与代码符号数（≤3 且首现括注作用）；支撑层每个符号是否随文解释；链接是否只出现在支撑层；结果数字及构成（59/26/3/1）是否与 packet 完全一致、未混入历史 63/89。
B 批判性自查与议题（不阻塞成稿）：你认为本章最薄弱的 1～2 处及原因；你对五段模板、黄金样例、硬性标准或事实包内容的异议与建议（不要自行改动，列出交我裁决）；需要我提供输入的开放问题。

边界：只写上述产出文件。不自行修改 00-README、01-fact-boundaries、packets、archive、简历与业务代码；发现它们的问题通过 B 部分随稿提出，由我决定是否变更。不运行评测。packet 事实不足时可读 F:/agent/akashic-agent-d565 对应源码核实，并在自查中记录所读文件。

交互分级：与简历主张冲突或无法确定的关键事实——先停下来问我再动笔；对规则、样例或结构有异议或更好的判断——随稿提交，不阻塞；研究顺序、表达方式等执行细节——自主决定。对任务理解有歧义先问。
```

### 3.4 chapter-proactive（第 4 站）

```text
为 Akashic Agent 面试指导书写「主动交互策略」章。

必读（按顺序，写作规则与事实以这些文件为准）：
1. F:/agent/akashic-agent/docs/interview-guide-workbench/00-README.md
2. F:/agent/akashic-agent/docs/interview-guide-workbench/01-fact-boundaries.md
3. F:/agent/akashic-agent/docs/interview-guide-workbench/packets/05-proactive.md
4. F:/agent/agent简历.md —— 本章对应「主动交互策略」条目
可选素材（只取事实，不继承结构与文风）：
F:/agent/akashic-agent/docs/interview-guide-workbench/drafts/archive/05-proactive.approved.md

产出：F:/agent/akashic-agent/docs/interview-guide-workbench/drafts/chapters/chapter-proactive.md
结构：口述层（五段模板，第一人称，约 1000～1600 字）＋ 支撑层（四板块，同 00-README 定义）。

参考线索（仅作起点，可质疑、替换或补充，交稿时说明你实际选定的重点及理由）：兴趣初筛与证据核验为什么拆两阶段；"普通 final text 没有发送权"的授权边界；事件 revision 去重与兴趣衰减池；本章缺端到端效果数字——口述层第 4 段按 00-README 讲验证方式与边界，不编数字（此判断本身也可质疑）。

交稿时在回复中提交两部分自查：
A 合规自查：五段是否齐全；口述层字数与代码符号数（≤3 且首现括注作用）；支撑层每个符号是否随文解释；链接是否只出现在支撑层；36 小时半衰期、20 步调查等参数是否都带"历史策略参数"限定。
B 批判性自查与议题（不阻塞成稿）：你认为本章最薄弱的 1～2 处及原因；你对五段模板、黄金样例、硬性标准或事实包内容的异议与建议（不要自行改动，列出交我裁决）；需要我提供输入的开放问题。

边界：只写上述产出文件。不自行修改 00-README、01-fact-boundaries、packets、archive、简历与业务代码；发现它们的问题通过 B 部分随稿提出，由我决定是否变更。不运行评测。packet 事实不足时可读 F:/agent/akashic-agent-d565 对应源码核实，并在自查中记录所读文件。

交互分级：与简历主张冲突或无法确定的关键事实——先停下来问我再动笔；对规则、样例或结构有异议或更好的判断——随稿提交，不阻塞；研究顺序、表达方式等执行细节——自主决定。对任务理解有歧义先问。
```

### 3.5 chapter-00-positioning（第 5 站，结构特殊：不用五段模板）

```text
为 Akashic Agent 面试指导书写「项目定位与统一 ReAct 地基」总述章。前置条件：四个能力章（chapter-tools-context、chapter-memory、chapter-terminalbench、chapter-proactive）均已写好，作为本章的直接素材。

必读（按顺序，写作规则与事实以这些文件为准）：
1. F:/agent/akashic-agent/docs/interview-guide-workbench/00-README.md
2. F:/agent/akashic-agent/docs/interview-guide-workbench/01-fact-boundaries.md
3. F:/agent/akashic-agent/docs/interview-guide-workbench/packets/01-positioning.md、packets/02-react-runtime.md
4. F:/agent/agent简历.md —— 总述段与四条能力
5. drafts/chapters/ 下四个能力章成稿（重点读各章口述层）

产出：F:/agent/akashic-agent/docs/interview-guide-workbench/drafts/chapters/chapter-00-positioning.md
结构（总述章专用，不用五段模板；结构细节如需调整，随稿建议）：
① 2～3 分钟项目介绍口述骨架——第一人称：这是什么项目、解决什么问题、四条能力各一句话、怎么统一在 ReAct 执行闭环上（约 400～700 字）；
② 四条能力与统一 ReAct 地基的关系——每条一段白话，说明它复用了什么、多了什么边界，读者读完后知道深挖去看哪一章；
③ 总览级高概率追问 3～5 个（"为什么自己搭而不是用框架""四个能力哪个最难"这类，自行判断哪些真正高概率）。
口述层硬性标准（符号≤3、无链接、无比喻）同样适用。

参考线索（仅作起点，可质疑、替换或补充）：四个能力一句话互不打架、指向一致；ReAct 是共同地基而非第五项成绩。

交稿时在回复中提交两部分自查：
A 合规自查：①②③是否齐全；介绍骨架是否在 2～3 分钟内；四个能力章的核心结论是否被准确引用且无矛盾。
B 批判性自查与议题（不阻塞成稿）：你认为本章最薄弱的 1～2 处及原因；你对总述章结构、黄金样例或事实包内容的异议与建议（不要自行改动，列出交我裁决）；需要我提供输入的开放问题。

边界：只写上述产出文件。不自行修改 00-README、01-fact-boundaries、packets、archive、简历、业务代码与四个能力章；发现它们的问题通过 B 部分随稿提出，由我决定是否变更。

交互分级：packets 01/02 与能力章成稿之间的矛盾、或与简历冲突的事实——先停下来问我再动笔；对结构或样式的异议——随稿提交，不阻塞；表达细节——自主决定。对任务理解有歧义先问。
```

### 3.6 appendix-comparisons（第 6 站，最后一站）

```text
为 Akashic Agent 面试指导书编写跨章技术对比附录，汇总全书各章的对比材料。

必读（按顺序）：
1. F:/agent/akashic-agent/docs/interview-guide-workbench/00-README.md
2. F:/agent/akashic-agent/docs/interview-guide-workbench/01-fact-boundaries.md —— 重点 §6 技术比较基准
3. drafts/chapters/ 下全部五份成稿 —— 重点读各章支撑层的「简历效果↔实现技术对照表」与「与主流替代方案对比」板块

产出：F:/agent/akashic-agent/docs/interview-guide-workbench/drafts/chapters/appendix-comparisons.md
结构（框架供参考，可随稿提出调整建议）：
① 全书「简历效果 ↔ 实现技术」总表：合并四章对照表，简历每句主张一行；
② 跨章技术选型汇总表：每个核心选型一行——选了什么 / 放弃了什么 / 对方更优的场景；
③ 共性设计模式提炼：从成稿归纳 3～5 条跨模块共性（如受限投影、显式授权、证据可回查、外部判定），每条注明出自哪章、有具体机制支撑，不空谈；
④ 通用概念链接索引：全书支撑层出现的 learnagent.wiki / 官方文档链接集中成一张表。
附录行文按技术文档标准，不需要口述层；对成稿内容只汇总不新写事实。

参考线索（仅作起点，可质疑、替换或补充）：②至少覆盖 01 §6 列出的四组对比（检索与重排、图补全、压缩谱系、评测判定）加上工具暴露与主动决策两组；③的共性模式必须能回链到具体章节。

交稿时在回复中提交两部分自查：
A 合规自查：①②是否覆盖全部四条简历能力与 01 §6 基准；③每条是否都能回链到具体章节；④链接是否完整；与成稿有无矛盾。
B 批判性自查与议题（不阻塞成稿）：你认为附录最薄弱的 1～2 处及原因；对结构或汇总口径的异议与建议（不要自行改动，列出交我裁决）；需要我提供输入的开放问题。

边界：只写上述产出文件。不自行修改 00-README、01-fact-boundaries、packets、archive、成稿与简历；发现它们的问题通过 B 部分随稿提出，由我决定是否变更。

交互分级：成稿之间存在矛盾——先停下来问我，不要自行改稿迁就；对结构或口径的异议——随稿提交，不阻塞；表格形式等表达细节——自主决定。对任务理解有歧义先问。
```

## 4. 验收要点（每章通用）

**口述层**

1. 五段齐全、顺序正确、第一人称；
2. 试讲计时 3～6 分钟（或字数 1000～1600）；
3. 代码符号 ≤3 且首次出现带括注作用；无链接、无 commit 号；
4. 无生活化比喻；技术密度与 00-README §5 黄金样例同档；
5. 第 4 段的数字都带口径；没有数字的章讲清验证方式与边界。

**支撑层**

6. 四板块齐全；随机抽 3 个符号——你都能说出作用（说不出的即返修）；
7. 对照表覆盖简历该条目的每一句主张；
8. 替代方案以最强形态出场，说清本项目的代价与对方更优的场景；
9. 追问有技术信息量，能从口述层自然引出。

**事实**

10. 数字与限定语和 `01-fact-boundaries.md`、对应 packet 一致；无禁用结论清单中的表述。

**随稿议题**

11. 逐条裁决 Agent 的批判性自查与议题：异议成立 → 修改 00-README / 事实包并告知改动；不成立 → 回复理由。不要无声忽略——否则 Agent 会学会沉默，批判通道名存实亡。同一条异议在多章反复出现时，优先怀疑规则本身有问题，而不是 Agent 有问题。

**章特有的验收点**：tools-context——"搜索解锁却没进 Schema"踩坑是否讲了现象、根因、修复、验证四步；memory——三路信号是否讲成协作而非三份列表相加；terminalbench——三类故障归因是否可复述；proactive——发送授权边界是否讲清；positioning——四个能力一句话是否互相不打架；appendix——③的共性模式是否都有成稿支撑。

## 5. 返修模板（定点反馈，防止整篇重写）

```text
对 F:/agent/akashic-agent/docs/interview-guide-workbench/drafts/chapters/chapter-X.md 做定点返修。只修改下面列出的问题，未提及的内容保持原样，不要整篇重写：

1. 位置【口述层·第 N 段】问题：（引用原句）期望：（要什么效果）
2. 位置【支撑层·机制详解·某条目】问题：（……）期望：（……）

完成后在回复中逐条说明改动，不要附加其他变更。
若你执行中发现返修点与其他内容冲突、或暴露出新问题，先停下来向我说明，等我确认后再动手。
```

## 6. 定稿指令（验收通过后，在原会话粘贴）

```text
本章验收通过。更新 F:/agent/akashic-agent/docs/interview-guide-workbench/00-README.md 第 8 节进度表中本章一行为「已验收 2026-09-XX」，不改动其他任何内容。
```

## 7. 常见偏差速查

| 现象 | 处理 |
| --- | --- |
| 口述层写成第三人称说明书 | 返修：按五段模板第一人称重写口述层 |
| 符号堆叠且无作用解释 | 返修：逐条补一句作用说明，或改用精确中文名 |
| 机制只有结论没有实现方式 | 返修：指出具体条目，要求落到实现层（支撑层） |
| 正文混入审计式边界罗列 | 返修：移入支撑层或删除；正文只保留对面试有用的边界 |
| 出现 packet / 01 之外的数字 | 返修：要求数字回链事实包，否则删除 |
| Agent 随稿提出规则或事实异议 | 不是拒绝对象：听取理由，由你裁决——成立你改，不成立回复原因。变更权始终在你，Agent 只建议 |
| 交稿只有合规自查、没有批判内容 | 提醒其补批判性自查与议题；模板填充不是合格交稿 |
| Agent 为确认细节频繁中断 | 提醒交互分级：事实冲突才阻塞，其余随稿；执行细节自主决定 |
| 产出走回旧 stage 结构 | 返修：以 00-README 产物定义为准，archive 结构不继承 |
