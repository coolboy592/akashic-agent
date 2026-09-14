# Stage 4：工具发现与上下文管理——执行阶段任务契约

本文件定义 Stage 4 的 `execute` 阶段任务，不绑定具体 Agent 产品或固定角色。承担本阶段的 Agent 独立完成源码研究、机制分析、方案比较、整章编写和自查；完成后将产物交给 `verify` 阶段，不得自行宣告用户验收。当前阶段状态以 `00-workflow-and-state.md` 为准。

## 目标与入口

读者掌握 Agent 基础，但不了解或遗忘本项目细节。本章围绕简历，讲清具体实现、设计理由、常见替代方案和重要局限，采用连贯的设计学习表达。可以重构旧稿，取消口播入口和回答骨架。

工作台根目录为 `F:/agent/akashic-agent/docs/interview-guide-workbench`。读取：

- `00-workflow-and-state.md` 第 2～5 节：协作边界、写作标准与事实规则；通用要求以此为准。
- `F:/agent/agent简历.md`。
- `packets/04-tools-and-context.md` 与 `drafts/stages/04-tools-context.approved.md`：研究起点和旧稿；旧稿尚未通过新标准复核。

`01-source-of-truth.md`、`03-terminology-and-claim-rules.md` 可按具体疑点补查。自行决定研究顺序、正文结构和自查方式，事实包不是封闭事实清单，不需要沿用旧稿的结构或结论。

## 历史范围与需要讲透的内容

唯一主仓库基线为 `d565ca44356b5d52fe8d00f8cb008c14fff56c0b`，已有源码副本 `F:/agent/akashic-agent-d565`。从该副本或指定提交读取相关调用方、插件、测试和已有运行材料，不使用主工作区当前迁移实现。

本章应讲透以下问题，组织方式由你决定：

- 本地与 MCP 工具如何注册、发现、进入实际模型请求并被调用；预加载、当轮解锁与容量限制怎样配合。
- 工具目录与搜索自身的 token 和轮次开销；按需暴露与全量 Schema 的适用条件及代价。
- 对话管理实际采用滑动窗口、模型摘要还是组合；触发条件、预算估算、压缩范围、摘要模型和提示词、生成与更新策略。
- 完整工具交互单元怎样保留；协议完整与摘要语义正确的区别；信息遗漏、累积失真和容量错误各自的处理与局限。
- 稳定指令、历史、动态记忆和工具 Schema 的实际组织，以及可能的缓存影响。
- `https://github.com/akashic-plugins/observe` 的相关观测实现：与历史基线的对应依据、数据来源、统计口径和展示方式。不能凭日期或 API 版本标签推断兼容性。已有克隆位于本任务目录的 `observe-research/`，可作研究入口，旧日志中的判断未经审核。

区分已有实现、已有运行统计和已完成对比实验。先查已有观测与记录，不因工作台未收录就建议重建功能，也不把已有看板当成优化效果证明。涉及其他章节只核对必要接口。

## 产物与边界

在工作台内写入：

- `drafts/stages/04-tools-context.candidate.md`：完整候选章。
- `drafts/stage4-execution/research-and-self-review.md`：简短记录关键来源、所用版本和未解决的问题，足以支持定点审核，无须另写流程审计报告。

必要临时材料可放在 `drafts/stage4-execution/`。保留已有日志与研究材料。不得覆盖旧 approved、修改正式指导书、简历或业务代码；不运行新评测，不暂存、提交、推送或公开分享。事实包若需纠正，在随稿依据中说明，由审核时处理。

Agent 的命令、API、模型、认证和会话配置由工作区之外的适配层负责，不写入任务契约；任何密钥都不得进入 Prompt、正文或日志。已删除的 `drafts/gpt-5.6-sol-full-draft.md` 不恢复或读取历史副本。

任务理解有歧义或不确定时，返回 `needs_user_decision` 并说明问题，不据此推进。范围内可通过阅读解决的实现问题自行研究。完成后读回正文并自查，返回 `status`、`artifacts`、`evidence`、`unresolved_issues` 和 `recommended_next_phase`；建议的下一阶段应为 `verify`，不得继续其他章节。
