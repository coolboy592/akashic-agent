# 插件 v3 生产替代清单

本文是 Issue [#394](https://github.com/kachofugetsu09/akashic-agent/issues/394) 的唯一执行清单。
[插件 v3 最终迁移地图](plugin-v3-final-migration-map.md)负责解释目标架构、现有 PR DAG 和删除顺序；
本文只记录每项能力是否已经具备可替代生产的证据。状态必须由实际 commit、测试和 Gate 推进，
不能用实现者自述或单个单元测试把项目标成完成。

## 1. 任务合同

### 1.1 目标与完成标准

目标是让 Akashic 的通用插件平台只接受 v3 namespace，以
`Context / Service / Fiber / Effect / typed event` 组合能力，同时保留 Akashic 的 candidate
验证、promotion、snapshot lease 和旧 generation drain。除 Default/Wake Proactive 两族的内部
领域实现外，所有已跟踪插件都迁入 v3；两族只保留私有 proactive 运行实现，不再取得通用 v2
插件 ABI。

只有下列条件全部成立，才能向维护者报告“可替代线上 Akashic”：

- [ ] Core 基线清单全部为 `READY`；
- [ ] 插件清单全部为 `READY` 或已批准的 `PRIVATE_LEGACY_RUNTIME`；
- [ ] `api_version = 2`、`Plugin`、`PluginContext`、`ToolHook` 和 Manager 固定贡献面不再构成
      production 插件兼容 API；
- [ ] v2 删除批次 A～J 全部完成，或只剩明确属于 Proactive 私有实现、无法被普通插件调用的代码；
- [ ] 精确锁定的跨仓组合、集中 E2E 和复制 workspace 数据安全演练全部通过；
- [ ] hua-home 正式 workspace、服务、渠道、凭据和端口在本任务中未被修改；正式替换另行批准。

### 1.2 Change intent

```yaml
change_type: migration
semantic_delta: breaking
capability_owner: mixed
consumer_scope:
  - akashic core plugin runtime
  - 21 locked external plugins
  - 8 in-tree plugin implementations
  - later admitted GitHub Watcher canonical source
runtime_patch: required
runtime_patch_reason: >-
  stable/candidate publication、generation lease、能力冲突、进程和渠道提交边界由 Core
  拥有；插件只拥有领域实现和自身 data schema。
authoritative_state_owner: >-
  SessionStore、Memory store、plugin data owner、Proactive state store、plugin artifact
  publisher 和外部 channel/process owner 各自保持不变。
invariants:
  - PLG-001 through PLG-014
  - SES-001 through SES-008
  - OUT-001 through OUT-005
  - WSP-001 through WSP-005
  - PRO-001 through PRO-003
  - CTRL-003
  - MOB-006
  - BAK-001
  - TST-001 through TST-008
protected_state:
  - sessions.db messages and attachments
  - consolidation_writes.db and compaction receipts
  - memory2.db and Markdown memory archives
  - akasha.db and deterministic sidecars
  - plugin-data
  - proactive.db, wake_proactive.db, drift.db, PROACTIVE_CONTEXT.md, and proactive_pending.md
  - schedules, quota, plugin reload journal, rollout fact, artifacts, manifest, pointers, and credentials
allowed_effects:
  - source, tests, docs, CI, and disposable test workspace changes
  - isolated local processes, loopback ports, and controlled external read-only probes
forbidden_effects:
  - writes to hua-home or its formal workspace
  - production channel delivery
  - deletion or rewrite of existing authoritative messages, memories, plugin-data, or proactive state,
    except the separately tested explicit Plugin Undo operation on a disposable copy
  - direct edits to installed plugin cache
rollback: >-
  Core recovery point is backup/plugin-v3-full-migration-base-20260816 at
  501dad1c86cfe2cf4c62982d4dde92e831110251. Each external plugin uses its own base commit and
  independent worktree; data tests use disposable copies and never become recovery owners.
```

### 1.3 状态定义

| 状态 | 含义 |
|---|---|
| `OPEN` | 还没有可审阅实现，或能力合同尚未闭合 |
| `IMPLEMENTED` | 有实现和定向测试，但尚未通过独立 review/组合 Gate |
| `CANDIDATE` | 精确 commit 已通过独立 review 与本族行为 Gate，尚未进入最终全量组合 |
| `READY` | 在最终集成 head 上通过适用的集中 E2E、数据 write-set 和 cleanup 验收 |
| `PRIVATE_LEGACY_RUNTIME` | 仅允许 Default/Wake Proactive 私有领域实现使用；不暴露通用 v2 插件入口 |
| `BLOCKED` | 缺 canonical source、凭据、外部环境或必须由维护者决定的语义 |

状态从 `CANDIDATE` 进入 `READY` 时必须记录 Core commit、插件 commit、scenario catalog 摘要和
Gate 报告。分支名、PR 号和浮动 ref 不能代替 commit。

## 2. Core 基线能力

### 2.1 已有候选底座复核

| ID | 能力 | 当前状态 | 进入 `READY` 的剩余证据 |
|---|---|---|---|
| C01 | Context / Service / Fiber / Effect 与逆序、抗取消清理 | `CANDIDATE` | 最终集成 head 的 kernel/cleanup 回归 |
| C02 | serial / parallel / transform / observe typed events | `CANDIDATE` | C16 四项 admission 收口后累计回归 |
| C03 | immutable topology、parent edge 与 composition revision | `CANDIDATE` | candidate/formal drift mutant 保持通过 |
| C04 | isolated candidate Root、atomic stable batch、promotion/lease/drain | `CANDIDATE` | full-fleet reload/discard/promote Gate |
| C05 | Validation / Health / Incident 分离与 runtime inspection 数据模型 | `CANDIDATE` | inspection 查询面与 full-fleet health 场景 |
| C06 | generation `data_root`、workspace roots 与 candidate 隔离 | `CANDIDATE` | 复制 workspace write-set 验证 |
| C07 | typed Tool 六段链与 exactly-once result | `CANDIDATE` | Tool 组合 Gate 纳入最终 exact lock |
| C08 | prepared context、Memory capability 与原子 assistant metadata commit | `CANDIDATE` | Akasha/Observe/Emotion 组合 Gate |
| C09 | Skill / Drift skill / Dashboard generation 投影 | `CANDIDATE` | 全量插件 collision、dispose、artifact immutability |
| C10 | passive WebUI stable snapshot E2E | `CANDIDATE` | 最终全量 WebUI-only E2E |

### 2.2 必须补齐的 Core seam

| ID | 能力 owner | 状态 | 验收 oracle | 首个真实 consumer |
|---|---|---|---|---|
| C11 | committed channel command catalog | `CANDIDATE` | command/provisional 独立复核 25 tests、累计 command/kernel/loader/Manager/hot-reload 339 tests、Basedpyright/compileall/diff-check 已通过；Terra xhigh review 无 P0/P1，待 Status Commands 首个 consumer Gate | Status Commands |
| C12 | scoped MCP capability | `OPEN` | candidate port/data 隔离、readiness、route、drain、失败零残留 | Calendar MCP |
| C13 | managed process capability | `OPEN` | start/ready/cancel/terminate/log Effect；同 generation 单实例 | Calendar MCP |
| C14 | inbound/outbound channel capability | `OPEN` | committed binding、stop/drain/swap、失败恢复旧代、无重复发送 | Feishu / QQBot |
| C15 | timer / proactive source / turn enqueue capability | `OPEN` | skip/failure 可区分、timer 回收、turn owner、无候选发送 | Calendar MCP |
| C16 | v3 admission/lifecycle 收口 | `CANDIDATE` | `4ba266ad` 已通过独立 review；non-callable listener、spawn coroutine、apply signature、wrong-task lifecycle 全部 fail-loud，malformed admission 零 data-dir 写入 | Core |
| C17 | mobile UI/query capability | `OPEN` | committed catalog、lease、bounded query、candidate 不发布 | Akasha / Observe |
| C18 | Core-private v3 generation metadata | `CANDIDATE` | `2d9fb408` 已通过独立 review；v3 stable load、candidate clone 与 formal rebuild 不再构造或读取 `PluginContext`，59 loader + 208 Manager/hot-reload 回归通过 | Core |
| C19 | full-fleet Health/Incident/Topology inspection | `CANDIDATE` | stable lease 按插件投影 current Fiber/Health、累计与 bounded Incident、Topology；mixed active/inactive v3 + v2 inspection 与 kernel/protocol 回归通过，独立 review 的 inactive projection P1 已关闭 | 全量 runtime |
| C20 | Proactive 私有兼容岛 | `OPEN` | Core-private registry 只接收六个内建 module identity；外部 v2 manifest/import/discovery fail-loud | Default/Wake Proactive |
| C21 | generation-scoped background job / LLM capability | `OPEN` | committed catalog、trigger/interval、LLM generation lease、cancel/drain、candidate 不调模型 | Emotion |

实现原则：C11～C17、C21 只由表中的首个真实 consumer 拉动，不提前复制
`commands()/mcp_servers()/managed_services()/channels()/jobs()/proactive_*()/mobile_ui()` 旧方法。

## 3. 每个插件的完成定义

每行插件只有同时满足以下检查项才能进入 `CANDIDATE`：

- [ ] canonical source、base commit 和 candidate commit 已固定；
- [ ] 模块只暴露 `api_version = 3` 与精确 `apply(ctx, config)`；
- [ ] capability、Service 依赖、listener 顺序、静态投影和 data/workspace roots 声明完整；
- [ ] `apply()` 在 candidate 环境不写正式 workspace、不发送、不占正式 endpoint；
- [ ] dispose/reload/cancel 后 task、Effect、listener、process、port 和 module 均清理；
- [ ] v2 与 v3 的正常、空、拒绝、错误和取消行为等价；批准的差异单独写 `semantic_delta`；
- [ ] 真实 `PluginManager` install → snapshot lease → consumer 行为链通过；
- [ ] 插件仓 CI 与 Core contract 固定 exact Core/protocol commit；
- [ ] 对应 v2 owner 已加入可删除 inventory，且没有未列出的 consumer。

单元测试和契约测试负责每个插件自身行为；不能为每个插件单独启动完整 Docker E2E。

## 4. 插件迁移账本

### 4.1 已有 v3 候选

| 插件 | v3 能力 | 状态 | 最终组合批次 |
|---|---|---|---|
| Citation | prompt protocol、assistant metadata | `CANDIDATE` | E1 |
| Meme | required citation service、prompt/media、Skill、Dashboard | `CANDIDATE` | E1 |
| Shell Restore | `tool.input.prepare` | `CANDIDATE` | E2 |
| Shell Safety | `tool.execution.authorize` | `CANDIDATE` | E2 |
| Tool Loop Guard | typed authorization、per-generation state | `CANDIDATE` | E2 |
| Default Memory | static Memory capability、result observer、Dashboard | `CANDIDATE` | E1 |

### 4.2 待迁移 external plugins

| 插件 | 当前 v2 能力 | 依赖 Core seam | 状态 | 最终组合批次 |
|---|---|---|---|---|
| Context Pressure | lifecycle / prompt context | C02/C08 | `OPEN` | E1 |
| Daynight Gate | proactive module / prompt gate | C15 | `OPEN` | E3 |
| Emotion | Dashboard、mobile、Drift Skill、proactive module、job/LLM | C09/C15/C17/C21 | `OPEN` | E1/E3 |
| Plugin Undo | command、before-turn、显式 interaction 撤销 | C02/C11 | `OPEN` | E1/E3 |
| Observe | Dashboard、mobile、committed event observers | C02/C17 | `OPEN` | E1 |
| Setup Helper | lifecycle、command | C02/C11 | `OPEN` | E3 |
| Status Commands | mobile、command、before-turn | C02/C11/C17 | `OPEN` | E3 |
| Calendar MCP | MCP、managed process、proactive source | C12/C13/C15 | `OPEN` | E2/E3 |
| Computer Use Linux | Skill、MCP | C09/C12 | `OPEN` | E2 |
| Feed MCP | Skill、MCP、proactive source | C09/C12/C15 | `OPEN` | E2/E3 |
| Feishu | channel | C14 | `OPEN` | E3 |
| Fitbit MCP | MCP、managed process、proactive source、mobile | C12/C13/C15/C17 | `OPEN` | E2/E3 |
| Steam MCP | Skill、MCP、proactive source | C09/C12/C15 | `OPEN` | E2/E3 |
| QQBot | channel | C14 | `OPEN` | E3 |
| Proactive Feedback | Dashboard、mobile、committed event observers | C02/C17 | `OPEN` | E1 |
| Huayue Skills | Skill roots | C09 | `OPEN` | E3 |

### 4.3 In-tree plugins 与保留族群

| 插件实现 | 目标 | 依赖 Core seam | 状态 | 最终组合批次 |
|---|---|---|---|---|
| Akasha | pure v3，保持 Memory engine、Dashboard 与 mobile recall | C08/C17/C18 | `OPEN` | E1/E4 |
| Default Proactive | v3 薄入口 + 原内部 runtime | C15/C20 | `OPEN` | E3/E4 |
| Proactive Flow | Default 族私有实现 | C20 | `OPEN` | E3/E4 |
| Drift Flow | Default 族私有实现 | C20 | `OPEN` | E3/E4 |
| Wake Proactive | v3 薄入口 + 原内部 runtime | C15/C20 | `OPEN` | E3/E4 |
| Wake Proactive Flow | Wake 族私有实现 | C20 | `OPEN` | E3/E4 |
| Wake Drift Flow | Wake 族私有实现 | C20 | `OPEN` | E3/E4 |

Default Memory 是第 8 个 in-tree 实现，已列在 4.1；本节列出其余七个。六个 Proactive
实现最终不得继续继承通用 `Plugin`、声明 `api_version = 2` 或让
`PluginManager` 保留 `proactive_*()` 固定聚合。允许保留的是领域 runtime、状态机、prompt、
dedupe、ack、cursor、hazard 和原数据库协议。

### 4.4 GitHub Watcher

| 项目 | 状态 | 解除阻塞条件 |
|---|---|---|
| canonical source 与公开凭据审计 | `BLOCKED` | 定位真实 repository、确认没有凭据和私有 artifact |
| v3 迁移 | `BLOCKED` | source 固定后使用 C02/C06/C15，不建立 GitHub 领域 Core Service |
| 行为 Gate | `BLOCKED` | fake/controlled client + 一次可控只读真实 API probe |

GitHub Watcher 在 canonical source 未确认前不计入“已完成”，也不允许通过复制 cache 制造候选。

## 5. v2 删除账本

删除顺序沿用[最终迁移地图](plugin-v3-final-migration-map.md#7-v2-物理删除清单)的 A～J。
每批删除 PR 必须先执行 consumer scan，再记录删前 owner、最后 consumer、替代能力和 Gate。

| 批次 | 对象 | 状态 |
|---|---|---|
| A | Default Memory legacy data name | `OPEN` |
| B | legacy assistant metadata slots | `OPEN` |
| C | legacy Dashboard ABI | `OPEN` |
| D | ToolHook ABI、catalog 与 traces | `OPEN` |
| E | v2 static-active / stable-health exception | `OPEN` |
| F | `PluginContext` | `OPEN` |
| G | v2 doctor / class discovery | `OPEN` |
| H | `Plugin` base、registry、Manager 固定能力方法 | `OPEN` |
| I | RuntimeSnapshot v2 固定字段 | `OPEN` |
| J | v2 lock、Gate 和 runtime 双路径 | `OPEN` |

最终 production scan 必须证明：普通插件无法通过 import、动态 discovery、manifest 或 cache
重新进入 v2；测试 fixture 若保留历史格式，必须位于明确的 migration-test namespace。

## 6. 验证分层与克制的 E2E

### 6.1 每个 PR 都运行，但不启动完整服务

1. 纯函数和领域行为等价测试；
2. 事件、Service、Effect、candidate/write-set 的 kernel oracle；
3. 真实 `PluginManager` install/lease/dispose 测试；
4. 修改文件的静态检查与仓库 contract；
5. 受影响族群的 exact-commit Gate。

### 6.2 只保留四个集中 E2E

| 批次 | 一次覆盖的组合 | 主要 oracle | 运行时机 |
|---|---|---|---|
| E1 Passive/Data/Mobile | Akasha、Default Memory、Citation、Meme、Context Pressure、Emotion、Observe、Proactive Feedback、Plugin Undo | prompt/recall/metadata/media、bounded mobile query/lease、SessionDB 普通 append-only；显式 `/undo` 按 `control_turn_id` 原子删除完整 interaction、embedding/reference 协调与恢复；Akasha/plugin-data write-set | 被动与数据族全部 `CANDIDATE` 后一次 |
| E2 Tool/MCP/Process | Restore、Safety、Loop Guard、Calendar/Computer Use/Feed/Fitbit/Steam | transform→authorize→invoke、readiness、端口、取消、process cleanup、受控外部只读调用 | MCP/process 族全部 `CANDIDATE` 后一次 |
| E3 Fleet/Channel/Proactive | Commands、Feishu/QQBot recording adapters、Daynight、Emotion、Calendar/Feed/Fitbit/Steam sources、Huayue Skills、Default/Wake 薄入口 | full boot、catalog、candidate discard/promote、reload；loopback channel 正向收发；固定时钟/模型/sink 的 enabled proactive empty/skip/source/model/delivery/restart | 全插件接线完成后一次 |
| E4 Production Rehearsal | E1～E3 的 exact heads + 复制的真实 workspace，WebUI-only | DB integrity、完整 write-set、artifact/pointer、restart、stop cleanup、恢复证据 | 删除 v2 后最终一次 |

E1～E3 使用一次性 workspace 和受控端点。E4 只能使用经过校验的副本；正式 hua-home workspace、
正式 channel credential、正式 proactive sender 和正式端口均不进入本任务。

### 6.3 数据安全 oracle

| 状态 | 正常允许变化 | 本迁移禁止变化 | 恢复/验收证据 |
|---|---|---|---|
| `sessions.db/messages` | E1/E4 测试 session 只追加测试 user/assistant rows | 既有正文 UPDATE/DELETE、跨 session 混写 | SQLite backup、integrity、row/write-set、session identity |
| Plugin Undo interaction | 只在 disposable copy 上由显式 `/undo` 和精确 `control_turn_id` 操作；删除前生成不可覆盖的完整 SessionDB backup；同一事务删除完整 user+assistant interaction 及匹配 `message_embeddings` 并回滚 cursor；Memory2 通过 superseded/active 与 `memory_replacements` 留替代证据；Akasha/pending 旧引用失效或重建 | 普通 turn 删除、仅删一侧 message、错误身份/cascade、覆盖旧 backup、硬删 Memory2 事实、遗留 Akasha/pending 引用、部分失败伪装成功；非目标 message、附件与 `seq` high-water 不得变化 | backup 路径与完整性、目标/保留 message IDs、embedding 差集、旧/新 cursor、非目标 rows/附件/`seq`、memory replacement、Akasha/pending 结果、audit 与失败恢复 |
| `memory2.db` / Markdown | 仅显式测试策略允许的新增 | 既有事实覆盖、删除、自动清理 | 前后摘要、表计数、档案备份 |
| `consolidation_writes.db` | 测试 compaction source 追加幂等 receipt | 既有 receipt 删除、同 key 内容漂移、跨库失配 | integrity、source_ref/kind、payload 与 source-plan digest |
| Akasha sidecars | disposable copy 可按固定输入重建 | 正式 sidecar 写入或残缺图替代成功 | 输入 hash、embedding coverage、parity |
| plugin-data | 测试 generation 在自己 root 内增加 | candidate 写 stable root、卸载删数据 | 路径归属、tree digest、candidate discard |
| proactive/wake/drift DB 与 Markdown | E3/E4 在显式启用时按原状态机更新测试副本；获授权 job 原子更新测试 `PROACTIVE_CONTEXT.md` | schema 偷迁、覆盖规则面板、重复发送、ack/cursor 丢失、提前清 pending | schema/file identity、continuity rows、recording sink、restart parity |
| plugin artifacts/pointers/runtime journal | 安装事务增加 immutable candidate、journal/rollout fact，提交后改 pointer/manifest | plugin/runtime 改 artifact bytes、失败后残留 pointer、删审计 journal | artifact digest、manifest、journal、rollout fact、stable/latest identity |
| credentials/config | 一次性测试配置可创建 | 输出、提交、复制或改写正式 secret | 文件 inventory、权限、脱敏报告 |

## 7. 多 Agent 分工规则

- 主 agent 是本清单、Core capability 合同、集成分支和最终 Gate 的唯一 writer。
- GitHubLuna 只在 Core seam 稳定后接收按仓库隔离的插件迁移；一个 agent 一个 repository/worktree/
  branch，必须先记录 base commit 和 allowed paths。
- 适合并行的单位是彼此不共享权威文件的插件仓库，例如 lifecycle 插件组与 MCP 插件组。
- Core seam、同一外部插件、跨仓 lock、迁移清单和最终 E2E 不能由多个 writer 并发修改。
- 每个 agent 必须提交 clean handoff commit；主 agent 复核 diff、测试和真实行为后才更新本表状态。
- 如果多个插件等待同一个未完成 Core seam，则保持等待，不让 agent 在插件仓复制临时兼容层。

## 8. 实施波次

- [x] W0：提交本清单，冻结状态定义、数据边界和 E2E 批次；
- [ ] W1：完成 C16、C18、C19，并复核 C01～C10；
- [ ] W2：以真实 consumer 依次完成 C11～C17；
- [ ] W3：并行迁移 lifecycle/metadata/command 插件；
- [ ] W4：并行迁移 MCP/process/channel 插件；
- [ ] W5：迁移 Akasha、Proactive Feedback，并建立 C20；
- [ ] W6：执行 A～I 删除批次，运行 production v2 consumer scan；
- [ ] W7：集中运行 E1～E3，关闭所有行为差异；
- [ ] W8：执行 J、运行 E4 与独立只读 review；
- [ ] W9：对账 exact heads、CI、报告与文档，只在全部为 `READY` 后汇报。
