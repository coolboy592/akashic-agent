# 插件 v3 被动回复组合接入点任务合同

- 状态：implementation candidate
- 日期：2026-08-15
- 关联条款：PLG-001～PLG-004、PLG-008～PLG-010、PLG-014、STA-001～STA-003
- 上游：[0036](../decisions/0036-plugin-composition-keeps-promotion-owner.md)、[Cordis 能力等价](cordis-plugin-capability-parity.md)、[lifecycle 接入点](plugin-lifecycle-seam-task-contract.md)、[candidate Root 隔离](plugin-candidate-root-isolation-task-contract.md)、[持久化状态地图](persistence-state-map.md)

## 1. 目标

为 Citation/Meme 组合迁移补齐两个 Core-owned 窄边界，不把领域实现收回 Core：

```text
┌────────────────────────── frozen snapshot / Root ──────────────────────────┐
│                                                                           │
│  turn.prompt_render ──serial──▶ Citation ──service──▶ Meme                │
│                                                                           │
│  turn.after_reasoning.preprocess ──▶ Citation ──▶ Meme                    │
│  turn.after_reasoning.cleanup    ──▶ Citation final cleanup               │
│                                     │                                     │
│                                     ├─ persist_user_metadata              │
│                                     └─ persist_assistant_metadata         │
│                                                                           │
│  declared workspace root: memes                                           │
│     stable/formal ───────────────▶ <workspace>/memes                      │
│     candidate ── isolated copy ──▶ <attempt-workspace>/memes              │
│                                      ▲                    ▲                │
│                                      │ plugin Root        │ Dashboard      │
└──────────────────────────────────────┴────────────────────┴────────────────┘
```

插件继续拥有 prompt 文本、引用提取、协议清理、meme catalog、图片选择与 Dashboard CRUD。
Core 只拥有 message 持久化提交点、workspace root 的 candidate 投影、Root/Dashboard 的同代
路径以及 Effect/generation 清理。

## 2. Change intent

```yaml
change_type: feature
semantic_delta: compatible
capability_owner: core
consumer_scope:
  - composition plugin api v3
  - passive Prompt and AfterReasoning lifecycle
  - candidate workspace isolation
  - v3 DashboardContext
runtime_patch: required
runtime_patch_reason: "只有 Core 能在 assistant message 提交点原子合并插件 metadata，并为 candidate/formal Root 分配同代 workspace 投影。"
authoritative_state_owner: "SessionStore owns persisted assistant rows; each plugin owns values written through the metadata seam; Core owns workspace and candidate lifetime."
client_only_alternative: "Not applicable; these are server lifecycle and generation publication boundaries."
```

## 3. Message metadata 合同

- `AfterReasoningCtx.persist_user_metadata` 是当前 Turn 的 user message 扩展字段出口。插件只声明
  待随本 Turn 的 user pending row 一次提交的字段，不获得 Session、SessionStore、SQL 或删除权限。
  message identity、输入正文、附件、显示正文、LLM 投影、控制 Turn 与其他 Core persistence 字段
  只能由 Core 写入；插件写入时 fail-loud。v3 字段与迁移期 `persist:user:*` legacy slot 重名时
  fail-loud。

- `AfterReasoningCtx.persist_assistant_metadata` 是当前 Turn 的可写字典。插件只声明待随最终
  assistant message 一次提交的扩展字段，不获得 SessionStore、SQL 或删除权限。
- `after_reasoning.seal_metadata` 是唯一 merge/validation owner：它把 v3 字典与 legacy slot
  合并为一次不可再改写的 commit input，与固定字段、退役字段或两套插件出口重名时
  fail-loud。该 slot 是 Core-private commit input，不是插件可写 export。
- user metadata 在 pending user row 构造前完成 merge/validation；assistant metadata 由
  `after_reasoning.seal_metadata` 冻结。user/assistant message 在 seal 前只构造成 pending rows，
  不挂入 Session；原 Phase DAG 中
  依赖 `persist_user` 的 late legacy writer/observer 继续运行且 writer 无需新增 `produces`
  声明。`_PersistAssistantMessageModule` 只 materialize assistant pending row；
  `SessionManager.append_messages()` 在同一无 await 临界段完成 SessionStore 原子 append、稳定
  ID 回填和 Session cache adoption，随后发生取消也不会造成 DB/cache 分叉。消息未提交时不会
  单独留下 metadata 或半条 Turn。
- 正常变化只有新 assistant row 随 Turn 追加；本 seam 不更新或删除旧 message。取消、失败
  与 session append 的恢复语义保持现状。
- v2 phase slot 继续存在到对应插件差分 Gate 通过；最终 v2 删除 PR 再移除 legacy
  `persist:user:*` 与 `persist:assistant:*` 插件出口。

## 4. 声明式 workspace root 合同

- v3 namespace 可选导出 `workspace_roots = ("name", ...)`。每项必须是无斜杠、无点段的
  顶层目录名；重复、绝对路径、嵌套路径和反斜杠均 fail-loud。`plugin-data` 是 Core
  generation 私有数据树，`runtime` 持有 candidate attempt 自身；二者都不能声明为共享
  产品 root，避免目标冲突或把 candidate copy 递归进自身。
- `ctx.workspace_root(name)` 与 `DashboardContext.workspace_root(name)` 只解析该插件已声明的
  root；未声明名字 fail-loud。它是支持 API 与 owner seam，不是同 UID Python 安全沙箱。
- formal/stable Root 解析到 Core 分配的正式 workspace。candidate 创建 Root 前把所有声明且
  已存在的目录复制到 attempt workspace；缺失目录保持缺失，非目录声明 fail-loud。相同名字
  在多个插件间只投影一次。Core 拒绝顶层 root 符号链接和 workspace 越界解析；candidate
  clone 的冻结声明必须与其 generation 声明一致。
- candidate 插件与 candidate Dashboard 必须读取同一个 attempt root；candidate mutation、
  discard、失败或取消不能改变正式 root。promotion 丢弃 candidate copy，再由 formal Root 和
  formal Dashboard 解析正式 root。
- workspace root 是产品级共享资产，不迁入 plugin-data；其中的增改删仍由具体插件领域
  合同和显式用户操作授权。Core 不为任意插件写入建立通用 transaction 或 rollback 假象。

## 5. Citation/Meme 顺序合同

- Citation 提供 `citation.protocol` service；Meme 用同名 typed ServiceKey 声明 `inject`。
  这让 provider 先完成 listener 注册，避免依赖插件 ID、安装顺序或 phase slot 名称。
- Prompt serial 顺序是 Citation protocol section 后 Meme catalog section。
- preprocess serial 顺序是 Citation 提取 cited IDs、保留 trailing meme tag，再由 Meme 消费
  tag 并附加 media；cleanup 由 Citation 清除其余 trailing protocol tag。
- listener 的注册和 service provider 都是 Fiber Effect；unload/reload 后不得残留。

## 6. 验证

- metadata：插件字段随同一 user/assistant row 持久化；固定/退役/legacy 重名均 fail-loud；
  SessionStore append 失败或取消时不产生部分 message、孤立 metadata 或第二次提交。
- workspace root：声明校验、stable 路径、candidate copy、同代 Dashboard 路径、candidate
  写后 discard 正式目录逐字节不变、formal promotion 使用正式路径、Root drain 后无 binding。
- lifecycle：精确 Citation/Meme provider commits 下，真实 Manager snapshot lease 驱动 Prompt
  与 AfterReasoning；断言 section 顺序、cited IDs、最终正文、media、meme tag、持久 row 和
  code-style tag 排除。
- 相关 composition loader/kernel/lifecycle/session tests、两套 Pyright、compileall、
  `git diff --check`、插件 tests、跨仓 exact-commit Gate 与公开 change-impact Gate 通过。

恢复点：`backup/plugin-v3-passive-response-before-20260815`。本 PR 不写正式 Akashic
workspace；测试只使用临时 workspace。
