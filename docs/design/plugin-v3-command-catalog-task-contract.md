# 插件 v3 committed command catalog 任务合同

- 状态：candidate / independent review passed
- 日期：2026-08-16
- 实现起点：`19f2cca2`
- 关联条款：PLG-001～PLG-004、PLG-008～PLG-010、PLG-014、TST-001～TST-008
- 上游：[v3 production readiness checklist](plugin-v3-production-readiness-checklist.md)、[lifecycle seam 合同](plugin-lifecycle-seam-task-contract.md)
- 参考：旧 capability lane `45eab635`、DSH `packages/interaction/commands`

## 1. 目标与边界

本 PR 给 v3 Fiber 提供人类命令注册能力，并把 immutable catalog 编译进 stable `RuntimeSnapshot`。
命令是用户在 channel 输入的控制请求，不是供模型选择的 Tool；命中发生在 Session acquisition 和模型调用前。
`semantic_delta: compatible`；现有 v2 before-turn command 继续工作，迁移期与 v3 claim 冲突时 fail-loud。

```text
v3 Fiber ── Effect register ──► PluginCommands in candidate Root
                                      │ freeze + collision
                                      ▼
candidate snapshot ── promotion ──► stable CommandRegistry
                                      │
channel discovery ◄───────────────────┤
passive input ── known slash command ─┴─► direct result / no Session / no model
```

本任务不迁移插件、不写 Session/database/workspace/plugin-data、不发送真实渠道消息，不把 catalog 合并进
`ToolRegistry`，也不复制 v2 `telegram_bot_commands()/mobile_bot_commands()` 作为新的插件 API。

## 2. 合同与失败语义

1. `COMMANDS = ServiceKey("core.commands")` 由每个 CompositionRoot 提供；插件用
   `PluginCommands.register(ctx, CommandDefinition(...))` 注册，Effect 反向注销 canonical name 与 aliases。
2. canonical name、alias 和迁移期 v2 claims 共享同一 namespace；duplicate 在 candidate/stable snapshot compile
   阶段 fail-loud。descriptor 固定 `name/description/input_hint/aliases/owner` 并进入 snapshot identity。
3. candidate registry 只存在 candidate snapshot；Manager/Telegram/Mobile/WebUI discovery 只读取 current stable
   snapshot。candidate discard、formal rebuild 与 payload replacement 后 catalog identity 必须一致。
4. known command 在 stable lease 内执行；unknown/non-command 继续原 lifecycle。handler 必须返回非空
   `CommandResult(success|error, text)`，异常或非法结果进入既有 turn error path，不伪装成功。
5. 第一版 universal descriptor 同时投影到现有 Telegram/Mobile discovery adapter；aliases 不作为展示项。
   最后一个 v2 consumer 迁走后删除两个 Manager 聚合属性和 bootstrap 固定传参。
6. v3 canonical name 采用 Telegram 与 Mobile 共同可接受的 `[a-z][a-z0-9_]{0,31}`；迁移期 v2
   claim 继续接受原有连字符，避免在迁移完成前偷改旧插件输入合同。Core 内建
   `/stop` 保留，canonical、alias 与 v2 claim 均不得占用 `stop`；description 最长 256 字符。
7. command admission 由 `AgentLoop` 在 model selection、Session、resume 与 `TurnStarted` 之前执行；
   `PassiveTurnPipeline` 保留 direct-call 入口，但已 admission 的输入不得重复执行 command。
8. Telegram discovery 在 stable publish 时事务性刷新；Core 将 candidate 保存在不可租用的
   provisional target，旧 stable 仍是唯一公开 `current`，但暂停新 lease，再发布远端 catalog。
   成功后才原子切换 `current`、开放 candidate 并 retire 旧 stable；失败则恢复旧远端 catalog，
   公开 stable pointer 从未经过 candidate，candidate 也从未接受 lease。
   Mobile discovery 每次从 exact stable snapshot provider 读取。纯 command catalog 切换只暂停新 admission，
   不调用 endpoint quiescer，不等待旧 stable lease；旧 lease 继续观察旧 snapshot，新 admission
   只能观察最终打开的新 snapshot。

## 3. 验证与停止条件

- unit：大小写、`@botname`、raw args、sync/async handler、非法结果、canonical/alias/legacy collision、
  `/stop` 保留、257 字符 description 拒绝、逆序 cleanup；
- real Manager mixed v2/v3 stable：collision 阻止 publish；candidate 不改变 stable discovery；promote 后才可见；
- stable snapshot lease 的真实 `PassiveTurnPipeline` known command 在 Session/context/model 前 short-circuit，unknown
  command 保持原 before-turn path；
- descriptor 任意字段变化会改变 snapshot identity，payload replacement 复制新的 registry；
- provisional store 的 finalize/rollback 都保持旧 stable 与 candidate lease 边界；远端 catalog 回调期间
  `current` 与全部 discovery 仍指向旧 stable，installed artifact stable pointer 也尚未切换，成功后才一起提升；
- command、loader、Manager、passive turn 定向回归，Basedpyright error-level、compileall、`git diff --check`。

任何 candidate command 提前公开、命令碰撞被覆盖、known command 创建 Session/调用模型、alias 出现在 discovery、
或 Root dispose 后 names/Effect 残留都停止交付。

## 4. 当前验证证据

- command/provisional/installed promotion/cancellation 独立复核：`25 passed`；
- command、kernel、loader、Manager、hot reload 累计回归：`339 passed`；
- 修改文件 Basedpyright：`0 errors, 0 warnings, 0 notes`；
- `compileall` 与 `git diff --check` 通过；
- Terra xhigh 独立 reviewer 对 provisional、rollback 与公开 observation 完整复核，无 P0/P1。

## 5. 回滚

代码恢复点为分支 `backup/plugin-v3-c11-pre-terra-provisional-fix-20260816`。本任务没有正式运行数据变更；
回滚只需撤销本 PR 的源码、测试和合同提交。
