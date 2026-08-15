# 插件 Tool v3 迁移组合 Gate 任务合同

- 状态：implemented candidate
- 日期：2026-08-15
- 关联条款：PLG-001、PLG-003～PLG-004、PLG-014、TST-001～TST-003、TST-007
- 上游合同：[插件 Tool 组合事件任务合同](plugin-tool-composition-events-task-contract.md)

## 1. 目标

在独立临时环境从公开仓库检出三个固定提交，使用真实 `PluginManager` 把 Shell Restore、Shell Safety 与 Tool Loop Guard 装入同一个 stable `RuntimeSnapshot`，再从正式 snapshot lease 经过 `ToolExecutor` 验证转换、授权和重复调用截断。

这张 PR 不修改 composition 内核，也不把插件实现复制到 Core。Core 只保存不可变组合身份、场景和跨仓验收器。

## 2. Owner 与语义变化

```yaml
change_type: feature
semantic_delta: none
capability_owner: mixed
consumer_scope:
  - shell_restore
  - shell_safety
  - tool_loop_guard
runtime_patch: none
runtime_patch_reason: "Core 已由上游 PR 提供 typed Tool composition seam；本任务只验证真实 consumer 组合。"
authoritative_state_owner: "各插件拥有自己的策略和 generation state；Core snapshot 拥有装配顺序。"
client_only_alternative: "不适用；这是运行时插件组合合同。"
```

## 3. 不变量与副作用

- 锁文件逐项固定 `repository / requested_ref / resolved_sha / change_source_pr_head`，三个 revision 都是同一 40 位 commit；报告另外固定 Gate version 和 R10 Tool seam 的协议 source commit/path/blob/hash。
- Gate 只在一次性目录创建 Git checkout、workspace、plugin home 和 plugin-data；不读取或写入正式 Akashic workspace、正式插件 cache、session、凭据或渠道。
- `PluginManager.load_all()` 必须发布一个包含三个插件的 stable Root；执行通过该 snapshot 的 lease 绑定，不允许手工另建 Root 代替正式装配。
- 固定顺序是 Restore transform → Safety authorize → Loop Guard authorize → invoker → result settle。插件自行拥有 parser、数据目录创建和重复状态；Core 不获得 shell 策略。
- Gate 使用记录型 invoker，不启动真实 shell。CI 完整检出 Core 历史以读取固定协议源；其他允许的外部效果只有公开插件 Git fetch。报告只写入已忽略的 `docker/debug/reports/plugin-composition-v3/`。
- 结束时释放 lease 并 `terminate_all()`；Gate 从正式 Root 断言 listener 与 Effect 已清空，临时目录随后删除。

## 4. 场景与 oracle

```text
raw ToolInput
     │
     ▼
┌──────────────────────┐
│ shell_restore        │  rm → mv -- ... <ctx.data_root>/restore
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ shell_safety         │  读取最终 ToolInput；拒绝 sudo mode/交互路径
└──────────┬───────────┘
           ▼
┌──────────────────────┐
│ tool_loop_guard      │  generation 内按 session 统计相同签名
└──────────┬───────────┘
           ▼
       fake invoker
```

必须通过：普通 `rm`、`sudo -nE rm`、`sudo -n --preserve-env=HOME rm` 均改写后调用 invoker；`sudo -n -s rm` 被 Safety 拒绝；同 session 第三次相同调用被 Loop Guard 拒绝。七次请求中 invoker 恰好执行五次。

## 5. 验证、回滚与后续删除边界

- 单元测试校验 lock schema、完整 SHA、插件集合顺序和 listener descriptor。
- 真实 Gate 从远端检出 exact commits，报告 Gate version、Tool seam 协议 source、Core runtime commit/tree、lock hash、插件 commit/tree、topology、场景 profile/hash、逐场景结果与 Root 清理结果。
- 本 PR 的回滚点是 `backup/plugin-tool-migration-gate-before-20260815`。
- 这项 Gate 证明三个试点插件已经能共同使用 v3 seam；它不授权删除 v2。物理删除仍要等待全部 legacy ToolHook consumer、注入路径和 trace consumer 完成迁移，并按上游合同中的 `V2_REMOVAL(tool-hooks)` 清单执行。
