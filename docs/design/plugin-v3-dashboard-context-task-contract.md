# 插件 v3 DashboardContext 任务合同

- 状态：implementation candidate
- 日期：2026-08-15
- 关联条款：PLG-001～PLG-004、PLG-009、PLG-014、STA-001～STA-003
- 上游：[0036](../decisions/0036-plugin-composition-keeps-promotion-owner.md)、[v3 包级 contribution 任务合同](plugin-v3-package-contributions-task-contract.md)、[plugin-data 任务合同](plugin-data-root-task-contract.md)

## 1. 目标

把 v3 Dashboard 从旧的 `register(app, plugin_dir, workspace)` 改为窄的
`register(app, DashboardContext)`：插件只取得自己的身份、源码目录、Core 分配的
`data_root` 和 candidate 标记，不取得整个 workspace、Memory engine 或 Memory store。

```text
┌──────────────────────────┐
│ stable/candidate snapshot│
│ generation + data owner  │
└─────────────┬────────────┘
              ▼
┌──────────────────────────┐
│ Core Dashboard host      │
│ route conflict + lease   │
└─────────────┬────────────┘
              ▼
┌──────────────────────────┐
│ DashboardContext         │
│ plugin_id / plugin_dir   │
│ data_root / validation   │
└─────────────┬────────────┘
              ▼
┌──────────────────────────┐
│ plugin-owned register    │
│ routes / reader / close  │
└──────────────────────────┘
```

本 PR 不迁移 Default Memory，不移动或改写
`observe/recall_inspector.jsonl`，不改变 Dashboard HTTP route schema，也不删除 v2。

## 2. Change intent

```yaml
change_type: feature
semantic_delta: compatible
capability_owner: core
consumer_scope:
  - composition plugin api v3
  - plugin dashboard host
runtime_patch: required
runtime_patch_reason: "Core owns snapshot leases, candidate isolation and each generation's data root; a plugin cannot safely derive them from workspace."
authoritative_state_owner: "Plugin owns its Dashboard implementation and files under data_root; Core owns path allocation, candidate clone, route publication and cleanup."
client_only_alternative: "Not applicable; the server selects Dashboard routes and generation leases."
```

## 3. v3 合同

- `DashboardContext` 是 frozen value，包含 `plugin_id`、`plugin_dir`、`data_root`、`validation`。
- v3 `plugin_enabled(context)` 在 `register` 前可选执行，必须返回 `bool`；v3
  `register(app, context)` 可以返回一个或多个具有 `close()` 的资源。
- v3 Dashboard 的临时 FastAPI app 不注入 `memory_admin` 或 `memory_store`。
  插件需要领域能力时，由自己的 composition `apply` 决定状态或实现 reader，不能从
  Dashboard 绕过已声明 capability。
- 正式 binding 使用 generation 的正式 `data_dir`。candidate binding 使用 validation
  workspace 下的同名 plugin-data；若 generation 尚未拥有 candidate 副本，Core 先复制
  当前正式 plugin-data，再调用插件。
- candidate Dashboard 的 module、route、closeable 与数据副本仍由 candidate generation
  scope 拥有。discard 或失败不修改正式数据；promotion 先关闭 validation binding，再用
  正式 `data_root` 重建 binding。
- route conflict、错误签名、非 bool predicate、数据复制或 register 失败保持 fail-loud，
  不发布部分 binding。
- 这是受支持 API 的能力收窄，不是同 UID Python 插件的安全沙箱。插件仍可能绕过
  `DashboardContext` 读取进程内对象；真正的恶意插件隔离需要独立进程和权限边界，
  不在本 PR 内用更多 Python wrapper 伪装解决。

## 4. v2 兼容与删除点

v2 继续取得旧三参数 ABI 与 `app.state.memory_admin/memory_store`，现有 Dashboard
行为不变。`V2_REMOVAL(dashboard-register)` 标记这条兼容分支。最后一个 v2 Dashboard
迁移并通过能力等价 Gate 后，删除：

1. `PluginDashboardHost` 的 `memory_admin`、`memory_store` 构造依赖和 app.state 注入；
2. `plugin_enabled(app)` 与 `register(app, plugin_dir, workspace)` 调用；
3. 所有只为旧 ABI 暴露整个 workspace 或 Memory 对象的测试与 adapter。

删除前不能给 v2 增加新的兼容能力。

## 5. 状态变化与恢复

- 本 PR 不直接迁移生产数据。测试只在一次性 workspace 创建 marker。
- candidate 正常增加 validation plugin-data 副本；副本不允许原位更新正式数据，candidate
  结束后随 validation root 物理删除。删除 owner 是 candidate generation scope。
- 正式 Dashboard 是否增加或更新 `data_root` 内文件由具体插件合同决定；Core 不解释、
  合并或删除插件文件。
- 回滚点：`backup/plugin-v3-dashboard-context-before-20260815`。revert 本 PR 后 v3
  Dashboard 暂时恢复旧三参数 ABI；已经存在的 plugin-data 不删除。

## 6. 验证

- stable v3 Dashboard 只取得 `DashboardContext`，FastAPI app 不含 Memory capability，
  binding 的 `runtime_data_root` 与 generation 一致。
- installed candidate 写入只落在 validation data root；discard 零正式残留，promotion 后
  formal binding 才写正式 data root。
- builtin direct candidate 先读到正式数据的隔离副本；candidate marker 不进入正式目录，
  formal marker 在 commit 后出现。
- v2 Dashboard hot reload、route conflict、snapshot lease 与异步 close 回归保持不变。
- loader、hot reload、Dashboard API、两套 Pyright、compileall、`git diff --check` 和公开
  change-impact Gate 通过。
