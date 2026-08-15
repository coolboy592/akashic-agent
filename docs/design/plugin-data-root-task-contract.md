# 插件 generation 数据根任务合同（R5a）

- 状态：implemented / reviewed
- 日期：2026-08-15
- 实现基线：`65a78b4f25473a40b5964f122c3d820a9f509c11`
- 关联条款：PLG-001～PLG-004、PLG-010、PLG-013～PLG-014、WSP-001
- 上游：[R2b candidate Root 隔离](plugin-candidate-root-isolation-task-contract.md)
- 参考：Cordis 的 Context 负责组合与作用域，不替领域插件预建万能存储 Service

## 1. 目标

Core 只为每棵插件 generation Fiber tree 绑定一个数据根；插件拥有根内 schema、文件格式、数据库和 I/O 实现：

```text
stable generation ──ctx.data_root──> <workspace>/plugin-data/<plugin-id>/

candidate Root    ──ctx.data_root──> <attempt>/workspace/plugin-data/<plugin-id>/
                                      │
                                      └── 插件自有文件、SQLite、游标或附件
```

嵌套 Fiber 继承同一个 `data_root`，因为它们是同一插件内的组件，不是新的插件身份。Core 的 artifact、Root 和 promotion owner 负责在 candidate/formal 之间替换实际路径；插件代码不判断自己处于候选还是正式环境。

## 2. 公共合同

- `ctx.data_root: Path` 是 v3 插件的数据入口；没有 Core-assigned `PluginRuntime` 时 fail-loud `PLUGIN_RUNTIME_UNAVAILABLE`。
- Core 保证 Manager 正式加载与候选克隆在 mount 前已准备对应目录；通用 CompositionRoot 的调用者负责提供存在的测试/实验目录。
- 插件可直接使用标准库或自有存储实现。Core 不解析 opaque plugin-data，不要求所有写入经过通用 writer，也不提供全局 `for_plugin(plugin_id)` 权限。
- `PluginDataAccess`、`ScopedPluginData` 和 `ExternalEffectGate` 从 `agent.plugin_composition` 公共导出移除；实现暂留 internal，只有 kernel test/隔离 experiment 可以直接引用。它们不是 v3 Core Service。
- 现有 `ctx.runtime` 暂留给尚未迁移的 v3 consumer；GitHub Watch 改用 `ctx.data_root` 并通过 exact-commit Gate 后，再单独收窄 runtime 公共面。

## 3. 持久化与隔离

- plugin-data 是插件拥有的权威持久状态；普通卸载保留，永久删除仍需要独立用户操作、备份与再次确认。
- candidate 只能取得 attempt-local clone；失败、丢弃或 promotion 后由 Root cleanup 清除。正式 data 不因 candidate apply 改变。
- 直接文件写入不伪装成 `CompositionAudit.writes`。候选隔离与晋升正确性依赖 Core 分配路径、Root freeze/dispose 和行为验证，而不是一个可绕过的万能 writer。
- 本 PR 不读取、迁移或删除正式 plugin-data，不修改正式 workspace。

## 4. 范围与验证

- Core：新增 `ctx.data_root`，收窄公共导出，标注 `PluginContext` v2 公共 API 删除点及 v3 internal metadata 前置迁移。
- Example/experiment：改为 Core 分配 Path、插件自己写 JSON；不再声明 `core.plugin-data` 或 `core.external-effects` ServiceKey。
- Oracle：缺 runtime fail-loud；嵌套 Fiber 共享根；candidate/formal 路径仍隔离；internal access helper 不在公共导出；隔离实验真实文件保留两次写入后的最终内容。
- Targeted：composition kernel/loader/experiment；cumulative：全部 plugin 相关回归、Basedpyright、compileall、`git diff --check`、公开 Change Gate。
- 回滚点：Git tag `backup/plugin-data-root-r5-before-20260815`。

## 5. 后续迁移与物理删除

1. GitHub Watch v3：`ctx.runtime.data_dir` → `ctx.data_root`，同时引入插件自有 `client_factory` fake seam。
2. Meme：当前共享 `<workspace>/memes` 是产品级素材目录，不能误迁入插件私有根；需在插件能力合同中明确由哪个领域 Service 提供。
3. 所有 v2 插件迁移后，先把 `ComposablePlugin` 当前借用 `PluginContext` 保存的 config、源码路径与 generation metadata 搬入 v3 internal record；再按 `V2_REMOVAL(plugin-context-api)` 删除 `PluginContext` 和 Manager 的 v2 构造/激活分支。
4. 所有 v3 consumer 不再读取 `ctx.runtime` 后，删除该过宽公共入口，只保留窄的 `data_root` 和后续经真实需求证明的资源入口。

## 6. 验证证据

- 定向：`65 passed`（composition kernel、loader、isolated experiment）。
- 累计：`555 passed`（plugin、tool executor、runtime inspection 相关回归）。
- 静态：Basedpyright `0 errors, 0 warnings`；`compileall` 与 `git diff --check` 通过。
- Consumer 扫描：Core 和本地 canonical plugin source 没有从 v3 公共入口导入三项 internal helper。
- 只读复审：Terra 最终结论无 P0/P1；覆盖 data root 继承、candidate/formal 隔离、公共导出、direct-path audit 合同及 v2 删除前置条件。
