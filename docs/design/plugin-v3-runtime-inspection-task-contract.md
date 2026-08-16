# 插件 v3 Runtime Inspection 任务合同

- 状态：candidate / independently reviewed
- 日期：2026-08-16
- 实现基线：`2d9fb408`
- 关联条款：PLG-008～PLG-010、PLG-014、CTRL-003、MOB-006、TST-001～TST-008
- 上游：[v3 production readiness checklist](plugin-v3-production-readiness-checklist.md)、[Health/Incident 合同](plugin-composition-health-incident-task-contract.md)

## 1. 目标与边界

本 PR 把 stable snapshot 内已有的 Fiber、Health、Incident 与 Topology 事实投影到只读 Runtime Inspection，
让上线前后的 operator 能按插件观察当前状态和最近故障。`semantic_delta: compatible`；现有文档、job、Skill、
MCP 查询和移动协议命令不变，只扩展 capability reply 的 plugin items。

```text
stable RuntimeSnapshot lease
          │
          ├── PluginGeneration ──► source / generation / api version
          └── CompositionRoot ───► Fiber / Health / Incident / Topology
                                      │
                                      ▼
                         bounded read-only plugin projection
```

本任务不创建第二份运行状态、不持久化 Incident、不修改 Root、不读取正式数据库或插件文件、不发送渠道消息，
也不把 v2 插件伪装成拥有 composition health。

## 2. 合同与失败语义

1. 查询必须持有 stable snapshot lease；没有已发布快照时继续 fail-loud。
2. v3 plugin item 同时返回 generation identity、当前 Fiber 状态、required/optional Health、累计 Incident 数、
   有界最近 Incident 和冻结 Topology identity/revision。nested Fiber 通过 parent edge 归到顶层插件 owner。
3. v2 plugin item 明确标记 `api_version = 2` 且 `composition = null`；不得从 legacy context 或日志推断假状态。
   静态 inactive 的 v3 generation 保留在冻结 Topology 中，但不属于 stable capability 投影，也不得阻断其他插件查询。
4. Incident 累计数由 CompositionRoot 在记录时按直接 Fiber owner 单调计数；最近详情继续受 Root buffer 与查询上限
   约束，不反向影响 readiness、snapshot identity 或 publication。
5. 查询期间 Root 被替换或 drain 时，lease 保持本次回答来自同一 snapshot；释放 lease 后不保存 Root 引用。

## 3. 验证与停止条件

- real Manager mixed v2/v3 stable snapshot：v3 顶层与 nested Fiber、optional degraded Health、Incident、Topology
  正确归组，v2 composition 为 null；
- 多次 Incident 超过查询详情上限时累计数仍准确、详情有界；
- plugin unload/terminate 不留下 inspection 自有 lease 或状态；
- runtime inspection、composition kernel、mobile protocol 定向回归，Basedpyright error-level、compileall、
  `git diff --check`。

任何查询改变 Root/Health、暴露 candidate snapshot、把 child 归给错误插件、泄漏无界 Incident，或让 legacy
插件获得伪造 composition 状态都停止交付。

## 4. 回滚

代码恢复点为分支 `backup/plugin-v3-c18-implemented-20260816` 的 `2d9fb408`。本任务没有运行数据变更；
回滚只需撤销本 PR 的源码、测试和合同提交。
