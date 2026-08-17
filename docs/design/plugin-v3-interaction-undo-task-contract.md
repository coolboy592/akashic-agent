# Plugin Undo v3 与 interaction 撤销协调合同

## 1. 目标与边界

Plugin Undo 只声明 `/undo` command；Core 是唯一 destructive owner。插件不得取得
`SessionManager`、`SessionStore`、memory engine、任意 SQL 或 workspace 写权限。

本任务只覆盖两种恢复：当前进程内失败/取消，以及 Core 进程崩溃后的启动重放。不新增断电、
任意停机点或跨设备恢复语义。

```text
/undo command
     │
     ▼
INTERACTION_UNDO ──► latest completed interaction fence
     │                         │
     │                         ▼
     │                 SessionStore transaction
     │                 backup + transcript + embedding
     │                         │
     ├── Akasha ──────────────► source gate + deterministic rebuild
     │
     └── Default Memory ──────► pending receipt ─► idempotent undo ─► completed
                                              └─ Core restart replay
```

## 2. 权威状态与不变量

1. `sessions.db/messages` 仍只由用户显式 `/undo` 减少；目标必须是同 Session 最后一个结构完整的
   `control_turn_id`，并在删除事务中重新核对，拒绝 selection 后的新 Turn 漂移。
2. 删除继续复用 `SessionStore.delete_interaction()`：事务前创建并验证完整 SQLite backup；正文、
   message attachments、message embeddings、compaction invalidation/cursor 和 source mutation audit
   同一事务提交。
3. Default Memory 的 `interaction_memory_reconciliations` receipt 与 source 删除同一事务创建。
   memory undo 成功后才标 completed；失败保留 pending，启动时在插件 command admission 前重放。
4. Akasha 继续由 `delete_interaction_source()` 封住 source event、删除 canonical source 并重建 sidecar；
   崩溃后按既有 source/index/memory identity mismatch 收敛，不复制第二套 journal。
5. candidate Root 只取得 topology-only service；任何调用在读取 Session 或创建 backup 前 fail-loud。
6. caller cancellation 不能截断已经开始的删除/记忆收敛；临界任务完成后才恢复
   `CancelledError`。

## 3. 失败语义

- 没有 completed interaction：返回 `None`，不创建 Session、backup 或 receipt。
- latest identity 漂移、active Session 或 pending compaction：删除事务整体失败，零 message write。
- Default Memory 失败：Session 删除保持 committed，receipt 保持 pending；公开结果必须明确
  `reconciliation_pending=true`，不得声称回滚或完整成功。
- pending receipt 与当前 memory engine 不匹配：Core 启动 fail-loud，不开放旧记忆查询。
- receipt 重放是幂等操作；memory 已完成但 receipt 尚未完成时，重启重复执行后再终结。

## 4. 最小验收

- formal Manager exact Root 提供服务；candidate 调用零正式写入并拒绝发布。
- selection 后追加新 completed Turn，旧 identity 删除必须失败且两组 rows 均保留。
- memory 失败时备份 integrity 为 `ok`、source rows 已按显式命令删除、pending receipt 可见；
  进程内 retry 后 completed。
- 模拟 Core 进程在 source commit 后退出，重新构造 SessionManager/Coordinator 后自动重放，
  DB/cache/memory receipt 一致。
- caller cancel 发生在 memory owner 阻塞期间，最终先完成 receipt、关闭 pending，再恢复取消。

## 5. 回滚

代码恢复点是 `backup/v3-interaction-undo-pre-20260817`。本任务测试只写临时 workspace；不写正式
workspace。回滚源码不能删除已经由用户显式 `/undo` 产生的 backup、source audit 或 pending
receipt。
