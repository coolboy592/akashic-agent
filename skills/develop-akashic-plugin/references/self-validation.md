# 插件与 Skill 候选自验证

候选验证的目的，是证明一个已提交 source 在隔离 generation 中能被真实加载和使用；它不是手工发布流程，也不授予 candidate 正式数据或外部 ownership。

## 1. 父 turn 可用的三个动作

```text
plugin-install    安装本 turn 的候选
plugin-uninstall  登记本 turn 结束后的卸载
plugin-revert     撤销本 turn 最近一次尚未提交的 install/uninstall
```

stable、candidate generation、lease、排空、提交、恢复和 Channel 切换由 Core 管理。不要查询或编辑 cache、pointer、manifest、workspace Skill 软链接或正式 plugin-data 来编排发布。

## 2. 安装候选

先完成 source test 并提交 Git HEAD；远程 source 还要确认安装所需 commit 已推送：

```bash
python main.py plugin-install \
  --source /absolute/path/to/committed-plugin \
  --marketplace local
```

成功返回至少要能确定：

- 静态 manifest、module identity 和 readiness 已通过；
- 候选属于当前父 turn，父 turn 仍使用原 stable；
- 当前父 turn 创建的 attached programmatic child 会自动绑定候选；
- 父 turn 正常结束且 lease 释放后，Core 才自动提交，下一 turn 才生效。

命令失败时按返回的阶段和对象修复 source；不把非零退出、空 catalog 或“可见”字符串当作成功。

## 3. 创建 attached child

从同一个 active turn 直接创建 child，不选择 generation、不 detach、不启动第二个 Gateway：

```bash
python main.py exec --new --json \
  "加载目标 Skill，使用新插件完成一个可独立断言的只读任务。"
```

Core 根据 parent turn lineage 绑定 `plugin_id + generation_id + source_revision`。命令返回 `execution_id` 后，用 `write_stdin(execution_id=..., chars="")` 读取新增 JSONL，直到唯一 terminal。记录：

```text
execution_id / thread_id / turn_id
plugin_id / generation_id / source_revision
tool items / arguments / status / result
final response / terminal / runtime evidence
```

默认 child 使用新 programmatic session，不沉淀语义记忆；可读取已有能力，但 candidate 的写型 Tool/MCP 必须落到事务、dry-run、隔离目标或明确授权。CLI 退出、父 turn cleanup 或连接断开会取消 attached child。

## 4. 行为 oracle

`terminal=completed` 和 final response 不是充分条件，至少核对：

```text
┌─ 身份
│  └─ child generation/source 与 install 返回值完全一致
├─ Skill（若有）
│  ├─ catalog source 是 plugin
│  ├─ SKILL.md 与引用资源可加载
│  └─ 真实请求遵循关键步骤
├─ Tool（若有）
│  ├─ tool item、arguments、status/result 正确
│  └─ 领域 before/after 符合目标
├─ MCP / process（若有）
│  ├─ candidate endpoint 使用隔离端口
│  ├─ readiness 与 required tool 通过
│  └─ cleanup 后无残留进程或 socket
└─ 状态
   ├─ SessionDB 保存 child turn、messages 和 tool trace
   ├─ 默认 semantic write set 为零
   └─ 正式 plugin-data、memory、外部效果保持不变
```

读取 domain owner 的实际状态，而不是只看 Agent 的自述。`message_push` 必须有真实 delivery receipt；Channel 必须有 stop/start ownership 证据；服务必须有进程、端口、readiness 和退出证据。

## 5. 通过、失败与递归

通过时不执行手工提交命令；正常结束父 turn，向用户说明“候选验证通过，本轮结束后系统自动切换，下一轮生效”。下一用户 turn 再核对 Core 的实际运行事实。

失败时先在同一 turn 撤销：

```bash
python main.py plugin-revert
```

撤销只覆盖同一 turn 最近一次尚未提交的 install/uninstall，不能回滚已经发布的历史版本。根据 child terminal、tool trace、reload journal、写集合和领域状态修复 canonical source，重新运行 source test、提交并递归安装。

下列任一情况都不得发布候选：

- 没有 attached child，或 child 取消、超时、失败；
- child identity 与候选不一致；
- Skill 只在 catalog 出现，未有正文加载和真实触发证据；
- Tool/MCP 只返回“成功”，没有 arguments、result、状态或 readiness 证据；
- 父 turn 非正常结束、已经撤销、或 Core 报告 lease/cleanup 未收束；
- candidate 写入正式 Session、memory、plugin-data，或产生未授权外部效果。

## 6. 独占 managed service 与 Channel

固定 listener 必须在 manifest 的 `[[processes]]` 与 module 的 typed declaration 中同时声明端口和 readiness。服务进程、MCP 和验证脚本都必须读取 Core 注入的 `port_env`，不能硬编码正式端口；Core 会复制 plugin-data、分配临时 loopback 端口并在候选目录启动。

正式 Channel ownership 不在 child 中复制。父 turn 之后 Core 执行：

```text
old Channel.stop → service switch → new Channel.start
       └─ 失败：恢复旧 generation，并报告残留与恢复结果
```

`stop()` 返回必须证明 ingress、在途工作和 ownership 已收束；`start()` 返回必须证明新代 ready。验证只证明候选行为，不证明外部账号、正式 bot 或远端服务已经切换。

## 7. 卸载登记

```bash
python main.py plugin-uninstall demo@local
```

成功只表示本 turn 已登记卸载；本 turn 可正常结束，之后 Core 才停止 endpoint、移除能力投影和 installed code，并保留 plugin-data。下一 turn 核对 manifest entry、cache、process/socket 清理和数据目录仍在；清理失败必须报告实际残留，不能假报完成。要取消同一 turn 的卸载，使用 `plugin-revert`。

## 8. 交付证据

最终报告列出 source commit、manifest/module check、source tests、install transaction、child identity、Skill/Tool/MCP oracle、write set、cleanup、父 turn 终态和下一 turn 结果。没有取得的层保持未知；一次性 workspace 里的通过只能称为“隔离候选验证完成”。
