---
name: develop-akashic-plugin
description: 创建、编写、修改并验证 Akashic v3 插件及插件内 Skill/MCP。用户要求创建插件、加入能力、安装候选、热重载后自测或递归自验证时使用。
---

# 开发并验证 Akashic 插件

只在插件的 canonical source 中修改文件。先确定 write set 并创建可恢复备份；不要直接编辑安装 cache、workspace 中的 Skill 投影、runtime pointer 或正式 plugin-data。

## 1. 先读当前合同

1. 进入目标仓库后先读 `docs/INDEX.md`、`docs/WORKFLOW.md` 及该仓库的本地指引。
2. 完整读取 [references/plugin-authoring.md](references/plugin-authoring.md)。它说明静态 manifest、module namespace 和 typed capability service。
3. 安装或行为验证前完整读取 [references/self-validation.md](references/self-validation.md)。
4. 只有子 turn 排队、超时、结果错误或行为证据不完整时，才读取 `references/runtime-diagnostics.md`，按真实 reload journal、SessionDB 和日志重建轨迹。
5. 以当前 `agent/plugin_composition/`、`agent/plugins/static_manifest.py` 和相邻 v3 样例为事实来源；旧文章或旧代码片段不构成 API。

## 2. 组织一个 v3 插件

最小 source 如下：

```text
plugin-repo/
├── akashic.plugin.toml       # 外部安装包必需
├── plugin.py                 # v3 module namespace
├── skills/<skill>/SKILL.md   # 可选
├── drift/skills/<skill>/     # 可选
├── mcp/                      # 可选
└── requirements.txt          # 只有确有 Python 依赖时才声明
```

`akashic.plugin.toml` 至少包含 `schema_version = 1`、`name`、`version`、`api_version = 3` 和 `entrypoint = "plugin.py"`。module namespace 同时导出同值的 `api_version`、`name`、`version`，以及唯一、无默认值、无额外参数的 `apply(ctx, config)`；函数可以是同步或异步的。能力通过 `Context` 上的 typed `ServiceKey` 获取或提供，不通过隐式全局状态注册。

```python
from agent.plugin_composition import Context, TOOL_CATALOG

api_version = 3
name = "example"
version = "1.0.0"
inject = (TOOL_CATALOG,)


async def apply(ctx: Context, config: object) -> None:
    """Register this generation's typed contributions."""

    _ = config
    tools = ctx.require(TOOL_CATALOG)
    # 用 PluginToolDefinition 注册声明；handler_export 指向 source 内的可调用导出。
    _ = tools
```

真实声明范例和字段表见 authoring reference；不要把 Core-private 的 Default/Wake proactive island 当作外部扩展入口，也不要导入其 factory、registry 或 bridge。

## 3. 实现与 source 验证

保持一个清楚的 capability owner：

- Tool、Command、Channel、MCP、managed process、proactive source、background job、mobile UI 和事件分别通过对应 typed service 注册。
- `skill_roots`、`drift_skill_roots`、`workspace_roots` 和 `dashboard_module` 是 module namespace 的静态声明；路径必须位于插件 source，workspace root 只能是插件拥有的顶层目录。
- import 阶段不启动进程、打开端口、创建正式数据库或发送外部消息。后台任务使用 `ctx.spawn`，资源使用 `ctx.effect`，监听使用 typed event key；它们随当前 Fiber 逆序清理。
- Skill 放在插件 source，由声明的 root 发布；不要先复制到 workspace。MCP 和 service 的 candidate readiness 必须可隔离，失败要暴露。

source test 至少覆盖：

1. manifest 能被解析，字段与 module 的 `name/version/api_version/entrypoint` 一致；
2. `plugin.py` 可从干净 checkout 导入，`apply` 签名精确；
3. 每个 Skill 的 frontmatter、引用资源和真实触发路径；
4. 每个 typed declaration 的 schema、readiness、生命周期和失败语义；
5. 取消、cleanup、candidate write set 与外部效果边界。

缺少依赖、导入失败、配置错误、命令失败和数据损坏必须 fail-loud；不要用空结果、宽泛异常或假成功绕过检查。

## 4. 安装并验证候选

先运行 source test，再提交 Git HEAD；远程 source 还要确认远端包含该 commit。父 Agent turn 只使用三个管理动作：

```text
plugin-install    安装或更新本 turn 的候选
plugin-uninstall  登记本 turn 结束后的卸载
plugin-revert     撤销本 turn 最近一次尚未提交的 install/uninstall
```

安装命令从 active turn 的 Shell 发起：

```bash
python main.py plugin-install \
  --source /absolute/path/to/committed-plugin \
  --marketplace local
```

成功只表示候选已准备；父 turn 仍使用原 generation，本 turn 创建的 attached programmatic child 才会自动绑定候选。不要指定 runtime、手工切换 generation、启动第二个 Gateway 或编辑 cache。

```text
source test → commit/push → plugin-install
    → attached child → identity + Skill/Tool 行为 oracle
    ├─ pass → 正常结束父 turn → Core 自动切换 → 下一 turn 生效
    └─ fail → plugin-revert → 根据真实轨迹修复 source 后递归
```

attached child 必须实际加载新增 Skill、调用新增 Tool 或完成领域 oracle；只看 final response、只问“是否可见”、只跑 catalog 检查都不够。记录 `execution_id`、`thread_id`、`turn_id`、`plugin_id`、candidate generation/source revision、tool items 和 terminal；超时或 queued 不推进时按 runtime diagnostics 定位，不重复安装相同 source。

## 5. endpoint、Channel 与副作用

固定 listener 必须通过静态 `[[processes]]`/typed managed-process declaration 声明端口、readiness 和超时；服务进程及同插件 MCP 必须读取 Core 注入的 `port_env`。候选验证使用隔离端口和 plugin-data 副本，candidate read-only MCP 之外的写能力必须使用事务、dry-run、隔离目标或明确授权。

Channel candidate 不接管正式 token、webhook 或 long-poll ownership。父 turn 结束后的切换顺序是：

```text
old Channel.stop → managed service switch → new Channel.start
       └─ 任一步失败：恢复并验证 old generation
```

`stop()` 返回必须证明 ingress 已停止、在途工作已收束且 ownership 已释放；`start()` 返回必须证明新代已 ready。`message_push` 以真实 delivery receipt 和目标 owner 证据为准，不能凭字符串推断外部效果。

## 6. 完成标准

只有以下事实同时成立才报告完成：

- canonical source 已按授权提交，安装所需 commit 可回源；
- manifest/module 静态检查、source tests 和 readiness 通过；
- attached child 的 candidate identity、Skill/Tool 轨迹和领域 oracle 通过；
- 父 turn 正常结束，下一 turn 的 Core 运行事实确认已切换，或明确报告恢复/清理失败；
- SessionDB、memory、正式 plugin-data 与未授权外部效果的 write set 为零。

一次性 workspace 中的行为验证只能报告“隔离候选验证完成”，不能升级为正式切换完成。最终报告简洁列出 source commit、验证 turn/child、关键 tool evidence、Core turn 后结果、备份位置和未验证边界。
