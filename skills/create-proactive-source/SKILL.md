---
name: create-proactive-source
description: 创建或更新由 Akashic v3 插件声明的主动信息源 MCP server，并完成本地与候选行为验证。
---

# 创建 Proactive 信息源

产出是一个 v3 插件 source：`akashic.plugin.toml`、`plugin.py`、MCP server、typed `ProactiveSourceDefinition`、可选配置与测试。不创建 `proactive_sources.json`，也不把 Default/Wake private island 当作公共 source API。

## 1. 选择通道

| 通道 | 用途 | ACK |
| --- | --- | ---: |
| `alert` | 紧急告警、提醒、异常 | 需要 |
| `content` | RSS、新闻、候选内容 | 需要 |
| `context` | 睡眠、在线状态等决策背景 | 不需要 |

一个插件或 source 可以同时提供多种通道；只选择真实使用的通道。`alert/content` 的 item 必须可被稳定唯一地 ACK，`context` 不要伪造消费确认。

## 2. Source 骨架与 manifest

```text
plugin-repo/
├── akashic.plugin.toml
├── plugin.py
├── mcp/
│   ├── run.py
│   └── requirements.txt       # 有依赖时才保留
└── tests/
```

```toml
schema_version = 1
name = "example_events"
version = "1.0.0"
api_version = 3
entrypoint = "plugin.py"

[[python]]
requirements = "mcp/requirements.txt"

[[mcp]]
name = "example"
command = ["python", "mcp/run.py"]
required_tools = ["fetch_events", "ack_events"]
candidate_read_only_tools = ["fetch_events"]
```

若 MCP 依赖固定 listener，在 manifest 中再写 `[[processes]]`，声明 `port_env`、`formal_port`、`readiness_path` 和 `startup_timeout_seconds`；`[[mcp]].endpoint_env` 指向同名 process。服务进程和 MCP 必须读取 Core 注入的 `port_env`，不可硬编码正式端口。

## 3. Typed source 声明

module namespace 只需导出 v3 identity、精确 `apply(ctx, config)` 和必要的 typed service：

```python
from agent.plugin_composition import (
    Context,
    MCP_SERVERS,
    PROACTIVE_COMPONENTS,
    McpServerDefinition,
    ProactiveSourceDefinition,
)

api_version = 3
name = "example_events"
version = "1.0.0"
inject = (MCP_SERVERS, PROACTIVE_COMPONENTS)


async def apply(ctx: Context, config: object) -> None:
    """Register the MCP-backed proactive source."""

    _ = config
    await ctx.require(MCP_SERVERS).register(
        ctx,
        McpServerDefinition(
            name="example",
            command=("python", "mcp/run.py"),
            required_tools=("fetch_events", "ack_events"),
            candidate_read_only_tools=("fetch_events",),
        ),
    )
    await ctx.require(PROACTIVE_COMPONENTS).register(
        ctx,
        ProactiveSourceDefinition(
            name="events",
            channels=("alert", "content"),
            mcp_server="example",
            fetch_tool="fetch_events",
            ack_tool="ack_events",
            fetch_page_size=50,
        ),
    )
```

`ProactiveSourceDefinition` 字段是：`name`、非空 `channels`、`mcp_server`、`fetch_tool`、可选 `ack_tool` 和非负 `fetch_page_size`。source 只登记声明；Core 负责 generation、MCP readiness、拉取调度、ACK、cursor 和取消。配置关闭时，可在 `apply` 内依据 typed `config` 不登记 source，但不要返回假 source 或静默降级。

## 4. MCP 数据合同

`alert` 与 `content` 的 fetch tool 返回 JSON 数组；每项至少包含：

```json
{
  "event_id": "stable-id",
  "kind": "alert-or-content",
  "source_type": "provider",
  "source_name": "example",
  "title": "Readable title",
  "content": "Bounded body"
}
```

`context` 可返回 JSON object 或 object array，但字段仍要有稳定 schema、时间和大小边界。ACK tool 接收 `event_ids: list[str]`，只确认已成功投递的原始 ID；部分失败要逐项暴露，不得把 fetch 成功当作 ACK 成功。MCP 自己拥有缓存新鲜度与 provider 访问错误，`fetch_tool` 只读取稳定快照。

## 5. 配置与状态

插件可以导出 typed `Config` 模型；配置位于：

```text
<workspace>/plugin-data/<plugin>-<marketplace>/config.local.toml
```

配置只决定该 generation 是否登记 source、source 参数和 MCP provider 选择；关闭 proactive 只移除 source，不关闭插件其他能力。MCP cache、cursor、ACK 和 provider token 要分别说明 owner；candidate validation 不得写正式 proactive 数据、SessionDB、memory 或远端 provider。

需要插件自己的持久文件时使用 `ctx.data_root`；需要产品级目录时先声明 `workspace_roots`，再使用 `ctx.workspace_root(name)`。卸载代码默认保留 plugin-data，删除数据必须通过另一个明确、可预览且可恢复的用户操作。

## 6. 验证

```text
manifest parse
  → module identity + apply(ctx, config)
  → MCP command / dependency / readiness
  → fetch schema 与边界
  → ACK 精确匹配 event_id
  → channel enabled/disabled 行为
  → attached child candidate oracle
  → cursor、write set、cleanup 与 restart
```

source test 至少覆盖：

- 干净 checkout 可导入，manifest 与 module identity 一致；
- `fetch_tool` 对 `alert/content/context` 的合法、空页、重复、坏字段和超限输入有明确结果；
- ACK 只确认已成功投递的原始 ID，失败项保留并可重试；
- provider 错误、MCP 退出、取消和 restart 不丢 cursor 或假报成功；
- `enabled = false` 时不发布 source，其他 typed capabilities 仍按合同工作；
- candidate 使用隔离 endpoint/workspace，正式数据和未授权外部效果 write set 为零；
- attached child 真实发现 source、调用 fetch，并留下 tool item、terminal 与 domain oracle。

完成 source test 后提交 Git HEAD，再按 [插件候选自验证](../develop-akashic-plugin/references/self-validation.md) 执行 `plugin-install`、attached child 和 turn 后核对。运行中只使用 typed `MCP_SERVERS` 与 `PROACTIVE_COMPONENTS`；Default/Wake private island 的 factory、registry 和 bridge 不属于外部 authoring API。
