# V2_REMOVAL(plugin-public-api-v2)：外部 v2 插件从本模块导入下面的 class、decorator、job
# 与领域 spec。最后一个 consumer 迁到 agent.plugin_composition 和 v3 contract 后删除这些
# public exports；仍被 Core 使用的 generation/scope 类型先移入 Core-private 模块。
from agent.plugins.base import Plugin
from agent.plugins.config import PluginConfig
from agent.plugins.context import PluginContext, PluginKVStore
from agent.plugins.scope import CleanupFailure, PluginScope
from agent.plugins.generation import (
    GateCheckResult,
    GateResult,
    PluginGeneration,
    PluginReadinessContext,
    PluginSemanticCheck,
)
from agent.plugins.jobs import (
    EventTrigger,
    IntervalTrigger,
    PluginJobContext,
    PluginJobSpec,
)
from agent.plugins.specs import (
    ManagedServiceSpec,
    McpServerSpec,
    MobileUiContribution,
    MobileUiNavigation,
    ProactiveSourceSpec,
    RegisteredProactiveSource,
)

__all__ = [
    "Plugin",
    "PluginConfig",
    "PluginContext",
    "PluginKVStore",
    "CleanupFailure",
    "PluginScope",
    "GateCheckResult",
    "GateResult",
    "PluginGeneration",
    "PluginReadinessContext",
    "PluginSemanticCheck",
    "EventTrigger",
    "IntervalTrigger",
    "PluginJobContext",
    "PluginJobSpec",
    "McpServerSpec",
    "ManagedServiceSpec",
    "MobileUiContribution",
    "MobileUiNavigation",
    "ProactiveSourceSpec",
    "RegisteredProactiveSource",
]
