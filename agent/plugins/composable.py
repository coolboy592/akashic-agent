from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from types import ModuleType
from typing import TYPE_CHECKING, cast

from agent.plugin_composition import Context, ServiceKey

if TYPE_CHECKING:
    from agent.plugins.context import PluginContext
    from agent.plugins.generation import PluginReadinessContext, PluginSemanticCheck


@dataclass
class ComposablePlugin:
    """Adapt one v3 namespace module to the composition kernel's apply contract."""

    module: ModuleType
    name: str
    version: str
    desc: str
    author: str
    inject: tuple[ServiceKey[object], ...]
    skill_roots: tuple[str, ...]
    drift_skill_roots: tuple[str, ...]
    dashboard_module: str | None
    _apply: Callable[[Context, object], object] = field(repr=False)
    context: PluginContext = field(init=False, repr=False)
    api_version: int = field(default=3, init=False)

    @classmethod
    def from_module(cls, module: ModuleType) -> ComposablePlugin:
        """Validate and freeze the named exports of one v3 plugin module."""

        # 1. Validate the namespace shape before Manager state is created.
        if getattr(module, "api_version", None) != 3:
            raise ValueError("v3 插件模块必须声明 api_version = 3")
        name = getattr(module, "name", None)
        version = getattr(module, "version", None)
        if not isinstance(name, str) or not name.strip() or name != name.strip():
            raise ValueError("v3 插件 name 必须是非空且无首尾空白的字符串")
        if (
            not isinstance(version, str)
            or not version.strip()
            or version != version.strip()
        ):
            raise ValueError("v3 插件 version 必须是非空且无首尾空白的字符串")
        apply = getattr(module, "apply", None)
        if not callable(apply):
            raise ValueError("v3 插件模块必须导出 apply(ctx, config)")

        # 2. Dependencies are typed ServiceKeys; ordering comes from providers.
        raw_inject = cast(object, getattr(module, "inject", ()))
        if not isinstance(raw_inject, (tuple, list)):
            raise ValueError("v3 插件 inject 必须是 ServiceKey 序列")
        raw_items = cast(tuple[object, ...] | list[object], raw_inject)
        if not all(isinstance(item, ServiceKey) for item in raw_items):
            raise ValueError("v3 插件 inject 必须是 ServiceKey 序列")
        inject = tuple(
            cast(ServiceKey[object], item)
            for item in raw_items
            if isinstance(item, ServiceKey)
        )
        if len(set(inject)) != len(inject):
            raise ValueError(f"v3 插件依赖重复: {name}")
        for export_name in ("static_semantic_checks", "readiness_semantic_checks"):
            export = getattr(module, export_name, None)
            if export is not None and not callable(export):
                raise ValueError(f"v3 插件 {export_name} 必须可调用")
        skill_roots = _string_tuple_export(module, "skill_roots")
        drift_skill_roots = _string_tuple_export(module, "drift_skill_roots")
        dashboard_module = getattr(module, "dashboard_module", None)
        if dashboard_module is not None and (
            not isinstance(dashboard_module, str)
            or not dashboard_module.strip()
            or dashboard_module != dashboard_module.strip()
        ):
            raise ValueError("v3 插件 dashboard_module 必须是非空字符串或 None")
        return cls(
            module=module,
            name=name,
            version=version,
            desc=str(getattr(module, "desc", "")),
            author=str(getattr(module, "author", "")),
            inject=inject,
            skill_roots=skill_roots,
            drift_skill_roots=drift_skill_roots,
            dashboard_module=cast(str | None, dashboard_module),
            _apply=cast(Callable[[Context, object], object], apply),
        )

    @property
    def ConfigModel(self) -> type[object] | None:
        return cast(type[object] | None, getattr(self.module, "Config", None))

    async def apply(self, ctx: Context) -> None:
        result = self._apply(ctx, self.context.config)
        if inspect.isawaitable(result):
            await result

    def static_semantic_checks(self) -> list[PluginSemanticCheck]:
        provider = getattr(self.module, "static_semantic_checks", None)
        if provider is None:
            return []
        return cast(list[PluginSemanticCheck], provider())

    async def readiness_semantic_checks(
        self,
        context: PluginReadinessContext,
    ) -> list[PluginSemanticCheck]:
        provider = getattr(self.module, "readiness_semantic_checks", None)
        if provider is None:
            return []
        result = provider(context)
        if inspect.isawaitable(result):
            result = await result
        return cast(list[PluginSemanticCheck], result)


def _string_tuple_export(module: ModuleType, name: str) -> tuple[str, ...]:
    raw = cast(object, getattr(module, name, ()))
    if not isinstance(raw, (tuple, list)):
        raise ValueError(f"v3 插件 {name} 必须是字符串序列")
    items = cast(tuple[object, ...] | list[object], raw)
    if any(
        not isinstance(item, str)
        or not item.strip()
        or item != item.strip()
        for item in items
    ):
        raise ValueError(f"v3 插件 {name} 必须只包含非空字符串")
    typed = tuple(cast(str, item) for item in items)
    if len(set(typed)) != len(typed):
        raise ValueError(f"v3 插件 {name} 不得重复")
    return typed
