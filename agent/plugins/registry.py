from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable, Any

# V2_REMOVAL(plugin-registry-v2)：_classes 与 decorator handler metadata 是 v2 全局注册面。
# v3 loader 只读 namespace，listener/service 由 Root 持有；最后一个 v2 class/decorator/tool
# consumer 迁走后删除 class discovery 与 metadata registry，只保留另有 owner 的实例查询前
# 先迁移该 consumer。

class MetadataKind(Enum):
    LIFECYCLE = auto()
    TOOL = auto()


class PluginEventType(Enum):
    BEFORE_TURN = "before_turn"
    BEFORE_REASONING = "before_reasoning"
    PROMPT_RENDER = "prompt_render"
    BEFORE_STEP = "before_step"
    AFTER_STEP = "after_step"
    AFTER_REASONING = "after_reasoning"
    AFTER_TURN = "after_turn"
    BEFORE_TOOL_CALL = "before_tool_call"
    AFTER_TOOL_RESULT = "after_tool_result"


@dataclass
class PluginHandlerMetadata:
    kind: MetadataKind
    event_type: PluginEventType | None
    handler: Callable[..., Any]
    handler_name: str
    plugin_module_path: str
    tool_name: str | None = None
    tool_schema: dict[str, Any] | None = None
    tool_risk: str | None = None
    tool_always_on: bool = False
    tool_search_hint: str | None = None
    priority: int = 0
    active: bool = True


class PluginHandlerRegistry:
    def __init__(self) -> None:
        self._handlers: list[PluginHandlerMetadata] = []

    def append(self, md: PluginHandlerMetadata) -> None:
        self._handlers.append(md)
        self._handlers.sort(key=lambda h: -h.priority)

    def get_by_module_path(self, mp: str) -> list[PluginHandlerMetadata]:
        return [h for h in self._handlers if h.plugin_module_path == mp]

    def remove_by_module_path(self, mp: str) -> None:
        self._handlers = [h for h in self._handlers if h.plugin_module_path != mp]

    def module_paths_under(self, root: str) -> set[str]:
        return {
            handler.plugin_module_path
            for handler in self._handlers
            if handler.plugin_module_path.startswith(f"{root}.")
        }


class PluginRegistry:
    def __init__(self) -> None:
        self._handlers = PluginHandlerRegistry()
        self._classes: dict[str, type] = {}
        self._instances: dict[str, object] = {}

    def register_class(self, cls: type) -> None:
        self._classes[cls.__module__] = cls

    def register_instance(self, mp: str, inst: object) -> None:
        self._instances[mp] = inst

    def get_class(self, mp: str) -> type | None:
        return self._classes.get(mp)

    def get_instance(self, mp: str) -> object | None:
        return self._instances.get(mp)

    def get_handlers_by_module_path(self, mp: str) -> list[PluginHandlerMetadata]:
        return self._handlers.get_by_module_path(mp)

    def remove_plugin(self, mp: str) -> None:
        self._handlers.remove_by_module_path(mp)
        _ = self._classes.pop(mp, None)
        _ = self._instances.pop(mp, None)

    def remove_module_tree(self, root: str) -> None:
        module_paths = {
            root,
            *(path for path in self._classes if path.startswith(f"{root}.")),
            *(path for path in self._instances if path.startswith(f"{root}.")),
            *self._handlers.module_paths_under(root),
        }
        for module_path in module_paths:
            self.remove_plugin(module_path)


plugin_registry = PluginRegistry()
