from __future__ import annotations

from abc import ABC, abstractmethod

from agent.tool_hooks.types import HookContext, HookEvent, HookOutcome

# V2_REMOVAL(tool-hooks)：全部 consumer 迁到 typed Tool events 后删除此接口与目录导出。

class ToolHook(ABC):
    name: str
    event: HookEvent

    @abstractmethod
    def matches(self, ctx: HookContext) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def run(self, ctx: HookContext) -> HookOutcome:
        raise NotImplementedError
