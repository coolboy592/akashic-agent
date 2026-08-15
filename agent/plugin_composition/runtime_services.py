from __future__ import annotations

from dataclasses import dataclass

from agent.plugin_composition.model import ServiceKey


@dataclass(frozen=True, slots=True)
class MemoryRuntimeInfo:
    """Describe the selected Core Memory runtime without exposing its engine."""

    name: str

    def __post_init__(self) -> None:
        if not self.name or self.name.strip() != self.name:
            raise ValueError("MemoryRuntimeInfo.name 必须是非空且无首尾空白的字符串")


MEMORY_RUNTIME = ServiceKey[MemoryRuntimeInfo]("core.memory.runtime")
