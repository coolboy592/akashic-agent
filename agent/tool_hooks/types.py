from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

ToolSource = Literal["passive", "proactive", "subagent"]
ToolExecStatus = Literal["success", "denied", "error"]


@dataclass
class ToolExecutionRequest:
    call_id: str
    tool_name: str
    arguments: dict[str, Any]
    source: ToolSource
    session_key: str = ""
    channel: str = ""
    chat_id: str = ""
    request_text: str = ""
    tool_batch: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    tool_batch_index: int = 0


def _empty_str_list() -> list[str]:
    return []


@dataclass
class ToolExecutionResult:
    status: ToolExecStatus
    output: Any
    final_arguments: dict[str, Any]
    extra_messages: list[str] = field(default_factory=_empty_str_list)
