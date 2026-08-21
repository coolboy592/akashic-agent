from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from agent.plugin_composition.model import ServiceKey


@dataclass(frozen=True, slots=True)
class InteractionUndoResult:
    """记录一次已提交 interaction 删除及其派生收敛状态。"""

    control_turn_id: str
    session_key: str
    message_ids: tuple[str, ...]
    backup_path: str
    reconciliation_pending: bool
    old_last_consolidated: int
    new_last_consolidated: int


UndoLatest = Callable[[str], Awaitable[InteractionUndoResult | None]]


class InteractionUndoService:
    """向插件暴露用户主动触发的窄 interaction 撤销命令。"""

    def __init__(self, undo_latest: UndoLatest | None) -> None:
        self._undo_latest = undo_latest

    @classmethod
    def candidate_validation(cls) -> InteractionUndoService:
        """创建只有拓扑身份、没有 destructive owner 的候选服务。"""

        return cls(None)

    async def undo_latest(self, session_key: str) -> InteractionUndoResult | None:
        """撤销一个既有 Session 最后的 completed interaction。"""

        if self._undo_latest is None:
            raise RuntimeError("candidate 验证期禁止撤销正式 interaction")
        return await self._undo_latest(session_key)


INTERACTION_UNDO = ServiceKey[InteractionUndoService]("core.interaction_undo")


__all__ = [
    "INTERACTION_UNDO",
    "InteractionUndoResult",
    "InteractionUndoService",
]
