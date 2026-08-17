from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Protocol, TypeVar, cast

from agent.plugin_composition.interaction_undo import InteractionUndoResult
from session.store import InteractionDeletion


class _MemoryDescriptor(Protocol):
    name: str


class _MemoryEngine(Protocol):
    def describe(self) -> _MemoryDescriptor: ...


class _DefaultMemoryUndo(Protocol):
    def undo_by_message_sources(
        self,
        message_ids: list[str],
        *,
        dry_run: bool = False,
    ) -> dict[str, object]: ...


class _AkashaInteractionUndo(Protocol):
    async def delete_interaction_source(
        self,
        control_turn_id: str,
        delete_source: Callable[[], InteractionDeletion | None],
    ) -> InteractionDeletion | None: ...


class _SessionManager(Protocol):
    @property
    def control_store(self): ...

    def invalidate(self, key: str) -> None: ...


class InteractionUndoCoordinator:
    """协调 Session truth 删除与当前 memory owner 的确定性收敛。"""

    def __init__(
        self,
        session_manager: _SessionManager,
        memory_engine: _MemoryEngine,
    ) -> None:
        self._sessions = session_manager
        self._memory = memory_engine
        self._lock = asyncio.Lock()

    async def recover_pending(self) -> None:
        """在开放插件命令前重放全部已提交的 Default Memory receipt。"""

        async with self._lock:
            await self._recover_pending_locked()

    async def undo_latest(self, session_key: str) -> InteractionUndoResult | None:
        """撤销最后一个 completed interaction，并在取消后完成临界收束。"""

        task = asyncio.create_task(
            self._undo_latest_critical(session_key),
            name=f"interaction-undo:{session_key}",
        )
        return await _finish_critical(task)

    async def _undo_latest_critical(
        self,
        session_key: str,
    ) -> InteractionUndoResult | None:
        """串行选择、删除和派生收敛，返回可观察的提交结果。"""

        async with self._lock:
            # 1. 旧 pending 必须先收敛，不能叠加第二次破坏性删除。
            await self._recover_pending_locked()
            store = self._sessions.control_store
            control_turn_id = await asyncio.to_thread(
                store.latest_completed_interaction_id,
                session_key,
            )
            if control_turn_id is None:
                return None

            # 2. Akasha 自己封住 source event；其他 engine 由 SessionStore 直接提交。
            engine_name = self._memory_name()
            if engine_name == "akasha":
                memory = cast(_AkashaInteractionUndo, self._memory)
                deletion = await memory.delete_interaction_source(
                    control_turn_id,
                    lambda: store.delete_interaction(
                        control_turn_id,
                        action_source="plugin_undo.interaction_delete",
                        expected_latest_session_key=session_key,
                    ),
                )
            else:
                deletion = await asyncio.to_thread(
                    store.delete_interaction,
                    control_turn_id,
                    action_source="plugin_undo.interaction_delete",
                    expected_latest_session_key=session_key,
                    reconciliation_owner=(
                        "default_memory" if engine_name == "default" else None
                    ),
                )
            if deletion is None:
                return None
            self._sessions.invalidate(deletion.session_key)

            # 3. Default Memory receipt 成功后终结；失败保留 pending 供重启重放。
            reconciliation_pending = False
            if deletion.reconciliation_id is not None:
                try:
                    await self._reconcile_default_memory(
                        deletion.reconciliation_id,
                        deletion.message_ids,
                    )
                except Exception:
                    reconciliation_pending = True
            return _public_result(deletion, reconciliation_pending)

    async def _recover_pending_locked(self) -> None:
        """重放幂等 memory undo，并在任一失败时保持启动 fail-loud。"""

        pending = self._sessions.control_store.pending_interaction_memory_reconciliations(
            "default_memory"
        )
        if not pending:
            return
        if self._memory_name() != "default":
            raise RuntimeError(
                "存在未完成 Default Memory interaction 撤销，当前 memory engine 不匹配"
            )
        for receipt in pending:
            await self._reconcile_default_memory(
                receipt.reconciliation_id,
                receipt.message_ids,
            )

    async def _reconcile_default_memory(
        self,
        reconciliation_id: str,
        message_ids: tuple[str, ...],
    ) -> None:
        """执行幂等 Default Memory undo，并持久化失败或完成事实。"""

        undo = getattr(self._memory, "undo_by_message_sources", None)
        if not callable(undo):
            raise RuntimeError("Default Memory 缺少 interaction undo owner")
        store = self._sessions.control_store
        try:
            result = await asyncio.to_thread(
                cast(_DefaultMemoryUndo, self._memory).undo_by_message_sources,
                list(message_ids),
                dry_run=False,
            )
            if not isinstance(result, dict):
                raise TypeError("Default Memory interaction undo 必须返回 dict receipt")
        except Exception as exc:
            await asyncio.to_thread(
                store.record_interaction_memory_reconciliation_failure,
                reconciliation_id,
                f"{type(exc).__name__}: {exc}",
            )
            raise
        await asyncio.to_thread(
            store.complete_interaction_memory_reconciliation,
            reconciliation_id,
        )

    def _memory_name(self) -> str:
        """读取已选 memory engine 的稳定 descriptor identity。"""

        descriptor = self._memory.describe()
        name = descriptor.name
        if not isinstance(name, str) or not name.strip():
            raise RuntimeError("memory engine descriptor name 无效")
        return name.strip()


def _public_result(
    deletion: InteractionDeletion,
    reconciliation_pending: bool,
) -> InteractionUndoResult:
    return InteractionUndoResult(
        control_turn_id=deletion.control_turn_id,
        session_key=deletion.session_key,
        message_ids=deletion.message_ids,
        backup_path=deletion.backup_path,
        reconciliation_pending=reconciliation_pending,
        old_last_consolidated=deletion.old_last_consolidated,
        new_last_consolidated=deletion.new_last_consolidated,
    )


T = TypeVar("T")


async def _finish_critical(task: asyncio.Task[T]) -> T:
    """等待临界任务完成，再把 caller cancellation 原样恢复。"""

    cancelled: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if cancelled is None:
                cancelled = exc
    result = task.result()
    if cancelled is not None:
        raise cancelled
    return result


__all__ = ["InteractionUndoCoordinator"]
