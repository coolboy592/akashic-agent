from __future__ import annotations

import asyncio
import sqlite3
import threading
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from agent.plugin_composition import InteractionUndoService
from agent.plugins.composable import ComposablePlugin
from agent.plugins.interaction_undo import InteractionUndoCoordinator
from agent.plugins.manager import PluginManager
from bus.event_bus import EventBus
from session.manager import SessionManager


class _DefaultMemory:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[tuple[str, ...]] = []

    def describe(self) -> SimpleNamespace:
        return SimpleNamespace(name="default")

    def undo_by_message_sources(
        self,
        message_ids: list[str],
        *,
        dry_run: bool = False,
    ) -> dict[str, object]:
        assert dry_run is False
        self.calls.append(tuple(message_ids))
        if self.fail:
            raise RuntimeError("memory write failed")
        return {"affected_ids": [], "restored_ids": []}


class _BlockingDefaultMemory(_DefaultMemory):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def undo_by_message_sources(
        self,
        message_ids: list[str],
        *,
        dry_run: bool = False,
    ) -> dict[str, object]:
        self.entered.set()
        assert self.release.wait(timeout=5)
        return super().undo_by_message_sources(message_ids, dry_run=dry_run)


def _seed_interaction(
    manager: SessionManager,
    *,
    session_key: str = "cli:undo",
    turn_id: str = "turn:undo",
) -> tuple[str, ...]:
    now = datetime.now(UTC).isoformat()
    rows = manager.control_store.persist_session(
        session_key,
        created_at=now,
        updated_at=now,
        metadata={},
        messages=[
            {
                "role": "user",
                "content": "question",
                "timestamp": now,
                "extra": {
                    "control_turn_id": turn_id,
                    "turn_input_ordinal": 0,
                },
            },
            {
                "role": "assistant",
                "content": "answer",
                "timestamp": now,
                "extra": {
                    "control_turn_id": turn_id,
                    "turn_terminal": True,
                    "turn_input_count": 1,
                },
            },
        ],
    )
    return tuple(str(row["id"]) for row in rows)


@pytest.mark.asyncio
async def test_candidate_interaction_undo_has_no_destructive_owner() -> None:
    service = InteractionUndoService.candidate_validation()
    with pytest.raises(RuntimeError, match="candidate 验证期禁止"):
        await service.undo_latest("cli:undo")


@pytest.mark.asyncio
async def test_default_memory_failure_keeps_durable_pending_receipt(tmp_path) -> None:
    manager = SessionManager(tmp_path)
    message_ids = _seed_interaction(manager)
    _ = manager.get_existing("cli:undo")
    memory = _DefaultMemory(fail=True)
    coordinator = InteractionUndoCoordinator(manager, memory)

    result = await coordinator.undo_latest("cli:undo")

    assert result is not None
    assert result.message_ids == message_ids
    assert result.reconciliation_pending is True
    assert result.old_last_consolidated == 0
    assert result.new_last_consolidated == 0
    assert manager._cache.get("cli:undo") is None
    assert manager.get_existing("cli:undo").messages == []
    assert manager.control_store.get_session_meta("cli:undo") is not None
    pending = manager.control_store.pending_interaction_memory_reconciliations(
        "default_memory"
    )
    assert len(pending) == 1
    assert pending[0].attempts == 1
    backup = sqlite3.connect(result.backup_path)
    try:
        assert backup.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    finally:
        backup.close()

    memory.fail = False
    await coordinator.recover_pending()
    assert memory.calls == [message_ids, message_ids]
    assert (
        manager.control_store.pending_interaction_memory_reconciliations(
            "default_memory"
        )
        == ()
    )
    manager.control_store.close()


@pytest.mark.asyncio
async def test_core_restart_replays_committed_memory_receipt(tmp_path) -> None:
    first = SessionManager(tmp_path)
    message_ids = _seed_interaction(first)
    deletion = first.control_store.delete_interaction(
        "turn:undo",
        action_source="test.process_crash",
        expected_latest_session_key="cli:undo",
        reconciliation_owner="default_memory",
    )
    assert deletion is not None
    first.control_store.close()

    restarted = SessionManager(tmp_path)
    memory = _DefaultMemory()
    await InteractionUndoCoordinator(restarted, memory).recover_pending()

    assert memory.calls == [message_ids]
    assert restarted.get_existing("cli:undo").messages == []
    assert (
        restarted.control_store.pending_interaction_memory_reconciliations(
            "default_memory"
        )
        == ()
    )
    restarted.control_store.close()


@pytest.mark.asyncio
async def test_caller_cancel_waits_for_committed_reconciliation(tmp_path) -> None:
    manager = SessionManager(tmp_path)
    message_ids = _seed_interaction(manager)
    memory = _BlockingDefaultMemory()
    coordinator = InteractionUndoCoordinator(manager, memory)

    task = asyncio.create_task(coordinator.undo_latest("cli:undo"))
    assert await asyncio.to_thread(memory.entered.wait, 5)
    task.cancel()
    memory.release.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert memory.calls == [message_ids]
    assert manager.get_existing("cli:undo").messages == []
    assert (
        manager.control_store.pending_interaction_memory_reconciliations(
            "default_memory"
        )
        == ()
    )
    manager.control_store.close()


def test_latest_interaction_fence_rejects_stale_selection(tmp_path) -> None:
    manager = SessionManager(tmp_path)
    first_ids = _seed_interaction(manager, turn_id="turn:first")
    assert (
        manager.control_store.latest_completed_interaction_id("cli:undo")
        == "turn:first"
    )
    second_ids = _seed_interaction(manager, turn_id="turn:second")

    with pytest.raises(RuntimeError, match="latest interaction 已变化"):
        manager.control_store.delete_interaction(
            "turn:first",
            expected_latest_session_key="cli:undo",
        )

    manager.invalidate("cli:undo")
    rows = manager.get_existing("cli:undo").messages
    assert tuple(str(row["id"]) for row in rows) == (*first_ids, *second_ids)
    manager.control_store.close()


@pytest.mark.asyncio
async def test_manager_provides_formal_service_and_candidate_cannot_write(
    tmp_path,
) -> None:
    workspace = tmp_path / "workspace"
    sessions = SessionManager(workspace)
    message_ids = _seed_interaction(sessions)
    memory = _DefaultMemory()
    plugin_dir = tmp_path / "plugins" / "undo_probe"
    plugin_dir.mkdir(parents=True)
    plugin_path = plugin_dir / "plugin.py"
    plugin_path.write_text(
        "from agent.plugin_composition import INTERACTION_UNDO\n"
        "api_version = 3\n"
        "name = 'undo_probe'\n"
        "version = '1.0.0'\n"
        "inject = (INTERACTION_UNDO,)\n"
        "service = None\n"
        "async def apply(ctx, config):\n"
        "    global service\n"
        "    service = ctx.require(INTERACTION_UNDO)\n",
        encoding="utf-8",
    )
    manager = PluginManager(
        plugin_dirs=[tmp_path / "plugins"],
        event_bus=EventBus(),
        workspace=workspace,
        session_manager=sessions,
        memory_engine=memory,
        installed_cache_root=tmp_path / "cache",
    )
    try:
        await manager.load_all()
        generation = manager.generation("undo_probe")
        stable = manager.current_snapshot
        assert generation is not None and stable is not None
        assert isinstance(generation.instance, ComposablePlugin)
        service = generation.instance.module.service
        assert isinstance(service, InteractionUndoService)
        assert "core.interaction_undo" in stable.composition_topology.services

        plugin_path.write_text(
            "from agent.plugin_composition import INTERACTION_UNDO\n"
            "api_version = 3\n"
            "name = 'undo_probe'\n"
            "version = '2.0.0'\n"
            "inject = (INTERACTION_UNDO,)\n"
            "async def apply(ctx, config):\n"
            "    await ctx.require(INTERACTION_UNDO).undo_latest('cli:undo')\n",
            encoding="utf-8",
        )
        candidate = await manager.prepare_candidate("undo_probe")
        assert candidate is None
        assert manager.prepared_generation("undo_probe") is None
        sessions.invalidate("cli:undo")
        assert tuple(
            str(row["id"]) for row in sessions.get_existing("cli:undo").messages
        ) == message_ids
        assert manager.current_snapshot is stable

        deletion = await service.undo_latest("cli:undo")
        assert deletion is not None
        assert deletion.message_ids == message_ids
        assert deletion.reconciliation_pending is False
        assert memory.calls == [message_ids]
        assert sessions.get_existing("cli:undo").messages == []
    finally:
        await manager.terminate_all()
        sessions.close()
