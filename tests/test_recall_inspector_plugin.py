from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from agent.lifecycle.composition import (
    CONTEXT_PREPARED_EVENT,
    run_composition_lifecycle,
)
from agent.lifecycle.types import BeforeTurnCtx
from agent.plugins.artifacts import ArtifactPointer, write_pointers
from agent.plugins.dashboard_host import DashboardBinding, PluginDashboardHost
from agent.plugins.manager import PluginManager
from agent.plugins.manifest import write_plugin_manifest
from agent.plugins.registry import plugin_registry
from agent.plugins.snapshot import bind_runtime_snapshot, reset_runtime_snapshot
from agent.tool_hooks.executor import ToolExecutor
from agent.tool_hooks.types import ToolExecutionRequest, ToolExecutionResult, ToolSource
from agent.tools.events import ToolResult
from bus.event_bus import EventBus
from plugins.default_memory.dashboard import RecallInspectorDashboardReader
from plugins.default_memory import plugin as default_memory_plugin
from plugins.default_memory.plugin import DefaultMemoryInspector


@pytest.fixture(autouse=True)
def _clean_plugin_registry():
    plugin_registry._instances.clear()
    yield
    plugin_registry._instances.clear()


def _default_memory_manager(tmp_path: Path, *, memory_name: str) -> PluginManager:
    plugin_root = tmp_path / "plugins"
    shutil.copytree(
        Path(__file__).parents[1] / "plugins" / "default_memory",
        plugin_root / "default_memory",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    return PluginManager(
        plugin_dirs=[plugin_root],
        event_bus=EventBus(),
        workspace=tmp_path / "workspace",
        installed_cache_root=tmp_path / "home" / "cache",
        memory_engine=SimpleNamespace(
            describe=lambda: SimpleNamespace(name=memory_name)
        ),
    )


@pytest.mark.asyncio
async def test_recall_inspector_records_context_and_recall(tmp_path: Path) -> None:
    data_root = tmp_path / "plugin-data"
    data_root.mkdir()
    plugin = DefaultMemoryInspector(data_root / "recall_inspector.jsonl")

    ctx = BeforeTurnCtx(
        session_key="cli:1",
        channel="cli",
        chat_id="1",
        content="还记得我喜欢什么吗",
        timestamp=datetime(2026, 5, 1, tzinfo=timezone.utc),
        skill_names=[],
        retrieved_memory_block="- [m1] 用户喜欢低压力创作\n- [m2] 用户偏好中文回复",
        retrieval_trace_raw={"route_decision": "RETRIEVE"},
        history_messages=(),
    )
    plugin.record_context_prepare(ctx)
    await plugin.record_recall_memory(
        ToolResult.from_execution(
            ToolExecutionRequest(
                call_id="call-1",
                source="passive",
                request_text="回忆用户偏好",
                tool_batch=(),
                tool_batch_index=0,
                session_key="cli:1",
                channel="cli",
                chat_id="1",
                tool_name="recall_memory",
                arguments={"query": "用户偏好"},
            ),
            ToolExecutionResult(
                status="success",
                final_arguments={"query": "用户偏好"},
                extra_messages=[],
                output=json.dumps(
                    {
                        "items": [
                            {
                                "id": "m3",
                                "memory_type": "preference",
                                "summary": "用户喜欢简单方案",
                                "score": 0.7,
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
            ),
        )
    )

    reader = RecallInspectorDashboardReader(data_root)
    items, total = reader.list_turns()

    assert total == 1
    assert items[0]["context_prepare_count"] == 2
    assert items[0]["recall_memory_count"] == 1
    assert items[0]["context_prepare"]["items"][0]["id"] == "m1"
    assert items[0]["recall_memory_calls"][0]["items"][0]["id"] == "m3"


@pytest.mark.parametrize("source", ["subagent", "proactive"])
@pytest.mark.asyncio
async def test_recall_inspector_keeps_v2_passive_source_boundary(
    tmp_path: Path,
    source: ToolSource,
) -> None:
    data_path = tmp_path / "recall_inspector.jsonl"
    inspector = DefaultMemoryInspector(data_path)

    await inspector.record_recall_memory(
        ToolResult.from_execution(
            ToolExecutionRequest(
                call_id=f"call-{source}",
                source=source,
                session_key=f"{source}:1",
                channel="web",
                chat_id="1",
                tool_name="recall_memory",
                arguments={"query": "不应记录"},
            ),
            ToolExecutionResult(
                status="success",
                final_arguments={"query": "不应记录"},
                extra_messages=[],
                output='{"items": []}',
            ),
        )
    )

    assert not data_path.exists()


def test_items_from_block_parses_each_summary_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    original = default_memory_plugin._split_summary_meta

    def record_call(summary: str) -> tuple[str, list[str]]:
        calls.append(summary)
        return original(summary)

    monkeypatch.setattr(default_memory_plugin, "_split_summary_meta", record_call)

    block = (
        "## 【相关历史】\n"
        "- [m1] 用户偏好中文回复（证据: 可回源原文；src: [m1]；有印象）\n"
        "- [m2] 用户喜欢短答案（证据: 记忆摘要；不确定）\n"
    )

    assert default_memory_plugin._items_from_block(block) == [
        {
            "id": "m1",
            "summary": "用户偏好中文回复",
            "tags": ["可回源原文", "有印象"],
            "section": "【相关历史】",
            "injected": True,
        },
        {
            "id": "m2",
            "summary": "用户喜欢短答案",
            "tags": ["记忆摘要", "不确定"],
            "section": "【相关历史】",
            "injected": True,
        },
    ]
    assert calls == [
        "用户偏好中文回复（证据: 可回源原文；src: [m1]；有印象）",
        "用户喜欢短答案（证据: 记忆摘要；不确定）",
    ]


def test_recall_inspector_uses_generation_root_without_legacy_fallback(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    data_root = workspace / "plugin-data" / "default_memory-builtin"
    legacy = workspace / "observe" / "recall_inspector.jsonl"
    data_root.mkdir(parents=True)
    legacy.parent.mkdir(parents=True)
    original = b'{"kind":"context_prepare","engine":"default","turn_id":"legacy"}\n'
    legacy.write_bytes(original)

    target = default_memory_plugin._resolve_recall_data_path(
        data_root=data_root,
    )
    assert target == data_root / "recall_inspector.jsonl"
    assert not target.exists()

    plugin = DefaultMemoryInspector(target)
    plugin.record_context_prepare(
        BeforeTurnCtx(
            session_key="cli:1",
            channel="cli",
            chat_id="1",
            content="测试召回记录隔离",
            timestamp=datetime(2026, 5, 1, tzinfo=timezone.utc),
            skill_names=[],
            retrieved_memory_block="",
            retrieval_trace_raw=None,
            history_messages=(),
        )
    )

    assert target.read_bytes() != original
    assert legacy.read_bytes() == original


@pytest.mark.asyncio
async def test_recall_inspector_v3_runs_from_formal_snapshot(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    manager = _default_memory_manager(tmp_path, memory_name="default")
    await manager.load_all()

    snapshot = manager.current_snapshot
    generation = manager.generation("default_memory")
    assert snapshot is not None and generation is not None
    root = snapshot.composition_root
    assert root is not None
    assert snapshot.composition_topology is not None
    assert len(snapshot.composition_topology.listeners) == 2
    data_path = generation.data_dir / "recall_inspector.jsonl"
    assert data_path.parent == generation.data_dir

    lease = manager.snapshot_store.lease()
    token = bind_runtime_snapshot(lease)
    try:
        await run_composition_lifecycle(
            CONTEXT_PREPARED_EVENT,
            BeforeTurnCtx(
                session_key="web:1",
                channel="web",
                chat_id="1",
                content="记得我的偏好吗",
                timestamp=datetime(2026, 8, 15, tzinfo=timezone.utc),
                skill_names=[],
                retrieved_memory_block="- [m1] 用户偏好简洁方案",
                retrieval_trace_raw=None,
                history_messages=(),
            ),
        )

        async def invoke(_: str, __: dict[str, Any]) -> str:
            return json.dumps(
                {
                    "items": [
                        {
                            "id": "m2",
                            "memory_type": "preference",
                            "summary": "用户偏好可回滚迁移",
                            "score": 0.9,
                        }
                    ]
                },
                ensure_ascii=False,
            )

        result = await ToolExecutor().execute(
            ToolExecutionRequest(
                call_id="call-1",
                tool_name="recall_memory",
                arguments={"query": "偏好"},
                source="passive",
                session_key="web:1",
                channel="web",
                chat_id="1",
            ),
            invoke,
        )
    finally:
        reset_runtime_snapshot(token)
        await lease.release()

    assert result.status == "success"
    reader = RecallInspectorDashboardReader(generation.data_dir)
    items, total = reader.list_turns()
    assert total == 1
    current = next(item for item in items if item["session_key"] == "web:1")
    assert current["context_prepare_count"] == 1
    assert current["recall_memory_count"] == 1

    host = PluginDashboardHost(
        core_routes=(),
    )
    host.prepare_snapshot(snapshot)
    assert len(snapshot.dashboard_bindings) == 1
    binding = snapshot.dashboard_bindings[0]
    assert isinstance(binding, DashboardBinding)
    assert binding.runtime_data_root == generation.data_dir.resolve()
    assert {route.path for route in binding.routes} == {
        "/api/dashboard/recall-inspector/overview",
        "/api/dashboard/recall-inspector/turns",
        "/api/dashboard/recall-inspector/turns/{turn_id}",
    }

    await manager.terminate_all()
    assert root.topology_view().listeners == ()
    assert root.receipt().effects == ()


@pytest.mark.asyncio
async def test_recall_inspector_candidate_writes_only_isolated_data_root(
    tmp_path: Path,
) -> None:
    manager = _default_memory_manager(tmp_path, memory_name="default")
    await manager.load_all()
    stable = manager.generation("default_memory")
    stable_snapshot = manager.current_snapshot
    assert stable is not None and stable_snapshot is not None
    stable_path = stable.data_dir / "recall_inspector.jsonl"
    stable_bytes = stable_path.read_bytes() if stable_path.exists() else b""

    plugin_path = tmp_path / "plugins" / "default_memory" / "plugin.py"
    plugin_path.write_text(
        plugin_path.read_text(encoding="utf-8").replace(
            'version = "3.0.0"',
            'version = "3.0.1"',
            1,
        ),
        encoding="utf-8",
    )
    candidate = await manager.prepare_candidate("default_memory")
    assert candidate is not None and candidate.runtime_snapshot is not None
    assert candidate.validation_workspace is not None
    validation_root = candidate.validation_workspace.parent
    candidate_root = candidate.runtime_snapshot.composition_root
    assert candidate_root is not None
    candidate_runtime = candidate_root.root_fiber.children[0].runtime
    assert candidate_runtime is not None
    candidate_path = candidate_runtime.data_dir / "recall_inspector.jsonl"

    result = await candidate_root.context.serial(
        CONTEXT_PREPARED_EVENT,
        BeforeTurnCtx(
            session_key="web:candidate",
            channel="web",
            chat_id="candidate",
            content="candidate only",
            timestamp=datetime(2026, 8, 15, tzinfo=timezone.utc),
            skill_names=[],
            retrieved_memory_block="",
            retrieval_trace_raw=None,
            history_messages=(),
        ),
    )
    assert result is None
    assert candidate_path.is_file()
    assert b"candidate only" in candidate_path.read_bytes()
    assert (stable_path.read_bytes() if stable_path.exists() else b"") == stable_bytes

    host = PluginDashboardHost(
        core_routes=(),
    )
    host.prepare_snapshot(candidate.runtime_snapshot)
    candidate_binding = candidate.runtime_snapshot.dashboard_bindings[0]
    assert isinstance(candidate_binding, DashboardBinding)
    assert candidate_binding.validation is True
    assert candidate_binding.runtime_data_root == candidate_runtime.data_dir.resolve()
    candidate_reader = RecallInspectorDashboardReader(candidate_runtime.data_dir)
    candidate_turns, candidate_total = candidate_reader.list_turns()
    assert candidate_total == 1
    assert candidate_turns[0]["session_key"] == "web:candidate"
    manager.bind_dashboard_preparer(
        host.prepare_snapshot,
        validation_releaser=host.release_validation,
    )

    await manager.discard_prepared("default_memory")
    assert not validation_root.exists()
    assert manager.current_snapshot is stable_snapshot
    assert (stable_path.read_bytes() if stable_path.exists() else b"") == stable_bytes

    next_candidate = await manager.prepare_candidate("default_memory")
    assert next_candidate is not None
    published = await manager.publish_prepared("default_memory")
    assert published["publication_state"] == "committed"

    lease = manager.snapshot_store.lease()
    token = bind_runtime_snapshot(lease)
    try:
        await run_composition_lifecycle(
            CONTEXT_PREPARED_EVENT,
            BeforeTurnCtx(
                session_key="web:formal",
                channel="web",
                chat_id="formal",
                content="formal only",
                timestamp=datetime(2026, 8, 15, tzinfo=timezone.utc),
                skill_names=[],
                retrieved_memory_block="",
                retrieval_trace_raw=None,
                history_messages=(),
            ),
        )
    finally:
        reset_runtime_snapshot(token)
        await lease.release()

    assert b"formal only" in stable_path.read_bytes()
    assert b"candidate only" not in stable_path.read_bytes()
    current = manager.current_snapshot
    assert current is not None
    formal_binding = current.dashboard_bindings[0]
    assert isinstance(formal_binding, DashboardBinding)
    assert formal_binding.validation is False
    assert formal_binding.runtime_data_root == stable.data_dir.resolve()
    await manager.terminate_all()


@pytest.mark.asyncio
async def test_installed_default_memory_candidate_preserves_static_projection(
    tmp_path: Path,
) -> None:
    cache = tmp_path / "home" / "cache" / "lab" / "default_memory"
    stable_root = cache / ".artifacts" / "3.0.0-aaaa"
    latest_root = cache / ".artifacts" / "3.0.1-bbbb"
    shutil.copytree(
        Path(__file__).parents[1] / "plugins" / "default_memory",
        stable_root,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    shutil.copytree(stable_root, latest_root)
    stable_manifest = stable_root / "akashic.plugin.toml"
    stable_manifest.write_text(
        "schema_version = 1\n"
        'name = "default_memory"\n'
        'version = "3.0.0"\n'
        "api_version = 3\n"
        'entrypoint = "plugin.py"\n',
        encoding="utf-8",
    )
    latest_manifest = latest_root / "akashic.plugin.toml"
    latest_manifest.write_text(
        stable_manifest.read_text(encoding="utf-8").replace(
            'version = "3.0.0"',
            'version = "3.0.1"',
            1,
        ),
        encoding="utf-8",
    )
    latest_plugin = latest_root / "plugin.py"
    latest_plugin.write_text(
        latest_plugin.read_text(encoding="utf-8").replace(
            'version = "3.0.0"',
            'version = "3.0.1"',
            1,
        ),
        encoding="utf-8",
    )
    stable_pointer = ArtifactPointer(".artifacts/3.0.0-aaaa")
    latest_pointer = ArtifactPointer(".artifacts/3.0.1-bbbb")
    write_pointers(cache, stable=stable_pointer, latest=stable_pointer)
    write_plugin_manifest(
        {"default_memory@lab": True},
        plugins_home=tmp_path / "home",
    )
    manager = PluginManager(
        plugin_dirs=[],
        event_bus=EventBus(),
        workspace=tmp_path / "workspace",
        installed_cache_root=tmp_path / "home" / "cache",
        memory_engine=SimpleNamespace(
            describe=lambda: SimpleNamespace(name="default")
        ),
    )
    await manager.load_all()
    manager.sync_skill_links()
    skill_link = (
        tmp_path / "workspace" / "drift" / "skills" / "audit-dirty-memories"
    )
    assert skill_link.exists()
    host = PluginDashboardHost(
        core_routes=(),
    )
    stable_snapshot = manager.current_snapshot
    assert stable_snapshot is not None
    host.prepare_initial_snapshot(stable_snapshot)
    manager.bind_dashboard_preparer(
        host.prepare_snapshot,
        validation_releaser=host.release_validation,
    )

    write_pointers(cache, stable=stable_pointer, latest=latest_pointer)
    status = (await manager.reconcile_changed())[0]
    assert status["publication_state"] == "latest_ready"
    candidate = manager.ready_candidate
    assert candidate is not None and candidate.runtime_snapshot is not None
    candidate_snapshot = candidate.runtime_snapshot
    assert "default_memory@lab" in {
        generation.plugin_id
        for generation in candidate_snapshot.active_generations()
    }
    candidate_binding = candidate_snapshot.dashboard_bindings[0]
    assert isinstance(candidate_binding, DashboardBinding)
    assert candidate_binding.validation is True
    candidate_root = candidate_snapshot.composition_root
    assert candidate_root is not None
    await candidate_root.context.serial(
        CONTEXT_PREPARED_EVENT,
        BeforeTurnCtx(
            session_key="web:installed-candidate",
            channel="web",
            chat_id="candidate",
            content="candidate projection",
            timestamp=datetime(2026, 8, 15, tzinfo=timezone.utc),
            skill_names=[],
            retrieved_memory_block="",
            retrieval_trace_raw=None,
            history_messages=(),
        ),
    )
    assert candidate_binding.runtime_data_root is not None
    candidate_reader = RecallInspectorDashboardReader(
        candidate_binding.runtime_data_root
    )
    candidate_turns, candidate_total = candidate_reader.list_turns()
    assert candidate_total == 1
    assert candidate_turns[0]["session_key"] == "web:installed-candidate"

    result = await manager.switch_ready("default_memory@lab")

    assert result["publication_state"] == "promoted"
    assert skill_link.exists()
    current = manager.current_snapshot
    generation = manager.generation("default_memory@lab")
    assert current is not None and generation is not None
    assert "default_memory@lab" in {
        item.plugin_id for item in current.active_generations()
    }
    formal_binding = current.dashboard_bindings[0]
    assert isinstance(formal_binding, DashboardBinding)
    assert formal_binding.validation is False
    assert formal_binding.runtime_data_root == generation.data_dir.resolve()
    formal_reader = RecallInspectorDashboardReader(generation.data_dir)
    formal_turns, _ = formal_reader.list_turns()
    assert not any(
        item["session_key"] == "web:installed-candidate"
        for item in formal_turns
    )
    await manager.terminate_all()


def test_recall_inspector_reader_reports_unavailable(tmp_path: Path) -> None:
    reader = RecallInspectorDashboardReader(tmp_path)

    assert reader.get_overview() == {"available": True, "total": 0, "latest_at": None}
    assert reader.list_turns() == ([], 0)
    assert reader.get_turn("missing") is None


def test_recall_inspector_reader_exposes_corrupt_jsonl(tmp_path: Path) -> None:
    data_path = tmp_path / "recall_inspector.jsonl"
    data_path.parent.mkdir(parents=True, exist_ok=True)
    data_path.write_text('{"kind":"context_prepare"}\nnot-json\n', encoding="utf-8")

    reader = RecallInspectorDashboardReader(tmp_path)

    with pytest.raises(ValueError, match="第 2 行损坏"):
        reader.list_turns()


@pytest.mark.asyncio
async def test_recall_inspector_v3_is_inactive_for_akasha(tmp_path: Path) -> None:
    manager = _default_memory_manager(tmp_path, memory_name="akasha")
    await manager.load_all()

    snapshot = manager.current_snapshot
    generation = manager.generation("default_memory")
    assert snapshot is not None and generation is not None
    assert "default_memory" not in {
        item.plugin_id for item in snapshot.active_generations()
    }
    assert snapshot.composition_topology is not None
    assert not any(
        "default_memory" in listener
        for listener in snapshot.composition_topology.listeners
    )
    _, _, promoted_skills = manager._prepare_skill_links_for_promotion(
        generation,
        snapshot,
    )
    assert "default_memory" not in {plugin.plugin_id for plugin in promoted_skills}
    manager.sync_skill_links()
    assert not (
        tmp_path / "workspace" / "drift" / "skills" / "audit-dirty-memories"
    ).exists()
    host = PluginDashboardHost(
        core_routes=(),
    )
    host.prepare_snapshot(snapshot)
    assert snapshot.dashboard_bindings == ()
    assert not (generation.data_dir / "recall_inspector.jsonl").exists()
    await manager.terminate_all()


def test_recall_inspector_reader_ignores_cross_memory_records(tmp_path: Path) -> None:
    data_path = tmp_path / "recall_inspector.jsonl"
    data_path.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {
            "kind": "context_prepare",
            "turn_id": "default-turn",
            "session_key": "telegram:1",
            "channel": "telegram",
            "chat_id": "1",
            "user_text": "默认记忆",
            "timestamp": "2026-05-17T00:22:00",
            "context_prepare": {"items": []},
        },
        {
            "kind": "context_prepare",
            "turn_id": "cross-turn",
            "session_key": "cross_mem:7674283004",
            "channel": "cross_mem",
            "chat_id": "7674283004",
            "user_text": "cross 记忆",
            "timestamp": "2026-05-17T00:21:00",
            "context_prepare": {"items": []},
        },
    ]
    data_path.write_text(
        "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n",
        encoding="utf-8",
    )

    reader = RecallInspectorDashboardReader(tmp_path)
    items, total = reader.list_turns()

    assert total == 1
    assert items[0]["turn_id"] == "default-turn"
