from __future__ import annotations

from pathlib import Path

import pytest

from agent.plugins.composable import ComposablePlugin
from agent.plugins.artifacts import ArtifactPointer, write_pointers
from agent.plugins.manager import PluginManager
from agent.plugins.manifest import write_plugin_manifest
from agent.plugins.registry import plugin_registry
from bus.event_bus import EventBus


@pytest.fixture(autouse=True)
def _clean_registry():
    plugin_registry._handlers._handlers.clear()
    plugin_registry._classes.clear()
    plugin_registry._instances.clear()
    yield
    plugin_registry._handlers._handlers.clear()
    plugin_registry._classes.clear()
    plugin_registry._instances.clear()


def _write_plugin(root: Path, name: str, source: str) -> Path:
    plugin_dir = root / name
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "plugin.py").write_text(source, encoding="utf-8")
    return plugin_dir


def _manager(tmp_path: Path) -> PluginManager:
    return PluginManager(
        plugin_dirs=[tmp_path / "plugins"],
        event_bus=EventBus(),
        tool_registry=None,
        workspace=tmp_path / "workspace",
        installed_cache_root=tmp_path / "home" / "cache",
    )


@pytest.mark.asyncio
async def test_v3_namespace_loader_waits_for_service_not_scan_order(
    tmp_path: Path,
) -> None:
    _write_plugin(
        tmp_path / "plugins",
        "a_consumer",
        "from pydantic import BaseModel\n"
        "from agent.plugin_composition import ServiceKey\n"
        "api_version = 3\n"
        "name = 'a_consumer'\n"
        "version = '1.0.0'\n"
        "VALUE = ServiceKey('fixture.value')\n"
        "inject = (VALUE,)\n"
        "observed = None\n"
        "disposed = False\n"
        "class Config(BaseModel):\n"
        "    suffix: str = 'default'\n"
        "async def apply(ctx, config):\n"
        "    global observed, disposed\n"
        "    observed = (ctx.require(VALUE), ctx.runtime.plugin_id, "
        "ctx.runtime.workspace.name, config.suffix)\n"
        "    def cleanup():\n"
        "        global disposed\n"
        "        disposed = True\n"
        "    await ctx.effect(lambda: cleanup, label='consumer')\n",
    )
    _write_plugin(
        tmp_path / "plugins",
        "z_provider",
        "from agent.plugin_composition import ServiceKey\n"
        "api_version = 3\n"
        "name = 'z_provider'\n"
        "version = '1.0.0'\n"
        "VALUE = ServiceKey('fixture.value')\n"
        "async def apply(ctx, config):\n"
        "    await ctx.provide(VALUE, 'ready')\n",
    )
    config_dir = tmp_path / "workspace" / "plugin-data" / "a_consumer-builtin"
    config_dir.mkdir(parents=True)
    (config_dir / "config.local.toml").write_text(
        "suffix = 'configured'\n",
        encoding="utf-8",
    )
    manager = _manager(tmp_path)

    await manager.load_all()

    consumer = manager.generation("a_consumer")
    snapshot = manager.current_snapshot
    assert consumer is not None and snapshot is not None
    assert isinstance(consumer.instance, ComposablePlugin)
    assert consumer.instance.module.observed == (
        "ready",
        "a_consumer",
        "workspace",
        "configured",
    )
    assert snapshot.composition_root is not None
    assert snapshot.composition_topology is not None
    assert snapshot.composition_topology.services == ("fixture.value",)
    assert tuple(item.name for item in snapshot.composition_topology.fibers) == (
        "a_consumer",
        "z_provider",
    )

    await manager.terminate_all()

    assert consumer.instance.module.disposed is True


@pytest.mark.asyncio
async def test_v3_loader_fails_loud_when_required_service_never_appears(
    tmp_path: Path,
) -> None:
    _write_plugin(
        tmp_path / "plugins",
        "waiting",
        "from agent.plugin_composition import ServiceKey\n"
        "api_version = 3\n"
        "name = 'waiting'\n"
        "version = '1.0.0'\n"
        "inject = (ServiceKey('never.provided'),)\n"
        "def apply(ctx, config):\n"
        "    raise AssertionError('pending plugin must not apply')\n",
    )
    manager = _manager(tmp_path)

    with pytest.raises(RuntimeError, match="never.provided"):
        await manager.load_all()

    await manager.terminate_all()


@pytest.mark.asyncio
async def test_v3_reload_keeps_old_root_until_snapshot_lease_drains(
    tmp_path: Path,
) -> None:
    plugin_dir = _write_plugin(
        tmp_path / "plugins",
        "reloadable",
        "api_version = 3\n"
        "name = 'reloadable'\n"
        "version = '1.0.0'\n"
        "marker = 'old'\n"
        "disposed = False\n"
        "async def apply(ctx, config):\n"
        "    def cleanup():\n"
        "        global disposed\n"
        "        disposed = True\n"
        "    await ctx.effect(lambda: cleanup, label=marker)\n",
    )
    manager = _manager(tmp_path)
    await manager.load_all()
    old_generation = manager.generation("reloadable")
    old_snapshot = manager.current_snapshot
    assert old_generation is not None and old_snapshot is not None
    lease = manager._snapshot_store.lease()

    (plugin_dir / "plugin.py").write_text(
        "api_version = 3\n"
        "name = 'reloadable'\n"
        "version = '1.0.0'\n"
        "marker = 'new'\n"
        "disposed = False\n"
        "async def apply(ctx, config):\n"
        "    def cleanup():\n"
        "        global disposed\n"
        "        disposed = True\n"
        "    await ctx.effect(lambda: cleanup, label=marker)\n",
        encoding="utf-8",
    )
    candidate = await manager.prepare_candidate("reloadable")
    assert candidate is not None

    result = await manager.publish_prepared("reloadable")

    assert result["publication_state"] == "committed"
    assert manager.current_snapshot is not old_snapshot
    assert old_generation.instance.module.disposed is False
    await lease.release()
    await manager._snapshot_store.retry_drains()
    assert old_generation.instance.module.disposed is True

    await manager.terminate_all()


@pytest.mark.asyncio
async def test_installed_v3_candidate_rebuilds_runtime_then_promotes(
    tmp_path: Path,
) -> None:
    plugin_base = tmp_path / "home" / "cache" / "lab" / "installed_v3"
    stable_root = plugin_base / ".artifacts" / "1.0.0-aaaa"
    latest_root = plugin_base / ".artifacts" / "2.0.0-bbbb"
    stable_root.mkdir(parents=True)
    latest_root.mkdir(parents=True)
    source = (
        "api_version = 3\n"
        "name = 'installed_v3'\n"
        "version = '1.0.0'\n"
        "applied = []\n"
        "disposed = []\n"
        "async def apply(ctx, config):\n"
        "    workspace = str(ctx.runtime.workspace)\n"
        "    applied.append(workspace)\n"
        "    def cleanup():\n"
        "        disposed.append(workspace)\n"
        "    await ctx.effect(lambda: cleanup, label='runtime')\n"
    )
    (stable_root / "plugin.py").write_text(source, encoding="utf-8")
    (latest_root / "plugin.py").write_text(
        source.replace("version = '1.0.0'", "version = '2.0.0'"),
        encoding="utf-8",
    )
    stable_pointer = ArtifactPointer(".artifacts/1.0.0-aaaa")
    latest_pointer = ArtifactPointer(".artifacts/2.0.0-bbbb")
    write_pointers(plugin_base, stable=stable_pointer, latest=stable_pointer)
    write_plugin_manifest(
        {"installed_v3@lab": True},
        plugins_home=tmp_path / "home",
    )
    manager = _manager(tmp_path)
    await manager.load_all()
    stable = manager.generation("installed_v3@lab")
    assert stable is not None
    stable_lease = manager.snapshot_store.lease()

    write_pointers(plugin_base, stable=stable_pointer, latest=latest_pointer)
    result = (await manager.reconcile_changed())[0]
    candidate = manager.ready_candidate

    assert result["publication_state"] == "latest_ready"
    assert candidate is not None
    assert "plugin-validation" in candidate.instance.module.applied[-1]
    promoted = await manager.switch_ready("installed_v3@lab")

    assert promoted["publication_state"] == "promoted"
    assert candidate.instance.module.applied[-1] == str(tmp_path / "workspace")
    assert any(
        "plugin-validation" in workspace
        for workspace in candidate.instance.module.disposed
    )
    assert stable.instance.module.disposed == []
    await stable_lease.release()
    await manager.snapshot_store.retry_drains()
    assert stable.instance.module.disposed == [str(tmp_path / "workspace")]

    await manager.terminate_all()
