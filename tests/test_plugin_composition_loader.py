from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

from agent.plugins.composable import ComposablePlugin
from agent.plugins.artifacts import ArtifactPointer, read_pointer, write_pointers
from agent.plugins.dashboard_host import DashboardBinding, PluginDashboardHost
from agent.plugins.generation import PluginGeneration
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
async def test_v3_loader_publishes_declared_package_contributions(
    tmp_path: Path,
) -> None:
    plugin_dir = _write_plugin(
        tmp_path / "plugins",
        "package_contributor",
        "api_version = 3\n"
        "name = 'package_contributor'\n"
        "version = '1.0.0'\n"
        "skill_roots = ('skills',)\n"
        "drift_skill_roots = ('drift/skills',)\n"
        "dashboard_module = 'dashboard.py'\n"
        "def apply(ctx, config): pass\n",
    )
    skill_dir = plugin_dir / "skills" / "package-skill"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: package-skill\ndescription: package skill\n---\nnormal body\n",
        encoding="utf-8",
    )
    drift_skill_dir = plugin_dir / "drift" / "skills" / "package-drift"
    drift_skill_dir.mkdir(parents=True)
    (drift_skill_dir / "SKILL.md").write_text(
        "---\nname: package-drift\ndescription: package drift\n---\ndrift body\n",
        encoding="utf-8",
    )
    (plugin_dir / "dashboard.py").write_text(
        "def register(app, plugin_dir, workspace):\n"
        "    @app.get('/api/dashboard/package-contributor')\n"
        "    def status(): return {'plugin': 'package_contributor'}\n",
        encoding="utf-8",
    )
    manager = _manager(tmp_path)

    await manager.load_all()

    generation = manager.generation("package_contributor")
    snapshot = manager.current_snapshot
    assert generation is not None and snapshot is not None
    assert generation.contributions.skill_roots == (
        (plugin_dir / "skills").resolve(),
    )
    assert generation.contributions.drift_skill_roots == (
        (plugin_dir / "drift" / "skills").resolve(),
    )
    assert generation.contributions.dashboard_module == (
        plugin_dir / "dashboard.py"
    ).resolve()
    active = {item.plugin_id: item for item in manager.active_plugins()}
    assert active["package_contributor"].skill_roots == (
        (plugin_dir / "skills").resolve(),
    )
    assert active["package_contributor"].drift_skill_roots == (
        (plugin_dir / "drift" / "skills").resolve(),
    )
    catalog_id = snapshot.skill_catalog_generation_id
    assert catalog_id is not None
    catalog = manager._skill_host.get(catalog_id)
    assert catalog is not None
    assert snapshot.plugin_skill_index is not None
    assert set(snapshot.plugin_skill_index.records) == {"package-skill"}
    assert set(catalog.drift.records) == {"package-drift"}
    assert snapshot.plugin_skill_index.records["package-skill"].root_dir != skill_dir

    dashboard_host = PluginDashboardHost(
        workspace=tmp_path / "workspace",
        memory_admin=object(),
        memory_store=object(),
        core_routes=(),
    )
    dashboard_host.prepare_snapshot(snapshot)
    assert len(snapshot.dashboard_bindings) == 1
    binding = snapshot.dashboard_bindings[0]
    assert isinstance(binding, DashboardBinding)
    assert binding.plugin_id == "package_contributor"
    assert [route.path for route in binding.routes] == [
        "/api/dashboard/package-contributor"
    ]

    await manager.terminate_all()


@pytest.mark.parametrize(
    ("declaration", "message"),
    [
        ("skill_roots = 'skills'", "skill_roots 必须是字符串序列"),
        ("drift_skill_roots = ('',)", "drift_skill_roots 必须只包含非空字符串"),
        ("skill_roots = ('skills', 'skills')", "skill_roots 不得重复"),
        ("dashboard_module = ''", "dashboard_module 必须是非空字符串或 None"),
    ],
)
def test_v3_namespace_rejects_invalid_package_contributions(
    declaration: str,
    message: str,
) -> None:
    from types import ModuleType

    module = ModuleType("invalid_v3_contribution")
    module.api_version = 3
    module.name = "invalid"
    module.version = "1.0.0"
    module.apply = lambda ctx, config: None
    exec(declaration, module.__dict__)

    with pytest.raises(ValueError, match=message):
        _ = ComposablePlugin.from_module(module)


def test_v3_namespace_freezes_package_contribution_lists() -> None:
    from types import ModuleType

    roots = ["skills"]
    module = ModuleType("frozen_v3_contribution")
    module.api_version = 3
    module.name = "frozen"
    module.version = "1.0.0"
    module.skill_roots = roots
    module.apply = lambda ctx, config: None

    plugin = ComposablePlugin.from_module(module)
    roots.append("mutated")

    assert plugin.skill_roots == ("skills",)


@pytest.mark.asyncio
async def test_v3_package_contribution_path_cannot_escape_plugin_root(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    outside = tmp_path / "plugins" / "outside"
    outside.mkdir(parents=True)
    _write_plugin(
        tmp_path / "plugins",
        "escaped_contributor",
        "api_version = 3\n"
        "name = 'escaped_contributor'\n"
        "version = '1.0.0'\n"
        "skill_roots = ('../outside',)\n"
        "def apply(ctx, config): pass\n",
    )
    manager = _manager(tmp_path)

    await manager.load_all()

    assert "插件 能力目录 越界" in caplog.text
    assert manager.current_snapshot is None
    assert manager.generation("escaped_contributor") is None


@pytest.mark.asyncio
async def test_v3_package_contribution_rejects_duplicate_resolved_roots(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    plugin_dir = _write_plugin(
        tmp_path / "plugins",
        "duplicate_contributor",
        "api_version = 3\n"
        "name = 'duplicate_contributor'\n"
        "version = '1.0.0'\n"
        "skill_roots = ('skills', './skills')\n"
        "def apply(ctx, config): pass\n",
    )
    (plugin_dir / "skills").mkdir()
    manager = _manager(tmp_path)

    await manager.load_all()

    assert "插件能力目录重复" in caplog.text
    assert manager.current_snapshot is None
    assert manager.generation("duplicate_contributor") is None


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

    assert manager.current_snapshot is None
    assert manager.active_plugins() == []
    assert manager._snapshot_store.retained_snapshot_ids == ()
    assert manager._active_generations == {}
    assert manager._scopes == {}
    assert not (
        tmp_path / "workspace" / "plugin-data" / "waiting-builtin"
    ).exists()

    await manager.terminate_all()


@pytest.mark.asyncio
async def test_mixed_stable_boot_publishes_one_complete_snapshot(
    tmp_path: Path,
) -> None:
    _write_plugin(
        tmp_path / "plugins",
        "legacy",
        "from agent.plugins import Plugin\n"
        "class LegacyPlugin(Plugin):\n"
        "    name = 'legacy'\n"
        "    def activate(self):\n"
        "        self.activated = True\n",
    )
    _write_plugin(
        tmp_path / "plugins",
        "consumer",
        "from agent.plugin_composition import ServiceKey\n"
        "api_version = 3\n"
        "name = 'consumer'\n"
        "version = '1.0.0'\n"
        "VALUE = ServiceKey('fixture.batch')\n"
        "inject = (VALUE,)\n"
        "async def apply(ctx, config):\n"
        "    assert ctx.require(VALUE) == 'ready'\n",
    )
    _write_plugin(
        tmp_path / "plugins",
        "provider",
        "from agent.plugin_composition import ServiceKey\n"
        "api_version = 3\n"
        "name = 'provider'\n"
        "version = '1.0.0'\n"
        "VALUE = ServiceKey('fixture.batch')\n"
        "async def apply(ctx, config):\n"
        "    await ctx.provide(VALUE, 'ready')\n",
    )
    manager = _manager(tmp_path)
    installed: list[object] = []
    original_install = manager._snapshot_store.install

    def record_install(snapshot: object) -> None:
        installed.append(snapshot)
        original_install(snapshot)  # type: ignore[arg-type]

    manager._snapshot_store.install = record_install  # type: ignore[method-assign]

    await manager.load_all()

    snapshot = manager.current_snapshot
    legacy = manager.generation("legacy")
    assert snapshot is not None and legacy is not None
    assert len(installed) == 1
    assert set(snapshot.generations) == {"consumer", "legacy", "provider"}
    assert snapshot.composition_root is not None
    assert snapshot.composition_topology is not None
    assert snapshot.composition_topology.services == ("fixture.batch",)
    assert getattr(legacy.instance, "activated") is True
    catalog_id = snapshot.skill_catalog_generation_id
    assert catalog_id is not None
    assert manager._skill_host.get(catalog_id) is not None

    await manager.terminate_all()

    assert manager._skill_host.get(catalog_id) is None


@pytest.mark.asyncio
async def test_failed_snapshot_install_restores_legacy_plugin_kv(
    tmp_path: Path,
) -> None:
    _write_plugin(
        tmp_path / "plugins",
        "legacy_kv",
        "from agent.plugins import Plugin\n"
        "class LegacyKvPlugin(Plugin):\n"
        "    name = 'legacy_kv'\n"
        "    async def prepare(self):\n"
        "        self.context.kv_store.set('value', 'changed')\n",
    )
    kv_path = (
        tmp_path
        / "workspace"
        / "plugin-data"
        / "legacy_kv-builtin"
        / ".kv.json"
    )
    kv_path.parent.mkdir(parents=True)
    original = '{"value":"original"}\n'
    kv_path.write_text(original, encoding="utf-8")
    manager = _manager(tmp_path)

    def reject_install(snapshot: object) -> None:
        del snapshot
        raise RuntimeError("install failed")

    manager._snapshot_store.install = reject_install  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="install failed"):
        await manager.load_all()

    assert kv_path.read_text(encoding="utf-8") == original
    assert manager.current_snapshot is None
    assert manager._active_generations == {}
    assert manager._scopes == {}


@pytest.mark.asyncio
async def test_cancelled_stable_batch_finishes_all_cleanup(tmp_path: Path) -> None:
    first_cleanup = tmp_path / "first-cleaned"
    blocking_started = tmp_path / "blocking-started"
    root_cleanup = tmp_path / "root-cleaned"
    _write_plugin(
        tmp_path / "plugins",
        "a_first",
        "from agent.plugins import Plugin\n"
        "class FirstPlugin(Plugin):\n"
        "    name = 'a_first'\n"
        "    async def prepare(self):\n"
        f"        self.context.defer('marker', lambda: open({str(first_cleanup)!r}, 'w').close())\n",
    )
    _write_plugin(
        tmp_path / "plugins",
        "b_root",
        "from pathlib import Path\n"
        "api_version = 3\n"
        "name = 'b_root'\n"
        "version = '1.0.0'\n"
        "async def apply(ctx, config):\n"
        f"    await ctx.effect(lambda: lambda: Path({str(root_cleanup)!r}).touch(), label='marker')\n",
    )
    _write_plugin(
        tmp_path / "plugins",
        "z_blocking",
        "import asyncio\n"
        "from pathlib import Path\n"
        "from agent.plugins import Plugin\n"
        "class BlockingPlugin(Plugin):\n"
        "    name = 'z_blocking'\n"
        "    async def prepare(self):\n"
        f"        Path({str(blocking_started)!r}).touch()\n"
        "        await asyncio.Event().wait()\n",
    )
    manager = _manager(tmp_path)
    original_discard = manager._discard_stable_batch
    discard_started = asyncio.Event()

    async def delayed_discard(*args: object, **kwargs: object) -> None:
        discard_started.set()
        await asyncio.sleep(0.05)
        await original_discard(*args, **kwargs)  # type: ignore[arg-type]

    manager._discard_stable_batch = delayed_discard  # type: ignore[method-assign]
    loading = asyncio.create_task(manager.load_all())
    while not blocking_started.exists():
        await asyncio.sleep(0)

    loading.cancel()
    await discard_started.wait()
    loading.cancel()
    with pytest.raises(asyncio.CancelledError):
        await loading

    assert first_cleanup.exists()
    assert root_cleanup.exists()
    assert manager.current_snapshot is None
    assert manager._snapshot_store.retained_snapshot_ids == ()
    assert manager._active_generations == {}
    assert manager._scopes == {}


@pytest.mark.asyncio
async def test_failed_legacy_participant_rebuilds_remaining_instances(
    tmp_path: Path,
) -> None:
    _write_plugin(
        tmp_path / "plugins",
        "a_good",
        "from agent.plugins import Plugin\n"
        "class GoodPlugin(Plugin):\n"
        "    name = 'a_good'\n",
    )
    _write_plugin(
        tmp_path / "plugins",
        "z_failed",
        "from agent.plugins import Plugin\n"
        "class FailedPlugin(Plugin):\n"
        "    name = 'z_failed'\n"
        "    async def prepare(self):\n"
        "        raise RuntimeError('rejected')\n",
    )
    manager = _manager(tmp_path)
    original_load_one = manager._load_one
    observed: list[object] = []
    module_paths: list[str] = []

    async def record_load(
        mod: dict[str, str],
        *,
        activate: bool = True,
        stage_stable: bool = False,
    ) -> PluginGeneration | None:
        generation = await original_load_one(
            mod,
            activate=activate,
            stage_stable=stage_stable,
        )
        if generation is not None and generation.plugin_id == "a_good":
            observed.append(generation.instance)
            module_paths.append(generation.module_path)
        return generation

    manager._load_one = record_load  # type: ignore[method-assign]

    await manager.load_all()

    active = manager.generation("a_good")
    assert active is not None
    assert len(observed) == 2
    assert observed[0] is not observed[1]
    assert active.instance is observed[1]
    assert module_paths[0] not in sys.modules
    assert module_paths[1] in sys.modules
    assert manager.generation("z_failed") is None

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
    active_root = manager.current_snapshot.composition_root
    assert active_root is not None
    active_runtime = active_root.root_fiber.children[0].runtime
    assert active_runtime is not None
    assert active_runtime.workspace == tmp_path / "workspace"
    assert candidate.validation_workspace is None
    assert old_generation.instance.module.disposed is False
    await lease.release()
    await manager._snapshot_store.retry_drains()
    assert old_generation.instance.module.disposed is True

    await manager.terminate_all()


@pytest.mark.asyncio
async def test_direct_v3_invariant_failure_never_applies_to_formal_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin_dir = _write_plugin(
        tmp_path / "plugins",
        "isolated_reload",
        "api_version = 3\n"
        "name = 'isolated_reload'\n"
        "version = '1.0.0'\n"
        "async def apply(ctx, config):\n"
        "    pass\n",
    )
    manager = _manager(tmp_path)
    await manager.load_all()
    stable_snapshot = manager.current_snapshot
    assert stable_snapshot is not None

    (plugin_dir / "plugin.py").write_text(
        "from pathlib import Path\n"
        "api_version = 3\n"
        "name = 'isolated_reload'\n"
        "version = '2.0.0'\n"
        "async def apply(ctx, config):\n"
        "    Path(ctx.data_root, 'apply-probe').write_text('candidate')\n",
        encoding="utf-8",
    )
    candidate = await manager.prepare_candidate("isolated_reload")
    assert candidate is not None and candidate.validation_workspace is not None
    validation_root = candidate.validation_workspace.parent
    candidate_snapshot = candidate.runtime_snapshot
    assert candidate_snapshot is not None
    candidate_root = candidate_snapshot.composition_root
    assert candidate_root is not None
    candidate_runtime = candidate_root.root_fiber.children[0].runtime
    assert candidate_runtime is not None
    clone_modules = {
        module_name
        for module_name in sys.modules
        if module_name.startswith(f"{candidate.module_path}__candidate_")
    }
    assert clone_modules
    assert (candidate_runtime.data_dir / "apply-probe").is_file()
    first_attempt_root = candidate_runtime.workspace.parent

    original_invariants = manager._post_publish_invariants
    async def fail_invariant(*_args: object) -> None:
        raise RuntimeError("candidate invariant failed")

    monkeypatch.setattr(manager, "_post_publish_invariants", fail_invariant)
    with pytest.raises(RuntimeError, match="candidate invariant failed"):
        await manager.publish_prepared("isolated_reload")

    formal_probe = (
        tmp_path
        / "workspace"
        / "plugin-data"
        / "isolated_reload-builtin"
        / "apply-probe"
    )
    assert not formal_probe.exists()
    assert manager.current_snapshot is stable_snapshot
    assert manager.prepared_generation("isolated_reload") is None
    assert candidate.scope.closed is True
    assert clone_modules.isdisjoint(sys.modules)
    assert not validation_root.exists()
    assert not first_attempt_root.exists()

    monkeypatch.setattr(manager, "_post_publish_invariants", original_invariants)
    second = await manager.prepare_candidate("isolated_reload")
    assert second is not None and second.runtime_snapshot is not None
    second_root = second.runtime_snapshot.composition_root
    assert second_root is not None
    second_runtime = second_root.root_fiber.children[0].runtime
    assert second_runtime is not None
    second_attempt_root = second_runtime.workspace.parent
    second_clone_modules = {
        module_name
        for module_name in sys.modules
        if module_name.startswith(f"{second.module_path}__candidate_")
    }
    assert second_clone_modules

    published = await manager.publish_prepared("isolated_reload")

    assert published["publication_state"] == "committed"
    assert formal_probe.read_text(encoding="utf-8") == "candidate"
    assert second_clone_modules.isdisjoint(sys.modules)
    assert not second_attempt_root.exists()

    await manager.terminate_all()


@pytest.mark.asyncio
async def test_cancelled_candidate_mount_cleans_partial_clones_and_data(
    tmp_path: Path,
) -> None:
    _write_plugin(
        tmp_path / "plugins",
        "a_first",
        "api_version = 3\n"
        "name = 'a_first'\n"
        "version = '1.0.0'\n"
        "async def apply(ctx, config):\n"
        "    await ctx.effect(lambda: None, label='first')\n",
    )
    _write_plugin(
        tmp_path / "plugins",
        "z_blocker",
        "import asyncio\n"
        "api_version = 3\n"
        "name = 'z_blocker'\n"
        "version = '1.0.0'\n"
        "async def apply(ctx, config):\n"
        "    if 'plugin-validation' not in str(ctx.runtime.workspace):\n"
        "        return\n"
        "    (ctx.runtime.workspace / 'blocker-entered').write_text('ready')\n"
        "    await asyncio.Event().wait()\n",
    )
    manager = _manager(tmp_path)
    await manager.load_all()
    stable_snapshot = manager.current_snapshot
    assert stable_snapshot is not None
    stable_root = stable_snapshot.composition_root
    validation_base = tmp_path / "workspace" / "runtime" / "plugin-validation"

    preparing = asyncio.create_task(manager.prepare_candidate("a_first"))
    marker: Path | None = None
    for _ in range(200):
        markers = list(validation_base.rglob("blocker-entered"))
        if markers:
            marker = markers[0]
            break
        await asyncio.sleep(0.01)
    if marker is None:
        preparing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await preparing
        pytest.fail("second candidate Fiber did not enter apply")

    attempt_root = marker.parent.parent
    clone_modules = {
        module_name
        for module_name in sys.modules
        if "__candidate_" in module_name
    }
    assert len(clone_modules) == 2
    assert all(plugin_registry.get_instance(name) is not None for name in clone_modules)

    preparing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await preparing

    assert manager.current_snapshot is stable_snapshot
    assert manager.current_snapshot.composition_root is stable_root
    assert manager.prepared_generation("a_first") is None
    assert clone_modules.isdisjoint(sys.modules)
    assert all(plugin_registry.get_instance(name) is None for name in clone_modules)
    assert not attempt_root.exists()
    assert not validation_base.exists() or not any(validation_base.iterdir())

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
        "skill_roots = ('skills',)\n"
        "drift_skill_roots = ('drift/skills',)\n"
        "dashboard_module = 'dashboard.py'\n"
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
    for root, version in ((stable_root, "v1"), (latest_root, "v2")):
        skill_dir = root / "skills" / "installed-skill"
        skill_dir.mkdir(parents=True)
        (skill_dir / "SKILL.md").write_text(
            f"---\ndescription: installed {version}\n---\nbody {version}\n",
            encoding="utf-8",
        )
        drift_dir = root / "drift" / "skills" / "installed-drift"
        drift_dir.mkdir(parents=True)
        (drift_dir / "SKILL.md").write_text(
            f"---\ndescription: drift {version}\n---\ndrift {version}\n",
            encoding="utf-8",
        )
        (root / "dashboard.py").write_text(
            "def register(app, plugin_dir, workspace): return None\n",
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
    stable_snapshot = manager.current_snapshot
    assert stable is not None and stable_snapshot is not None
    assert stable_snapshot.plugin_skill_index is not None
    assert "body v1" in stable_snapshot.plugin_skill_index.get(
        "installed-skill"
    ).content  # type: ignore[union-attr]
    stable_lease = manager.snapshot_store.lease()

    write_pointers(plugin_base, stable=stable_pointer, latest=latest_pointer)
    result = (await manager.reconcile_changed())[0]
    candidate = manager.ready_candidate

    assert result["publication_state"] == "latest_ready"
    assert candidate is not None
    candidate_snapshot = candidate.runtime_snapshot
    assert candidate_snapshot is not None
    assert candidate_snapshot.plugin_skill_index is not None
    assert "body v2" in candidate_snapshot.plugin_skill_index.get(
        "installed-skill"
    ).content  # type: ignore[union-attr]
    assert candidate.contributions.dashboard_module == (
        latest_root / "dashboard.py"
    ).resolve()
    candidate_root = candidate_snapshot.composition_root
    stable_root_runtime = manager.current_snapshot.composition_root
    assert candidate_root is not None
    assert candidate_root is not stable_root_runtime
    candidate_runtime = candidate_root.root_fiber.children[0].runtime
    assert candidate_runtime is not None
    assert "plugin-validation" in str(candidate_runtime.workspace)
    assert candidate.validation_workspace is not None
    validation_root = candidate.validation_workspace.parent
    clone_modules = {
        module_name
        for module_name in sys.modules
        if module_name.startswith(f"{candidate.module_path}__candidate_")
    }
    assert clone_modules
    promoted = await manager.switch_ready("installed_v3@lab")

    assert promoted["publication_state"] == "promoted"
    promoted_snapshot = manager.current_snapshot
    assert promoted_snapshot is not None
    assert promoted_snapshot.plugin_skill_index is not None
    assert "body v2" in promoted_snapshot.plugin_skill_index.get(
        "installed-skill"
    ).content  # type: ignore[union-attr]
    promoted_catalog_id = promoted_snapshot.skill_catalog_generation_id
    assert promoted_catalog_id is not None
    promoted_catalog = manager._skill_host.get(promoted_catalog_id)
    assert promoted_catalog is not None
    assert "drift v2" in promoted_catalog.drift.get(
        "installed-drift"
    ).content  # type: ignore[union-attr]
    assert promoted_snapshot.generations[
        "installed_v3@lab"
    ].contributions.dashboard_module == (latest_root / "dashboard.py").resolve()
    assert candidate.instance.module.applied[-1] == str(tmp_path / "workspace")
    assert clone_modules.isdisjoint(sys.modules)
    assert not validation_root.exists()
    assert stable.instance.module.disposed == []
    await stable_lease.release()
    await manager.snapshot_store.retry_drains()
    assert stable.instance.module.disposed == [str(tmp_path / "workspace")]

    await manager.terminate_all()


@pytest.mark.asyncio
async def test_installed_v3_dashboard_uses_validation_workspace_until_promotion(
    tmp_path: Path,
) -> None:
    plugin_base = tmp_path / "home" / "cache" / "lab" / "dashboard_v3"
    stable_root = plugin_base / ".artifacts" / "1.0.0-aaaa"
    latest_root = plugin_base / ".artifacts" / "2.0.0-bbbb"
    stable_root.mkdir(parents=True)
    latest_root.mkdir(parents=True)
    source = (
        "api_version = 3\n"
        "name = 'dashboard_v3'\n"
        "version = '1.0.0'\n"
        "dashboard_module = 'dashboard.py'\n"
        "def apply(ctx, config): pass\n"
    )
    (stable_root / "plugin.py").write_text(source, encoding="utf-8")
    (latest_root / "plugin.py").write_text(
        source.replace("version = '1.0.0'", "version = '2.0.0'"),
        encoding="utf-8",
    )
    (stable_root / "dashboard.py").write_text(
        "def register(app, plugin_dir, workspace): return None\n",
        encoding="utf-8",
    )
    (latest_root / "dashboard.py").write_text(
        "def register(app, plugin_dir, workspace):\n"
        "    (workspace / 'dashboard-v2-registered').write_text('ready')\n"
        "    class Closeable:\n"
        "        def close(self):\n"
        "            (workspace / 'dashboard-v2-closed').write_text('closed')\n"
        "    return Closeable()\n",
        encoding="utf-8",
    )
    stable_pointer = ArtifactPointer(".artifacts/1.0.0-aaaa")
    latest_pointer = ArtifactPointer(".artifacts/2.0.0-bbbb")
    write_pointers(plugin_base, stable=stable_pointer, latest=stable_pointer)
    write_plugin_manifest(
        {"dashboard_v3@lab": True},
        plugins_home=tmp_path / "home",
    )
    manager = _manager(tmp_path)
    await manager.load_all()
    stable_snapshot = manager.current_snapshot
    assert stable_snapshot is not None
    dashboard_host = PluginDashboardHost(
        workspace=tmp_path / "workspace",
        memory_admin=object(),
        memory_store=object(),
        core_routes=(),
    )
    dashboard_host.prepare_initial_snapshot(stable_snapshot)
    manager.bind_dashboard_preparer(
        dashboard_host.prepare_snapshot,
        validation_releaser=dashboard_host.release_validation,
    )

    write_pointers(plugin_base, stable=stable_pointer, latest=latest_pointer)
    assert (await manager.reconcile_changed())[0]["publication_state"] == "latest_ready"
    candidate = manager.ready_candidate
    assert candidate is not None and candidate.runtime_snapshot is not None
    validation_workspace = candidate.validation_workspace
    assert validation_workspace is not None
    candidate_binding = candidate.runtime_snapshot.dashboard_bindings[0]
    assert isinstance(candidate_binding, DashboardBinding)
    assert candidate_binding.validation is True
    assert candidate_binding.runtime_workspace == validation_workspace.resolve()
    assert (validation_workspace / "dashboard-v2-registered").is_file()
    assert not (tmp_path / "workspace" / "dashboard-v2-registered").exists()
    validation_root = validation_workspace.parent
    validation_module = candidate_binding.module_name

    await manager.drop_candidate("dashboard_v3@lab")

    assert manager.current_snapshot is stable_snapshot
    assert not validation_root.exists()
    assert validation_module not in sys.modules
    assert not (tmp_path / "workspace" / "dashboard-v2-registered").exists()

    write_pointers(plugin_base, stable=stable_pointer, latest=latest_pointer)
    assert (await manager.reconcile_changed())[0]["publication_state"] == "latest_ready"
    promoted_candidate = manager.ready_candidate
    assert promoted_candidate is not None
    promoted_validation_workspace = promoted_candidate.validation_workspace
    assert promoted_validation_workspace is not None
    promoted_validation_root = promoted_validation_workspace.parent

    promoted = await manager.switch_ready("dashboard_v3@lab")

    assert promoted["publication_state"] == "promoted"
    current = manager.current_snapshot
    assert current is not None
    formal_binding = current.dashboard_bindings[0]
    assert isinstance(formal_binding, DashboardBinding)
    assert formal_binding.validation is False
    assert formal_binding.runtime_workspace == (tmp_path / "workspace").resolve()
    assert (tmp_path / "workspace" / "dashboard-v2-registered").is_file()
    assert not promoted_validation_root.exists()
    await manager.terminate_all()


@pytest.mark.asyncio
async def test_installed_v3_candidate_health_blocks_promotion_until_recovered(
    tmp_path: Path,
) -> None:
    plugin_base = tmp_path / "home" / "cache" / "lab" / "installed_v3"
    stable_artifact = plugin_base / ".artifacts" / "1.0.0-aaaa"
    latest_artifact = plugin_base / ".artifacts" / "2.0.0-bbbb"
    stable_artifact.mkdir(parents=True)
    latest_artifact.mkdir(parents=True)
    source = (
        "api_version = 3\n"
        "name = 'installed_v3'\n"
        "version = '1.0.0'\n"
        "health = None\n"
        "async def apply(ctx, config):\n"
        "    global health\n"
        "    health = await ctx.health('worker', required=True)\n"
    )
    (stable_artifact / "plugin.py").write_text(source, encoding="utf-8")
    (latest_artifact / "plugin.py").write_text(
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
    stable_snapshot = manager.current_snapshot
    assert stable_snapshot is not None

    write_pointers(plugin_base, stable=stable_pointer, latest=latest_pointer)
    assert (await manager.reconcile_changed())[0]["publication_state"] == "latest_ready"
    candidate = manager.ready_candidate
    assert candidate is not None and candidate.runtime_snapshot is not None
    candidate_root = candidate.runtime_snapshot.composition_root
    assert candidate_root is not None
    clone_name = next(
        name
        for name in sys.modules
        if name.startswith(f"{candidate.module_path}__candidate_")
    )
    candidate_health = sys.modules[clone_name].health
    candidate_health.degrade("validation worker unavailable")

    with pytest.raises(RuntimeError, match="required_degraded"):
        await manager.switch_ready("installed_v3@lab")

    assert manager.current_snapshot is stable_snapshot
    assert manager.ready_candidate is candidate
    assert candidate_root.root_fiber.children[0].state.value == "active"

    candidate_health.recover()
    promoted = await manager.switch_ready("installed_v3@lab")

    assert promoted["publication_state"] == "promoted"
    assert manager.ready_candidate is None
    assert clone_name not in sys.modules
    await manager.terminate_all()


@pytest.mark.asyncio
async def test_installed_v3_candidate_incident_overflow_blocks_promotion(
    tmp_path: Path,
) -> None:
    plugin_base = tmp_path / "home" / "cache" / "lab" / "installed_v3"
    stable_artifact = plugin_base / ".artifacts" / "1.0.0-aaaa"
    latest_artifact = plugin_base / ".artifacts" / "2.0.0-bbbb"
    stable_artifact.mkdir(parents=True)
    latest_artifact.mkdir(parents=True)
    source = (
        "api_version = 3\n"
        "name = 'installed_v3'\n"
        "version = '1.0.0'\n"
        "saved_ctx = None\n"
        "async def apply(ctx, config):\n"
        "    global saved_ctx\n"
        "    saved_ctx = ctx\n"
    )
    (stable_artifact / "plugin.py").write_text(source, encoding="utf-8")
    (latest_artifact / "plugin.py").write_text(
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
    stable_snapshot = manager.current_snapshot

    write_pointers(plugin_base, stable=stable_pointer, latest=latest_pointer)
    assert (await manager.reconcile_changed())[0]["publication_state"] == "latest_ready"
    candidate = manager.ready_candidate
    assert candidate is not None and candidate.runtime_snapshot is not None
    clone_name = next(
        name
        for name in sys.modules
        if name.startswith(f"{candidate.module_path}__candidate_")
    )
    candidate_context = sys.modules[clone_name].saved_ctx
    for index in range(1025):
        candidate_context.report_incident("probe", f"failure {index}")

    with pytest.raises(RuntimeError, match="incident_overflowed"):
        await manager.switch_ready("installed_v3@lab")

    assert manager.current_snapshot is stable_snapshot
    assert manager.ready_candidate is candidate
    dropped = await manager.drop_candidate("installed_v3@lab")
    assert dropped["publication_state"] == "discarded"
    assert clone_name not in sys.modules
    await manager.terminate_all()


@pytest.mark.asyncio
async def test_installed_v3_owner_commit_failure_discards_production_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin_base = tmp_path / "home" / "cache" / "lab" / "installed_v3"
    stable_artifact = plugin_base / ".artifacts" / "1.0.0-aaaa"
    latest_artifact = plugin_base / ".artifacts" / "2.0.0-bbbb"
    stable_artifact.mkdir(parents=True)
    latest_artifact.mkdir(parents=True)
    source = (
        "api_version = 3\n"
        "name = 'installed_v3'\n"
        "version = '1.0.0'\n"
        "disposed = False\n"
        "async def apply(ctx, config):\n"
        "    def cleanup():\n"
        "        global disposed\n"
        "        disposed = True\n"
        "    await ctx.effect(lambda: cleanup, label='runtime')\n"
    )
    (stable_artifact / "plugin.py").write_text(source, encoding="utf-8")
    (latest_artifact / "plugin.py").write_text(
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
    stable_snapshot = manager.current_snapshot
    assert stable is not None and stable_snapshot is not None

    write_pointers(plugin_base, stable=stable_pointer, latest=latest_pointer)
    assert (await manager.reconcile_changed())[0]["publication_state"] == "latest_ready"
    candidate = manager.ready_candidate
    assert candidate is not None and candidate.reload_tx_id is not None
    original_activate = manager._activate_published_generation

    def fail_owner_commit(*_args: object) -> None:
        raise RuntimeError("candidate owner commit failed")

    monkeypatch.setattr(manager, "_activate_published_generation", fail_owner_commit)
    with pytest.raises(RuntimeError, match="candidate owner commit failed"):
        await manager.switch_ready("installed_v3@lab")

    assert manager.current_snapshot is stable_snapshot
    assert manager.generation("installed_v3@lab") is stable
    assert manager.ready_candidate is None
    assert manager.latest_snapshot is stable_snapshot
    assert candidate.instance.module.disposed is True
    assert candidate.scope.closed is True
    assert read_pointer(plugin_base, "stable") == stable_pointer
    assert read_pointer(plugin_base, "latest") == stable_pointer
    assert stable.instance.module.disposed is False

    monkeypatch.setattr(manager, "_activate_published_generation", original_activate)
    await manager.terminate_all()


@pytest.mark.asyncio
async def test_v2_only_candidate_clones_stable_v3_root_and_data(
    tmp_path: Path,
) -> None:
    _write_plugin(
        tmp_path / "plugins",
        "stable_v3",
        "from pathlib import Path\n"
        "api_version = 3\n"
        "name = 'stable_v3'\n"
        "version = '1.0.0'\n"
        "applied = []\n"
        "health = None\n"
        "async def apply(ctx, config):\n"
        "    global health\n"
        "    applied.append(str(ctx.runtime.workspace))\n"
        "    health = await ctx.health('worker', required=True)\n"
        "    Path(ctx.data_root, 'composition-probe').write_text('ready')\n",
    )
    plugin_base = tmp_path / "home" / "cache" / "lab" / "legacy"
    stable_artifact = plugin_base / ".artifacts" / "1.0.0-aaaa"
    latest_artifact = plugin_base / ".artifacts" / "2.0.0-bbbb"
    stable_artifact.mkdir(parents=True)
    latest_artifact.mkdir(parents=True)
    legacy_source = (
        "from agent.plugins import Plugin\n"
        "class LegacyPlugin(Plugin):\n"
        "    name = 'legacy'\n"
        "    version = '1.0.0'\n"
    )
    (stable_artifact / "plugin.py").write_text(legacy_source, encoding="utf-8")
    (latest_artifact / "plugin.py").write_text(
        legacy_source.replace("1.0.0", "2.0.0"),
        encoding="utf-8",
    )
    stable_pointer = ArtifactPointer(".artifacts/1.0.0-aaaa")
    latest_pointer = ArtifactPointer(".artifacts/2.0.0-bbbb")
    write_pointers(plugin_base, stable=stable_pointer, latest=stable_pointer)
    write_plugin_manifest(
        {"legacy@lab": True},
        plugins_home=tmp_path / "home",
    )
    manager = _manager(tmp_path)
    await manager.load_all()
    stable_snapshot = manager.current_snapshot
    stable_v3 = manager.generation("stable_v3")
    assert stable_snapshot is not None and stable_v3 is not None
    stable_root = stable_snapshot.composition_root
    assert stable_root is not None
    assert stable_v3.instance.module.applied == [str(tmp_path / "workspace")]
    stable_v3.instance.module.health.degrade("stable worker unavailable")
    assert stable_root.receipt().required_degraded == ("stable_v3:worker",)

    write_pointers(plugin_base, stable=stable_pointer, latest=latest_pointer)
    result = (await manager.reconcile_changed())[0]
    candidate = manager.ready_candidate

    assert result["publication_state"] == "latest_ready"
    assert candidate is not None
    assert not isinstance(candidate.instance, ComposablePlugin)
    candidate_snapshot = candidate.runtime_snapshot
    assert candidate_snapshot is not None
    candidate_root = candidate_snapshot.composition_root
    assert candidate_root is not None
    assert candidate_root is not stable_root
    assert candidate_root.topology_identity() == stable_root.topology_identity()
    assert manager.current_snapshot is stable_snapshot
    candidate_runtime = candidate_root.root_fiber.children[0].runtime
    assert candidate_runtime is not None
    assert "plugin-validation" in str(candidate_runtime.workspace)
    assert candidate_runtime.data_dir != stable_v3.data_dir
    assert (candidate_runtime.data_dir / "composition-probe").read_text() == "ready"
    assert stable_v3.instance.module.applied == [str(tmp_path / "workspace")]
    clone_modules = {
        module_name
        for module_name in sys.modules
        if module_name.startswith(f"{stable_v3.module_path}__candidate_")
    }
    assert clone_modules
    attempt_root = candidate_runtime.workspace.parent

    dropped = await manager.drop_candidate("legacy@lab")

    assert dropped["publication_state"] == "discarded"
    assert clone_modules.isdisjoint(sys.modules)
    assert not attempt_root.exists()
    assert manager.current_snapshot is stable_snapshot
    assert manager.current_snapshot.composition_root is stable_root

    write_pointers(plugin_base, stable=stable_pointer, latest=latest_pointer)
    promoted_result = (await manager.reconcile_changed())[0]
    promoted_candidate = manager.ready_candidate
    assert promoted_result["publication_state"] == "latest_ready"
    assert promoted_candidate is not None
    promoted_snapshot = promoted_candidate.runtime_snapshot
    assert promoted_snapshot is not None
    promoted_root = promoted_snapshot.composition_root
    assert promoted_root is not None
    promoted_runtime = promoted_root.root_fiber.children[0].runtime
    assert promoted_runtime is not None
    promoted_attempt_root = promoted_runtime.workspace.parent
    promoted_clone_modules = {
        module_name
        for module_name in sys.modules
        if module_name.startswith(f"{stable_v3.module_path}__candidate_")
    }
    assert promoted_clone_modules

    promoted = await manager.switch_ready("legacy@lab")

    assert promoted["publication_state"] == "promoted"
    assert manager.current_snapshot is not stable_snapshot
    assert manager.current_snapshot.composition_root is stable_root
    assert stable_root.receipt().required_degraded == ("stable_v3:worker",)
    assert promoted_clone_modules.isdisjoint(sys.modules)
    assert not promoted_attempt_root.exists()

    await manager.terminate_all()
