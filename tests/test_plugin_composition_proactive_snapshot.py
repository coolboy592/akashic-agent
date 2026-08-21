from pathlib import Path
import pytest

from agent.plugin_composition import (
    MCP_SERVERS,
    PROACTIVE_COMPONENTS,
    CompositionRoot,
    McpServerDefinition,
    PluginProactiveComponents,
    PluginRuntime,
    ProactiveModuleDefinition,
    ProactiveSourceDefinition,
)
from agent.plugin_composition.mcp_slots import PluginMcpServers
from agent.plugins.generation import GateResult, PluginContributions, PluginGeneration
from agent.plugins.generation_activity_host import ActivityHost
from agent.plugins.generation_proactive_host import ProactiveActivityAdapter
from agent.plugins.manager import PluginManager
from agent.plugins.scope import PluginScope
from agent.plugins.snapshot import RuntimeSnapshotCompiler, RuntimeSnapshotStore
from bus.event_bus import EventBus


def _generation(plugin_dir: Path) -> PluginGeneration:
    return PluginGeneration(
        plugin_id="calendar",
        generation_id="calendar:test",
        module_path="plugins.calendar",
        source_revision="source",
        config_revision="config",
        plugin_dir=plugin_dir,
        data_dir=plugin_dir / "data",
        config=None,
        instance=object(),
        scope=PluginScope("calendar"),
        contributions=PluginContributions(manifest={}),
        gate_result=GateResult(
            gate_id="gate",
            plugin_id="calendar",
            candidate_revision="source",
            status="passed",
            checks=(),
        ),
    )


@pytest.mark.asyncio
async def test_snapshot_freezes_exact_proactive_catalog_without_execution(
    tmp_path: Path,
) -> None:
    plugin_dir = tmp_path / "calendar"
    plugin_dir.mkdir()
    (plugin_dir / "mcp.py").write_text("print('probe')\n", encoding="utf-8")
    root = CompositionRoot("calendar:test")
    servers = PluginMcpServers(root.instance_token)
    components = PluginProactiveComponents(root.instance_token)
    _ = await root.context.provide(MCP_SERVERS, servers)
    _ = await root.context.provide(PROACTIVE_COMPONENTS, components)
    async def apply(ctx) -> None:
        await ctx.require(MCP_SERVERS).register(
            ctx,
            McpServerDefinition(
                name="calendar",
                command=("python", "mcp.py"),
                required_tools=("fetch_events", "ack_events"),
                candidate_read_only_tools=("fetch_events",),
            ),
        )
        await ctx.require(PROACTIVE_COMPONENTS).register(
            ctx,
            ProactiveSourceDefinition(
                name="calendar",
                channels=("alert",),
                mcp_server="calendar",
                fetch_tool="fetch_events",
                ack_tool="ack_events",
            ),
        )
        await ctx.require(PROACTIVE_COMPONENTS).register(
            ctx,
            ProactiveModuleDefinition(
                slot="proactive.calendar",
                lifecycle_id="default.proactive.frame.v1",
                produces=("calendar.alerts",),
                handler_export="runtime.handle_calendar",
            ),
        )

    _ = await root.mount(
        apply,
        name="calendar",
        inject=(MCP_SERVERS, PROACTIVE_COMPONENTS),
        runtime=PluginRuntime(
            plugin_id="calendar",
            plugin_dir=plugin_dir,
            data_dir=plugin_dir / "data",
            workspace=plugin_dir / "workspace",
            config=None,
        ),
    )
    generation = _generation(plugin_dir)
    snapshot = RuntimeSnapshotCompiler().compile(
        {generation.plugin_id: generation},
        composition_root=root,
    )

    catalog = snapshot.proactive_component_catalog
    assert catalog is not None
    assert catalog.source("calendar") is not None
    assert catalog.module("proactive.calendar") is not None
    assert snapshot.proactive_component_catalog_identity == catalog.identity
    assert catalog.root_instance_token is root.instance_token
    store = RuntimeSnapshotStore()
    store.install(snapshot)
    await store.close()


@pytest.mark.asyncio
async def test_snapshot_rejects_proactive_ack_in_candidate_allowlist(
    tmp_path: Path,
) -> None:
    plugin_dir = tmp_path / "calendar"
    plugin_dir.mkdir()
    (plugin_dir / "mcp.py").write_text("print('probe')\n", encoding="utf-8")
    root = CompositionRoot("calendar:test")
    servers = PluginMcpServers(root.instance_token)
    components = PluginProactiveComponents(root.instance_token)
    _ = await root.context.provide(MCP_SERVERS, servers)
    _ = await root.context.provide(PROACTIVE_COMPONENTS, components)

    async def apply(ctx) -> None:
        await ctx.require(MCP_SERVERS).register(
            ctx,
            McpServerDefinition(
                name="calendar",
                command=("python", "mcp.py"),
                required_tools=("fetch_events", "ack_events"),
                candidate_read_only_tools=("fetch_events", "ack_events"),
            ),
        )
        await ctx.require(PROACTIVE_COMPONENTS).register(
            ctx,
            ProactiveSourceDefinition(
                name="calendar",
                channels=("alert",),
                mcp_server="calendar",
                fetch_tool="fetch_events",
                ack_tool="ack_events",
            ),
        )

    _ = await root.mount(
        apply,
        name="calendar",
        inject=(MCP_SERVERS, PROACTIVE_COMPONENTS),
        runtime=PluginRuntime(
            plugin_id="calendar",
            plugin_dir=plugin_dir,
            data_dir=plugin_dir / "data",
            workspace=plugin_dir / "workspace",
            config=None,
        ),
    )

    with pytest.raises(RuntimeError, match="ack tool 不得进入 candidate allowlist"):
        RuntimeSnapshotCompiler().compile(
            {"calendar": _generation(plugin_dir)},
            composition_root=root,
        )
    await root.dispose()


async def _module_root(plugin_dir: Path) -> CompositionRoot:
    root = CompositionRoot("calendar:test")
    components = PluginProactiveComponents(root.instance_token)
    _ = await root.context.provide(PROACTIVE_COMPONENTS, components)

    async def apply(ctx) -> None:
        await ctx.require(PROACTIVE_COMPONENTS).register(
            ctx,
            ProactiveModuleDefinition(
                slot="proactive.calendar",
                lifecycle_id="default.proactive.frame.v1",
                produces=("calendar.alerts",),
                handler_export="runtime.handle_calendar",
            ),
        )

    _ = await root.mount(
        apply,
        name="calendar",
        inject=(PROACTIVE_COMPONENTS,),
        runtime=PluginRuntime(
            plugin_id="calendar",
            plugin_dir=plugin_dir,
            data_dir=plugin_dir / "data",
            workspace=plugin_dir / "workspace",
            config=None,
        ),
    )
    return root


@pytest.mark.asyncio
async def test_manager_provides_and_compiles_proactive_service(
    tmp_path: Path,
) -> None:
    plugin_dir = tmp_path / "plugins" / "calendar"
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "plugin.py").write_text(
        "from agent.plugin_composition import (\n"
        "    PROACTIVE_COMPONENTS, ProactiveModuleDefinition,\n"
        ")\n"
        "api_version = 3\n"
        "name = 'calendar'\n"
        "version = '1.0.0'\n"
        "inject = (PROACTIVE_COMPONENTS,)\n"
        "async def apply(ctx, config):\n"
        "    await ctx.require(PROACTIVE_COMPONENTS).register(ctx,\n"
        "        ProactiveModuleDefinition(\n"
        "            slot='proactive.calendar',\n"
        "            lifecycle_id='default.proactive.frame.v1',\n"
        "            handler_export='handle_calendar',\n"
        "        )\n"
        "    )\n"
        "async def handle_calendar(ctx, frame):\n"
        "    return frame\n",
        encoding="utf-8",
    )
    manager = PluginManager(
        plugin_dirs=[tmp_path / "plugins"],
        event_bus=EventBus(),
        tool_registry=None,
        workspace=tmp_path / "workspace",
        installed_cache_root=tmp_path / "cache",
    )
    manager.bind_activity_host(ActivityHost((ProactiveActivityAdapter(),)))

    await manager.load_all()

    snapshot = manager.current_snapshot
    assert snapshot is not None
    catalog = snapshot.proactive_component_catalog
    assert catalog is not None
    binding = catalog.module("proactive.calendar")
    assert binding is not None
    assert binding.generation_id == manager.generation("calendar").generation_id
    await manager.terminate_all()
