from __future__ import annotations

from pathlib import Path

import pytest

from agent.plugin_composition import (
    MCP_SERVERS,
    CompositionError,
    CompositionRoot,
    EndpointEnv,
    McpServerDefinition,
    PluginRuntime,
)
from agent.plugin_composition.mcp_slots import (
    PluginMcpServers,
    _freeze_plugin_mcp_servers,
)
from agent.plugins.manager import PluginManager
from agent.plugins.registry import plugin_registry
from agent.plugins.snapshot import RuntimeSnapshot
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


def _runtime(plugin_dir: Path) -> PluginRuntime:
    return PluginRuntime(
        plugin_id=plugin_dir.name,
        plugin_dir=plugin_dir,
        data_dir=plugin_dir / "data",
        workspace=plugin_dir / "workspace",
        config=None,
    )


def _definition(*, candidate_backend: str = "recording") -> McpServerDefinition:
    return McpServerDefinition(
        name="calendar",
        command=("python", "mcp.py"),
        cwd=".",
        env={"MODE": "stdio"},
        required_tools=("get_events",),
        candidate_read_only_tools=("get_events",),
        endpoint_env=(EndpointEnv("PORT", "calendar_api"),),
        candidate_env={"CALENDAR_BACKEND": candidate_backend},
    )


def _plugin_dir(root: Path, name: str = "calendar") -> Path:
    plugin_dir = root / name
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "mcp.py").write_text("print('probe')\n", encoding="utf-8")
    return plugin_dir


@pytest.mark.asyncio
async def test_mcp_registry_freezes_descriptor_health_and_cleanup(
    tmp_path: Path,
) -> None:
    plugin_dir = _plugin_dir(tmp_path)
    root = CompositionRoot("mcp-registry")
    servers = PluginMcpServers(root.instance_token)
    _ = await root.context.provide(MCP_SERVERS, servers)

    async def apply(ctx) -> None:
        await ctx.require(MCP_SERVERS).register(ctx, _definition())

    fiber = await root.mount(
        apply,
        name="calendar",
        inject=(MCP_SERVERS,),
        runtime=_runtime(plugin_dir),
    )
    registry = _freeze_plugin_mcp_servers(servers, root.instance_token)
    binding = registry["calendar"]
    assert binding.descriptor.owner == "calendar"
    assert binding.descriptor.endpoint_env == (
        EndpointEnv("PORT", "calendar_api"),
    )
    assert binding.health.healthy
    assert binding.is_live()
    assert registry.identity == registry.catalog_digest

    await fiber.dispose()
    assert _freeze_plugin_mcp_servers(servers, root.instance_token) is registry
    assert not binding.is_live()
    assert root.receipt().effects == ("root:service:core.mcp_servers",)
    await root.dispose()


@pytest.mark.asyncio
async def test_mcp_registry_rejects_duplicate_frozen_and_reserved_env(
    tmp_path: Path,
) -> None:
    plugin_dir = _plugin_dir(tmp_path)
    root = CompositionRoot("mcp-invalid")
    servers = PluginMcpServers(root.instance_token)
    _ = await root.context.provide(MCP_SERVERS, servers)
    captured = None

    async def apply(ctx) -> None:
        nonlocal captured
        captured = ctx
        await ctx.require(MCP_SERVERS).register(ctx, _definition())

    _ = await root.mount(
        apply,
        name="calendar",
        inject=(MCP_SERVERS,),
        runtime=_runtime(plugin_dir),
    )
    _ = _freeze_plugin_mcp_servers(servers, root.instance_token)
    assert captured is not None
    with pytest.raises(CompositionError, match="已冻结"):
        await servers.register(captured, _definition())
    await root.dispose()

    root = CompositionRoot("mcp-reserved")
    servers = PluginMcpServers(root.instance_token)
    _ = await root.context.provide(MCP_SERVERS, servers)

    async def reserved(ctx) -> None:
        definition = McpServerDefinition(
            name="calendar",
            command=("python",),
            env={"AKASHIC_WORKSPACE": "/tmp/escape"},
        )
        await ctx.require(MCP_SERVERS).register(ctx, definition)

    _ = await root.mount(
        reserved,
        name="calendar",
        inject=(MCP_SERVERS,),
        runtime=_runtime(plugin_dir),
    )
    assert not root.receipt().ready
    assert any(
        "env 无效" in (fiber.error or "") for fiber in root.receipt().fibers
    )
    assert len(_freeze_plugin_mcp_servers(servers, root.instance_token)) == 0
    await root.dispose()


@pytest.mark.asyncio
async def test_mcp_registry_identity_ignores_runtime_root(tmp_path: Path) -> None:
    identities: list[str] = []
    for suffix in ("candidate", "formal"):
        plugin_dir = _plugin_dir(tmp_path / suffix)
        root = CompositionRoot(f"mcp-{suffix}")
        servers = PluginMcpServers(root.instance_token)
        _ = await root.context.provide(MCP_SERVERS, servers)

        async def apply(ctx) -> None:
            await ctx.require(MCP_SERVERS).register(ctx, _definition())

        _ = await root.mount(
            apply,
            name="calendar",
            inject=(MCP_SERVERS,),
            runtime=_runtime(plugin_dir),
        )
        identities.append(
            _freeze_plugin_mcp_servers(servers, root.instance_token).identity
        )
        await root.dispose()
    assert identities[0] == identities[1]


@pytest.mark.asyncio
async def test_mcp_registry_rejects_missing_command_and_escaped_cwd(
    tmp_path: Path,
) -> None:
    plugin_dir = _plugin_dir(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (plugin_dir / "outside-link").symlink_to(outside, target_is_directory=True)
    definitions = (
        McpServerDefinition(name="missing", command=("python", "missing.py")),
        McpServerDefinition(name="escaped", command=("python",), cwd="outside-link"),
    )
    for index, definition in enumerate(definitions):
        root = CompositionRoot(f"mcp-path-{index}")
        servers = PluginMcpServers(root.instance_token)
        _ = await root.context.provide(MCP_SERVERS, servers)

        async def apply(ctx, definition=definition) -> None:
            await ctx.require(MCP_SERVERS).register(ctx, definition)

        _ = await root.mount(
            apply,
            name="calendar",
            inject=(MCP_SERVERS,),
            runtime=_runtime(plugin_dir),
        )
        assert not root.receipt().ready
        assert (
            len(_freeze_plugin_mcp_servers(servers, root.instance_token)) == 0
        )
        await root.dispose()


@pytest.mark.asyncio
async def test_plugins_cannot_freeze_shared_mcp_declarations_early(
    tmp_path: Path,
) -> None:
    root = CompositionRoot("mcp-core-freeze-owner")
    servers = PluginMcpServers(root.instance_token)
    _ = await root.context.provide(MCP_SERVERS, servers)

    definitions = (
        _definition(),
        McpServerDefinition(name="contacts", command=("python", "mcp.py")),
    )
    for name, definition in zip(("calendar", "contacts"), definitions, strict=True):
        plugin_dir = _plugin_dir(tmp_path, name)

        async def apply(ctx, definition=definition, name=name) -> None:
            service = ctx.require(MCP_SERVERS)
            if name == "calendar":
                with pytest.raises(AttributeError):
                    _ = getattr(service, "freeze")
            await service.register(ctx, definition)

        fiber = await root.mount(
            apply,
            name=name,
            inject=(MCP_SERVERS,),
            runtime=_runtime(plugin_dir),
        )
        assert fiber.state.value == "active"

    frozen = _freeze_plugin_mcp_servers(servers, root.instance_token)
    assert tuple(frozen) == ("calendar", "contacts")
    await root.dispose()


@pytest.mark.asyncio
async def test_mcp_registry_rejects_context_from_another_root(
    tmp_path: Path,
) -> None:
    root_a = CompositionRoot("mcp-root-a")
    root_b = CompositionRoot("mcp-root-b")
    servers_a = PluginMcpServers(root_a.instance_token)
    servers_b = PluginMcpServers(root_b.instance_token)
    _ = await root_a.context.provide(MCP_SERVERS, servers_a)
    _ = await root_b.context.provide(MCP_SERVERS, servers_b)
    plugin_dir = _plugin_dir(tmp_path)

    async def apply(ctx) -> None:
        await servers_a.register(ctx, _definition())

    _ = await root_b.mount(
        apply,
        name="calendar",
        inject=(MCP_SERVERS,),
        runtime=_runtime(plugin_dir),
    )

    assert any(
        "插件 MCP 声明 Service 不属于当前 Root" in (fiber.error or "")
        for fiber in root_b.receipt().fibers
    )
    assert root_a.receipt().health == ()
    assert root_b.receipt().health == ()
    assert root_a.receipt().effects == ("root:service:core.mcp_servers",)
    assert root_b.receipt().effects == ("root:service:core.mcp_servers",)
    assert len(_freeze_plugin_mcp_servers(servers_a, root_a.instance_token)) == 0
    assert len(_freeze_plugin_mcp_servers(servers_b, root_b.instance_token)) == 0

    await root_b.dispose()
    await root_a.dispose()


def _manager(tmp_path: Path) -> PluginManager:
    return PluginManager(
        plugin_dirs=[tmp_path / "plugins"],
        event_bus=EventBus(),
        tool_registry=None,
        workspace=tmp_path / "workspace",
        installed_cache_root=tmp_path / "home" / "cache",
    )


def _plugin_source(version: str) -> str:
    return (
        "from agent.plugin_composition import (\n"
        "    MANAGED_PROCESSES, MCP_SERVERS, EndpointEnv,\n"
        "    ManagedProcessDefinition, McpServerDefinition,\n"
        ")\n"
        "api_version = 3\n"
        "name = 'calendar'\n"
        f"version = '{version}'\n"
        "inject = (MCP_SERVERS, MANAGED_PROCESSES)\n"
        "async def apply(ctx, config):\n"
        "    await ctx.require(MANAGED_PROCESSES).register(\n"
        "        ctx, ManagedProcessDefinition(\n"
        "            name='calendar_api', command=('python', 'api.py'),\n"
        "            formal_port=18000, readiness_path='/health',\n"
        "        ),\n"
        "    )\n"
        "    await ctx.require(MCP_SERVERS).register(\n"
        "        ctx, McpServerDefinition(\n"
        "            name='calendar', command=('python', 'mcp.py'),\n"
        "            required_tools=('get_events',),\n"
        "            candidate_read_only_tools=('get_events',),\n"
        "            endpoint_env=(EndpointEnv('PORT', 'calendar_api'),),\n"
        f"            candidate_env={{'VERSION': '{version}'}},\n"
        "        ),\n"
        "    )\n"
    )


def _write_manager_plugin(tmp_path: Path, version: str) -> Path:
    plugin_dir = _plugin_dir(tmp_path / "plugins")
    (plugin_dir / "plugin.py").write_text(
        _plugin_source(version),
        encoding="utf-8",
    )
    (plugin_dir / "api.py").write_text("print('api')\n", encoding="utf-8")
    return plugin_dir


def _write_static_manager_plugin(tmp_path: Path, version: str) -> Path:
    plugin_dir = _plugin_dir(tmp_path / "plugins")
    (plugin_dir / "entry.py").write_text(
        _plugin_source(version),
        encoding="utf-8",
    )
    (plugin_dir / "api.py").write_text("print('api')\n", encoding="utf-8")
    (plugin_dir / "requirements.txt").write_text("", encoding="utf-8")
    interpreter = plugin_dir / ".venv" / "bin" / "python"
    interpreter.parent.mkdir(parents=True)
    interpreter.write_text("", encoding="utf-8")
    interpreter.chmod(0o755)
    (plugin_dir / "akashic.plugin.toml").write_text(
        "schema_version = 1\n"
        "name = \"calendar\"\n"
        f"version = \"{version}\"\n"
        "api_version = 3\n"
        "entrypoint = \"entry.py\"\n\n"
        "[[python]]\n"
        "requirements = \"requirements.txt\"\n\n"
        "[[mcp]]\n"
        "name = \"calendar\"\n"
        "command = [\"python\", \"mcp.py\"]\n"
        "required_tools = [\"get_events\"]\n"
        "candidate_read_only_tools = [\"get_events\"]\n"
        "endpoint_env = [{env = \"PORT\", process = \"calendar_api\"}]\n"
        f"candidate_env = {{VERSION = \"{version}\"}}\n\n"
        "[[process]]\n"
        "name = \"calendar_api\"\n"
        "command = [\"python\", \"api.py\"]\n"
        "port_env = \"PORT\"\n"
        "formal_port = 18000\n"
        "readiness_path = \"/health\"\n",
        encoding="utf-8",
    )
    return plugin_dir


@pytest.mark.asyncio
async def test_static_manifest_is_admission_source_and_reconciles_mcp_root(
    tmp_path: Path,
) -> None:
    plugin_dir = _write_static_manager_plugin(tmp_path, "1")
    manager = _manager(tmp_path)

    await manager.load_all()
    generation = manager._active_generations["calendar"]  # pyright: ignore[reportPrivateUsage]
    assert generation.entrypoint == "entry.py"
    assert generation.static_manifest is not None
    assert generation.static_manifest.mcp_servers[0].name == "calendar"
    assert generation.static_manifest.managed_processes[0].name == "calendar_api"
    runtime_commands = dict(generation.static_runtime_commands)
    assert runtime_commands["mcp:calendar"][0] == str(
        plugin_dir / ".venv" / "bin" / "python"
    )
    assert runtime_commands["process:calendar_api"][0] == str(
        plugin_dir / ".venv" / "bin" / "python"
    )
    assert manager.current_snapshot is not None
    assert manager.current_snapshot.mcp_server_registry is not None
    assert manager.current_snapshot.managed_process_registry is not None

    (plugin_dir / "entry.py").write_text(
        _plugin_source("2"),
        encoding="utf-8",
    )
    (plugin_dir / "akashic.plugin.toml").write_text(
        (plugin_dir / "akashic.plugin.toml")
        .read_text(encoding="utf-8")
        .replace('version = "1"', 'version = "2"')
        .replace('VERSION = "1"', 'VERSION = "2"'),
        encoding="utf-8",
    )
    candidate = await manager.prepare_candidate("calendar")
    assert candidate is not None
    assert candidate.entrypoint == "entry.py"
    assert candidate.static_manifest is not None
    await manager.discard_prepared("calendar")
    await manager.terminate_all()


@pytest.mark.asyncio
async def test_static_candidate_without_formal_data_leaves_no_formal_directory(
    tmp_path: Path,
) -> None:
    _ = _write_static_manager_plugin(tmp_path, "1")
    manager = _manager(tmp_path)
    formal_data = tmp_path / "workspace" / "plugin-data" / "calendar-builtin"

    candidate = await manager.prepare_candidate("calendar")

    assert candidate is not None
    assert candidate.validation_workspace is not None
    assert candidate.runtime_snapshot is not None
    candidate_root = candidate.runtime_snapshot.composition_root
    assert candidate_root is not None
    runtime = candidate_root.root_fiber.children[0].runtime
    assert runtime is not None and runtime.data_dir.is_dir()
    assert candidate.validation_data_inventory == ()
    assert not formal_data.exists()

    await manager.discard_prepared("calendar")
    assert not formal_data.exists()
    assert not candidate.validation_workspace.parent.exists()
    await manager.terminate_all()


@pytest.mark.asyncio
async def test_static_first_publication_rolls_back_new_formal_data_on_commit_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = _write_static_manager_plugin(tmp_path, "1")
    manager = _manager(tmp_path)
    formal_data = tmp_path / "workspace" / "plugin-data" / "calendar-builtin"
    candidate = await manager.prepare_candidate("calendar")
    assert candidate is not None

    def fail_owner_commit(*_args: object) -> None:
        raise RuntimeError("owner commit failed")

    monkeypatch.setattr(
        manager,
        "_activate_published_generation",
        fail_owner_commit,
    )
    with pytest.raises(RuntimeError, match="owner commit failed"):
        await manager.publish_prepared("calendar")

    assert manager.current_snapshot is None
    assert not formal_data.exists()
    assert candidate.scope.closed is True

    monkeypatch.undo()
    replacement = await manager.prepare_candidate("calendar")
    assert replacement is not None
    result = await manager.publish_prepared("calendar")
    assert result["publication_state"] == "committed"
    assert formal_data.is_dir()
    await manager.terminate_all()


@pytest.mark.asyncio
async def test_static_manifest_declarations_do_not_activate_inactive_plugin(
    tmp_path: Path,
) -> None:
    plugin_dir = _write_static_manager_plugin(tmp_path, "1")
    entrypoint = plugin_dir / "entry.py"
    entrypoint.write_text(
        entrypoint.read_text(encoding="utf-8")
        + "\ndef is_active(services):\n"
        + "    return False\n",
        encoding="utf-8",
    )
    manager = _manager(tmp_path)

    await manager.load_all()

    snapshot = manager.current_snapshot
    assert snapshot is not None
    assert snapshot.composition_active_plugin_ids == frozenset()
    assert snapshot.mcp_server_registry is not None
    assert len(snapshot.mcp_server_registry) == 0
    assert snapshot.managed_process_registry is not None
    assert len(snapshot.managed_process_registry) == 0
    await manager.terminate_all()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("old", "new", "message"),
    (
        ('required_tools = ["get_events"]', 'required_tools = ["other"]', "MCP 声明"),
        ("formal_port = 18000", "formal_port = 18001", "managed process 声明"),
    ),
)
async def test_static_manifest_runtime_drift_excludes_failed_stable_plugin(
    tmp_path: Path,
    old: str,
    new: str,
    message: str,
) -> None:
    plugin_dir = _write_static_manager_plugin(tmp_path, "1")
    manifest = plugin_dir / "akashic.plugin.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace(old, new),
        encoding="utf-8",
    )
    manager = _manager(tmp_path)

    await manager.load_all()

    assert manager.current_snapshot is None
    assert manager._active_generations == {}  # pyright: ignore[reportPrivateUsage]
    gate = manager.latest_gate("calendar")
    assert gate is not None and gate.status == "failed"
    assert message in str(gate.checks[-1].evidence)
    await manager.terminate_all()


@pytest.mark.asyncio
async def test_manager_keeps_candidate_mcp_registry_private_until_publish(
    tmp_path: Path,
) -> None:
    plugin_dir = _write_manager_plugin(tmp_path, "1")
    manager = _manager(tmp_path)
    await manager.load_all()
    stable = manager.current_snapshot
    assert stable is not None and stable.mcp_server_registry is not None
    stable_registry = stable.mcp_server_registry
    assert stable_registry["calendar"].definition.candidate_env["VERSION"] == "1"

    (plugin_dir / "plugin.py").write_text(_plugin_source("2"), encoding="utf-8")
    candidate = await manager.prepare_candidate("calendar")
    assert candidate is not None and candidate.runtime_snapshot is not None
    candidate_registry = candidate.runtime_snapshot.mcp_server_registry
    assert candidate_registry is not None
    assert candidate_registry["calendar"].definition.candidate_env["VERSION"] == "2"
    assert manager.current_snapshot is stable
    assert manager.current_snapshot.mcp_server_registry is stable_registry

    result = await manager.publish_prepared("calendar")
    assert result["publication_state"] == "committed"
    current = manager.current_snapshot
    assert current is not None and current.mcp_server_registry is not None
    assert current.mcp_server_registry is not candidate_registry
    assert current.mcp_server_registry["calendar"].definition.candidate_env["VERSION"] == "2"
    await manager.terminate_all()


@pytest.mark.asyncio
async def test_manager_rejects_mcp_registry_drift_before_publish(
    tmp_path: Path,
) -> None:
    plugin_dir = _write_manager_plugin(tmp_path, "1")
    manager = _manager(tmp_path)
    await manager.load_all()
    stable = manager.current_snapshot
    assert stable is not None and stable.mcp_server_registry is not None
    stable_registry = stable.mcp_server_registry

    (plugin_dir / "plugin.py").write_text(_plugin_source("2"), encoding="utf-8")
    candidate = await manager.prepare_candidate("calendar")
    assert candidate is not None and candidate.runtime_snapshot is not None

    def replace_with_stable_registry(snapshot: RuntimeSnapshot) -> None:
        snapshot.mcp_server_registry = stable_registry
        snapshot.mcp_server_registry_identity = stable_registry.identity

    async def release_validation(_snapshot: RuntimeSnapshot) -> None:
        return None

    manager.bind_dashboard_preparer(
        replace_with_stable_registry,
        validation_releaser=release_validation,
    )
    with pytest.raises(RuntimeError, match="MCP registry"):
        await manager.publish_prepared("calendar")
    assert manager.current_snapshot is stable
    assert manager.current_snapshot.mcp_server_registry is stable_registry

    await manager.discard_prepared("calendar")
    await manager.terminate_all()


@pytest.mark.asyncio
async def test_manager_rejects_mcp_endpoint_without_same_owner_process(
    tmp_path: Path,
) -> None:
    plugin_dir = _plugin_dir(tmp_path / "plugins")
    (plugin_dir / "plugin.py").write_text(
        "from agent.plugin_composition import (\n"
        "    MCP_SERVERS, EndpointEnv, McpServerDefinition,\n"
        ")\n"
        "api_version = 3\n"
        "name = 'calendar'\n"
        "version = '1'\n"
        "inject = (MCP_SERVERS,)\n"
        "async def apply(ctx, config):\n"
        "    await ctx.require(MCP_SERVERS).register(\n"
        "        ctx, McpServerDefinition(\n"
        "            name='calendar', command=('python', 'mcp.py'),\n"
        "            endpoint_env=(EndpointEnv('PORT', 'calendar_api'),),\n"
        "        ),\n"
        "    )\n",
        encoding="utf-8",
    )
    manager = _manager(tmp_path)

    await manager.load_all()

    assert manager.generation("calendar") is None
    gate = manager.latest_gate("calendar")
    assert gate is not None and gate.status == "failed"
    assert "缺少同 owner managed process" in gate.failure_reason
    await manager.terminate_all()
