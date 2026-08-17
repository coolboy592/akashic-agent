from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from agent.plugin_composition import CompositionRoot, PluginRuntime
from agent.plugin_composition.proactive import (
    AckCommitted,
    AckFailure,
    AckSkipped,
    FetchEmpty,
    FetchFailure,
    FetchItems,
    FetchSkip,
    PROACTIVE_COMPONENTS,
    PluginProactiveComponents,
    ProactiveModuleDefinition,
    ProactiveSourceDefinition,
    _freeze_plugin_proactive_components,
)
from agent.plugin_composition.model import CompositionError


def _runtime(tmp_path: Path, plugin_id: str = "calendar") -> PluginRuntime:
    plugin_dir = tmp_path / plugin_id
    plugin_dir.mkdir(parents=True)
    return PluginRuntime(
        plugin_id=plugin_id,
        plugin_dir=plugin_dir,
        data_dir=tmp_path / "data" / plugin_id,
        workspace=tmp_path / "workspace",
        config=None,
    )


def _source(name: str = "upcoming_events") -> ProactiveSourceDefinition:
    return ProactiveSourceDefinition(
        name=name,
        channels=("alert", "content"),
        mcp_server="calendar",
        fetch_tool="get_proactive_events",
        ack_tool="acknowledge_events",
        fetch_page_size=50,
    )


def _module(slot: str = "proactive.gate.daynight") -> ProactiveModuleDefinition:
    return ProactiveModuleDefinition(
        slot=slot,
        lifecycle_id="default.proactive.frame.v1",
        requires=("proactive:input",),
        produces=("proactive:gate:pass_probability",),
        collects=("proactive:signal",),
        handler_export="apply_daynight_gate",
    )


@pytest.mark.asyncio
async def test_proactive_registry_freezes_source_module_health_and_live_fence(
    tmp_path: Path,
) -> None:
    root = CompositionRoot("proactive-registry")
    service = PluginProactiveComponents(root.instance_token)
    _ = await root.context.provide(PROACTIVE_COMPONENTS, service)

    async def apply(ctx) -> None:
        facade = ctx.require(PROACTIVE_COMPONENTS)
        await facade.register(ctx, _source())
        await facade.register(ctx, _module())

    fiber = await root.mount(
        apply,
        name="calendar",
        inject=(PROACTIVE_COMPONENTS,),
        runtime=_runtime(tmp_path),
    )
    catalog = _freeze_plugin_proactive_components(service, root.instance_token)
    assert catalog.source("upcoming_events") is not None
    assert catalog.module("proactive.gate.daynight") is not None
    source = catalog.source("calendar:upcoming_events")
    module = catalog.module("calendar:proactive.gate.daynight")
    assert source is not None and module is not None
    assert source.generation_id == "proactive-registry"
    assert module.generation_id == "proactive-registry"
    assert source.is_live() and module.is_live()
    assert source.descriptor.owner == "calendar"
    assert module.descriptor.handler_export == "apply_daynight_gate"
    assert catalog.identity == catalog.catalog_digest
    assert _freeze_plugin_proactive_components(service, root.instance_token) is catalog

    await fiber.dispose()
    assert not source.is_live() and not module.is_live()
    assert _freeze_plugin_proactive_components(service, root.instance_token) is catalog
    await root.dispose()


@pytest.mark.asyncio
async def test_proactive_catalog_identity_ignores_root_and_runtime_paths(
    tmp_path: Path,
) -> None:
    identities: list[str] = []
    for suffix in ("candidate", "formal"):
        root = CompositionRoot(f"proactive-{suffix}")
        service = PluginProactiveComponents(root.instance_token)
        _ = await root.context.provide(PROACTIVE_COMPONENTS, service)

        async def apply(ctx) -> None:
            await ctx.require(PROACTIVE_COMPONENTS).register(ctx, _source())
            await ctx.require(PROACTIVE_COMPONENTS).register(ctx, _module())

        _ = await root.mount(
            apply,
            name="calendar",
            inject=(PROACTIVE_COMPONENTS,),
            runtime=_runtime(tmp_path / suffix),
        )
        identities.append(
            _freeze_plugin_proactive_components(service, root.instance_token).identity
        )
        await root.dispose()
    assert identities[0] == identities[1]


@pytest.mark.asyncio
async def test_proactive_candidate_freeze_has_no_execution_surface(
    tmp_path: Path,
) -> None:
    root = CompositionRoot("proactive-candidate")
    service = PluginProactiveComponents(root.instance_token)
    _ = await root.context.provide(PROACTIVE_COMPONENTS, service)
    calls: list[str] = []

    async def apply(ctx) -> None:
        await ctx.require(PROACTIVE_COMPONENTS).register(ctx, _source())
        await ctx.require(PROACTIVE_COMPONENTS).register(ctx, _module())

    _ = await root.mount(
        apply,
        name="calendar",
        inject=(PROACTIVE_COMPONENTS,),
        runtime=_runtime(tmp_path),
    )
    catalog = _freeze_plugin_proactive_components(service, root.instance_token)
    calls.append(catalog.identity)
    assert len(calls) == 1
    assert catalog.source_descriptors[0].fetch_tool == "get_proactive_events"
    assert catalog.module_descriptors[0].handler_export == "apply_daynight_gate"
    await root.dispose()


@pytest.mark.asyncio
async def test_proactive_rejects_duplicate_and_mutation_after_freeze(
    tmp_path: Path,
) -> None:
    root = CompositionRoot("proactive-freeze")
    service = PluginProactiveComponents(root.instance_token)
    _ = await root.context.provide(PROACTIVE_COMPONENTS, service)
    captured = None

    async def apply(ctx) -> None:
        nonlocal captured
        captured = ctx
        await ctx.require(PROACTIVE_COMPONENTS).register(ctx, _source())

    _ = await root.mount(
        apply,
        name="calendar",
        inject=(PROACTIVE_COMPONENTS,),
        runtime=_runtime(tmp_path),
    )
    _ = _freeze_plugin_proactive_components(service, root.instance_token)
    assert captured is not None
    with pytest.raises(CompositionError, match="已冻结"):
        await service.register(captured, _source())
    await root.dispose()


@pytest.mark.asyncio
async def test_proactive_rejects_other_root_and_duplicate_active_declarations(
    tmp_path: Path,
) -> None:
    root_a = CompositionRoot("proactive-a")
    root_b = CompositionRoot("proactive-b")
    service_a = PluginProactiveComponents(root_a.instance_token)
    service_b = PluginProactiveComponents(root_b.instance_token)
    _ = await root_a.context.provide(PROACTIVE_COMPONENTS, service_a)
    _ = await root_b.context.provide(PROACTIVE_COMPONENTS, service_b)

    async def apply_wrong_root(ctx) -> None:
        await service_a.register(ctx, _source())

    _ = await root_b.mount(
        apply_wrong_root,
        name="calendar",
        inject=(PROACTIVE_COMPONENTS,),
        runtime=_runtime(tmp_path),
    )
    assert any(
        "Service 不属于当前 Root" in (fiber.error or "")
        for fiber in root_b.receipt().fibers
    )

    async def apply_duplicate(ctx) -> None:
        facade = ctx.require(PROACTIVE_COMPONENTS)
        await facade.register(ctx, _source())
        await facade.register(ctx, _source())

    _ = await root_a.mount(
        apply_duplicate,
        name="calendar-duplicate",
        inject=(PROACTIVE_COMPONENTS,),
        runtime=_runtime(tmp_path, "calendar-duplicate"),
    )
    assert not root_a.receipt().ready
    assert len(_freeze_plugin_proactive_components(service_a, root_a.instance_token).sources) == 0
    await root_a.dispose()
    await root_b.dispose()


@pytest.mark.asyncio
async def test_proactive_allows_same_names_from_different_plugin_owners(
    tmp_path: Path,
) -> None:
    root = CompositionRoot("proactive-multiple-owners")
    service = PluginProactiveComponents(root.instance_token)
    _ = await root.context.provide(PROACTIVE_COMPONENTS, service)

    async def apply(ctx) -> None:
        facade = ctx.require(PROACTIVE_COMPONENTS)
        await facade.register(ctx, _source("events"))
        await facade.register(ctx, _module("proactive.gate.shared"))

    calendar = await root.mount(
        apply,
        name="calendar",
        inject=(PROACTIVE_COMPONENTS,),
        runtime=_runtime(tmp_path, "calendar"),
    )
    feed = await root.mount(
        apply,
        name="feed",
        inject=(PROACTIVE_COMPONENTS,),
        runtime=_runtime(tmp_path, "feed"),
    )
    catalog = _freeze_plugin_proactive_components(service, root.instance_token)
    assert calendar.state.value == "active" and feed.state.value == "active"
    assert set(catalog.sources) == {"calendar:events", "feed:events"}
    assert set(catalog.modules) == {
        "calendar:proactive.gate.shared",
        "feed:proactive.gate.shared",
    }
    assert {
        binding.generation_id for binding in catalog.sources.values()
    } == {"proactive-multiple-owners"}
    await root.dispose()


def test_proactive_results_are_frozen_and_do_not_merge_fetch_ack_states() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    results = (
        FetchItems(({"id": "1"},), "next"),
        FetchEmpty("next"),
        FetchSkip("rate limited", now),
        FetchFailure("offline", True),
        AckCommitted(("1",)),
        AckSkipped("already acknowledged"),
        AckFailure("offline", False),
    )
    assert results[0].items == ({"id": "1"},)
    with pytest.raises(TypeError):
        results[0].items[0]["id"] = "2"  # type: ignore[index]
    assert isinstance(results[1], FetchEmpty)
    assert isinstance(results[4], AckCommitted)
    with pytest.raises((AttributeError, TypeError)):
        results[0].cursor = "changed"  # type: ignore[misc]


@pytest.mark.parametrize(
    "factory",
    (
        lambda: ProactiveSourceDefinition("bad", ("unknown",), "calendar", "fetch"),
        lambda: ProactiveSourceDefinition("bad", ("alert",), "calendar", "fetch", fetch_page_size=-1),
        lambda: ProactiveModuleDefinition("bad.slot", "frame.v1", handler_export="run"),
        lambda: ProactiveModuleDefinition("proactive.gate", "frame.v1", handler_export="bad export"),
    ),
)
def test_proactive_definitions_reject_invalid_contract(factory) -> None:
    with pytest.raises((TypeError, ValueError)):
        factory()
