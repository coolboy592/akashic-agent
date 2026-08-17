from __future__ import annotations

import asyncio
import json
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from agent.plugin_composition.model import FiberState
from agent.plugin_composition.proactive import (
    AckCommitted,
    AckFailure,
    ProactiveCatalog,
    ProactiveModuleBinding,
    ProactiveModuleDefinition,
    ProactiveModuleDescriptor,
    ProactiveSourceBinding,
    ProactiveSourceDefinition,
    ProactiveSourceDescriptor,
)
from agent.plugins.composable import ComposablePlugin
from agent.plugins.generation_activity_host import ActivityCatalog, ActivityHost
from agent.plugins.generation_proactive_host import (
    ProactiveActivityAdapter,
    ProactiveModuleContext,
)
from agent.plugins.mcp_generation_host import McpCallResult
from agent.plugins.snapshot import RuntimeSnapshotLease
from proactive_v2.frame import new_proactive_frame


class _Store:
    async def release_lease(self, snapshot: Any) -> None:
        return None


class _Fiber:
    def __init__(self) -> None:
        self.activation_token = object()
        self.state = FiberState.ACTIVE


class _Health:
    healthy = True


class _Route:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, object], str]] = []
        self.ack_response: object = _UNSET
        self.block_fetch = False
        self.fetch_started = asyncio.Event()
        self.release_fetch = asyncio.Event()

    async def call(
        self,
        server: str,
        tool_name: str,
        arguments: dict[str, object],
        *,
        snapshot_lease: RuntimeSnapshotLease,
    ) -> object:
        self.calls.append(
            (server, tool_name, dict(arguments), snapshot_lease.snapshot.snapshot_id)
        )
        if self.block_fetch and tool_name == "fetch_events":
            self.fetch_started.set()
            await self.release_fetch.wait()
        if tool_name == "fetch_events":
            return McpCallResult(
                status="success",
                output=json.dumps({"items": [{"id": "event-1"}], "cursor": "next"}),
            )
        if self.ack_response is not _UNSET:
            return self.ack_response
        return McpCallResult(
            status="success",
            output=json.dumps({"status": "committed", "ids": arguments["event_ids"]}),
        )


_UNSET = object()


def _plugin(handler: object) -> ComposablePlugin:
    module = ModuleType("calendar_plugin")
    module.api_version = 3
    module.name = "calendar"
    module.version = "1.0.0"
    module.apply = _apply
    module.runtime = SimpleNamespace(handle_calendar=handler)
    return ComposablePlugin.from_module(module)


async def _apply(ctx: object, config: object) -> None:
    return None


def _fixture(handler: object):
    fiber = _Fiber()
    source_definition = ProactiveSourceDefinition(
        name="calendar",
        channels=("alert",),
        mcp_server="calendar",
        fetch_tool="fetch_events",
        ack_tool="ack_events",
        fetch_page_size=20,
    )
    source_descriptor = ProactiveSourceDescriptor(
        owner="calendar",
        name=source_definition.name,
        channels=source_definition.channels,
        mcp_server=source_definition.mcp_server,
        fetch_tool=source_definition.fetch_tool,
        ack_tool=source_definition.ack_tool,
        fetch_page_size=source_definition.fetch_page_size,
    )
    source_binding = ProactiveSourceBinding(
        descriptor=source_descriptor,
        generation_id="calendar:generation-1",
        definition=source_definition,
        owner_fiber=fiber,
        activation_token=fiber.activation_token,
        health=_Health(),
    )
    module_definition = ProactiveModuleDefinition(
        slot="proactive.calendar",
        lifecycle_id="default.proactive.frame.v1",
        produces=("calendar.alerts",),
        handler_export="runtime.handle_calendar",
    )
    module_descriptor = ProactiveModuleDescriptor(
        owner="calendar",
        lifecycle_id=module_definition.lifecycle_id,
        slot=module_definition.slot,
        requires=module_definition.requires,
        produces=module_definition.produces,
        collects=module_definition.collects,
        handler_export=module_definition.handler_export,
        domain_effect=module_definition.domain_effect,
    )
    module_binding = ProactiveModuleBinding(
        descriptor=module_descriptor,
        generation_id="calendar:generation-1",
        definition=module_definition,
        owner_fiber=fiber,
        activation_token=fiber.activation_token,
        health=_Health(),
    )
    catalog = ProactiveCatalog(
        {"calendar:calendar": source_binding},
        {"calendar:proactive.calendar": module_binding},
        root_instance_token=object(),
    )
    plugin = _plugin(handler)
    snapshot = SimpleNamespace(
        snapshot_id="snapshot-1",
        generations={
            "calendar": SimpleNamespace(
                generation_id="calendar:generation-1",
                instance=plugin,
            )
        },
        proactive_component_catalog=catalog,
        background_job_catalog=None,
        private_proactive_catalog=None,
    )
    store = _Store()
    target_lease = RuntimeSnapshotLease(store, snapshot)
    execution_lease = RuntimeSnapshotLease(store, snapshot)
    route = _Route()
    adapter = ProactiveActivityAdapter(route)
    activity_catalog = ActivityCatalog(proactive=catalog, background_jobs=None)
    return adapter, activity_catalog, target_lease, execution_lease, route


@pytest.mark.asyncio
async def test_prepare_materialize_closed_and_finalize_opens_sync_admission() -> None:
    calls: list[ProactiveModuleContext] = []

    async def handler(ctx: ProactiveModuleContext, frame):
        calls.append(ctx)
        frame.slots["calendar"] = "ready"
        return frame

    adapter, activity_catalog, target, execution, route = _fixture(handler)
    plan = adapter.prepare_components("tx-1", target, activity_catalog)

    assert adapter.handler_resolution_count == 0
    assert adapter.source_fetch_invocations == 0
    assert adapter.module_invocations == 0
    assert route.calls == []

    runtime = await adapter.materialize_closed("tx-1", plan)
    assert not runtime.active
    assert runtime.admission_open is False
    assert route.calls == []
    assert calls == []
    with pytest.raises(RuntimeError, match="admission"):
        await runtime.source("calendar").fetch(execution)

    adapter.finalize_components("tx-1", runtime)
    assert runtime.active
    assert adapter.active_binding is runtime

    fetched = await runtime.source("calendar").fetch(execution, cursor="old")
    assert fetched.items == ({"id": "event-1"},)
    assert fetched.cursor == "next"
    acknowledged = await runtime.source("calendar").ack(execution, ["event-1"])
    assert acknowledged == AckCommitted(("event-1",))

    frame = new_proactive_frame("session-1")
    result = await runtime.module("proactive.calendar").transform(execution, frame)
    assert result.slots == {"calendar": "ready"}
    assert calls[0].snapshot_id == "snapshot-1"
    assert route.calls == [
        (
            "calendar",
            "fetch_events",
            {"cursor": "old", "limit": 20},
            "snapshot-1",
        ),
        ("calendar", "ack_events", {"event_ids": ["event-1"]}, "snapshot-1"),
    ]

    await adapter.stop_components("tx-2", runtime)
    assert runtime.active is False
    with pytest.raises(RuntimeError, match="admission"):
        await runtime.module("proactive.calendar").transform(execution, frame)
    await adapter.restore_components("tx-2", runtime)
    assert runtime.active
    await adapter.close_components("shutdown", runtime)
    assert adapter.active_binding is None
    assert target.active


@pytest.mark.asyncio
async def test_execution_requires_the_exact_snapshot_lease_and_cancel_propagates() -> (
    None
):
    async def handler(ctx: ProactiveModuleContext, frame):
        return frame

    adapter, activity_catalog, target, execution, route = _fixture(handler)
    plan = adapter.prepare_components("tx-1", target, activity_catalog)
    runtime = await adapter.materialize_closed("tx-1", plan)
    adapter.finalize_components("tx-1", runtime)

    wrong_snapshot = SimpleNamespace(
        snapshot_id="snapshot-1",
        generations=target.snapshot.generations,
        proactive_component_catalog=target.snapshot.proactive_component_catalog,
    )
    wrong_lease = RuntimeSnapshotLease(_Store(), wrong_snapshot)
    with pytest.raises(RuntimeError, match="identity"):
        await runtime.source("calendar").fetch(wrong_lease)
    assert adapter.source_fetch_invocations == 0

    route.block_fetch = True
    request = asyncio.create_task(runtime.source("calendar").fetch(execution))
    await route.fetch_started.wait()
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    assert adapter.source_fetch_invocations == 1
    assert runtime.active
    route.release_fetch.set()

    await adapter.close_components("shutdown", runtime)
    assert target.active


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ack_response",
    [
        None,
        "",
        "acknowledged",
        '{"ids":["event-1"]}',
        {"status": "unknown", "ids": ["event-1"]},
        {"ids": ["event-1"]},
    ],
    ids=[
        "none",
        "empty-text",
        "plain-text",
        "json-without-status",
        "unknown-status",
        "missing-status",
    ],
)
async def test_ack_never_infers_committed_from_ambiguous_route_output(
    ack_response: object,
) -> None:
    async def handler(ctx: ProactiveModuleContext, frame):
        return frame

    adapter, activity_catalog, target, execution, route = _fixture(handler)
    plan = adapter.prepare_components("tx-1", target, activity_catalog)
    runtime = await adapter.materialize_closed("tx-1", plan)
    adapter.finalize_components("tx-1", runtime)
    route.ack_response = ack_response

    result = await runtime.source("calendar").ack(execution, ["event-1"])

    assert isinstance(result, AckFailure)
    assert not isinstance(result, AckCommitted)
    assert route.calls[-1] == (
        "calendar",
        "ack_events",
        {"event_ids": ["event-1"]},
        "snapshot-1",
    )
    await adapter.close_components("shutdown", runtime)


@pytest.mark.asyncio
async def test_failed_materialize_resolves_no_partial_binding_or_lease() -> None:
    async def handler(ctx: ProactiveModuleContext, frame):
        return frame

    adapter, activity_catalog, target, _execution, _route = _fixture(handler)
    target.snapshot.generations["calendar"].instance = object()
    plan = adapter.prepare_components("tx-1", target, activity_catalog)

    with pytest.raises(RuntimeError, match="ComposablePlugin"):
        await adapter.materialize_closed("tx-1", plan)

    assert adapter.active_binding is None
    assert adapter.handler_resolution_count == 0
    assert target.active


@pytest.mark.asyncio
async def test_activity_host_finalize_opens_the_child_at_the_shared_boundary() -> None:
    async def handler(ctx: ProactiveModuleContext, frame):
        return frame

    adapter, _catalog, target, _execution, route = _fixture(handler)
    host = ActivityHost((adapter,))
    transaction = await host.prepare_transaction(target)
    assert route.calls == []

    await host.pause_and_drain(transaction)
    staged = await host.materialize_closed(transaction)
    runtime = staged.child_bindings[adapter.name]
    assert runtime.active is False
    assert route.calls == []

    host.finalize(transaction)
    assert runtime.active
    assert host.active is not None
    assert host.active.admission_open
    await host.open(transaction)
    assert not target.active

    await host.close()
    assert adapter.active_binding is None
