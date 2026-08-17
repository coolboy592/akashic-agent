from __future__ import annotations

import asyncio
from types import MappingProxyType, SimpleNamespace
from typing import Any

import pytest

from agent.plugin_composition.proactive import (
    AckCommitted,
    FetchFailure,
    FetchItems,
    FetchSkip,
)
from agent.plugins.generation_activity_host import ActivityBinding, ActivityHost
from agent.plugins.generation_proactive_bridge import CommittedProactiveBridge
from agent.plugins.generation_proactive_host import ProactiveRuntimeBinding
from agent.plugins.snapshot import RuntimeSnapshotLease, get_current_runtime_lease
from proactive_v2.config import ProactiveConfig
from proactive_v2.frame import new_proactive_frame
from proactive_v2.loop import ProactiveLoop
from proactive_v2.mcp_sources import fetch_sources_async


class _Store:
    def __init__(self, snapshot: object) -> None:
        self.snapshot = snapshot
        self.leases = 0

    async def acquire(self) -> RuntimeSnapshotLease:
        self.leases += 1
        return RuntimeSnapshotLease(self, self.snapshot)  # type: ignore[arg-type]

    def fork_lease(self, source: RuntimeSnapshotLease) -> RuntimeSnapshotLease:
        if not source.active:
            raise RuntimeError("source lease inactive")
        self.leases += 1
        return RuntimeSnapshotLease(self, self.snapshot)  # type: ignore[arg-type]

    async def release_lease(self, snapshot: object) -> None:
        assert snapshot is self.snapshot
        self.leases -= 1


class _Source:
    def __init__(self, name: str = "calendar") -> None:
        self.descriptor = SimpleNamespace(
            owner=name,
            name=name,
            channels=("alert",),
            fetch_tool="fetch_events",
            ack_tool="ack_events",
        )
        self.fetch_leases: list[RuntimeSnapshotLease] = []
        self.ack_leases: list[RuntimeSnapshotLease] = []

    @property
    def name(self) -> str:
        return str(self.descriptor.name)

    async def fetch(
        self,
        lease: RuntimeSnapshotLease,
        *,
        cursor: str | None = None,
    ) -> FetchItems:
        self.fetch_leases.append(lease)
        if cursor is None:
            return FetchItems(({"event_id": "one", "kind": "alert"},), "next")
        return FetchItems(({"event_id": "two", "kind": "alert"},), None)

    async def ack(
        self,
        lease: RuntimeSnapshotLease,
        ids: list[str],
        *,
        feedback: str | None = None,
    ) -> AckCommitted:
        self.ack_leases.append(lease)
        return AckCommitted(tuple(ids))


class _Module:
    def __init__(self) -> None:
        self.descriptor = SimpleNamespace(
            slot="proactive.calendar",
            lifecycle_id="default.proactive.frame.v1",
            requires=(),
            produces=("calendar:seen",),
            collects=(),
        )
        self.leases: list[RuntimeSnapshotLease] = []

    async def transform(self, lease: RuntimeSnapshotLease, frame):
        self.leases.append(lease)
        frame.slots["calendar:seen"] = True
        return frame


class _BaseGateway:
    async def call(self, *args: object, **kwargs: object) -> object:
        raise AssertionError("v3 virtual source must not use legacy gateway")


class _SkippedSource(_Source):
    async def fetch(self, lease, *, cursor=None):
        return FetchSkip("rate-limited")


class _FailedSource(_Source):
    async def fetch(self, lease, *, cursor=None):
        return FetchFailure("backend-down", retryable=False)


@pytest.mark.asyncio
async def test_real_tick_holds_activity_lease_across_v3_source_module_and_ack() -> None:
    source = _Source()
    module = _Module()
    snapshot = SimpleNamespace(snapshot_id="stable-v3")
    store = _Store(snapshot)
    runtime = ProactiveRuntimeBinding(
        transaction_id="tx",
        snapshot_id=snapshot.snapshot_id,
        catalog_identity="catalog",
        catalog=None,
        snapshot=snapshot,
        sources=MappingProxyType({"calendar:calendar": source}),  # type: ignore[arg-type]
        modules=MappingProxyType({"calendar:module": module}),  # type: ignore[arg-type]
        admission_open=True,
    )
    host = ActivityHost(())
    host._active = ActivityBinding(  # type: ignore[attr-defined]
        snapshot_id=snapshot.snapshot_id,
        catalog_identity="catalog",
        child_bindings=MappingProxyType({"proactive_components": runtime}),
        admission_open=True,
    )
    bridge = CommittedProactiveBridge(host)
    gateway = bridge.gateway(_BaseGateway(), runtime)
    registered = bridge.registered_sources(runtime)
    lifecycle = bridge.lifecycle_modules(
        runtime,
        lifecycle_id="default.proactive.frame.v1",
    )

    async def run_tick(session_key: str) -> float:
        lease = get_current_runtime_lease()
        assert lease is not None and lease.snapshot is snapshot
        assert host.active is not None and host.active.in_flight == 1
        spec = registered[0].spec
        fetched = await gateway.call(spec.server, spec.fetch_tool, {})
        assert [item["event_id"] for item in fetched] == ["one", "two"]
        await gateway.call(
            spec.server,
            spec.ack_tool,
            {"event_ids": ["one", "two"]},
        )
        frame = await lifecycle[0].run(new_proactive_frame(session_key))
        assert frame.slots["calendar:seen"] is True
        return 0.5

    loop = object.__new__(ProactiveLoop)
    loop._cfg = ProactiveConfig()
    loop._provider = object()
    loop._runtime_snapshot_store = store
    loop._proactive_bridge = bridge
    loop._reload_lock = asyncio.Lock()
    loop._sense = SimpleNamespace(target_session_key=lambda: "telegram:1")
    loop._proactive_kernel = SimpleNamespace(run_tick=run_tick)

    async def switch_snapshot(target: object) -> None:
        assert target is snapshot

    loop._switch_snapshot = switch_snapshot

    assert await loop._tick() == 0.5
    observed_leases = [*source.fetch_leases, *source.ack_leases, *module.leases]
    assert len(source.fetch_leases) == 2
    assert source.fetch_leases[0] is source.fetch_leases[1]
    assert len({id(lease) for lease in observed_leases}) == 3
    assert all(not lease.active for lease in observed_leases)
    assert host.active.in_flight == 0
    assert store.leases == 0


@pytest.mark.asyncio
async def test_v3_skip_and_failure_remain_distinct_in_source_aggregation() -> None:
    snapshot = SimpleNamespace(snapshot_id="typed-results")
    store = _Store(snapshot)
    lease = await store.acquire()
    skipped = _SkippedSource("skipped")
    failed = _FailedSource("failed")
    runtime = ProactiveRuntimeBinding(
        transaction_id="tx",
        snapshot_id=snapshot.snapshot_id,
        catalog_identity="catalog",
        catalog=None,
        snapshot=snapshot,
        sources=MappingProxyType({"skipped:key": skipped, "failed:key": failed}),  # type: ignore[arg-type]
        admission_open=True,
    )
    bridge = CommittedProactiveBridge(ActivityHost(()))
    gateway = bridge.gateway(_BaseGateway(), runtime)
    registered = bridge.registered_sources(runtime)
    token = bridge.bind_execution(lease)
    try:
        channels = await fetch_sources_async(gateway, registered)
    finally:
        bridge.reset_execution(token)
        await lease.release()

    assert channels.skipped == {"skipped:skipped": ("rate-limited", None)}
    assert channels.failures == {"failed:failed": ("backend-down", False)}
    assert channels == {"alert": [], "content": [], "context": []}
    assert store.leases == 0


def test_snapshot_without_private_binding_fails_loud() -> None:
    bridge = CommittedProactiveBridge(ActivityHost(()))
    loop = object.__new__(ProactiveLoop)
    loop._proactive_bridge = bridge
    snapshot = SimpleNamespace(
        proactive_modules=(),
        proactive_lifecycles=(),
        proactive_module_factories=(),
        proactive_runtime_factories=(),
        proactive_sources={},
        private_proactive_catalog=None,
    )

    with pytest.raises(RuntimeError, match="Private proactive Activity binding"):
        loop._apply_snapshot(snapshot)
