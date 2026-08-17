from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from agent.plugin_composition.background_jobs import (
    BackgroundJobBinding,
    BackgroundJobCatalog,
    BackgroundJobDefinition,
    BackgroundJobDescriptor,
    CoreEvent,
    CoreEventTrigger,
)
from agent.plugin_composition.model import FiberState
from agent.plugins.composable import ComposablePlugin
from agent.plugins.generation_job_host import (
    BackgroundJobActivityAdapter,
    DriftFinishedEvent,
)
from agent.plugins.job_outcome_ledger import JobOutcomeLedger, JobOutcomeState
from agent.plugins.snapshot import (
    RuntimeSnapshotLease,
    bind_runtime_snapshot,
    reset_runtime_snapshot,
)
from bus.events_lifecycle import DriftFinished
from bus.event_bus import EventBus


class _Store:
    def __init__(self, snapshot: Any) -> None:
        self.snapshot = snapshot
        self.leases = 0

    async def acquire(self, snapshot_id: str) -> RuntimeSnapshotLease:
        assert snapshot_id == self.snapshot.snapshot_id
        self.snapshot.lease_count += 1
        self.leases += 1
        return RuntimeSnapshotLease(self, self.snapshot)

    async def release_lease(self, snapshot: Any) -> None:
        snapshot.lease_count -= 1
        self.leases -= 1

    def fork_lease(self, source: RuntimeSnapshotLease) -> RuntimeSnapshotLease:
        assert source.active
        assert source.snapshot is self.snapshot
        self.snapshot.lease_count += 1
        self.leases += 1
        return RuntimeSnapshotLease(self, self.snapshot)


@dataclass
class _Fiber:
    activation_token: object
    state: FiberState = FiberState.ACTIVE


@dataclass
class _Health:
    healthy: bool = True


def _module(handler: Any, *, name: str = "drift") -> ComposablePlugin:
    module = ModuleType(f"{name}_module")
    module.api_version = 3
    module.name = name
    module.version = "1.0.0"
    module.apply = _apply
    module.merge_pending = handler
    return ComposablePlugin.from_module(module)


async def _apply(ctx: Any, config: Any) -> None:
    return None


def _fixture(
    tmp_path,
    handler: Any,
    *,
    model_role: str | None = None,
    provider: object | None = None,
    debounce_seconds: int = 0,
    coalesce: bool = True,
    clock: Any | None = None,
):
    plugin = _module(handler)
    definition = BackgroundJobDefinition(
        name="merge_pending",
        triggers=(CoreEventTrigger(CoreEvent.DRIFT_FINISHED),),
        handler_export="merge_pending",
        model_role=model_role,
        debounce_seconds=debounce_seconds,
        coalesce=coalesce,
    )
    descriptor = BackgroundJobDescriptor(
        owner="drift",
        name=definition.name,
        triggers=definition.triggers,
        debounce_seconds=definition.debounce_seconds,
        coalesce=definition.coalesce,
        handler_export=definition.handler_export,
        retry_policy=definition.retry_policy,
        documents_scope=definition.documents_scope,
        model_role=definition.model_role,
    )
    fiber = _Fiber(object())
    binding = BackgroundJobBinding(
        generation_id="generation-1",
        plugin_id="drift",
        name=definition.name,
        descriptor=descriptor,
        definition=definition,
        owner_fiber=fiber,
        activation_token=fiber.activation_token,
        required_health=_Health(),
    )
    catalog = BackgroundJobCatalog(
        {"drift:merge_pending": binding},
        root_instance_token=object(),
    )
    generation = SimpleNamespace(
        generation_id="generation-1",
        instance=plugin,
        source_revision="source-1",
    )
    snapshot = SimpleNamespace(
        snapshot_id="snapshot-1",
        background_job_catalog=catalog,
        generations={"drift": generation},
        lease_count=0,
    )
    store = _Store(snapshot)
    snapshot.lease_count += 1
    store.leases += 1
    target_lease = RuntimeSnapshotLease(store, snapshot)
    ledger = JobOutcomeLedger(tmp_path / "outcomes.sqlite")
    adapter = BackgroundJobActivityAdapter(
        EventBus(),
        store,
        model_provider=provider,
        ledger=ledger,
        clock=clock,
    )
    plan = adapter.prepare_components("tx-1", target_lease, catalog)
    return adapter, plan, target_lease, store, ledger


def _event(event_id: str) -> DriftFinished:
    return DriftFinished(
        event_id=event_id,
        session_key="session",
        skill_name="skill",
        status="completed",
        briefing="briefing",
        message_result="ok",
        timestamp=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_prepare_is_pure_and_materialized_binding_is_closed(tmp_path) -> None:
    calls: list[object] = []

    async def handler(ctx) -> None:
        calls.append(ctx)

    adapter, plan, target_lease, store, ledger = _fixture(tmp_path, handler)
    assert adapter.handler_resolution_count == 0
    assert adapter.subscription_count == 0
    assert adapter.timer_count == 0

    runtime = await adapter.materialize_closed("tx-1", plan)
    assert adapter.handler_resolution_count == 1
    assert runtime.admission_open is False
    assert runtime.subscription_count == 1
    assert calls == []

    await adapter.enqueue_event(runtime, _event("event-1"))
    await asyncio.sleep(0)
    assert calls == []

    adapter.finalize_components("tx-1", runtime)
    await adapter.enqueue_event(runtime, _event("event-1"))
    await adapter.drain(runtime)
    outcome = ledger.find_by_event(
        plugin_id="drift",
        job_name="merge_pending",
        event_id="event-1",
    )
    assert len(calls) == 1
    assert outcome is not None
    assert outcome.state is JobOutcomeState.SUCCEEDED
    assert dict(outcome.event_payload or {}) == {
        "event_id": "event-1",
        "session_key": "session",
        "skill_name": "skill",
        "status": "completed",
        "briefing": "briefing",
        "message_result": "ok",
        "timestamp": calls[0].event.timestamp.isoformat(),
    }
    assert store.leases == 1  # the publication lease remains owned by the caller

    await adapter.close_components("tx-1", runtime)
    await target_lease.release()
    assert store.leases == 0
    assert adapter.subscription_count == 0
    assert adapter.timer_count == 0


@pytest.mark.asyncio
async def test_exact_handler_and_event_id_dedupe_use_first_binding(tmp_path) -> None:
    calls: list[str] = []

    async def first(ctx) -> None:
        calls.append("first")

    async def second(ctx) -> None:
        calls.append("second")

    adapter, plan, target_lease, _store, ledger = _fixture(tmp_path, first)
    runtime = await adapter.materialize_closed("tx-1", plan)
    await adapter.open(runtime)
    event = _event("same-event")
    await adapter.enqueue_event(runtime, event)
    await adapter.enqueue_event(runtime, event)
    await adapter.drain(runtime)
    assert calls == ["first"]
    assert len(ledger.list_all()) == 1

    await adapter.close_components("tx-1", runtime)
    await target_lease.release()


@pytest.mark.asyncio
async def test_debounce_uses_core_clock_without_changing_event_dedupe(tmp_path) -> None:
    now = datetime(2026, 8, 17, 4, 0, tzinfo=timezone.utc)
    calls: list[str] = []

    async def handler(ctx) -> None:
        calls.append(ctx.event.event_id)

    adapter, plan, target_lease, _store, ledger = _fixture(
        tmp_path,
        handler,
        debounce_seconds=60,
        clock=lambda: now,
    )
    runtime = await adapter.materialize_closed("tx-1", plan)
    await adapter.open(runtime)
    await adapter.enqueue_event(runtime, _event("debounce-1"))
    await adapter.drain(runtime)
    await adapter.enqueue_event(runtime, _event("debounce-2"))
    await adapter.drain(runtime)
    assert calls == ["debounce-1"]
    assert len(ledger.list_all()) == 2
    assert all(record.state is not JobOutcomeState.RUNNING for record in ledger.list_all())

    now = now.replace(minute=1, second=1)
    await adapter.enqueue_event(runtime, _event("debounce-3"))
    await adapter.drain(runtime)
    assert calls == ["debounce-1", "debounce-3"]
    await adapter.close_components("tx-1", runtime)
    await target_lease.release()


@pytest.mark.asyncio
async def test_llm_lease_is_invocation_scoped_and_invalid_after_handler(tmp_path) -> None:
    class Provider:
        def __init__(self) -> None:
            self.calls = 0

        async def chat(self, **kwargs: object) -> object:
            self.calls += 1
            return SimpleNamespace(content="model-answer", usage=None)

    provider = Provider()
    saved: list[object] = []

    async def handler(ctx) -> None:
        saved.append(ctx.llm)
        assert await ctx.llm.generate_text(prompt="hello") == "model-answer"

    adapter, plan, target_lease, _store, ledger = _fixture(
        tmp_path,
        handler,
        provider=provider,
    )
    runtime = await adapter.materialize_closed("tx-1", plan)
    await adapter.open(runtime)
    await adapter.enqueue_event(runtime, _event("llm-event"))
    await adapter.drain(runtime)
    assert provider.calls == 1
    with pytest.raises(RuntimeError, match="已失效"):
        await saved[0].generate_text(prompt="after terminal")
    outcome = ledger.find_by_event(
        plugin_id="drift",
        job_name="merge_pending",
        event_id="llm-event",
    )
    assert outcome is not None and outcome.state is JobOutcomeState.SUCCEEDED
    await adapter.close_components("tx-1", runtime)
    await target_lease.release()


@pytest.mark.asyncio
async def test_cancel_running_releases_snapshot_lease_and_marks_cancelled(tmp_path) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class Provider:
        async def chat(self, **kwargs: object) -> object:
            started.set()
            await release.wait()
            return SimpleNamespace(content="never", usage=None)

    async def handler(ctx) -> None:
        await ctx.llm.generate_text(prompt="blocked")

    adapter, plan, target_lease, store, ledger = _fixture(
        tmp_path,
        handler,
        provider=Provider(),
    )
    runtime = await adapter.materialize_closed("tx-1", plan)
    await adapter.open(runtime)
    await adapter.enqueue_event(runtime, _event("cancel-event"))
    for _ in range(100):
        if runtime.running:
            break
        await asyncio.sleep(0)
    await started.wait()
    assert store.leases == 2
    await adapter.cancel_running(runtime)
    outcome = ledger.find_by_event(
        plugin_id="drift",
        job_name="merge_pending",
        event_id="cancel-event",
    )
    assert outcome is not None and outcome.state is JobOutcomeState.CANCELLED
    assert store.leases == 1
    await adapter.close_components("tx-1", runtime)
    await target_lease.release()


@pytest.mark.asyncio
async def test_completed_child_failure_prevents_handler_success(tmp_path) -> None:
    async def fail_child() -> None:
        raise RuntimeError("child failed")

    async def handler(ctx) -> None:
        ctx.spawn_child(fail_child(), name="failing-child")
        await asyncio.sleep(0)

    adapter, plan, target_lease, _store, ledger = _fixture(tmp_path, handler)
    runtime = await adapter.materialize_closed("tx-1", plan)
    await adapter.open(runtime)
    await adapter.enqueue_event(runtime, _event("child-failure"))
    await adapter.drain(runtime)

    outcome = ledger.find_by_event(
        plugin_id="drift",
        job_name="merge_pending",
        event_id="child-failure",
    )
    assert outcome is not None and outcome.state is JobOutcomeState.FAILED
    assert outcome.error == "RuntimeError: child failed"
    await adapter.close_components("tx-1", runtime)
    await target_lease.release()


@pytest.mark.asyncio
async def test_cancel_intent_wins_when_handler_swallows_cancelled_error(tmp_path) -> None:
    started = asyncio.Event()

    async def handler(ctx) -> str:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return "swallowed"

    adapter, plan, target_lease, store, ledger = _fixture(tmp_path, handler)
    runtime = await adapter.materialize_closed("tx-1", plan)
    await adapter.open(runtime)
    await adapter.enqueue_event(runtime, _event("swallowed-cancel"))
    await started.wait()

    await adapter.cancel_running(runtime)

    outcome = ledger.find_by_event(
        plugin_id="drift",
        job_name="merge_pending",
        event_id="swallowed-cancel",
    )
    assert outcome is not None and outcome.state is JobOutcomeState.CANCELLED
    assert store.leases == 1
    await adapter.close_components("tx-1", runtime)
    await target_lease.release()


@pytest.mark.asyncio
async def test_close_components_drains_queued_request_after_running_cancel(tmp_path) -> None:
    started = asyncio.Event()

    class Provider:
        async def chat(self, **kwargs: object) -> object:
            started.set()
            await asyncio.Event().wait()
            return SimpleNamespace(content="never", usage=None)

    async def handler(ctx) -> None:
        await ctx.llm.generate_text(prompt="blocked")

    adapter, plan, target_lease, store, ledger = _fixture(
        tmp_path,
        handler,
        provider=Provider(),
        coalesce=False,
    )
    runtime = await adapter.materialize_closed("tx-1", plan)
    await adapter.open(runtime)
    await adapter.enqueue_event(runtime, _event("running-close"))
    await started.wait()
    await adapter.enqueue_event(runtime, _event("queued-close"))
    assert len(runtime.queued) == 1

    await asyncio.wait_for(adapter.close_components("tx-1", runtime), timeout=1)

    assert runtime.closed is True
    assert runtime.worker_task is None
    assert ledger.find_by_event(
        plugin_id="drift",
        job_name="merge_pending",
        event_id="running-close",
    ).state is JobOutcomeState.CANCELLED
    assert ledger.find_by_event(
        plugin_id="drift",
        job_name="merge_pending",
        event_id="queued-close",
    ).state is JobOutcomeState.CANCELLED
    assert store.leases == 1
    await target_lease.release()


@pytest.mark.asyncio
async def test_queued_request_selects_model_generation_at_execution_start(tmp_path) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    model_ids: list[str] = []

    class Provider:
        def __init__(self) -> None:
            self.calls = 0
            self.registry = SimpleNamespace(
                current=SimpleNamespace(generation_id="model-1")
            )

        async def chat(self, **kwargs: object) -> object:
            self.calls += 1
            if self.calls == 1:
                started.set()
                await release.wait()
            return SimpleNamespace(content="ok", usage=None)

    provider = Provider()

    async def handler(ctx) -> None:
        model_ids.append(ctx.model_generation_id)
        await ctx.llm.generate_text(prompt="model")

    adapter, plan, target_lease, _store, ledger = _fixture(
        tmp_path,
        handler,
        provider=provider,
        coalesce=False,
    )
    runtime = await adapter.materialize_closed("tx-1", plan)
    await adapter.open(runtime)
    await adapter.enqueue_event(runtime, _event("model-running"))
    await started.wait()
    await adapter.enqueue_event(runtime, _event("model-queued"))
    provider.registry.current.generation_id = "model-2"
    release.set()
    await adapter.drain(runtime)

    assert model_ids == ["model-1", "model-2"]
    queued_outcome = ledger.find_by_event(
        plugin_id="drift",
        job_name="merge_pending",
        event_id="model-queued",
    )
    assert queued_outcome is not None
    assert queued_outcome.state is JobOutcomeState.SUCCEEDED
    assert queued_outcome.model_generation_id == "model-2"
    await adapter.close_components("tx-1", runtime)
    await target_lease.release()


@pytest.mark.asyncio
async def test_event_accepted_before_pause_is_admitted_on_old_binding(tmp_path) -> None:
    calls: list[str] = []

    async def handler(ctx) -> None:
        calls.append(ctx.event.event_id)

    adapter, plan, target_lease, store, ledger = _fixture(tmp_path, handler)
    runtime = await adapter.materialize_closed("tx-1", plan)
    await adapter.open(runtime)

    source_lease = await store.acquire("snapshot-1")
    token = bind_runtime_snapshot(source_lease)
    try:
        await adapter.pause(runtime)
        adapter._on_event(runtime, _event("accepted-before-pause"))
    finally:
        reset_runtime_snapshot(token)
        await source_lease.release()

    await adapter.stop_components("tx-1", runtime)

    assert calls == ["accepted-before-pause"]
    outcome = ledger.find_by_event(
        plugin_id="drift",
        job_name="merge_pending",
        event_id="accepted-before-pause",
    )
    assert outcome is not None and outcome.state is JobOutcomeState.SUCCEEDED
    await adapter.close_components("shutdown", runtime)
    await target_lease.release()


@pytest.mark.asyncio
async def test_materialize_rejects_non_async_or_wrong_handler_signature(tmp_path) -> None:
    def sync_handler(ctx) -> None:
        return None

    adapter, plan, target_lease, _store, _ledger = _fixture(tmp_path, sync_handler)
    with pytest.raises(TypeError, match="必须是 async"):
        await adapter.materialize_closed("tx-1", plan)
    assert adapter.subscription_count == 0
    assert adapter.timer_count == 0
    await target_lease.release()


def test_drift_event_requires_non_empty_event_id() -> None:
    with pytest.raises(ValueError, match="event_id"):
        _event(" ")
