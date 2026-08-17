from __future__ import annotations

import asyncio
from types import ModuleType, SimpleNamespace
from typing import Any, cast

import pytest

from agent.plugins.generation_activity_host import (
    ActivityCatalog,
    ActivityHost,
)
from agent.plugins.composable import ComposablePlugin
from agent.plugins.generation import (
    GateResult,
    PluginContributions,
    PluginGeneration,
)
from agent.plugins.manager import PluginManager
from agent.plugins.private_proactive import PrivateProactiveCatalog
from agent.plugins.scope import PluginScope
from agent.plugins.snapshot import RuntimeSnapshotCompiler, RuntimeSnapshotStore
from agent.plugin_composition import CompositionRoot
from bus.event_bus import EventBus


class _RecordingChild:
    name = "recording"

    def __init__(self) -> None:
        self.events: list[str] = []
        self.materialized = 0

    def prepare_components(self, transaction_id, target_lease, target_catalog):
        assert target_lease.active
        assert isinstance(target_catalog, ActivityCatalog)
        self.events.append("prepare")
        return target_lease.snapshot.snapshot_id

    async def stop_components(self, transaction_id, old_binding):
        self.events.append(f"stop:{old_binding}")

    async def materialize_closed(self, transaction_id, plan):
        self.materialized += 1
        binding = f"binding:{plan}:{self.materialized}"
        self.events.append(f"materialize:{plan}")
        return binding

    def finalize_components(self, transaction_id, binding):
        self.events.append(f"finalize:{binding}")

    def pause_components(self, binding):
        self.events.append(f"pause:{binding}")

    async def restore_components(self, transaction_id, old_binding):
        self.events.append(f"restore:{old_binding}")

    async def close_components(self, transaction_id, binding):
        self.events.append(f"close:{binding}")


def _stable_lease(revision: str):
    store = RuntimeSnapshotStore()
    snapshot = RuntimeSnapshotCompiler().compile({}, snapshot_revision=revision)
    store.install(snapshot)
    return store, store.lease(snapshot.snapshot_id)


def test_activity_identity_includes_exact_handler_generation() -> None:
    descriptor = SimpleNamespace(owner="probe")
    catalog = SimpleNamespace(identity="same", descriptors=(descriptor,))
    first = SimpleNamespace(
        proactive_component_catalog=catalog,
        background_job_catalog=None,
        private_proactive_catalog=None,
        generations={
            "probe": SimpleNamespace(
                generation_id="generation-a",
                source_revision="revision-a",
            )
        },
    )
    second = SimpleNamespace(
        proactive_component_catalog=catalog,
        background_job_catalog=None,
        private_proactive_catalog=None,
        generations={
            "probe": SimpleNamespace(
                generation_id="generation-b",
                source_revision="revision-b",
            )
        },
    )

    assert PluginManager._activity_catalog_identity(cast(Any, first)) != (
        PluginManager._activity_catalog_identity(cast(Any, second))
    )


@pytest.mark.asyncio
async def test_activity_host_prepare_is_pure_and_open_controls_admission() -> None:
    child = _RecordingChild()
    host = ActivityHost((child,))
    store, target_lease = _stable_lease("initial")

    transaction = await host.prepare_transaction(target_lease)

    assert child.events == ["prepare"]
    assert child.materialized == 0
    with pytest.raises(RuntimeError, match="admission"):
        host.acquire(target_lease)

    await host.pause_and_drain(transaction)
    staged = await host.materialize_closed(transaction)
    assert not staged.admission_open
    host.finalize(transaction)
    assert host.active is staged
    assert staged.admission_open

    await host.open(transaction)

    source_lease = store.lease(staged.snapshot_id)
    lease = host.acquire(source_lease)
    assert lease.binding is staged
    await source_lease.release()
    await lease.release()
    assert not target_lease.active
    await host.close()
    await store.close()


@pytest.mark.asyncio
async def test_activity_host_drain_waits_exact_old_in_flight_and_rollback_restores() -> None:
    child = _RecordingChild()
    host = ActivityHost((child,))
    old_store, old_target = _stable_lease("old")
    initial = await host.prepare_transaction(old_target)
    await host.pause_and_drain(initial)
    old_binding = await host.materialize_closed(initial)
    host.finalize(initial)
    await host.open(initial)
    accepted_source = old_store.lease(old_binding.snapshot_id)
    accepted = host.acquire(accepted_source)
    await accepted_source.release()

    new_store, new_target = _stable_lease("new")
    transaction = await host.prepare_transaction(new_target)
    drain = asyncio.create_task(host.pause_and_drain(transaction))
    await asyncio.sleep(0)
    assert not drain.done()
    assert not old_binding.admission_open
    rejected_snapshot_lease = old_store.lease(old_binding.snapshot_id)
    with pytest.raises(RuntimeError, match="admission"):
        host.acquire(rejected_snapshot_lease)
    await rejected_snapshot_lease.release()

    await accepted.release()
    await drain
    staged = await host.materialize_closed(transaction)
    host.finalize(transaction)
    await host.rollback(transaction)

    assert host.active is old_binding
    assert old_binding.admission_open
    assert not staged.admission_open
    assert not new_target.active
    assert any(event.startswith("stop:") for event in child.events)
    assert any(event.startswith("close:") for event in child.events)
    assert any(event.startswith("restore:") for event in child.events)

    await host.close()
    await old_store.close()
    await new_store.close()


@pytest.mark.asyncio
async def test_activity_host_materialize_failure_closes_partial_children() -> None:
    class _FailingChild(_RecordingChild):
        name = "failing"

        async def materialize_closed(self, transaction_id, plan):
            raise RuntimeError("materialize failed")

    first = _RecordingChild()
    failing = _FailingChild()
    host = ActivityHost((first, failing))
    store, target = _stable_lease("failure")
    transaction = await host.prepare_transaction(target)
    await host.pause_and_drain(transaction)

    with pytest.raises(RuntimeError, match="materialize failed"):
        await host.materialize_closed(transaction)

    assert any(event.startswith("close:") for event in first.events)
    await host.rollback(transaction)
    assert not target.active
    await store.close()


@pytest.mark.asyncio
async def test_activity_host_prepare_failure_discards_prior_child_plans() -> None:
    class _PreparedChild(_RecordingChild):
        def discard_plan(self, transaction_id, plan):
            self.events.append(f"discard:{plan}")

    class _FailingPrepareChild(_RecordingChild):
        name = "failing-prepare"

        def prepare_components(self, transaction_id, target_lease, target_catalog):
            raise RuntimeError("prepare failed")

    child = _PreparedChild()
    host = ActivityHost((child, _FailingPrepareChild()))
    store, target = _stable_lease("prepare-failure")

    with pytest.raises(RuntimeError, match="prepare failed"):
        await host.prepare_transaction(target)

    assert child.events == ["prepare", f"discard:{target.snapshot.snapshot_id}"]
    assert not target.active
    await store.close()


@pytest.mark.asyncio
async def test_activity_host_retains_failed_rollback_for_exact_retry() -> None:
    class _FailCloseOnceChild(_RecordingChild):
        def __init__(self) -> None:
            super().__init__()
            self.fail_close = True

        async def close_components(self, transaction_id, binding):
            self.events.append(f"close:{binding}")
            if self.fail_close:
                self.fail_close = False
                raise RuntimeError("close failed")

    child = _FailCloseOnceChild()
    host = ActivityHost((child,))
    store, target = _stable_lease("rollback-retry")
    transaction = await host.prepare_transaction(target)
    await host.pause_and_drain(transaction)
    _ = await host.materialize_closed(transaction)

    with pytest.raises(RuntimeError, match="close failed"):
        await host.rollback(transaction)
    assert target.active
    await host.retry_recovery()

    assert not target.active
    assert host.active is None
    assert child.events.count(
        "close:binding:" + transaction.target_snapshot_id + ":1"
    ) == 2
    await store.close()


@pytest.mark.asyncio
async def test_committed_old_cleanup_failure_pauses_new_until_retry() -> None:
    class _FailOldCloseOnce(_RecordingChild):
        def __init__(self) -> None:
            super().__init__()
            self.fail_close = False

        async def close_components(self, transaction_id, binding):
            self.events.append(f"close:{binding}")
            if self.fail_close:
                self.fail_close = False
                raise RuntimeError("old cleanup failed")

    child = _FailOldCloseOnce()
    host = ActivityHost((child,))
    old_store, old_target = _stable_lease("old-commit-cleanup")
    initial = await host.prepare_transaction(old_target)
    await host.pause_and_drain(initial)
    _ = await host.materialize_closed(initial)
    host.finalize(initial)
    await host.open(initial)

    new_store, new_target = _stable_lease("new-commit-cleanup")
    transaction = await host.prepare_transaction(new_target)
    await host.pause_and_drain(transaction)
    staged = await host.materialize_closed(transaction)
    host.finalize(transaction)
    child.fail_close = True
    with pytest.raises(RuntimeError, match="old cleanup failed"):
        await host.open(transaction)

    assert host.active is staged
    assert not staged.admission_open
    assert child.events[-1] == f"pause:{staged.child_bindings['recording']}"
    await host.retry_recovery()
    assert staged.admission_open
    assert not new_target.active
    await host.close()
    await old_store.close()
    await new_store.close()


@pytest.mark.asyncio
async def test_activity_host_restores_child_that_changed_before_stop_failed() -> None:
    class _StopAfterChangeChild(_RecordingChild):
        async def stop_components(self, transaction_id, old_binding):
            self.events.append(f"stop:{old_binding}")
            raise RuntimeError("stop failed after change")

    child = _StopAfterChangeChild()
    host = ActivityHost((child,))
    old_store, old_target = _stable_lease("old-stop-failure")
    initial = await host.prepare_transaction(old_target)
    await host.pause_and_drain(initial)
    old_binding = await host.materialize_closed(initial)
    host.finalize(initial)
    await host.open(initial)

    new_store, new_target = _stable_lease("new-stop-failure")
    transaction = await host.prepare_transaction(new_target)
    with pytest.raises(RuntimeError, match="stop failed after change"):
        await host.pause_and_drain(transaction)
    await host.rollback(transaction)

    assert child.events[-2:] == [
        f"stop:{old_binding.child_bindings['recording']}",
        f"restore:{old_binding.child_bindings['recording']}",
    ]
    assert host.active is old_binding
    assert old_binding.admission_open
    await host.close()
    await old_store.close()
    await new_store.close()


@pytest.mark.asyncio
async def test_activity_host_retry_preserves_selected_rollback_direction() -> None:
    class _CloseOnceChild(_RecordingChild):
        def __init__(self) -> None:
            super().__init__()
            self.fail_next_close = False

        async def close_components(self, transaction_id, binding):
            self.events.append(f"close:{binding}")
            if self.fail_next_close:
                self.fail_next_close = False
                raise RuntimeError("staged close failed")

    child = _CloseOnceChild()
    host = ActivityHost((child,))
    old_store, old_target = _stable_lease("rollback-direction-old")
    initial = await host.prepare_transaction(old_target)
    await host.pause_and_drain(initial)
    old_binding = await host.materialize_closed(initial)
    host.finalize(initial)
    await host.open(initial)

    new_store, new_target = _stable_lease("rollback-direction-new")
    transaction = await host.prepare_transaction(new_target)
    await host.pause_and_drain(transaction)
    await host.materialize_closed(transaction)
    host.finalize(transaction)
    child.fail_next_close = True
    with pytest.raises(RuntimeError, match="staged close failed"):
        await host.rollback(transaction)

    await host.retry_recovery()

    assert host.active is old_binding
    assert old_binding.admission_open
    assert not new_target.active
    await host.close()
    await old_store.close()
    await new_store.close()


@pytest.mark.asyncio
async def test_activity_host_cancelled_drain_can_rollback_and_reopen_old() -> None:
    child = _RecordingChild()
    host = ActivityHost((child,))
    old_store, old_target = _stable_lease("old-cancel")
    initial = await host.prepare_transaction(old_target)
    await host.pause_and_drain(initial)
    old_binding = await host.materialize_closed(initial)
    host.finalize(initial)
    await host.open(initial)
    accepted_source = old_store.lease(old_binding.snapshot_id)
    accepted = host.acquire(accepted_source)
    await accepted_source.release()

    new_store, new_target = _stable_lease("new-cancel")
    transaction = await host.prepare_transaction(new_target)
    drain = asyncio.create_task(host.pause_and_drain(transaction))
    await asyncio.sleep(0)
    drain.cancel()
    with pytest.raises(asyncio.CancelledError):
        await drain

    await host.rollback(transaction)

    assert host.active is old_binding
    assert old_binding.admission_open
    await accepted.release()
    await host.close()
    await old_store.close()
    await new_store.close()


@pytest.mark.asyncio
async def test_activity_admission_lease_pins_exact_snapshot_until_release() -> None:
    drained: list[str] = []

    async def on_drained(snapshot) -> None:
        drained.append(snapshot.snapshot_id)

    child = _RecordingChild()
    host = ActivityHost((child,))
    store = RuntimeSnapshotStore(on_drained=on_drained)
    compiler = RuntimeSnapshotCompiler()
    old = compiler.compile({}, snapshot_revision="pin-old")
    new = compiler.compile({}, snapshot_revision="pin-new")
    store.install(old)
    initial = await host.prepare_transaction(store.lease(old.snapshot_id))
    await host.pause_and_drain(initial)
    binding = await host.materialize_closed(initial)
    host.finalize(initial)
    await host.open(initial)
    source_lease = store.lease(old.snapshot_id)
    accepted = host.acquire(source_lease)
    await source_lease.release()

    await store.commit(store.begin_publish(new))
    await asyncio.sleep(0)

    assert drained == []
    assert old.lease_count == 1
    await accepted.release()
    await store.retry_drains()
    assert drained == [old.snapshot_id]
    await host.close()
    await store.close()


@pytest.mark.asyncio
async def test_manager_activity_only_change_uses_closed_provisional_boundary(
    tmp_path,
) -> None:
    child = _RecordingChild()
    host = ActivityHost((child,))
    manager = PluginManager(
        [],
        event_bus=EventBus(),
        workspace=tmp_path / "workspace",
    )
    manager.bind_activity_host(host)
    compiler = RuntimeSnapshotCompiler()
    old_root = CompositionRoot("activity-old-root")
    new_root = CompositionRoot("activity-new-root")
    old = compiler.compile(
        {},
        snapshot_revision="activity-old",
        composition_root=old_root,
    )
    new = compiler.compile(
        {},
        snapshot_revision="activity-new",
        composition_root=new_root,
    )
    old.proactive_component_catalog = cast(
        Any,
        SimpleNamespace(
            identity="activity-old",
            root_instance_token=old_root.instance_token,
            descriptors=(),
        ),
    )
    old.proactive_component_catalog_identity = "activity-old"
    new.proactive_component_catalog = cast(
        Any,
        SimpleNamespace(
            identity="activity-new",
            root_instance_token=new_root.instance_token,
            descriptors=(),
        ),
    )
    new.proactive_component_catalog_identity = "activity-new"
    await manager._publish_committed_snapshot(old)
    old_binding = cast(Any, host.active).child_bindings["recording"]
    child.events.clear()
    await manager._publish_committed_snapshot(new)

    assert manager.current_snapshot is new
    assert new.accepting_leases
    assert host.active is not None
    assert host.active.snapshot_id == new.snapshot_id
    assert host.active.admission_open
    assert child.events == [
        "prepare",
        f"stop:{old_binding}",
        f"materialize:{new.snapshot_id}",
        f"finalize:binding:{new.snapshot_id}:2",
        f"close:{old_binding}",
    ]
    await host.close()
    await manager._snapshot_store.close()


@pytest.mark.asyncio
async def test_initial_stable_activity_prepare_failure_discards_pending_candidate(
    tmp_path,
) -> None:
    class _FailingChild:
        name = "private_proactive"

        def prepare_components(self, transaction_id, target_lease, target_catalog):
            raise RuntimeError("private prepare failed")

        async def stop_components(self, transaction_id, old_binding):
            return None

        async def materialize_closed(self, transaction_id, plan):
            return None

        def finalize_components(self, transaction_id, binding):
            return None

        def pause_components(self, binding):
            return None

        async def restore_components(self, transaction_id, old_binding):
            return None

        async def close_components(self, transaction_id, binding):
            return None

    host = ActivityHost((_FailingChild(),))
    manager = PluginManager(
        [],
        event_bus=EventBus(),
        workspace=tmp_path / "workspace",
    )
    manager.bind_activity_host(host)
    root = CompositionRoot("initial-stable-failure")
    module = ModuleType("plugins.test_stable_failure")
    module.api_version = 3
    module.name = "staged"
    module.version = "test"
    module.apply = lambda ctx, config: None
    instance = ComposablePlugin.from_module(module)
    generation = PluginGeneration(
        plugin_id="staged",
        generation_id="staged:generation",
        module_path="plugins.test_stable_failure",
        source_revision="source",
        config_revision="config",
        plugin_dir=tmp_path / "plugin",
        data_dir=tmp_path / "plugin-data",
        config=None,
        instance=instance,
        scope=PluginScope("staged"),
        contributions=PluginContributions(manifest={"name": "staged"}),
        gate_result=GateResult(
            gate_id="test",
            plugin_id="staged",
            candidate_revision="source",
            status="passed",
            checks=(),
        ),
    )
    private_catalog = PrivateProactiveCatalog(
        (),
        root_instance_token=root.instance_token,
    )
    snapshot = RuntimeSnapshotCompiler().compile(
        {generation.plugin_id: generation},
        snapshot_revision="initial-stable-failure",
        composition_root=root,
        private_proactive_catalog=private_catalog,
    )

    transaction = manager.snapshot_store.begin_publish(snapshot)

    with pytest.raises(RuntimeError, match="private prepare failed"):
        await manager._commit_snapshot_with_publication_participants(
            transaction,
            old_commands=(),
            new_commands=(),
            promote_latest=False,
        )

    assert manager.snapshot_store.pending_transaction is transaction
    await manager._discard_stable_batch(
        [generation],
        snapshot=snapshot,
        catalog_id=None,
    )

    assert manager.snapshot_store.pending_transaction is None
    assert manager.snapshot_store.pending_candidate is None
    await manager.snapshot_store.close()
