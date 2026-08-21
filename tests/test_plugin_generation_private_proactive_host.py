from __future__ import annotations

import importlib
from types import ModuleType, SimpleNamespace
from typing import Any, cast

import pytest

from agent.plugins.generation_activity_host import ActivityCatalog, ActivityHost
from agent.plugins.generation_private_proactive_host import PrivateProactiveHost
from agent.plugins.private_proactive import (
    PRIVATE_PROACTIVE_DEFINITIONS,
    PrivateProactiveCatalog,
    PrivateProactiveRegistry,
)
from agent.plugins.snapshot import RuntimeSnapshotLease


class _Store:
    async def release_lease(self, snapshot: object) -> None:
        return None


def _catalog(
    *,
    excluded: frozenset[str] = frozenset(),
) -> PrivateProactiveCatalog:
    registry = PrivateProactiveRegistry()
    for definition in PRIVATE_PROACTIVE_DEFINITIONS:
        if definition.member in excluded:
            continue
        module = importlib.import_module(f"plugins.{definition.member}.plugin")
        assert isinstance(module, ModuleType)
        registry.register(
            module,
            source_revision=f"source:{definition.member}",
            generation_id=f"generation:{definition.member}",
        )
    return registry.freeze()


def _lease(
    *,
    catalog: PrivateProactiveCatalog | None,
    snapshot_id: str = "private-snapshot",
) -> RuntimeSnapshotLease:
    snapshot = SimpleNamespace(
        snapshot_id=snapshot_id,
        proactive_component_catalog=None,
        background_job_catalog=None,
        private_proactive_catalog=catalog,
    )
    return RuntimeSnapshotLease(cast(Any, _Store()), cast(Any, snapshot))


@pytest.mark.asyncio
async def test_prepare_is_descriptor_only_and_materialize_is_closed() -> None:
    catalog = _catalog()
    target = _lease(catalog=catalog)
    host = PrivateProactiveHost("default")
    activity_catalog = ActivityCatalog(None, None, private_proactive=catalog)

    plan = host.prepare_components("tx-1", target, activity_catalog)

    assert plan.member_names == (
        "default_proactive",
        "proactive_flow",
        "drift_flow",
    )
    assert host.export_resolution_count == 0
    assert host.lifecycle_invocation_count == 0
    assert host.factory_instantiation_count == 0

    binding = await host.materialize_closed("tx-1", plan)

    assert binding.admission_open is False
    assert binding.active is False
    assert host.export_resolution_count == 5
    assert host.lifecycle_invocation_count == 1
    # Runtime provider plus the three ordered flow factories, including the
    # primary member's DefaultModuleFactory.
    assert host.factory_instantiation_count == 4

    host.finalize_components("tx-1", binding)
    assert binding.active
    await host.close_components("shutdown:direct", binding)


@pytest.mark.asyncio
async def test_empty_catalog_materializes_non_consumable_noop_binding() -> None:
    target = _lease(catalog=None)
    host = PrivateProactiveHost("default")
    activity_catalog = ActivityCatalog(None, None)

    plan = host.prepare_components("tx-1", target, activity_catalog)
    binding = await host.materialize_closed("tx-1", plan)
    host.finalize_components("tx-1", binding)

    assert binding.catalog is None
    assert binding.family is None
    assert binding.active is False
    assert host.export_resolution_count == 0
    assert host.lifecycle_invocation_count == 0
    assert host.factory_instantiation_count == 0

    await host.close_components("shutdown:direct", binding)


@pytest.mark.parametrize(
    ("family", "members"),
    (
        (
            "default",
            ("default_proactive", "proactive_flow", "drift_flow"),
        ),
        (
            "wake",
            ("wake_proactive", "wake_proactive_flow", "wake_drift_flow"),
        ),
    ),
)
@pytest.mark.parametrize("missing_index", range(3))
def test_partial_family_rejected_during_prepare(
    family: str,
    members: tuple[str, ...],
    missing_index: int,
) -> None:
    catalog = _catalog(excluded=frozenset({members[missing_index]}))
    target = _lease(catalog=catalog)
    host = PrivateProactiveHost(family)

    with pytest.raises(RuntimeError, match="family 不完整"):
        host.prepare_components(
            "tx-partial",
            target,
            ActivityCatalog(None, None, private_proactive=catalog),
        )

    assert host.export_resolution_count == 0
    assert host.lifecycle_invocation_count == 0
    assert host.factory_instantiation_count == 0


@pytest.mark.asyncio
async def test_activity_host_shutdown_closes_private_binding_with_synthetic_id() -> None:
    catalog = _catalog()
    target = _lease(catalog=catalog)
    child = PrivateProactiveHost("default")
    activity = ActivityHost((child,))

    transaction = await activity.prepare_transaction(target)
    await activity.pause_and_drain(transaction)
    binding = await activity.materialize_closed(transaction)
    activity.finalize(transaction)
    await activity.open(transaction)

    assert activity.active is binding
    child_binding = cast(Any, binding.child_bindings["private_proactive"])
    assert child.active is child_binding
    await activity.close()

    assert child_binding.closed
    assert child.active is None


@pytest.mark.asyncio
async def test_reload_closes_previous_private_binding_across_transactions() -> None:
    child = PrivateProactiveHost("default")
    activity = ActivityHost((child,))

    first = await activity.prepare_transaction(
        _lease(catalog=_catalog(), snapshot_id="private-snapshot-v1")
    )
    await activity.pause_and_drain(first)
    first_binding = await activity.materialize_closed(first)
    first_private = cast(Any, first_binding.child_bindings["private_proactive"])
    activity.finalize(first)
    await activity.open(first)

    second = await activity.prepare_transaction(
        _lease(catalog=_catalog(), snapshot_id="private-snapshot-v2")
    )
    await activity.pause_and_drain(second)
    second_binding = await activity.materialize_closed(second)
    second_private = cast(Any, second_binding.child_bindings["private_proactive"])
    activity.finalize(second)
    await activity.open(second)

    assert first_private.closed
    assert child.active is second_private
    await activity.close()


@pytest.mark.asyncio
async def test_reload_rollback_restores_previous_private_binding_across_transactions() -> None:
    child = PrivateProactiveHost("default")
    activity = ActivityHost((child,))

    first = await activity.prepare_transaction(
        _lease(catalog=_catalog(), snapshot_id="private-snapshot-v1")
    )
    await activity.pause_and_drain(first)
    first_binding = await activity.materialize_closed(first)
    first_private = cast(Any, first_binding.child_bindings["private_proactive"])
    activity.finalize(first)
    await activity.open(first)

    second = await activity.prepare_transaction(
        _lease(catalog=_catalog(), snapshot_id="private-snapshot-v2")
    )
    await activity.pause_and_drain(second)
    await activity.rollback(second)

    assert child.active is first_private
    assert first_private.active
    await activity.close()
