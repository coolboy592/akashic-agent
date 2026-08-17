"""Core-private ActivityHost child for the Default/Wake proactive island."""

from __future__ import annotations

from dataclasses import dataclass, field
from collections.abc import Callable
from typing import cast

from agent.plugin_composition.proactive import ProactiveCatalog
from agent.plugins.generation_activity_host import ActivityCatalog
from agent.plugins.private_proactive import (
    PRIVATE_PROACTIVE_DEFINITIONS,
    PrivateFamily,
    PrivateProactiveCatalog,
    PrivateProactiveMember,
)
from agent.plugins.snapshot import RuntimeSnapshotLease
from proactive_v2.lifecycle import ProactiveLifecycleSpec


@dataclass(frozen=True, slots=True)
class PrivateProactivePlan:
    """Freeze exact descriptors before the shared host drains the old binding."""

    transaction_id: str
    snapshot_id: str
    catalog_identity: str
    target_lease: RuntimeSnapshotLease
    catalog: PrivateProactiveCatalog | None
    family: PrivateFamily | None
    member_names: tuple[str, ...]
    export_names: tuple[tuple[str, ...], ...]
    source_catalog: ProactiveCatalog | None
    members: tuple[PrivateProactiveMember, ...]


@dataclass(slots=True)
class PrivateProactiveBinding:
    """Hold only closed factory instances and the exact snapshot identity."""

    transaction_id: str
    snapshot_id: str
    catalog_identity: str
    catalog: PrivateProactiveCatalog | None
    family: PrivateFamily | None
    snapshot: object = field(repr=False)
    lifecycle: ProactiveLifecycleSpec | None
    runtime_factory: object | None
    module_factories: tuple[object, ...]
    source_catalog: ProactiveCatalog | None
    admission_open: bool = False
    stopped: bool = False
    closed: bool = False

    @property
    def active(self) -> bool:
        return (
            self.catalog is not None
            and self.family is not None
            and self.admission_open
            and not self.stopped
            and not self.closed
        )


class PrivateProactiveHost:
    """Implement the ActivityChildAdapter without owning publication or leases."""

    name = "private_proactive"
    _family: PrivateFamily | None

    def __init__(self, family: str | None = None) -> None:
        if family not in {None, "default", "wake"}:
            raise ValueError(f"private proactive family 无效: {family!r}")
        self._family = cast(PrivateFamily | None, family)
        self._plans: dict[str, PrivateProactivePlan] = {}
        self._bindings: dict[str, PrivateProactiveBinding] = {}
        self._active: PrivateProactiveBinding | None = None
        self._export_resolution_count = 0
        self._lifecycle_invocation_count = 0
        self._factory_instantiation_count = 0

    @property
    def active(self) -> PrivateProactiveBinding | None:
        return self._active

    @property
    def export_resolution_count(self) -> int:
        return self._export_resolution_count

    @property
    def lifecycle_invocation_count(self) -> int:
        return self._lifecycle_invocation_count

    @property
    def factory_instantiation_count(self) -> int:
        return self._factory_instantiation_count

    def prepare_components(
        self,
        transaction_id: str,
        target_lease: RuntimeSnapshotLease,
        target_catalog: ActivityCatalog,
    ) -> PrivateProactivePlan:
        """Build a pure plan from the exact target lease and private catalog."""

        # 1. Validate the immutable publication boundary.
        if not target_lease.active:
            raise RuntimeError("private proactive target lease 已失效")
        catalog = target_catalog.private_proactive
        snapshot_catalog = target_lease.snapshot.private_proactive_catalog
        if catalog is not None and snapshot_catalog is not catalog:
            raise RuntimeError("private proactive target catalog 与 snapshot 不匹配")
        if catalog is None and snapshot_catalog is not None:
            raise RuntimeError("private proactive target catalog 缺少 snapshot catalog")
        if target_catalog.private_proactive_identity != (
            None if catalog is None else catalog.identity
        ):
            raise RuntimeError("private proactive target catalog identity 已漂移")
        family = None if catalog is None else self._select_family(catalog)
        members = () if catalog is None else catalog.family(cast(PrivateFamily, family))
        if catalog is not None and not members:
            raise RuntimeError(f"private proactive family 缺少 member: {family}")

        # 2. Freeze only descriptors.  Export lookup and all code execution wait
        # for materialize_closed after ActivityHost has drained the old binding.
        plan = PrivateProactivePlan(
            transaction_id=transaction_id,
            snapshot_id=target_lease.snapshot.snapshot_id,
            catalog_identity="" if catalog is None else catalog.identity,
            target_lease=target_lease,
            catalog=catalog,
            family=family,
            member_names=tuple(member.member for member in members),
            export_names=tuple(member.export_names for member in members),
            source_catalog=target_catalog.proactive,
            members=members,
        )
        self._plans[transaction_id] = plan
        return plan

    async def stop_components(
        self,
        transaction_id: str,
        old_binding: PrivateProactiveBinding,
    ) -> None:
        """Close old private admission after ActivityHost has drained callers."""

        self._require_binding(old_binding)
        old_binding.admission_open = False
        old_binding.stopped = True

    async def materialize_closed(
        self,
        transaction_id: str,
        plan: PrivateProactivePlan,
    ) -> PrivateProactiveBinding:
        """Instantiate only factory objects while the target admission is closed."""

        self._require_plan(transaction_id, plan)
        if not plan.target_lease.active:
            raise RuntimeError("private proactive target snapshot lease 已释放")
        if plan.target_lease.snapshot.snapshot_id != plan.snapshot_id:
            raise RuntimeError("private proactive plan snapshot identity 不匹配")
        if plan.catalog is None:
            binding = PrivateProactiveBinding(
                transaction_id=transaction_id,
                snapshot_id=plan.snapshot_id,
                catalog_identity="",
                catalog=None,
                family=None,
                snapshot=plan.target_lease.snapshot,
                lifecycle=None,
                runtime_factory=None,
                module_factories=(),
                source_catalog=plan.source_catalog,
            )
            self._bindings[binding.snapshot_id] = binding
            return binding
        try:
            primary = plan.members[0]
            runtime_export = _first_export(
                primary,
                ("DefaultRuntimeFactory", "WakeRuntimeFactory"),
                self,
            )
            lifecycle_export = _first_export(
                primary,
                ("build_default_lifecycle", "build_wake_lifecycle"),
                self,
            )
            self._lifecycle_invocation_count += 1
            lifecycle_builder = cast(Callable[[], ProactiveLifecycleSpec], lifecycle_export)
            lifecycle = lifecycle_builder()
            module_exports = tuple(
                _module_export(member, self)
                for member in plan.members
            )
            runtime_factory = _instantiate(runtime_export, cast(PrivateFamily, plan.family), self)
            module_factories = tuple(
                _instantiate(factory, cast(PrivateFamily, plan.family), self)
                for factory in module_exports
            )
            binding = PrivateProactiveBinding(
                transaction_id=transaction_id,
                snapshot_id=plan.snapshot_id,
                catalog_identity=plan.catalog_identity,
                catalog=plan.catalog,
                family=plan.family,
                snapshot=plan.target_lease.snapshot,
                lifecycle=lifecycle,
                runtime_factory=runtime_factory,
                module_factories=module_factories,
                source_catalog=plan.source_catalog,
            )
        except BaseException:
            self._plans.pop(transaction_id, None)
            raise
        self._bindings[binding.snapshot_id] = binding
        return binding

    def finalize_components(
        self,
        transaction_id: str,
        binding: PrivateProactiveBinding,
    ) -> None:
        """Open private admission synchronously at ActivityHost's pointer boundary."""

        self._require_transaction_binding(transaction_id, binding)
        if binding.closed:
            raise RuntimeError("private proactive binding 已关闭")
        binding.admission_open = True
        binding.stopped = False
        self._active = binding
        self._plans.pop(transaction_id, None)

    def discard_plan(self, transaction_id: str, plan: PrivateProactivePlan) -> None:
        """Discard a plan when a later Activity child rejects the transaction."""

        if self._plans.get(transaction_id) is plan:
            self._plans.pop(transaction_id, None)

    def pause_components(self, binding: PrivateProactiveBinding) -> None:
        """Reject private execution while ActivityHost retains a failed transaction."""

        self._require_transaction_binding(binding.transaction_id, binding)
        binding.admission_open = False

    async def restore_components(
        self,
        transaction_id: str,
        old_binding: PrivateProactiveBinding,
    ) -> None:
        """Restore the old private binding during the shared rollback path."""

        self._require_binding(old_binding)
        if old_binding.closed:
            raise RuntimeError("private proactive binding 已关闭")
        old_binding.stopped = False
        old_binding.admission_open = True
        self._active = old_binding

    async def close_components(
        self,
        transaction_id: str,
        binding: PrivateProactiveBinding,
    ) -> None:
        """Drop one closed binding without touching the ActivityHost lease."""

        self._require_binding(binding)
        if binding.closed:
            return
        binding.admission_open = False
        binding.stopped = True
        binding.closed = True
        if self._bindings.get(binding.snapshot_id) is binding:
            self._bindings.pop(binding.snapshot_id, None)
        if self._active is binding:
            self._active = None
        self._plans.pop(binding.transaction_id, None)

    async def aclose(self) -> None:
        """Close all materialized private bindings during shutdown."""

        for binding in tuple(self._bindings.values()):
            await self.close_components("shutdown", binding)

    def _select_family(self, catalog: PrivateProactiveCatalog) -> PrivateFamily:
        if self._family is not None:
            family: PrivateFamily = self._family
            self._require_complete_family(catalog, family)
            return family
        available_families: tuple[PrivateFamily, ...] = ("default", "wake")
        families = tuple(
            family for family in available_families if catalog.family(family)
        )
        if len(families) != 1:
            raise RuntimeError("private proactive family 不唯一，请由 Core 显式指定")
        selected = cast(PrivateFamily, families[0])
        self._require_complete_family(catalog, selected)
        return selected

    @staticmethod
    def _require_complete_family(
        catalog: PrivateProactiveCatalog,
        family: PrivateFamily,
    ) -> None:
        """Reject a partial family before any export or lifecycle code runs."""

        expected = tuple(
            definition
            for definition in PRIVATE_PROACTIVE_DEFINITIONS
            if definition.family == family
        )
        members = catalog.family(family)
        actual = tuple(member.definition for member in members)
        if actual != expected:
            expected_names = tuple(item.member for item in expected)
            actual_names = tuple(item.member for item in members)
            raise RuntimeError(
                "private proactive family 不完整或顺序不匹配: "
                f"family={family} expected={expected_names} actual={actual_names}"
            )

    def _require_plan(self, transaction_id: str, plan: PrivateProactivePlan) -> None:
        if self._plans.get(transaction_id) is not plan:
            raise RuntimeError("private proactive plan 已失效")
        if plan.transaction_id != transaction_id:
            raise RuntimeError("private proactive plan transaction 不匹配")

    def _require_binding(self, binding: PrivateProactiveBinding) -> None:
        if self._bindings.get(binding.snapshot_id) is not binding:
            raise RuntimeError("private proactive binding 不属于当前 adapter")

    def _require_transaction_binding(
        self,
        transaction_id: str,
        binding: PrivateProactiveBinding,
    ) -> None:
        self._require_binding(binding)
        is_shutdown = transaction_id == "shutdown" or transaction_id.startswith(
            "shutdown:"
        )
        if binding.transaction_id != transaction_id and not is_shutdown:
            raise RuntimeError("private proactive binding transaction 不匹配")


def _first_export(
    member: PrivateProactiveMember,
    names: tuple[str, ...],
    host: PrivateProactiveHost,
) -> object:
    for name in names:
        if name in member.export_names:
            host._export_resolution_count += 1
            return member.resolve_export(name)
    raise RuntimeError(f"private proactive member 缺少 export: {member.member}")


def _module_export(member: PrivateProactiveMember, host: PrivateProactiveHost) -> object:
    for name in member.export_names:
        if name.endswith("ModuleFactory"):
            host._export_resolution_count += 1
            return member.resolve_export(name)
    raise RuntimeError(f"private proactive member 缺少 module factory: {member.member}")


def _instantiate(factory: object, family: PrivateFamily, host: PrivateProactiveHost) -> object:
    if not isinstance(factory, type):
        raise RuntimeError(f"private proactive {family} factory export 必须是 class")
    host._factory_instantiation_count += 1
    return factory()


__all__ = (
    "PrivateProactiveBinding",
    "PrivateProactiveHost",
    "PrivateProactivePlan",
)
