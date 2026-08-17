"""Generation-bound execution facade for the v3 proactive catalog.

The activity host owns publication, admission, and the target snapshot lease.
This module only materializes closed source/module facades and checks the exact
lease again at each execution boundary.
"""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Protocol, TypeAlias, cast

from agent.plugin_composition.proactive import (
    AckCommitted,
    AckFailure,
    AckResult,
    AckSkipped,
    FetchEmpty,
    FetchFailure,
    FetchItems,
    FetchResult,
    FetchSkip,
    ProactiveCatalog,
    ProactiveModuleBinding,
    ProactiveModuleDefinition,
    ProactiveSourceBinding,
)
from agent.plugins.composable import ComposablePlugin
from agent.plugins.generation_activity_host import ActivityCatalog
from agent.plugins.generation_job_host import DomainEffectContext, ProactiveDomainEffects
from agent.plugins.snapshot import RuntimeSnapshotLease
from proactive_v2.frame import ProactiveFrame

ProactiveModuleOutcome: TypeAlias = ProactiveFrame


class ProactiveMcpRoute(Protocol):
    """Describe the narrow MCP call surface required by a source facade."""

    async def call(
        self,
        server: str,
        tool_name: str,
        arguments: Mapping[str, object],
        *,
        snapshot_lease: RuntimeSnapshotLease,
    ) -> object: ...


@dataclass(frozen=True, slots=True)
class ProactiveModuleContext:
    """Expose only one module invocation's snapshot and domain-effect view."""

    snapshot_id: str
    generation_id: str
    slot: str
    domain_effects: ProactiveDomainEffects | None = None


@dataclass(frozen=True, slots=True)
class ProactiveActivityPlan:
    """Freeze catalog bindings and the exact target lease before materialization."""

    transaction_id: str
    snapshot_id: str
    catalog_identity: str
    target_lease: RuntimeSnapshotLease
    catalog: ProactiveCatalog | None
    source_bindings: tuple[tuple[str, ProactiveSourceBinding], ...]
    module_bindings: tuple[tuple[str, ProactiveModuleBinding], ...]

    @property
    def sources(self) -> tuple[ProactiveSourceBinding, ...]:
        return tuple(binding for _, binding in self.source_bindings)

    @property
    def modules(self) -> tuple[ProactiveModuleBinding, ...]:
        return tuple(binding for _, binding in self.module_bindings)

    @property
    def bindings(self) -> tuple[ProactiveSourceBinding | ProactiveModuleBinding, ...]:
        return (*self.sources, *self.modules)


@dataclass(frozen=True, slots=True)
class _MaterializedModule:
    binding: ProactiveModuleBinding
    handler: Callable[
        [ProactiveModuleContext, ProactiveFrame],
        Awaitable[ProactiveModuleOutcome | None],
    ]
    domain_effect_lookup: Callable[
        [DomainEffectContext], object | Awaitable[object]
    ] | None = None


@dataclass(slots=True)
class ProactiveRuntimeBinding:
    """Closed or open source/module facades from one exact activity plan."""

    transaction_id: str
    snapshot_id: str
    catalog_identity: str
    catalog: ProactiveCatalog | None
    snapshot: object = field(repr=False)
    sources: Mapping[str, ProactiveSourceFacade] = field(default_factory=dict)
    modules: Mapping[str, ProactiveModuleFacade] = field(default_factory=dict)
    admission_open: bool = False
    stopped: bool = False
    closed: bool = False
    _adapter: ProactiveActivityAdapter | None = field(repr=False, default=None)

    @property
    def active(self) -> bool:
        return self.admission_open and not self.stopped and not self.closed

    def source(self, name: str) -> ProactiveSourceFacade:
        """Resolve one canonical or unique bare source name."""

        facade = self.sources.get(name)
        if facade is not None:
            return facade
        matches = tuple(item for item in self.sources.values() if item.name == name)
        if len(matches) != 1:
            raise KeyError(f"Proactive source 不存在或不唯一: {name}")
        return matches[0]

    def module(self, slot: str) -> ProactiveModuleFacade:
        """Resolve one canonical or unique bare module slot."""

        facade = self.modules.get(slot)
        if facade is not None:
            return facade
        matches = tuple(item for item in self.modules.values() if item.slot == slot)
        if len(matches) != 1:
            raise KeyError(f"Proactive module 不存在或不唯一: {slot}")
        return matches[0]


class ProactiveSourceFacade:
    """Execute one source's fetch/ack tools through an exact snapshot lease."""

    __slots__ = ("_runtime", "_key", "_binding")

    def __init__(
        self,
        runtime: ProactiveRuntimeBinding,
        key: str,
        binding: ProactiveSourceBinding,
    ) -> None:
        self._runtime = runtime
        self._key = key
        self._binding = binding

    @property
    def key(self) -> str:
        return self._key

    @property
    def name(self) -> str:
        return self._binding.descriptor.name

    @property
    def descriptor(self):
        return self._binding.descriptor

    async def fetch(
        self,
        snapshot_lease: RuntimeSnapshotLease,
        *,
        cursor: str | None = None,
    ) -> FetchResult:
        """Fetch one typed page after validating admission and exact identity."""

        self._runtime_owner()._authorize(self._runtime, snapshot_lease, self._binding)
        self._runtime_owner()._source_fetch_invocations += 1
        definition = self._binding.definition
        arguments: dict[str, object] = {}
        if cursor is not None:
            arguments["cursor"] = cursor
        if definition.fetch_page_size:
            arguments["limit"] = definition.fetch_page_size
        try:
            raw = await self._runtime_owner()._call_route(
                snapshot_lease,
                self._binding,
                definition.mcp_server,
                definition.fetch_tool,
                arguments,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            return FetchFailure(_error_text(error), retryable=True)
        return _fetch_result(raw)

    async def ack(
        self,
        snapshot_lease: RuntimeSnapshotLease,
        ids: Sequence[str] | None = None,
        *,
        event_ids: Sequence[str] | None = None,
        feedback: str | None = None,
    ) -> AckResult:
        """Acknowledge source IDs as a separate typed stage."""

        self._runtime_owner()._authorize(self._runtime, snapshot_lease, self._binding)
        if ids is None:
            ids = event_ids
        elif event_ids is not None:
            raise TypeError("Proactive ack 不能同时提供 ids 与 event_ids")
        if ids is None:
            raise TypeError("Proactive ack 必须提供 ids")
        normalized_ids = tuple(ids)
        if any(
            not isinstance(item, str) or not item.strip() for item in normalized_ids
        ):
            raise TypeError("Proactive ack ids 必须是非空字符串")
        if not normalized_ids:
            return AckSkipped("no_ids")
        definition = self._binding.definition
        if not definition.ack_tool:
            return AckSkipped("ack_not_declared")
        self._runtime_owner()._ack_invocations += 1
        arguments: dict[str, object] = {"event_ids": list(normalized_ids)}
        if feedback is not None:
            arguments["feedback"] = feedback
        try:
            raw = await self._runtime_owner()._call_route(
                snapshot_lease,
                self._binding,
                definition.mcp_server,
                definition.ack_tool,
                arguments,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            return AckFailure(_error_text(error), retryable=True)
        return _ack_result(raw, normalized_ids)

    fetch_page = fetch
    acknowledge = ack

    def _runtime_owner(self) -> ProactiveActivityAdapter:
        return cast(ProactiveActivityAdapter, getattr(self._runtime, "_adapter"))


class ProactiveModuleFacade:
    """Run one exact async module export over the current frame projection."""

    __slots__ = ("_runtime", "_key", "_materialized")

    def __init__(
        self,
        runtime: ProactiveRuntimeBinding,
        key: str,
        materialized: _MaterializedModule,
    ) -> None:
        self._runtime = runtime
        self._key = key
        self._materialized = materialized

    @property
    def key(self) -> str:
        return self._key

    @property
    def slot(self) -> str:
        return self._materialized.binding.descriptor.slot

    @property
    def descriptor(self):
        return self._materialized.binding.descriptor

    async def transform(
        self,
        snapshot_lease: RuntimeSnapshotLease,
        frame: ProactiveFrame,
    ) -> ProactiveModuleOutcome:
        """Run the exact module handler while its binding and lease remain live."""

        adapter = self._runtime_owner()
        binding = self._materialized.binding
        adapter._authorize(self._runtime, snapshot_lease, binding)
        if not isinstance(frame, ProactiveFrame):
            raise TypeError("Proactive module frame 必须是 ProactiveFrame")
        effects: ProactiveDomainEffects | None = None
        if binding.definition.domain_effect is not None:
            lookup = self._materialized.domain_effect_lookup
            if lookup is None:
                raise RuntimeError(
                    "Proactive module domain_effect 缺少 exact lookup export"
                )
            tick_id = _proactive_tick_id(frame)
            semantic_job_id = f"{binding.descriptor.owner}:{binding.descriptor.slot}"
            effects = ProactiveDomainEffects(
                context=DomainEffectContext(
                    invocation_id=f"proactive:{semantic_job_id}:{tick_id}",
                    plugin_id=binding.descriptor.owner,
                    job_name=binding.descriptor.slot,
                    semantic_job_id=semantic_job_id,
                    event_id=tick_id,
                    snapshot_id=self._runtime.snapshot_id,
                    effect_id=binding.definition.domain_effect,
                    idempotency_key=f"{tick_id}:{semantic_job_id}",
                    attempt=1,
                    generation_id=binding.generation_id,
                    tick_id=tick_id,
                ),
                lookup=lookup,
            )
        adapter._module_invocations += 1
        context = ProactiveModuleContext(
            snapshot_id=self._runtime.snapshot_id,
            generation_id=binding.generation_id,
            slot=binding.descriptor.slot,
            domain_effects=effects,
        )
        try:
            result = await self._materialized.handler(context, frame)
            if result is None:
                return frame
            if not isinstance(result, ProactiveFrame):
                raise TypeError("Proactive module handler 必须返回 ProactiveFrame 或 None")
            return result
        finally:
            if effects is not None:
                effects.close()

    run = transform

    def _runtime_owner(self) -> ProactiveActivityAdapter:
        return cast(ProactiveActivityAdapter, getattr(self._runtime, "_adapter"))


class ProactiveActivityAdapter:
    """Materialize the proactive child without owning publication or its lease."""

    name = "proactive_components"

    def __init__(self, mcp_route: object | None = None):
        self._mcp_route = mcp_route
        self._plans: dict[str, ProactiveActivityPlan] = {}
        self._bindings: dict[str, ProactiveRuntimeBinding] = {}
        self._active: ProactiveRuntimeBinding | None = None
        self._handler_resolution_count = 0
        self._source_fetch_invocations = 0
        self._ack_invocations = 0
        self._module_invocations = 0

    @property
    def active_binding(self) -> ProactiveRuntimeBinding | None:
        return self._active

    @property
    def handler_resolution_count(self) -> int:
        return self._handler_resolution_count

    @property
    def source_fetch_invocations(self) -> int:
        return self._source_fetch_invocations

    @property
    def source_invocation_count(self) -> int:
        return self._source_fetch_invocations

    @property
    def ack_invocations(self) -> int:
        return self._ack_invocations

    @property
    def module_invocations(self) -> int:
        return self._module_invocations

    @property
    def source_count(self) -> int:
        return sum(len(binding.sources) for binding in self._bindings.values())

    @property
    def module_count(self) -> int:
        return sum(len(binding.modules) for binding in self._bindings.values())

    def prepare_components(
        self,
        transaction_id: str,
        target_lease: RuntimeSnapshotLease,
        target_catalog: ActivityCatalog | ProactiveCatalog,
    ) -> ProactiveActivityPlan:
        """Validate the frozen target without resolving handlers or starting work."""

        # 1. Validate only the Activity-owned target identity.
        _require_transaction_id(transaction_id)
        if not isinstance(target_lease, RuntimeSnapshotLease):
            raise TypeError("Proactive target lease 必须是 RuntimeSnapshotLease")
        if not target_lease.active:
            raise RuntimeError("Proactive target snapshot lease 已失效")
        if isinstance(target_catalog, ActivityCatalog):
            catalog = target_catalog.proactive
        elif isinstance(target_catalog, ProactiveCatalog):
            catalog = target_catalog
        else:
            raise TypeError(
                "Proactive target catalog 必须是 ActivityCatalog 或 ProactiveCatalog"
            )
        snapshot = target_lease.snapshot
        snapshot_catalog = snapshot.proactive_component_catalog
        if snapshot_catalog is not catalog:
            if snapshot_catalog is None or catalog is None:
                raise RuntimeError("Proactive target catalog 与 snapshot 不匹配")
            if snapshot_catalog.identity != catalog.identity:
                raise RuntimeError(
                    "Proactive target catalog 与 snapshot identity 不匹配"
                )
        if transaction_id in self._plans:
            raise RuntimeError("Proactive transaction 已存在")

        # 2. Freeze exact source/module bindings; no handler, route, or lease is touched.
        source_bindings: tuple[tuple[str, ProactiveSourceBinding], ...] = ()
        module_bindings: tuple[tuple[str, ProactiveModuleBinding], ...] = ()
        if catalog is not None:
            source_bindings = tuple(catalog.sources.items())
            module_bindings = tuple(catalog.modules.items())
            for _, binding in (*source_bindings, *module_bindings):
                generation = snapshot.generations.get(binding.owner)
                if generation is None:
                    raise RuntimeError(
                        f"Proactive owner 不属于 target snapshot: {binding.owner}"
                    )
                if generation.generation_id != binding.generation_id:
                    raise RuntimeError(
                        "Proactive binding generation identity 不匹配: "
                        f"{binding.owner}:{binding.generation_id}"
                    )
        plan = ProactiveActivityPlan(
            transaction_id=transaction_id,
            snapshot_id=snapshot.snapshot_id,
            catalog_identity="" if catalog is None else catalog.identity,
            target_lease=target_lease,
            catalog=catalog,
            source_bindings=source_bindings,
            module_bindings=module_bindings,
        )
        self._plans[transaction_id] = plan
        return plan

    async def materialize_closed(
        self,
        transaction_id: str,
        plan: ProactiveActivityPlan,
    ) -> ProactiveRuntimeBinding:
        """Resolve exact module exports and return a closed, non-admitting binding."""

        # 1. Check the pure plan before crossing into handler resolution.
        expected = self._plans.get(transaction_id)
        if expected is not plan or plan.transaction_id != transaction_id:
            raise RuntimeError("Proactive materialization plan 已失效")
        if not plan.target_lease.active:
            raise RuntimeError("Proactive target snapshot lease 已释放")
        snapshot = plan.target_lease.snapshot
        if snapshot.snapshot_id != plan.snapshot_id:
            raise RuntimeError("Proactive target snapshot identity 已变化")
        if plan.snapshot_id in self._bindings:
            raise RuntimeError("Proactive snapshot 已 materialize")

        # 2. Resolve only exact ComposablePlugin exports from this target snapshot.
        resolved: list[_MaterializedModule] = []
        try:
            for key, binding in plan.module_bindings:
                generation = snapshot.generations.get(binding.owner)
                if (
                    generation is None
                    or generation.generation_id != binding.generation_id
                ):
                    raise RuntimeError(
                        f"Proactive module generation 不可用: {binding.owner}:{binding.descriptor.slot}"
                    )
                if (
                    binding.descriptor.handler_export
                    != binding.definition.handler_export
                ):
                    raise RuntimeError(
                        "Proactive module descriptor/export 不一致: "
                        f"{binding.owner}:{binding.descriptor.slot}"
                    )
                handler = self._resolve_handler(
                    generation.instance,
                    binding.definition,
                )
                lookup = None
                if binding.definition.domain_effect is not None:
                    if (
                        binding.descriptor.domain_effect_lookup_export
                        != binding.definition.domain_effect_lookup_export
                    ):
                        raise RuntimeError(
                            "Proactive module descriptor/lookup export 不一致: "
                            f"{binding.owner}:{binding.descriptor.slot}"
                        )
                    lookup = self._resolve_domain_lookup(
                        generation.instance,
                        binding.definition.domain_effect_lookup_export,
                    )
                resolved.append(
                    _MaterializedModule(
                        binding=binding,
                        handler=handler,
                        domain_effect_lookup=lookup,
                    )
                )
            runtime = ProactiveRuntimeBinding(
                transaction_id=transaction_id,
                snapshot_id=plan.snapshot_id,
                catalog_identity=plan.catalog_identity,
                catalog=plan.catalog,
                snapshot=snapshot,
            )
            runtime._adapter = self
            runtime.modules = MappingProxyType(
                {
                    key: ProactiveModuleFacade(runtime, key, item)
                    for (key, _), item in zip(
                        plan.module_bindings, resolved, strict=True
                    )
                }
            )
            runtime.sources = MappingProxyType(
                {
                    key: ProactiveSourceFacade(runtime, key, binding)
                    for key, binding in plan.source_bindings
                }
            )
            self._bindings[runtime.snapshot_id] = runtime
            self._handler_resolution_count += len(resolved)
            return runtime
        except BaseException:
            # There are no child-owned tasks or leases to release; leave no partial binding.
            self._bindings.pop(plan.snapshot_id, None)
            self._plans.pop(transaction_id, None)
            raise

    def discard_plan(self, transaction_id: str, plan: ProactiveActivityPlan) -> None:
        """Discard a plan when a later Activity child rejects the transaction."""

        if self._plans.get(transaction_id) is plan:
            self._plans.pop(transaction_id, None)

    async def stop_components(
        self,
        transaction_id: str,
        old_binding: ProactiveRuntimeBinding,
    ) -> None:
        """Close child admission after ActivityHost has drained accepted work."""

        self._require_binding(transaction_id, old_binding)
        old_binding.admission_open = False
        old_binding.stopped = True

    def finalize_components(
        self,
        transaction_id: str,
        binding: ProactiveRuntimeBinding,
    ) -> None:
        """Synchronously open child admission at ActivityHost's pointer boundary."""

        self._require_binding(transaction_id, binding)
        if binding.transaction_id != transaction_id:
            raise RuntimeError("Proactive finalize transaction 与 binding 不匹配")
        if binding.closed:
            raise RuntimeError("Proactive binding 已关闭")
        binding.admission_open = True
        binding.stopped = False
        self._active = binding
        self._plans.pop(transaction_id, None)

    def pause_components(self, binding: ProactiveRuntimeBinding) -> None:
        """Synchronously reject new source/module calls during cleanup recovery."""

        if self._bindings.get(binding.snapshot_id) is not binding:
            raise RuntimeError("Proactive binding 不属于当前 adapter")
        binding.admission_open = False

    async def restore_components(
        self,
        transaction_id: str,
        old_binding: ProactiveRuntimeBinding,
    ) -> None:
        """Reopen an old child after ActivityHost rolls back a publication."""

        self._require_binding(transaction_id, old_binding)
        old_binding.stopped = False
        old_binding.admission_open = True
        self._active = old_binding

    async def close_components(
        self,
        transaction_id: str,
        binding: ProactiveRuntimeBinding,
    ) -> None:
        """Close one binding and drop only this adapter's in-memory references."""

        self._require_binding(transaction_id, binding)
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
        """Close every materialized child without touching publication leases."""

        for binding in tuple(self._bindings.values()):
            await self.close_components("shutdown", binding)

    def _authorize(
        self,
        runtime: ProactiveRuntimeBinding,
        snapshot_lease: RuntimeSnapshotLease,
        binding: ProactiveSourceBinding | ProactiveModuleBinding,
    ) -> None:
        if runtime.closed or runtime.stopped or not runtime.admission_open:
            raise RuntimeError("Proactive admission 已关闭")
        if not isinstance(snapshot_lease, RuntimeSnapshotLease):
            raise TypeError("Proactive execution lease 必须是 RuntimeSnapshotLease")
        if not snapshot_lease.active:
            raise RuntimeError("Proactive execution lease 已失效")
        if snapshot_lease.snapshot is not runtime.snapshot:
            raise RuntimeError("Proactive execution lease snapshot identity 不匹配")
        snapshot_catalog = runtime.snapshot.proactive_component_catalog
        if snapshot_catalog is not runtime.catalog:
            if snapshot_catalog is None or runtime.catalog is None:
                raise RuntimeError("Proactive runtime catalog 已变化")
            if snapshot_catalog.identity != runtime.catalog.identity:
                raise RuntimeError("Proactive runtime catalog identity 已变化")
        if not binding.is_live():
            raise RuntimeError("Proactive binding 已失效或 owner Fiber 不再 live")
        generation = runtime.snapshot.generations.get(binding.owner)
        if generation is None:
            raise RuntimeError("Proactive binding owner 已不在 snapshot")
        if generation.generation_id != binding.generation_id:
            raise RuntimeError("Proactive binding generation identity 已变化")

    async def _call_route(
        self,
        snapshot_lease: RuntimeSnapshotLease,
        binding: ProactiveSourceBinding,
        server: str,
        tool_name: str,
        arguments: Mapping[str, object],
    ) -> object:
        route = self._mcp_route
        if route is None:
            raise RuntimeError("Proactive source 没有 Core-owned MCP route")
        selected = _select_route(route, binding.generation_id, server)
        invoker = getattr(selected, "call", None)
        if invoker is None:
            if not callable(selected):
                raise TypeError("Proactive MCP route 必须提供 async call")
            invoker = selected
        result = _invoke_route(
            invoker,
            snapshot_lease,
            binding.generation_id,
            server,
            tool_name,
            arguments,
        )
        if not inspect.isawaitable(result):
            raise TypeError("Proactive MCP route.call 必须返回 awaitable")
        return await cast(Awaitable[object], result)

    def _resolve_handler(
        self,
        instance: object,
        definition: ProactiveModuleDefinition,
    ) -> Callable[
        [ProactiveModuleContext, ProactiveFrame],
        Awaitable[ProactiveModuleOutcome | None],
    ]:
        if not isinstance(instance, ComposablePlugin):
            raise RuntimeError("Proactive module owner 不是 ComposablePlugin")
        value: object = instance.module
        export = definition.handler_export
        for segment in export.replace(":", ".").split("."):
            if not segment:
                raise RuntimeError(f"Proactive module handler_export 无效: {export}")
            try:
                value = getattr(value, segment)
            except AttributeError as error:
                raise RuntimeError(
                    f"Proactive module handler_export 不存在: {instance.name}:{export}"
                ) from error
        if not inspect.iscoroutinefunction(value):
            raise TypeError(f"Proactive module handler 必须是 async: {export}")
        signature = inspect.signature(value)
        try:
            signature.bind(None, None)
        except TypeError as error:
            raise TypeError(
                "Proactive module handler 必须精确接受 ctx, frame: " + export
            ) from error
        if len(signature.parameters) != 2:
            raise TypeError(
                "Proactive module handler 必须精确接受 ctx, frame: " + export
            )
        return cast(
            Callable[
                [ProactiveModuleContext, ProactiveFrame],
                Awaitable[ProactiveModuleOutcome | None],
            ],
            value,
        )

    def _resolve_domain_lookup(
        self,
        instance: object,
        lookup_export: str | None,
    ) -> Callable[[DomainEffectContext], object | Awaitable[object]]:
        """Resolve one exact synchronous domain receipt lookup export."""

        if lookup_export is None:
            raise RuntimeError("Proactive module domain effect 缺少 lookup export")
        if not isinstance(instance, ComposablePlugin):
            raise RuntimeError("Proactive module owner 不是 ComposablePlugin")
        value: object = instance.module
        for segment in lookup_export.replace(":", ".").split("."):
            if not segment:
                raise RuntimeError(
                    f"Proactive module domain_effect_lookup_export 无效: {lookup_export}"
                )
            try:
                value = getattr(value, segment)
            except AttributeError as error:
                raise RuntimeError(
                    "Proactive module domain lookup export 不存在: "
                    f"{instance.name}:{lookup_export}"
                ) from error
        if not callable(value):
            raise TypeError("Proactive module domain lookup export 必须是 callable")
        if inspect.iscoroutinefunction(value):
            raise TypeError("Proactive module domain lookup export 必须是同步 callable")
        signature = inspect.signature(value)
        try:
            signature.bind(None)
        except TypeError as error:
            raise TypeError(
                "Proactive module domain lookup export 必须精确接受一个 context"
            ) from error
        if len(signature.parameters) != 1:
            raise TypeError(
                "Proactive module domain lookup export 必须精确接受一个 context"
            )
        return cast(
            Callable[[DomainEffectContext], object | Awaitable[object]],
            value,
        )

    def _require_binding(
        self,
        transaction_id: str,
        binding: ProactiveRuntimeBinding,
    ) -> None:
        _require_transaction_id(transaction_id)
        if binding.closed:
            raise RuntimeError("Proactive binding 已关闭")
        if self._bindings.get(binding.snapshot_id) is not binding:
            raise RuntimeError("Proactive binding 不属于当前 adapter")


# Keep the generation-host spelling aligned with the background-job child.
GenerationProactiveHost = ProactiveActivityAdapter


class _RouteFailure:
    __slots__ = ("error",)

    def __init__(self, error: str) -> None:
        self.error = error


def _select_route(route: object, generation_id: str, server: str) -> object:
    if isinstance(route, Mapping):
        selected = route.get((generation_id, server))
        if selected is None:
            selected = route.get(server)
        if selected is None:
            raise RuntimeError(f"Proactive MCP route 不存在: {generation_id}:{server}")
        return selected
    route_for = getattr(route, "route_for", None)
    if callable(route_for):
        return route_for(generation_id, server)
    return route


def _invoke_route(
    invoker: Callable[..., object],
    snapshot_lease: RuntimeSnapshotLease,
    generation_id: str,
    server: str,
    tool_name: str,
    arguments: Mapping[str, object],
) -> object:
    try:
        signature = inspect.signature(invoker)
    except (TypeError, ValueError) as error:
        raise TypeError("Proactive MCP route.call 必须可检查签名") from error
    parameters = tuple(signature.parameters.values())
    if "snapshot_lease" in signature.parameters:
        return invoker(
            server,
            tool_name,
            arguments,
            snapshot_lease=snapshot_lease,
        )
    if "lease" in signature.parameters:
        return invoker(
            server,
            tool_name,
            arguments,
            lease=snapshot_lease,
        )
    positional = tuple(
        parameter
        for parameter in parameters
        if parameter.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    )
    has_varargs = any(
        parameter.kind is inspect.Parameter.VAR_POSITIONAL for parameter in parameters
    )
    if has_varargs or len(positional) >= 5:
        return invoker(snapshot_lease, generation_id, server, tool_name, arguments)
    if len(positional) == 4:
        return invoker(snapshot_lease, server, tool_name, arguments)
    if len(positional) >= 3:
        return invoker(server, tool_name, arguments)
    if len(positional) >= 2:
        return invoker(tool_name, arguments)
    raise TypeError("Proactive MCP route.call 参数不足")


def _fetch_result(raw: object) -> FetchResult:
    payload = _decode_route_result(raw)
    if isinstance(payload, _RouteFailure):
        return FetchFailure(payload.error, retryable=True)
    if isinstance(payload, (FetchItems, FetchEmpty, FetchSkip, FetchFailure)):
        return payload
    try:
        if payload is None:
            return FetchEmpty()
        if isinstance(payload, list):
            return FetchItems(tuple(payload))
        if isinstance(payload, Mapping):
            status = str(payload.get("status", "")).lower()
            if status in {"empty", "fetch_empty"}:
                return FetchEmpty(_optional_cursor(payload.get("cursor")))
            if status in {"skip", "fetch_skip"}:
                return FetchSkip(
                    str(payload["reason"]),
                    payload.get("retry_at"),
                )
            if status in {"failure", "fetch_failure", "error"}:
                return FetchFailure(
                    str(payload["error"]),
                    bool(payload.get("retryable", True)),
                )
            if "items" in payload:
                items = payload["items"]
                if not isinstance(items, (list, tuple)):
                    raise TypeError("fetch items 必须是序列")
                return FetchItems(
                    tuple(items),
                    _optional_cursor(payload.get("cursor", payload.get("next_cursor"))),
                )
            raise TypeError("fetch result 缺少 items/status")
        raise TypeError(f"fetch result 类型无效: {type(payload).__name__}")
    except (KeyError, TypeError, ValueError) as error:
        return FetchFailure(_error_text(error), retryable=False)


def _ack_result(raw: object, ids: tuple[str, ...]) -> AckResult:
    payload = _decode_route_result(raw)
    if isinstance(payload, _RouteFailure):
        return AckFailure(payload.error, retryable=True)
    if isinstance(payload, (AckCommitted, AckSkipped, AckFailure)):
        return payload
    if payload is None:
        return AckFailure("ACK response 缺少明确 committed 结果", retryable=True)
    if isinstance(payload, str):
        return AckFailure(
            "ACK response 必须是 typed result 或带 committed status 的 JSON object",
            retryable=False,
        )
    if not isinstance(payload, Mapping):
        return AckFailure(
            "ACK response 类型无效；不能从无 status 的序列推断 committed",
            retryable=False,
        )

    status = payload.get("status")
    if not isinstance(status, str) or not status.strip():
        return AckFailure("ACK response 缺少明确 status", retryable=False)
    normalized_status = status.strip().lower()
    if normalized_status in {"skipped", "ack_skipped"}:
        reason = payload.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            return AckFailure("ACK skipped response 缺少 reason", retryable=False)
        return AckSkipped(reason)
    if normalized_status in {"failure", "ack_failure", "error"}:
        error = payload.get("error")
        if not isinstance(error, str) or not error.strip():
            return AckFailure("ACK failure response 缺少 error", retryable=False)
        retryable = payload.get("retryable", True)
        if not isinstance(retryable, bool):
            return AckFailure("ACK failure retryable 必须是 bool", retryable=False)
        return AckFailure(error, retryable=retryable)
    if normalized_status not in {"committed", "ack_committed", "success"}:
        return AckFailure(
            f"ACK response status 未授权为 committed: {status}",
            retryable=False,
        )
    values = payload.get("ids")
    if not isinstance(values, (list, tuple)):
        return AckFailure(
            "ACK committed response 必须包含 ids 序列",
            retryable=False,
        )
    try:
        committed_ids = tuple(cast(Sequence[str], values))
        if not committed_ids:
            raise ValueError("ACK committed response 的 ids 不能为空")
        if any(item not in ids for item in committed_ids):
            raise ValueError("ACK committed response 包含未请求的 id")
        return AckCommitted(committed_ids)
    except (TypeError, ValueError) as error:
        return AckFailure(_error_text(error), retryable=False)


def _decode_route_result(raw: object) -> object:
    status = getattr(raw, "status", None)
    if isinstance(raw, Mapping):
        status = raw.get("status")
        if status in {"success", "tool_error"}:
            if status == "tool_error":
                return _RouteFailure(str(raw.get("output", "MCP tool error")))
            raw = raw.get("output")
            status = None
    if status in {"success", "tool_error"}:
        if status == "tool_error":
            return _RouteFailure(str(getattr(raw, "output", "MCP tool error")))
        raw = getattr(raw, "output", None)
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as error:
            return _RouteFailure(_error_text(error))
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return raw
    return raw


def _optional_cursor(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("fetch cursor 必须是字符串或 None")
    return value


def _require_transaction_id(value: object) -> None:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError("transaction_id 必须是非空且无首尾空白的字符串")


def _proactive_tick_id(frame: ProactiveFrame) -> str:
    """Derive the stable tick identity shared by one frame retry."""

    session_key = frame.input.session_key
    started_at = frame.input.started_at
    if not isinstance(session_key, str) or not session_key.strip():
        raise TypeError("Proactive tick session_key 必须是非空字符串")
    if (
        not isinstance(started_at, datetime)
        or started_at.tzinfo is None
        or started_at.utcoffset() is None
    ):
        raise TypeError("Proactive tick started_at 必须是 timezone-aware datetime")
    return f"{session_key}:{started_at.isoformat()}"


def _error_text(error: BaseException) -> str:
    message = str(error).strip()
    return f"{type(error).__name__}: {message}" if message else type(error).__name__


__all__ = [
    "GenerationProactiveHost",
    "DomainEffectContext",
    "ProactiveDomainEffects",
    "ProactiveActivityPlan",
    "ProactiveActivityAdapter",
    "ProactiveMcpRoute",
    "ProactiveModuleContext",
    "ProactiveModuleFacade",
    "ProactiveModuleOutcome",
    "ProactiveRuntimeBinding",
    "ProactiveSourceFacade",
]
