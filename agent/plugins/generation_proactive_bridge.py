"""Bridge the committed v3 proactive catalog into the existing Core tick runtime."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from agent.plugin_composition.proactive import (
    AckCommitted,
    AckFailure,
    AckSkipped,
    FetchEmpty,
    FetchFailure,
    FetchItems,
    FetchSkip,
)
from agent.plugins.generation_activity_host import ActivityHost
from agent.plugins.generation_proactive_host import (
    ProactiveModuleFacade,
    ProactiveRuntimeBinding,
    ProactiveSourceFacade,
)
from agent.plugins.snapshot import RuntimeSnapshotLease
from agent.plugins.specs import ProactiveSourceSpec, RegisteredProactiveSource
from proactive_v2.frame import ProactiveFrame
from proactive_v2.mcp_sources import (
    McpGateway,
    SourceFetchFailed,
    SourceFetchSkipped,
)

if TYPE_CHECKING:
    from agent.plugins.generation_private_proactive_host import PrivateProactiveBinding


_V3_SERVER_PREFIX = "__v3_proactive__:"
_CURRENT_EXECUTION_LEASE: ContextVar[RuntimeSnapshotLease | None] = ContextVar(
    "v3_proactive_execution_lease",
    default=None,
)


class CommittedProactiveBridge:
    """Project one exact Activity binding into source and lifecycle adapters."""

    def __init__(self, activity_host: ActivityHost) -> None:
        self._activity_host = activity_host

    @property
    def activity_host(self) -> ActivityHost:
        return self._activity_host

    def runtime_for(self, snapshot: object) -> ProactiveRuntimeBinding:
        """Resolve the exact proactive child already finalized with this snapshot."""

        binding = self._activity_host.active
        snapshot_id = getattr(snapshot, "snapshot_id", None)
        if binding is None or binding.snapshot_id != snapshot_id:
            raise RuntimeError("Proactive Activity binding 与 stable snapshot 不匹配")
        runtime = binding.child_bindings.get("proactive_components")
        if not isinstance(runtime, ProactiveRuntimeBinding):
            raise RuntimeError("Activity binding 缺少 proactive_components child")
        if runtime.snapshot is not snapshot:
            raise RuntimeError("Proactive child 没有绑定 exact snapshot")
        return runtime

    def private_binding_for(self, snapshot: object) -> PrivateProactiveBinding:
        """Resolve the exact Core-private Default/Wake child for one snapshot."""

        from agent.plugins.generation_private_proactive_host import (
            PrivateProactiveBinding,
        )

        binding = self._activity_host.active
        snapshot_id = getattr(snapshot, "snapshot_id", None)
        if binding is None or binding.snapshot_id != snapshot_id:
            raise RuntimeError("Private proactive Activity binding 与 snapshot 不匹配")
        private = binding.child_bindings.get("private_proactive")
        if not isinstance(private, PrivateProactiveBinding):
            raise RuntimeError("Activity binding 缺少 private proactive child")
        if private.snapshot is not snapshot:
            raise RuntimeError("private proactive child 没有绑定 exact snapshot")
        catalog = getattr(snapshot, "private_proactive_catalog", None)
        if private.catalog is not catalog:
            raise RuntimeError("private proactive child catalog identity 不匹配")
        if not private.active:
            raise RuntimeError("private proactive Activity binding 尚未开放")
        return private

    def registered_sources(
        self,
        runtime: ProactiveRuntimeBinding,
    ) -> list[RegisteredProactiveSource]:
        """Expose v3 sources through collision-free legacy runtime descriptors."""

        return [
            RegisteredProactiveSource(
                plugin_id=facade.descriptor.owner,
                spec=ProactiveSourceSpec(
                    id=facade.name,
                    channels=cast(Any, facade.descriptor.channels),
                    server=_virtual_server(key),
                    fetch_tool=facade.descriptor.fetch_tool,
                    ack_tool=facade.descriptor.ack_tool,
                    fetch_page_size=0,
                ),
            )
            for key, facade in runtime.sources.items()
        ]

    def lifecycle_modules(
        self,
        runtime: ProactiveRuntimeBinding,
        *,
        lifecycle_id: str,
    ) -> list[object]:
        """Adapt exact v3 module facades to the existing proactive DAG runner."""

        return [
            _V3LifecycleModule(self, facade)
            for facade in runtime.modules.values()
            if facade.descriptor.lifecycle_id == lifecycle_id
        ]

    def gateway(
        self,
        base: McpGateway,
        runtime: ProactiveRuntimeBinding,
    ) -> McpGateway:
        """Route only virtual v3 source servers through exact source facades."""

        sources = {
            _virtual_server(key): facade for key, facade in runtime.sources.items()
        }
        return _CommittedSourceGateway(self, base, sources)

    def bind_execution(
        self,
        snapshot_lease: RuntimeSnapshotLease,
    ) -> Token[RuntimeSnapshotLease | None]:
        """Publish one parent tick lease to child source tasks without current lookup."""

        if not snapshot_lease.active:
            raise RuntimeError("v3 proactive execution source lease 已失效")
        return _CURRENT_EXECUTION_LEASE.set(snapshot_lease)

    def reset_execution(self, token: Token[RuntimeSnapshotLease | None]) -> None:
        _CURRENT_EXECUTION_LEASE.reset(token)

    def fork_execution_lease(self) -> RuntimeSnapshotLease:
        source = _CURRENT_EXECUTION_LEASE.get()
        if source is None or not source.active:
            raise RuntimeError("v3 proactive execution 缺少 exact RuntimeSnapshotLease")
        return source.fork()


@dataclass(frozen=True, slots=True)
class _V3LifecycleModule:
    bridge: CommittedProactiveBridge
    facade: ProactiveModuleFacade

    @property
    def slot(self) -> str:
        return self.facade.descriptor.slot

    @property
    def requires(self) -> tuple[str, ...]:
        return self.facade.descriptor.requires

    @property
    def produces(self) -> tuple[str, ...]:
        return self.facade.descriptor.produces

    @property
    def collects(self) -> tuple[str, ...]:
        return self.facade.descriptor.collects

    async def run(self, frame: ProactiveFrame) -> ProactiveFrame:
        lease = self.bridge.fork_execution_lease()
        try:
            return await self.facade.transform(lease, frame)
        finally:
            await _release_critical(lease)


class _CommittedSourceGateway:
    """Translate legacy source polling into exact typed v3 fetch and ack calls."""

    def __init__(
        self,
        bridge: CommittedProactiveBridge,
        base: McpGateway,
        sources: Mapping[str, ProactiveSourceFacade],
    ) -> None:
        self._bridge = bridge
        self._base = base
        self._sources = dict(sources)

    async def call(
        self,
        server: str,
        tool_name: str,
        args: dict[str, Any],
        *,
        timeout: float | None = None,
    ) -> Any:
        facade = self._sources.get(server)
        if facade is None:
            return await self._base.call(server, tool_name, args, timeout=timeout)
        lease = self._bridge.fork_execution_lease()
        descriptor = facade.descriptor
        try:
            if tool_name == descriptor.fetch_tool:
                return await self._fetch_all(facade, lease)
            if descriptor.ack_tool and tool_name == descriptor.ack_tool:
                ids = args.get("event_ids")
                if not isinstance(ids, list) or any(not isinstance(item, str) for item in ids):
                    raise TypeError("v3 proactive ack event_ids 必须是字符串 list")
                feedback = args.get("feedback")
                if feedback is not None and not isinstance(feedback, str):
                    raise TypeError("v3 proactive ack feedback 必须是字符串")
                result = await facade.ack(lease, ids, feedback=feedback)
                if isinstance(result, AckCommitted):
                    return {"status": "committed", "ids": list(result.ids)}
                if isinstance(result, AckSkipped):
                    return {"status": "skipped", "reason": result.reason}
                assert isinstance(result, AckFailure)
                raise RuntimeError(result.error)
            raise RuntimeError(f"v3 proactive source tool 未声明: {server}.{tool_name}")
        finally:
            await _release_critical(lease)

    async def _fetch_all(
        self,
        facade: ProactiveSourceFacade,
        lease: Any,
    ) -> object:
        items: list[object] = []
        cursor: str | None = None
        seen: set[str] = set()
        for _ in range(256):
            result = await facade.fetch(lease, cursor=cursor)
            if isinstance(result, FetchItems):
                items.extend(_thaw(item) for item in result.items)
                if result.cursor is None:
                    break
                if result.cursor in seen:
                    raise RuntimeError("v3 proactive source cursor 重复")
                seen.add(result.cursor)
                cursor = result.cursor
                continue
            if isinstance(result, FetchEmpty):
                if result.cursor is None:
                    break
                if result.cursor in seen:
                    raise RuntimeError("v3 proactive source cursor 重复")
                seen.add(result.cursor)
                cursor = result.cursor
                continue
            if isinstance(result, FetchSkip):
                raise SourceFetchSkipped(result.reason, result.retry_at)
            assert isinstance(result, FetchFailure)
            raise SourceFetchFailed(result.error, result.retryable)
        else:
            raise RuntimeError("v3 proactive source 超过 256 页")
        if facade.descriptor.channels == ("context",) and len(items) == 1:
            return items[0]
        return items


def _virtual_server(key: str) -> str:
    return _V3_SERVER_PREFIX + key


def _thaw(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    if isinstance(value, frozenset):
        return [_thaw(item) for item in sorted(value, key=repr)]
    return value


async def _release_critical(lease: RuntimeSnapshotLease) -> None:
    task = asyncio.create_task(lease.release())
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    await task
    if cancelled:
        raise asyncio.CancelledError


__all__ = ["CommittedProactiveBridge"]
