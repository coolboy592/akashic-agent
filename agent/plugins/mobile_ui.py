from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from typing import Protocol, cast

from agent.plugin_composition import (
    MobileUiBinding,
    MobileUiDescriptor,
    MobileUiQueryHandler,
    MobileUiRpcInvalidRequest,
)
from agent.plugins.generation import MobileUiAsset, PluginGeneration
from agent.plugins.manager import PluginManager
from agent.plugins.snapshot import RuntimeSnapshot

MOBILE_UI_QUERY_TIMEOUT_SECONDS = 20.0
MOBILE_UI_QUERY_WORKERS = 8
MOBILE_UI_QUERY_QUEUE_LIMIT = 16
logger = logging.getLogger(__name__)


class MobileUiProvider(Protocol):
    def catalog(self) -> dict[str, object]: ...

    def asset(
        self,
        plugin_id: str,
        plugin_revision: str,
        kind: str,
        sha256: str,
    ) -> dict[str, object]: ...

    async def query(
        self,
        plugin_id: str,
        plugin_revision: str,
        method: str,
        payload: dict[str, object],
        *,
        session_id: str | None,
        turn_id: str | None,
    ) -> dict[str, object]: ...


class PluginMobileUiProvider:
    """从插件快照提供版本化移动资源和只读查询。"""

    def __init__(self, manager: PluginManager) -> None:
        self._manager = manager
        self._executor = ThreadPoolExecutor(
            max_workers=MOBILE_UI_QUERY_WORKERS,
            thread_name_prefix="mobile-plugin-ui",
        )
        self._draining_queries: set[asyncio.Task[dict[str, object]]] = set()
        self._admission_lock = asyncio.Lock()
        self._admitted_queries = 0

    def catalog(self) -> dict[str, object]:
        """返回当前 generation 的轻量目录与内容摘要。"""

        snapshot = self._manager.current_snapshot
        items = [] if snapshot is None else self._catalog_items(snapshot)
        encoded = json.dumps(
            items,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
        return {
            "catalog_revision": hashlib.sha256(encoded).hexdigest(),
            "items": items,
        }

    def asset(
        self,
        plugin_id: str,
        plugin_revision: str,
        kind: str,
        sha256: str,
    ) -> dict[str, object]:
        """只在版本和摘要完全匹配时返回单个资源。"""

        snapshot = self._require_snapshot()
        generation = self._active_generation(snapshot, plugin_id)
        if generation.source_revision != plugin_revision:
            raise MobileUiStaleRevision(plugin_id)
        binding = self._mobile_ui_binding(snapshot, generation)
        if binding is None:
            raise MobileUiPluginUnavailable(plugin_id)
        content, expected_sha256 = _asset_content(binding.asset, kind)
        if expected_sha256 != sha256:
            raise MobileUiStaleRevision(plugin_id)
        return {
            "plugin_id": plugin_id,
            "plugin_revision": plugin_revision,
            "kind": kind,
            "sha256": expected_sha256,
            "content": content,
        }

    async def query(
        self,
        plugin_id: str,
        plugin_revision: str,
        method: str,
        payload: dict[str, object],
        *,
        session_id: str | None,
        turn_id: str | None,
    ) -> dict[str, object]:
        """在线程池执行只读 handler，并在超时后继续持有快照到线程退出。"""

        await self._reserve_query_slot()
        task = asyncio.create_task(
            self._run_query(
                plugin_id,
                plugin_revision,
                method,
                payload,
                session_id=session_id,
                turn_id=turn_id,
            )
        )
        task.add_done_callback(self._query_done)
        try:
            async with asyncio.timeout(MOBILE_UI_QUERY_TIMEOUT_SECONDS):
                return await asyncio.shield(task)
        except TimeoutError as error:
            self._drain_query(task)
            raise MobileUiQueryTimeout(
                f"插件 mobile UI query 超时: {plugin_id}.{method}"
            ) from error
        except asyncio.CancelledError:
            self._drain_query(task)
            raise

    async def _reserve_query_slot(self) -> None:
        """在提交线程池前拒绝超过有界 worker+queue 容量的查询。"""

        async with self._admission_lock:
            limit = MOBILE_UI_QUERY_WORKERS + MOBILE_UI_QUERY_QUEUE_LIMIT
            if self._admitted_queries >= limit:
                raise MobileUiQueryOverloaded(
                    "插件 mobile UI query 队列已满"
                )
            self._admitted_queries += 1

    def _query_done(self, completed: asyncio.Task[dict[str, object]]) -> None:
        self._admitted_queries -= 1
        if self._admitted_queries < 0:
            raise RuntimeError("插件 mobile UI query admission 计数失衡")
        self._draining_queries.discard(completed)
        if not completed.cancelled():
            _ = completed.exception()

    async def _run_query(
        self,
        plugin_id: str,
        plugin_revision: str,
        method: str,
        payload: dict[str, object],
        *,
        session_id: str | None,
        turn_id: str | None,
    ) -> dict[str, object]:
        """让一次线程查询完整占有对应插件 generation。"""

        async with await self._manager.snapshot_store.acquire() as snapshot:
            generation = self._active_generation(snapshot, plugin_id)
            if generation.source_revision != plugin_revision:
                raise MobileUiStaleRevision(plugin_id)
            binding = self._mobile_ui_binding(snapshot, generation)
            if binding is None:
                raise MobileUiPluginUnavailable(plugin_id)
            try:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    self._executor,
                    lambda: binding.query(
                        method,
                        payload,
                        session_id=session_id,
                        turn_id=turn_id,
                    ),
                )
            except MobileUiRpcInvalidRequest:
                raise
            except Exception as error:
                logger.exception("插件 mobile UI query 执行失败: %s.%s", plugin_id, method)
                raise MobileUiRpcExecutionError(
                    f"插件 mobile UI query 执行失败: {plugin_id}.{method}"
                ) from error
            try:
                normalized = _normalize_rpc_result(result, plugin_id=plugin_id, method=method)
            except Exception as error:
                logger.exception("插件 mobile UI query 返回无效: %s.%s", plugin_id, method)
                raise MobileUiRpcExecutionError(
                    f"插件 mobile UI query 返回无效: {plugin_id}.{method}"
                ) from error
            return normalized

    def _drain_query(self, task: asyncio.Task[dict[str, object]]) -> None:
        self._draining_queries.add(task)

    def _require_snapshot(self) -> RuntimeSnapshot:
        snapshot = self._manager.current_snapshot
        if snapshot is None:
            raise MobileUiPluginUnavailable("runtime snapshot unavailable")
        return snapshot

    @staticmethod
    def _active_generation(
        snapshot: RuntimeSnapshot,
        plugin_id: str,
    ) -> PluginGeneration:
        generation = snapshot.generations.get(plugin_id)
        if generation is None or generation not in snapshot.active_generations():
            raise MobileUiPluginUnavailable(plugin_id)
        return generation

    @staticmethod
    def _mobile_ui_binding(
        snapshot: RuntimeSnapshot,
        generation: PluginGeneration,
    ) -> MobileUiBinding | None:
        """Resolve v3 handlers from this snapshot, or use the frozen v2 triple."""

        # 1. v3 handlers live only in the exact composition Root registry.
        registry = getattr(snapshot, "mobile_ui_registry", None)
        if registry is not None:
            binding = registry.binding(generation.plugin_id)
            if binding is not None:
                if _has_legacy_mobile_ui_contribution(generation):
                    raise RuntimeError(
                        "插件同时存在 v2 与 v3 Mobile UI contribution: "
                        f"{generation.plugin_id}"
                    )
                if not binding.is_live():
                    return None
                return None if not binding.available() else binding

        # 2. v2 remains a frozen compatibility triple until its consumer moves.
        contributions = generation.contributions
        asset = getattr(contributions, "mobile_ui_asset", None)
        query = getattr(contributions, "mobile_ui_query", None)
        available = getattr(contributions, "mobile_ui_available", None)
        legacy_snapshot_shape = not hasattr(
            contributions,
            "mobile_ui_query",
        ) and not hasattr(contributions, "mobile_ui_available")
        # 仅兼容缺少三元组字段的旧快照；当前 contribution 缺字段视为损坏。
        if asset is not None and legacy_snapshot_shape:
            query = getattr(generation.instance, "mobile_ui_query", None)
            available = getattr(generation.instance, "mobile_ui_available", None)
        if asset is None:
            if query is not None or available is not None:
                raise RuntimeError(
                    "插件 v2 Mobile UI contribution 状态不一致: "
                    f"{generation.plugin_id}"
                )
            return None
        if query is None or available is None:
            raise RuntimeError(
                "插件 v2 Mobile UI contribution 缺少 handler: "
                f"{generation.plugin_id}"
            )
        if not available():
            return None
        return MobileUiBinding(
            descriptor=_legacy_descriptor(generation.plugin_id, asset),
            asset=asset,
            query=cast(MobileUiQueryHandler, query),
            available=available,
        )

    @staticmethod
    def _catalog_items(snapshot: RuntimeSnapshot) -> list[dict[str, object]]:
        items: list[dict[str, object]] = []
        for generation in snapshot.active_generations():
            binding = PluginMobileUiProvider._mobile_ui_binding(snapshot, generation)
            if binding is None:
                continue
            asset = binding.asset
            navigation: dict[str, object] | None = None
            if asset.navigation_label is not None:
                navigation = {
                    "label": asset.navigation_label,
                    "description": asset.navigation_description,
                }
            items.append(
                {
                    "id": generation.plugin_id,
                    "revision": generation.source_revision,
                    "module_sha256": asset.module_sha256,
                    "module_bytes": asset.module_bytes,
                    "stylesheet_sha256": asset.stylesheet_sha256,
                    "stylesheet_bytes": asset.stylesheet_bytes,
                    "navigation": navigation,
                    "slots": list(asset.slots),
                }
            )
        return items


def _asset_content(asset: MobileUiAsset, kind: str) -> tuple[str, str]:
    if kind == "module":
        return asset.module, asset.module_sha256
    if kind == "stylesheet" and asset.stylesheet_sha256 is not None:
        return asset.stylesheet, asset.stylesheet_sha256
    raise MobileUiPluginUnavailable(f"mobile UI asset 不存在: {kind}")


class MobileUiPluginUnavailable(LookupError):
    pass


class MobileUiStaleRevision(LookupError):
    pass


class MobileUiQueryTimeout(TimeoutError):
    pass


class MobileUiQueryOverloaded(RuntimeError):
    pass


class MobileUiRpcExecutionError(RuntimeError):
    pass


def _has_legacy_mobile_ui_contribution(generation: PluginGeneration) -> bool:
    contributions = generation.contributions
    return (
        getattr(contributions, "mobile_ui_asset", None) is not None
        or getattr(contributions, "mobile_ui_query", None) is not None
        or getattr(contributions, "mobile_ui_available", None) is not None
    )


def _legacy_descriptor(
    plugin_id: str,
    asset: MobileUiAsset,
) -> MobileUiDescriptor:
    return MobileUiDescriptor(
        owner=plugin_id,
        module_sha256=asset.module_sha256,
        module_bytes=asset.module_bytes,
        stylesheet_sha256=asset.stylesheet_sha256,
        stylesheet_bytes=asset.stylesheet_bytes,
        navigation_label=asset.navigation_label,
        navigation_description=asset.navigation_description,
        slots=asset.slots,
    )


def _normalize_rpc_result(
    result: object,
    *,
    plugin_id: str,
    method: str,
) -> dict[str, object]:
    """校验并规范化插件 RPC 返回对象。"""

    # 1. 校验返回结构和严格 JSON 值域
    if not isinstance(result, Mapping):
        raise TypeError(f"插件 mobile UI RPC 必须返回对象: {plugin_id}.{method}")
    mapping = cast(Mapping[object, object], result)
    normalized: dict[str, object] = {}
    active_containers: set[int] = set()
    for key, value in mapping.items():
        if not isinstance(key, str):
            raise TypeError(
                f"插件 mobile UI RPC 返回键必须是字符串: {plugin_id}.{method}"
            )
        _validate_json_value(
            value,
            plugin_id=plugin_id,
            method=method,
            active_containers=active_containers,
        )
        normalized[key] = value
    encoded = json.dumps(
        normalized,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )

    # 2. 限制移动端单次响应体积
    if len(encoded.encode("utf-8")) > 192 * 1024:
        raise ValueError(f"插件 mobile UI RPC 返回超过 192 KiB: {plugin_id}.{method}")
    return normalized


def _validate_json_value(
    value: object,
    *,
    plugin_id: str,
    method: str,
    active_containers: set[int],
) -> None:
    """Reject values outside the finite, recursively JSON-compatible result ABI."""

    if value is None or isinstance(value, (bool, int, str)):
        return
    if isinstance(value, float):
        if math.isfinite(value):
            return
        raise TypeError(
            f"插件 mobile UI RPC 返回浮点数必须有限: {plugin_id}.{method}"
        )
    if not isinstance(value, (list, dict)):
        raise TypeError(
            f"插件 mobile UI RPC 返回值不是严格 JSON 类型: {plugin_id}.{method}"
        )
    container_id = id(value)
    if container_id in active_containers:
        raise TypeError(f"插件 mobile UI RPC 返回值存在循环: {plugin_id}.{method}")
    active_containers.add(container_id)
    try:
        if isinstance(value, list):
            for item in value:
                _validate_json_value(
                    item,
                    plugin_id=plugin_id,
                    method=method,
                    active_containers=active_containers,
                )
            return
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(
                    "插件 mobile UI RPC 嵌套对象键必须是字符串: "
                    f"{plugin_id}.{method}"
                )
            _validate_json_value(
                item,
                plugin_id=plugin_id,
                method=method,
                active_containers=active_containers,
            )
    finally:
        active_containers.remove(container_id)
