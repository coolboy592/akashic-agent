from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from agent.plugin_composition.model import CompositionError, FiberState

if TYPE_CHECKING:
    from agent.plugin_composition.context import Fiber

P = TypeVar("P")
R = TypeVar("R")


def _validate_event_name(name: str) -> None:
    if not name or name.strip() != name:
        raise ValueError("事件名称必须是非空且无首尾空白的字符串")


@dataclass(frozen=True, slots=True)
class EmitEventKey(Generic[P]):
    name: str

    def __post_init__(self) -> None:
        _validate_event_name(self.name)


@dataclass(frozen=True, slots=True)
class SerialEventKey(Generic[P, R]):
    name: str

    def __post_init__(self) -> None:
        _validate_event_name(self.name)


@dataclass(frozen=True, slots=True)
class ParallelEventKey(Generic[P]):
    name: str

    def __post_init__(self) -> None:
        _validate_event_name(self.name)


@dataclass(frozen=True, slots=True)
class Bail(Generic[R]):
    value: R


EventKey = EmitEventKey[object] | SerialEventKey[object, object] | ParallelEventKey[object]
EventListener = Callable[[object], object]


@dataclass(frozen=True, slots=True)
class _Listener:
    owner: "Fiber"
    callback: EventListener


class EventRegistry:
    """Own typed listeners and execute one frozen listener list per dispatch."""

    def __init__(self) -> None:
        self._listeners: dict[EventKey, list[_Listener]] = {}
        self._modes: dict[str, type[object]] = {}

    def register(
        self,
        owner: "Fiber",
        key: EventKey,
        callback: EventListener,
    ) -> Callable[[], None]:
        """Validate and publish one listener until its owning Effect closes."""

        # 1. One event name has one dispatch contract for the whole Root.
        mode = type(key)
        existing_mode = self._modes.get(key.name)
        if existing_mode is not None and existing_mode is not mode:
            raise CompositionError(
                "EVENT_MODE_CONFLICT",
                f"事件 {key.name} 已声明为 {existing_mode.__name__}",
            )
        if isinstance(key, EmitEventKey) and _is_async_callable(callback):
            raise CompositionError(
                "ASYNC_LISTENER_ON_EMIT",
                f"同步事件 {key.name} 不能注册异步 listener",
            )
        if isinstance(key, ParallelEventKey) and not _is_async_callable(callback):
            raise CompositionError(
                "SYNC_LISTENER_ON_PARALLEL",
                f"并发事件 {key.name} 只能注册异步 listener",
            )

        # 2. Registration order is the only listener order contract.
        self._modes[key.name] = mode
        listener = _Listener(owner=owner, callback=callback)
        listeners = self._listeners.setdefault(key, [])
        listeners.append(listener)

        def remove() -> None:
            current = self._listeners.get(key)
            if current is None or listener not in current:
                return
            current.remove(listener)
            if current:
                return
            del self._listeners[key]
            _ = self._modes.pop(key.name, None)

        return remove

    def emit(self, key: EmitEventKey[P], payload: P) -> None:
        for listener in self._active_listeners(cast(EventKey, key)):
            result = listener.callback(payload)
            if inspect.isawaitable(result):
                _close_unexpected_awaitable(result)
                raise CompositionError(
                    "ASYNC_RESULT_FROM_EMIT",
                    f"同步事件 {key.name} 的 listener 返回了 awaitable",
                )

    async def serial(
        self,
        key: SerialEventKey[P, R],
        payload: P,
    ) -> Bail[R] | None:
        for listener in self._active_listeners(cast(EventKey, key)):
            result = listener.callback(payload)
            if inspect.isawaitable(result):
                result = await result
            if result is None:
                continue
            if isinstance(result, Bail):
                return cast(Bail[R], result)
            raise CompositionError(
                "INVALID_SERIAL_RESULT",
                f"串行事件 {key.name} 的 listener 只能返回 None 或 Bail",
            )
        return None

    async def parallel(self, key: ParallelEventKey[P], payload: P) -> None:
        listeners = self._active_listeners(cast(EventKey, key))
        tasks = [
            asyncio.create_task(
                _run_parallel_listener(listener.callback, payload),
                name=f"plugin-event:{key.name}:{listener.owner.name}",
            )
            for listener in listeners
        ]
        if not tasks:
            return
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError as cancellation:
            for task in tasks:
                _ = task.cancel()
            await _drain_tasks(tasks)
            raise cancellation
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise BaseExceptionGroup(f"并发事件失败: {key.name}", errors)

    def registrations(self) -> tuple[str, ...]:
        return tuple(
            f"{_mode_name(key)}:{key.name}:{listener.owner.name}"
            for key, listeners in self._listeners.items()
            for listener in listeners
        )

    def _active_listeners(self, key: EventKey) -> tuple[_Listener, ...]:
        return tuple(
            listener
            for listener in self._listeners.get(key, ())
            if listener.owner.state == FiberState.ACTIVE
        )


async def _run_parallel_listener(callback: EventListener, payload: object) -> None:
    result = callback(payload)
    if not inspect.isawaitable(result):
        raise CompositionError(
            "SYNC_RESULT_FROM_PARALLEL",
            "并发事件 listener 没有返回 awaitable",
        )
    _ = await result


async def _drain_tasks(tasks: list[asyncio.Task[None]]) -> None:
    """Drain cancelled listeners despite repeated caller cancellation."""

    drain = asyncio.gather(*tasks, return_exceptions=True)
    while not drain.done():
        try:
            _ = await asyncio.shield(drain)
        except asyncio.CancelledError:
            continue
    _ = drain.result()


def _is_async_callable(callback: EventListener) -> bool:
    return inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(
        getattr(callback, "__call__", None)
    )


def _close_unexpected_awaitable(result: object) -> None:
    if inspect.iscoroutine(result):
        result.close()
    elif isinstance(result, asyncio.Future):
        _ = result.cancel()


def _mode_name(key: EventKey) -> str:
    if isinstance(key, EmitEventKey):
        return "emit"
    if isinstance(key, SerialEventKey):
        return "serial"
    return "parallel"
