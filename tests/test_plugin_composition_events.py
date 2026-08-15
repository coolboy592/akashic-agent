from __future__ import annotations

import asyncio

import pytest

from agent.plugin_composition import (
    Bail,
    CompositionError,
    CompositionRoot,
    Effect,
    EmitEventKey,
    FiberState,
    ParallelEventKey,
    SerialEventKey,
    ServiceKey,
)

NOTICE = EmitEventKey[str]("notice")
TRANSFORM = SerialEventKey[list[str], str]("transform")
OBSERVE = ParallelEventKey[str]("observe")
DEPENDENCY = ServiceKey[str]("event-dependency")


@pytest.mark.asyncio
async def test_emit_runs_sync_listeners_in_registration_order() -> None:
    observed: list[str] = []
    root = CompositionRoot("emit-order")

    async def first(ctx) -> None:
        await ctx.on(NOTICE, lambda payload: observed.append(f"first:{payload}"))

    async def second(ctx) -> None:
        await ctx.on(NOTICE, lambda payload: observed.append(f"second:{payload}"))

    await root.mount(first, name="first")
    await root.mount(second, name="second")
    root.context.emit(NOTICE, "ready")

    assert observed == ["first:ready", "second:ready"]
    assert "event:EmitEventKey:notice" in root.receipt().effects[0]


@pytest.mark.asyncio
async def test_topology_identity_preserves_listener_registration_order() -> None:
    async def build(order: tuple[str, str]) -> str:
        root = CompositionRoot("event-order-identity")

        for owner in order:
            async def plugin(ctx) -> None:
                await ctx.on(NOTICE, lambda _: None)

            await root.mount(plugin, name=owner)
        return root.topology_identity()

    first_then_second = await build(("first", "second"))
    second_then_first = await build(("second", "first"))

    assert first_then_second != second_then_first


@pytest.mark.asyncio
async def test_topology_view_exposes_ordered_listener_revision() -> None:
    root = CompositionRoot("event-view")

    async def first(ctx) -> None:
        await ctx.on(NOTICE, lambda _: None)

    async def second(ctx) -> None:
        await ctx.on(NOTICE, lambda _: None)

    await root.mount(first, name="first")
    await root.mount(second, name="second")
    view = root.topology_view()

    assert view.identity == root.topology_identity()
    assert view.listeners == (
        "emit:notice:first",
        "emit:notice:second",
    )
    assert len(view.identity) == 64


@pytest.mark.asyncio
async def test_listener_remove_and_restore_keeps_hash_but_advances_revision() -> None:
    root = CompositionRoot("event-revision")
    effects: list[Effect] = []

    async def plugin(ctx) -> None:
        effects.append(await ctx.on(NOTICE, lambda _: None))

    fiber = await root.mount(plugin, name="listener")
    compiled = root.topology_view()

    await effects.pop().aclose()
    removed = root.topology_view()
    effects.append(await fiber.context.on(NOTICE, lambda _: None))
    restored = root.topology_view()

    assert removed.identity != compiled.identity
    assert restored.identity == compiled.identity
    assert restored.composition_revision == compiled.composition_revision + 2
    await root.dispose()


@pytest.mark.asyncio
async def test_emit_rejects_async_listener_during_registration() -> None:
    root = CompositionRoot("emit-async-listener")

    async def listener(_: str) -> None:
        return None

    async def plugin(ctx) -> None:
        await ctx.on(NOTICE, listener)

    fiber = await root.mount(plugin, name="broken")

    assert fiber.state == FiberState.FAILED
    assert any(
        "ASYNC_LISTENER_ON_EMIT" in incident.message
        for incident in root.receipt().incidents
    )


@pytest.mark.asyncio
async def test_emit_rejects_awaitable_returned_by_sync_listener() -> None:
    root = CompositionRoot("emit-awaitable-result")

    async def delayed() -> None:
        return None

    async def plugin(ctx) -> None:
        await ctx.on(NOTICE, lambda _: delayed())

    await root.mount(plugin, name="wrapper")

    with pytest.raises(CompositionError) as caught:
        root.context.emit(NOTICE, "ready")
    assert caught.value.code == "ASYNC_RESULT_FROM_EMIT"


@pytest.mark.asyncio
async def test_event_name_cannot_change_dispatch_mode_while_registered() -> None:
    root = CompositionRoot("event-mode-conflict")
    conflicting = SerialEventKey[str, str](NOTICE.name)

    async def emit_plugin(ctx) -> None:
        await ctx.on(NOTICE, lambda _: None)

    async def serial_plugin(ctx) -> None:
        await ctx.on(conflicting, lambda _: None)

    await root.mount(emit_plugin, name="emit-owner")
    fiber = await root.mount(serial_plugin, name="serial-owner")

    assert fiber.state == FiberState.FAILED
    assert any(
        "EVENT_MODE_CONFLICT" in incident.message
        for incident in root.receipt().incidents
    )


@pytest.mark.asyncio
async def test_serial_awaits_in_order_and_only_explicit_bail_stops() -> None:
    observed: list[str] = []
    payload: list[str] = []
    root = CompositionRoot("serial-bail")

    async def first_handler(value: list[str]) -> None:
        await asyncio.sleep(0)
        value.append("first")
        observed.append("first")

    def second_handler(value: list[str]) -> Bail[str]:
        value.append("second")
        observed.append("second")
        return Bail("stop")

    async def first(ctx) -> None:
        await ctx.on(TRANSFORM, first_handler)

    async def second(ctx) -> None:
        await ctx.on(TRANSFORM, second_handler)

    async def third(ctx) -> None:
        await ctx.on(TRANSFORM, lambda value: observed.append("third"))

    await root.mount(first, name="first")
    await root.mount(second, name="second")
    await root.mount(third, name="third")

    result = await root.context.serial(TRANSFORM, payload)

    assert result == Bail("stop")
    assert payload == ["first", "second"]
    assert observed == ["first", "second"]


@pytest.mark.asyncio
async def test_serial_rejects_implicit_truthy_result() -> None:
    root = CompositionRoot("serial-invalid-result")

    async def plugin(ctx) -> None:
        await ctx.on(TRANSFORM, lambda _: "implicit-stop")

    await root.mount(plugin, name="invalid")

    with pytest.raises(CompositionError) as caught:
        await root.context.serial(TRANSFORM, [])
    assert caught.value.code == "INVALID_SERIAL_RESULT"


@pytest.mark.asyncio
async def test_parallel_starts_together_and_aggregates_all_failures() -> None:
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    release = asyncio.Event()
    root = CompositionRoot("parallel-errors")

    async def first(_: str) -> None:
        first_started.set()
        await release.wait()
        raise ValueError("first")

    async def second(_: str) -> None:
        second_started.set()
        await release.wait()
        raise RuntimeError("second")

    async def plugin_a(ctx) -> None:
        await ctx.on(OBSERVE, first)

    async def plugin_b(ctx) -> None:
        await ctx.on(OBSERVE, second)

    await root.mount(plugin_a, name="first")
    await root.mount(plugin_b, name="second")
    dispatch = asyncio.create_task(root.context.parallel(OBSERVE, "event"))
    await asyncio.gather(first_started.wait(), second_started.wait())
    release.set()

    with pytest.raises(BaseExceptionGroup) as caught:
        await dispatch
    assert {type(error) for error in caught.value.exceptions} == {
        ValueError,
        RuntimeError,
    }


@pytest.mark.asyncio
async def test_parallel_cancellation_drains_every_listener() -> None:
    started = [asyncio.Event(), asyncio.Event()]
    cleaned = [asyncio.Event(), asyncio.Event()]
    root = CompositionRoot("parallel-cancel")

    def listener(index: int):
        async def run(_: str) -> None:
            started[index].set()
            try:
                await asyncio.Future()
            finally:
                cleaned[index].set()

        return run

    async def first(ctx) -> None:
        await ctx.on(OBSERVE, listener(0))

    async def second(ctx) -> None:
        await ctx.on(OBSERVE, listener(1))

    await root.mount(first, name="first")
    await root.mount(second, name="second")
    dispatch = asyncio.create_task(root.context.parallel(OBSERVE, "event"))
    await asyncio.gather(*(event.wait() for event in started))
    _ = dispatch.cancel()
    await asyncio.sleep(0)
    _ = dispatch.cancel()

    with pytest.raises(asyncio.CancelledError):
        await dispatch
    assert all(event.is_set() for event in cleaned)


@pytest.mark.asyncio
async def test_serial_uses_one_frozen_listener_list_per_dispatch() -> None:
    observed: list[str] = []
    root = CompositionRoot("serial-frozen-list")
    second_fiber = None

    async def first_handler(_: list[str]) -> None:
        assert second_fiber is not None
        observed.append("first")
        await second_fiber.dispose()

    async def first_plugin(ctx) -> None:
        await ctx.on(TRANSFORM, first_handler)

    async def second_plugin(ctx) -> None:
        await ctx.on(TRANSFORM, lambda _: observed.append("second"))

    await root.mount(first_plugin, name="first")
    second_fiber = await root.mount(second_plugin, name="second")

    result = await root.context.serial(TRANSFORM, [])

    assert result is None
    assert observed == ["first", "second"]
    assert second_fiber.state == FiberState.DISPOSED


@pytest.mark.asyncio
async def test_dependency_loss_removes_listener_and_restore_registers_once() -> None:
    observed: list[str] = []
    root = CompositionRoot("event-dependency")

    class Consumer:
        name = "consumer"
        inject = (DEPENDENCY,)

        async def apply(self, ctx) -> None:
            await ctx.on(NOTICE, lambda payload: observed.append(payload))

    class Provider:
        name = "provider"
        inject = ()

        async def apply(self, ctx) -> None:
            await ctx.provide(DEPENDENCY, "ready")

    consumer = await root.mount(Consumer())
    provider = await root.mount(Provider())
    root.context.emit(NOTICE, "first")

    await provider.dispose()
    assert consumer.state == FiberState.PENDING
    root.context.emit(NOTICE, "missing")

    await root.mount(Provider(), name="replacement")
    root.context.emit(NOTICE, "second")

    assert observed == ["first", "second"]


@pytest.mark.asyncio
async def test_spawned_task_is_cancelled_with_owning_fiber() -> None:
    started = asyncio.Event()
    cleaned = asyncio.Event()
    root = CompositionRoot("spawn-cleanup")

    async def worker() -> None:
        started.set()
        try:
            await asyncio.Future()
        finally:
            cleaned.set()

    async def plugin(ctx) -> None:
        _ = await ctx.spawn(worker(), name="worker")

    fiber = await root.mount(plugin, name="task-owner")
    await started.wait()
    assert "task-owner:task:worker" in root.receipt().effects
    await fiber.dispose()

    assert cleaned.is_set()
    assert fiber.effects == []


@pytest.mark.asyncio
async def test_spawned_task_failure_is_visible_to_candidate_readiness() -> None:
    failed = asyncio.Event()
    recovered = asyncio.Event()
    attempts = 0
    root = CompositionRoot("spawn-failure")

    async def worker() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            failed.set()
            raise RuntimeError("background failed")
        recovered.set()
        await asyncio.Event().wait()

    async def plugin(ctx) -> None:
        _ = await ctx.spawn(worker(), name="broken-worker")

    fiber = await root.mount(plugin, name="task-owner")
    await failed.wait()
    await asyncio.sleep(0)

    receipt = root.receipt()
    assert receipt.ready is False
    assert receipt.required_degraded == ("task-owner:task:broken-worker",)
    assert any(
        incident.kind == "task_failure"
        and "background failed" in incident.message
        for incident in receipt.incidents
    )

    await fiber.restart()
    await recovered.wait()
    recovered_receipt = root.receipt()
    assert recovered_receipt.ready is True
    assert recovered_receipt.required_degraded == ()
    assert any(
        "background failed" in incident.message
        for incident in recovered_receipt.incidents
    )
    await root.dispose()
