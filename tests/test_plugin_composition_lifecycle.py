from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncIterator, cast

import pytest

from agent.core.response_parser import ResponseMetadata
from agent.lifecycle.composition import (
    AFTER_REASONING_CLEANUP_EVENT,
    AFTER_REASONING_PREPROCESS_EVENT,
    CONTEXT_PREPARED_EVENT,
    PROMPT_RENDER_EVENT,
    run_composition_lifecycle,
)
from agent.lifecycle.phases.before_turn import (
    BeforeTurnFrame,
    default_before_turn_modules,
)
from agent.lifecycle.phases.after_reasoning import (
    AfterReasoningFrame,
    default_after_reasoning_modules,
)
from agent.lifecycle.phases.prompt_render import (
    PromptRenderFrame,
    default_prompt_render_modules,
)
from agent.lifecycle.types import AfterReasoningCtx, BeforeTurnCtx, PromptRenderCtx
from agent.plugin_composition import Bail, CompositionError, CompositionRoot
from agent.plugins.snapshot import (
    RuntimeSnapshotCompiler,
    RuntimeSnapshotStore,
    bind_runtime_snapshot,
    reset_runtime_snapshot,
)
from bus.event_bus import EventBus


@asynccontextmanager
async def _bound_root(root: CompositionRoot) -> AsyncIterator[None]:
    store = RuntimeSnapshotStore()
    store.install(RuntimeSnapshotCompiler().compile({}, composition_root=root))
    lease = store.lease()
    token = bind_runtime_snapshot(lease)
    try:
        yield
    finally:
        reset_runtime_snapshot(token)
        await lease.release()
        await store.close()
        await root.dispose()


def _prompt_ctx() -> PromptRenderCtx:
    return PromptRenderCtx(
        session_key="session",
        channel="test",
        chat_id="chat",
        content="hello",
        media=None,
        timestamp=datetime.now(),
        history=[],
        skill_names=[],
        retrieved_memory_block="",
        disabled_sections=set(),
        turn_injection_prompt="",
    )


def _before_turn_ctx() -> BeforeTurnCtx:
    return BeforeTurnCtx(
        session_key="session",
        channel="test",
        chat_id="chat",
        content="hello",
        timestamp=datetime.now(),
        retrieved_memory_block="memory",
        retrieval_trace_raw={"trace": 1},
        history_messages=(),
    )


def _answer_ctx() -> AfterReasoningCtx:
    return AfterReasoningCtx(
        session_key="session",
        channel="test",
        chat_id="chat",
        tools_used=(),
        thinking=None,
        response_metadata=ResponseMetadata(raw_text="hello"),
        streamed=False,
        tool_chain=(),
        context_retry={},
        reply="hello",
    )


@pytest.mark.asyncio
async def test_prompt_seam_runs_before_legacy_phase_modules() -> None:
    order: list[str] = []
    root = CompositionRoot("prompt-seam")

    async def plugin(ctx) -> None:
        await ctx.on(PROMPT_RENDER_EVENT, lambda _: order.append("composition"))

    class LegacyModule:
        slot = "legacy.prompt"
        requires = ("prompt_render.emit", "prompt:ctx")

        async def run(self, frame):
            order.append("legacy-phase")
            return frame

    await root.mount(plugin, name="prompt-plugin")
    bus = EventBus()
    bus.on(PromptRenderCtx, lambda _: order.append("event-bus"))
    modules = default_prompt_render_modules(
        bus,
        cast(Any, object()),
        plugin_modules=cast(Any, [LegacyModule()]),
    )
    slots = [cast(str, getattr(module, "slot")) for module in modules]
    frame = PromptRenderFrame(
        input=cast(Any, None),
        slots={"prompt:ctx": _prompt_ctx()},
    )

    async with _bound_root(root):
        for module in modules[
            slots.index("prompt_render.emit") : slots.index("legacy.prompt") + 1
        ]:
            frame = await module.run(frame)

    assert order == ["event-bus", "composition", "legacy-phase"]
    assert slots.index("legacy.prompt") < slots.index("prompt_render.collect_exports")


@pytest.mark.asyncio
async def test_context_prepared_seam_runs_after_legacy_before_turn_modules() -> None:
    order: list[str] = []
    observed: list[BeforeTurnCtx] = []
    root = CompositionRoot("context-prepared-seam")

    async def plugin(ctx) -> None:
        def observe(payload: BeforeTurnCtx) -> None:
            order.append("composition")
            assert payload.extra_hints == ["legacy hint"]
            observed.append(payload)

        await ctx.on(CONTEXT_PREPARED_EVENT, observe)

    class LegacyModule:
        slot = "legacy.before_turn"
        requires = ("before_turn.emit", "session:ctx")

        async def run(self, frame):
            order.append("legacy-phase")
            frame.slots["session:extra_hint:legacy"] = "legacy hint"
            return frame

    await root.mount(plugin, name="context-plugin")
    bus = EventBus()
    bus.on(BeforeTurnCtx, lambda _: order.append("event-bus"))
    modules = default_before_turn_modules(
        bus,
        cast(Any, object()),
        cast(Any, object()),
        plugin_modules=cast(Any, [LegacyModule()]),
    )
    slots = [cast(str, getattr(module, "slot")) for module in modules]
    payload = _before_turn_ctx()
    frame = BeforeTurnFrame(
        input=cast(Any, None),
        slots={"session:ctx": payload},
    )

    async with _bound_root(root):
        for module in modules[
            slots.index("before_turn.emit") :
            slots.index("before_turn.composition_context_prepared") + 1
        ]:
            frame = await module.run(frame)

    assert order == ["event-bus", "legacy-phase", "composition"]
    assert observed == [payload]
    assert slots.index("before_turn.collect_exports") < slots.index(
        "before_turn.composition_context_prepared"
    ) < slots.index("before_turn.return")


@pytest.mark.asyncio
async def test_context_prepared_seam_is_noop_without_composition_root() -> None:
    store = RuntimeSnapshotStore()
    store.install(RuntimeSnapshotCompiler().compile({}))
    lease = store.lease()
    token = bind_runtime_snapshot(lease)

    try:
        await run_composition_lifecycle(CONTEXT_PREPARED_EVENT, _before_turn_ctx())
    finally:
        reset_runtime_snapshot(token)
        await lease.release()
        await store.close()


@pytest.mark.asyncio
async def test_lifecycle_seam_is_noop_without_runtime_binding() -> None:
    await run_composition_lifecycle(CONTEXT_PREPARED_EVENT, _before_turn_ctx())


@pytest.mark.asyncio
async def test_lifecycle_seam_rejects_inherited_wrong_task_binding() -> None:
    observed: list[str] = []
    root = CompositionRoot("wrong-task-lifecycle")

    async def plugin(ctx) -> None:
        await ctx.on(CONTEXT_PREPARED_EVENT, lambda _: observed.append("called"))

    await root.mount(plugin, name="observer")
    async with _bound_root(root):
        task = asyncio.create_task(
            run_composition_lifecycle(
                CONTEXT_PREPARED_EVENT,
                _before_turn_ctx(),
            )
        )
        with pytest.raises(CompositionError) as caught:
            await task

    assert caught.value.code == "RUNTIME_SNAPSHOT_BINDING_MISMATCH"
    assert observed == []


@pytest.mark.asyncio
async def test_lifecycle_seam_rejects_released_owner_lease() -> None:
    root = CompositionRoot("inactive-lifecycle")
    store = RuntimeSnapshotStore()
    store.install(RuntimeSnapshotCompiler().compile({}, composition_root=root))
    lease = store.lease()
    token = bind_runtime_snapshot(lease)
    await lease.release()

    try:
        with pytest.raises(CompositionError) as caught:
            await run_composition_lifecycle(
                CONTEXT_PREPARED_EVENT,
                _before_turn_ctx(),
            )
    finally:
        reset_runtime_snapshot(token)
        await store.close()
        await root.dispose()

    assert caught.value.code == "RUNTIME_SNAPSHOT_BINDING_INACTIVE"


@pytest.mark.asyncio
async def test_answer_seams_preserve_legacy_module_positions() -> None:
    order: list[str] = []
    root = CompositionRoot("answer-seam")

    async def plugin(ctx) -> None:
        await ctx.on(
            AFTER_REASONING_PREPROCESS_EVENT,
            lambda _: order.append("preprocess"),
        )
        await ctx.on(
            AFTER_REASONING_CLEANUP_EVENT,
            lambda _: order.append("cleanup"),
        )

    class LegacyPre:
        slot = "legacy.answer_pre"
        requires = ("after_reasoning.build_ctx", "reasoning:ctx")

        async def run(self, frame):
            order.append("legacy-pre")
            return frame

    class LegacyPost:
        slot = "legacy.answer_post"
        requires = ("after_reasoning.emit", "reasoning:ctx")

        async def run(self, frame):
            order.append("legacy-post")
            return frame

    await root.mount(plugin, name="answer-plugin")
    bus = EventBus()
    bus.on(AfterReasoningCtx, lambda _: order.append("event-bus"))
    modules = default_after_reasoning_modules(
        bus,
        cast(Any, object()),
        plugin_modules=cast(Any, [LegacyPre(), LegacyPost()]),
    )
    slots = [cast(str, getattr(module, "slot")) for module in modules]
    frame = AfterReasoningFrame(
        input=cast(Any, None),
        slots={"reasoning:ctx": _answer_ctx()},
    )

    async with _bound_root(root):
        for module in modules[
            slots.index("legacy.answer_pre") :
            slots.index("after_reasoning.composition_cleanup") + 1
        ]:
            frame = await module.run(frame)

    assert order == [
        "legacy-pre",
        "preprocess",
        "event-bus",
        "legacy-post",
        "cleanup",
    ]


@pytest.mark.asyncio
async def test_lifecycle_seam_rejects_bail() -> None:
    root = CompositionRoot("lifecycle-bail")

    async def plugin(ctx) -> None:
        await ctx.on(PROMPT_RENDER_EVENT, lambda _: Bail("blocked"))

    await root.mount(plugin, name="bailing-plugin")
    async with _bound_root(root):
        with pytest.raises(CompositionError) as caught:
            await run_composition_lifecycle(PROMPT_RENDER_EVENT, _prompt_ctx())

    assert caught.value.code == "LIFECYCLE_BAIL_NOT_ALLOWED"
