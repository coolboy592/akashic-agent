from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import AsyncIterator
from typing import Any

import pytest

from agent.plugin_composition import Bail, CompositionRoot
from agent.plugins.snapshot import (
    RuntimeSnapshotCompiler,
    RuntimeSnapshotStore,
    bind_runtime_snapshot,
    reset_runtime_snapshot,
)
from agent.tool_hooks.base import ToolHook
from agent.tool_hooks.executor import ToolExecutor
from agent.tool_hooks.types import HookContext, HookOutcome, ToolExecutionRequest
from agent.tools.events import (
    TOOL_EXECUTION_AUTHORIZE,
    TOOL_INPUT_PREPARE,
    TOOL_RESULT,
    ToolInput,
    ToolResult,
)


class _SpyHook(ToolHook):
    def __init__(
        self,
        *,
        name: str,
        event: str,
        matched: bool = True,
        outcome: HookOutcome | None = None,
    ) -> None:
        self.name = name
        self.event = event
        self._matched = matched
        self._outcome = outcome or HookOutcome()
        self.calls: list[HookContext] = []
        self._match_error: Exception | None = None
        self._run_error: Exception | None = None

    def matches(self, ctx: HookContext) -> bool:
        if self._match_error is not None:
            raise self._match_error
        return self._matched

    async def run(self, ctx: HookContext) -> HookOutcome:
        if self._run_error is not None:
            raise self._run_error
        self.calls.append(ctx)
        return self._outcome


async def _invoke(tool_name: str, arguments: dict[str, Any]) -> Any:
    return {"tool": tool_name, "arguments": dict(arguments)}


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


class _OrderHook(ToolHook):
    event = "pre_tool_use"

    def __init__(self, name: str, order: list[str]) -> None:
        self.name = name
        self._order = order

    def matches(self, ctx: HookContext) -> bool:
        return True

    async def run(self, ctx: HookContext) -> HookOutcome:
        self._order.append(self.name)
        if self.name == "legacy-restore":
            command = str(ctx.current_arguments["command"])
            return HookOutcome(
                updated_input={"command": command.replace("rm ", "mv ", 1)}
            )
        if self.name == "legacy-safety" and str(
            ctx.current_arguments["command"]
        ).startswith("rm "):
            return HookOutcome(decision="deny", reason="unsafe rm")
        return HookOutcome()


class _PostOrderHook(_OrderHook):
    event = "post_tool_use"

    async def run(self, ctx: HookContext) -> HookOutcome:
        self._order.append(self.name)
        return HookOutcome()


def test_tool_executor_pre_hook_can_update_arguments() -> None:
    hook = _SpyHook(
        name="rewrite",
        event="pre_tool_use",
        outcome=HookOutcome(updated_input={"x": 2}),
    )
    executor = ToolExecutor([hook])

    result = asyncio.run(
        executor.execute(
            ToolExecutionRequest(
                call_id="c1",
                tool_name="dummy",
                arguments={"x": 1},
                source="passive",
            ),
            _invoke,
        )
    )

    assert result.status == "success"
    assert result.final_arguments == {"x": 2}
    assert result.output == {"tool": "dummy", "arguments": {"x": 2}}
    assert hook.calls[0].request.arguments == {"x": 1}


def test_tool_executor_denied_is_not_error() -> None:
    hook = _SpyHook(
        name="deny",
        event="pre_tool_use",
        outcome=HookOutcome(decision="deny", reason="blocked"),
    )
    executor = ToolExecutor([hook])

    result = asyncio.run(
        executor.execute(
            ToolExecutionRequest(
                call_id="c1",
                tool_name="dummy",
                arguments={"x": 1},
                source="passive",
            ),
            _invoke,
        )
    )

    assert result.status == "denied"
    assert result.output == "blocked"


def test_tool_executor_post_hook_only_adds_extra_message() -> None:
    hook = _SpyHook(
        name="post",
        event="post_tool_use",
        outcome=HookOutcome(extra_message="hint"),
    )
    executor = ToolExecutor([hook])

    result = asyncio.run(
        executor.execute(
            ToolExecutionRequest(
                call_id="c1",
                tool_name="dummy",
                arguments={"x": 1},
                source="passive",
            ),
            _invoke,
        )
    )

    assert result.status == "success"
    assert result.output == {"tool": "dummy", "arguments": {"x": 1}}
    assert result.extra_messages == ["hint"]


def test_tool_executor_post_error_hook_cannot_swallow_error() -> None:
    hook = _SpyHook(
        name="post_error",
        event="post_tool_error",
        outcome=HookOutcome(extra_message="logged"),
    )
    executor = ToolExecutor([hook])

    async def _broken(_tool_name: str, _arguments: dict[str, Any]) -> Any:
        raise RuntimeError("boom")

    result = asyncio.run(
        executor.execute(
            ToolExecutionRequest(
                call_id="c1",
                tool_name="dummy",
                arguments={},
                source="passive",
            ),
            _broken,
        )
    )

    assert result.status == "error"
    assert result.output == "工具执行出错: boom"
    assert result.extra_messages == ["logged"]


def test_tool_executor_hook_exception_becomes_controlled_error() -> None:
    hook = _SpyHook(name="boom_hook", event="pre_tool_use")
    hook._run_error = RuntimeError("hook boom")
    executor = ToolExecutor([hook])

    result = asyncio.run(
        executor.execute(
            ToolExecutionRequest(
                call_id="c1",
                tool_name="dummy",
                arguments={"x": 1},
                source="passive",
            ),
            _invoke,
        )
    )

    assert result.status == "error"
    assert "boom_hook" in result.output
    assert "hook boom" in result.output


def test_tool_executor_post_tool_use_hook_failure_does_not_pollute_success() -> None:
    hook = _SpyHook(name="boom_hook", event="post_tool_use")
    hook._run_error = RuntimeError("post hook boom")
    executor = ToolExecutor([hook])

    result = asyncio.run(
        executor.execute(
            ToolExecutionRequest(
                call_id="c1",
                tool_name="dummy",
                arguments={"x": 1},
                source="passive",
            ),
            _invoke,
        )
    )

    assert result.status == "success"
    assert result.output == {"tool": "dummy", "arguments": {"x": 1}}
    assert result.post_hook_trace[-1].reason == "hook failed: post hook boom"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("restore_version", "safety_version", "expected_prefix"),
    [
        ("v2", "v2", ["legacy-restore", "legacy-safety"]),
        ("v3", "v2", ["v3-restore", "legacy-safety"]),
        ("v2", "v3", ["legacy-restore", "v3-safety"]),
        ("v3", "v3", ["v3-restore", "v3-safety"]),
    ],
)
async def test_tool_migration_sandwich_preserves_all_four_combinations(
    restore_version: str,
    safety_version: str,
    expected_prefix: list[str],
) -> None:
    order: list[str] = []
    observed: list[ToolResult] = []
    root = CompositionRoot(f"tool-sandwich:{restore_version}:{safety_version}")

    async def composition(ctx) -> None:
        if restore_version == "v3":
            def restore(tool_input: ToolInput) -> ToolInput:
                order.append("v3-restore")
                arguments = tool_input.mutable_arguments()
                command = str(arguments["command"])
                arguments["command"] = command.replace("rm ", "mv ", 1)
                return tool_input.with_arguments(arguments)

            _ = await ctx.on(TOOL_INPUT_PREPARE, restore)
        if safety_version == "v3":
            def authorize(tool_input: ToolInput):
                order.append("v3-safety")
                if str(tool_input.arguments["command"]).startswith("rm "):
                    return Bail("unsafe rm")
                return None

            _ = await ctx.on(TOOL_EXECUTION_AUTHORIZE, authorize)

        def observe(result: ToolResult) -> None:
            order.append("v3-result")
            observed.append(result)

        _ = await ctx.on(TOOL_RESULT, observe)

    _ = await root.mount(composition, name="composition")
    hooks: list[ToolHook] = []
    if restore_version == "v2":
        hooks.append(_OrderHook("legacy-restore", order))
    if safety_version == "v2":
        hooks.append(_OrderHook("legacy-safety", order))
    hooks.append(_PostOrderHook("legacy-post", order))
    executor = ToolExecutor(hooks)

    async def invoke(tool_name: str, arguments: dict[str, Any]) -> str:
        order.append("invoke")
        return f"{tool_name}:{arguments['command']}"

    async with _bound_root(root):
        result = await executor.execute(
            ToolExecutionRequest(
                call_id="call-1",
                tool_name="shell",
                arguments={"command": "rm file.txt"},
                source="passive",
                session_key="session",
            ),
            invoke,
        )

    assert result.status == "success"
    assert result.final_arguments == {"command": "mv file.txt"}
    assert order == [*expected_prefix, "invoke", "legacy-post", "v3-result"]
    assert len(observed) == 1
    assert observed[0].status == "success"
    assert observed[0].arguments == {"command": "mv file.txt"}


@pytest.mark.asyncio
async def test_v3_authorize_denial_is_observed_without_invoking() -> None:
    observed: list[ToolResult] = []
    invoked = False
    root = CompositionRoot("tool-authorize-deny")

    async def composition(ctx) -> None:
        _ = await ctx.on(
            TOOL_EXECUTION_AUTHORIZE,
            lambda _: Bail("blocked by v3"),
        )
        _ = await ctx.on(TOOL_RESULT, observed.append)

    _ = await root.mount(composition, name="authorizer")
    executor = ToolExecutor()

    async def invoke(_: str, __: dict[str, Any]) -> str:
        nonlocal invoked
        invoked = True
        return "unreachable"

    async with _bound_root(root):
        result = await executor.execute(
            ToolExecutionRequest(
                call_id="call-2",
                tool_name="shell",
                arguments={"command": "sudo pacman -S pkg"},
                source="passive",
            ),
            invoke,
        )

    assert result.status == "denied"
    assert result.output == "blocked by v3"
    assert invoked is False
    assert [item.status for item in observed] == ["denied"]


@pytest.mark.asyncio
async def test_v3_prepare_failure_records_incident_and_settles_error() -> None:
    observed: list[ToolResult] = []
    root = CompositionRoot("tool-prepare-failure")

    def fail(_: ToolInput) -> ToolInput:
        raise RuntimeError("prepare failed")

    async def composition(ctx) -> None:
        _ = await ctx.on(TOOL_INPUT_PREPARE, fail)
        _ = await ctx.on(TOOL_RESULT, observed.append)

    _ = await root.mount(composition, name="preparer")

    async with _bound_root(root):
        result = await ToolExecutor().execute(
            ToolExecutionRequest(
                call_id="call-3",
                tool_name="dummy",
                arguments={"x": 1},
                source="passive",
            ),
            _invoke,
        )
        incidents = root.receipt().incidents

    assert result.status == "error"
    assert "prepare failed" in str(result.output)
    assert [item.status for item in observed] == ["error"]
    assert (incidents[-1].owner, incidents[-1].kind) == (
        "preparer",
        "transform_failure",
    )


@pytest.mark.asyncio
async def test_v3_authorize_failure_records_incident_and_settles_error() -> None:
    observed: list[ToolResult] = []
    root = CompositionRoot("tool-authorize-failure")

    def fail(_: ToolInput) -> None:
        raise RuntimeError("authorize failed")

    async def composition(ctx) -> None:
        _ = await ctx.on(TOOL_EXECUTION_AUTHORIZE, fail)
        _ = await ctx.on(TOOL_RESULT, observed.append)

    _ = await root.mount(composition, name="authorizer")

    async with _bound_root(root):
        result = await ToolExecutor().execute(
            ToolExecutionRequest(
                call_id="call-authorize-failure",
                tool_name="dummy",
                arguments={"x": 1},
                source="passive",
            ),
            _invoke,
        )
        incidents = root.receipt().incidents

    assert result.status == "error"
    assert "authorize failed" in str(result.output)
    assert [item.status for item in observed] == ["error"]
    assert (incidents[-1].owner, incidents[-1].kind) == (
        "authorizer",
        "serial_failure",
    )


@pytest.mark.asyncio
async def test_v3_authorize_rejects_invalid_bail_with_owner_incident() -> None:
    observed: list[ToolResult] = []
    invoked = False
    root = CompositionRoot("tool-authorize-invalid-bail")

    async def composition(ctx) -> None:
        _ = await ctx.on(TOOL_EXECUTION_AUTHORIZE, lambda _: Bail(7))
        _ = await ctx.on(TOOL_RESULT, observed.append)

    _ = await root.mount(composition, name="bad-authorizer")
    assert root.topology_view().listeners == (
        "serial:tool.execution.authorize"
        "[bail=akashic.tool-deny-reason.v1]:bad-authorizer",
        "observe:tool.result:bad-authorizer",
    )

    async def invoke(_: str, __: dict[str, Any]) -> str:
        nonlocal invoked
        invoked = True
        return "unreachable"

    async with _bound_root(root):
        result = await ToolExecutor().execute(
            ToolExecutionRequest(
                call_id="call-invalid-bail",
                tool_name="dummy",
                arguments={"x": 1},
                source="passive",
            ),
            invoke,
        )
        incidents = root.receipt().incidents

    assert result.status == "error"
    assert "akashic.tool-deny-reason.v1" in str(result.output)
    assert invoked is False
    assert [item.status for item in observed] == ["error"]
    assert (incidents[-1].owner, incidents[-1].kind) == (
        "bad-authorizer",
        "serial_failure",
    )


def test_tool_input_replace_cannot_bypass_recursive_freeze() -> None:
    request = ToolExecutionRequest(
        call_id="call-replace",
        tool_name="dummy",
        arguments={"nested": [1]},
        source="passive",
    )
    original = ToolInput.from_request(request, request.arguments)
    raw = {"nested": [2]}

    replaced = replace(original, arguments=raw)
    raw["nested"].append(3)

    assert replaced.same_call(original)
    assert replaced.arguments["nested"] == (2,)
    assert replaced.mutable_arguments() == {"nested": [2]}
    with pytest.raises(TypeError, match="JSON"):
        _ = replace(original, arguments={"bad": object()})


@pytest.mark.asyncio
async def test_legacy_non_json_rewrite_settles_one_error_result() -> None:
    observed: list[ToolResult] = []
    invoked = False
    root = CompositionRoot("legacy-non-json")

    async def composition(ctx) -> None:
        _ = await ctx.on(TOOL_RESULT, observed.append)

    _ = await root.mount(composition, name="observer")
    hook = _SpyHook(
        name="legacy-invalid",
        event="pre_tool_use",
        outcome=HookOutcome(updated_input={"bad": object()}),
    )

    async def invoke(_: str, __: dict[str, Any]) -> str:
        nonlocal invoked
        invoked = True
        return "unreachable"

    async with _bound_root(root):
        result = await ToolExecutor([hook]).execute(
            ToolExecutionRequest(
                call_id="call-legacy-invalid",
                tool_name="dummy",
                arguments={"x": 1},
                source="passive",
            ),
            invoke,
        )

    assert result.status == "error"
    assert "legacy-invalid" in str(result.output)
    assert invoked is False
    assert [item.status for item in observed] == ["error"]
    assert observed[0].arguments == {"x": 1}


@pytest.mark.asyncio
async def test_v3_result_observer_failure_does_not_change_settled_result() -> None:
    root = CompositionRoot("tool-result-failure")

    def fail(result: ToolResult) -> None:
        result.arguments["nested"]["items"].append(2)

    async def composition(ctx) -> None:
        _ = await ctx.on(TOOL_RESULT, fail)

    _ = await root.mount(composition, name="observer")

    async with _bound_root(root):
        result = await ToolExecutor().execute(
            ToolExecutionRequest(
                call_id="call-4",
                tool_name="dummy",
                arguments={"nested": {"items": [1]}},
                source="passive",
            ),
            _invoke,
        )
        incidents = root.receipt().incidents

    assert result.status == "success"
    assert result.final_arguments == {"nested": {"items": [1]}}
    assert (incidents[-1].owner, incidents[-1].kind) == (
        "observer",
        "observer_failure",
    )


@pytest.mark.asyncio
async def test_preflight_runs_v3_admission_without_publishing_result() -> None:
    observed: list[ToolResult] = []
    root = CompositionRoot("tool-preflight")

    async def composition(ctx) -> None:
        _ = await ctx.on(
            TOOL_INPUT_PREPARE,
            lambda item: item.with_arguments({"x": 2}),
        )
        _ = await ctx.on(
            TOOL_EXECUTION_AUTHORIZE,
            lambda item: Bail("x denied") if item.arguments["x"] == 2 else None,
        )
        _ = await ctx.on(TOOL_RESULT, observed.append)

    _ = await root.mount(composition, name="preflight")

    async with _bound_root(root):
        result = await ToolExecutor().preflight(
            ToolExecutionRequest(
                call_id="call-5",
                tool_name="dummy",
                arguments={"x": 1},
                source="passive",
            )
        )

    assert result.status == "denied"
    assert result.final_arguments == {"x": 2}
    assert observed == []
