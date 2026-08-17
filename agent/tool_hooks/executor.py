from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from agent.plugin_composition import CompositionError
from agent.tool_hooks.base import ToolHook
from agent.tool_hooks.types import (
    HookContext,
    HookTraceItem,
    ToolExecutionRequest,
    ToolExecutionResult,
)
from agent.tools.events import (
    TOOL_EXECUTION_AUTHORIZE,
    TOOL_INPUT_PREPARE,
    TOOL_RESULT,
    ToolInput,
    ToolResult,
)

if TYPE_CHECKING:
    from agent.plugin_composition import CompositionRoot

ToolInvoker = Callable[[str, dict[str, Any]], Awaitable[Any]]


class HookExecutionError(RuntimeError):
    def __init__(self, hook_name: str, event: str, cause: Exception) -> None:
        self.hook_name = hook_name
        self.event = event
        self.cause = cause
        super().__init__(f"hook {hook_name} ({event}) failed: {cause}")


class ToolExecutor:
    def __init__(self, hooks: Sequence[ToolHook] | None = None) -> None:
        self._hooks = list(hooks or [])

    def add_hooks(self, hooks: Sequence[ToolHook]) -> None:
        self._hooks.extend(hooks)

    async def execute(
        self,
        request: ToolExecutionRequest,
        invoker: ToolInvoker,
    ) -> ToolExecutionResult:
        """执行单次工具调用。

        request 描述“这次想调用什么工具、带什么参数”；
        invoker 是真实执行入口（通常是 ToolRegistry.execute）。

        固定流程：
        1. v3 prepare → legacy pre → v3 authorize
        2. invoker：用最终参数执行真实工具
        3. legacy post → v3 immutable result
        """
        legacy_hooks, composition_root = self._runtime_extensions()
        current_arguments = dict(request.arguments)
        extra_messages: list[str] = []
        pre_trace: list[HookTraceItem] = []
        post_trace: list[HookTraceItem] = []

        try:
            # 1. v3 prepare 先转换参数；迁移完成后删除后续 legacy 夹层。
            current_arguments = await self._run_input_prepare(
                composition_root,
                request,
                current_arguments,
            )
        except Exception as exc:
            return await self._settle(
                composition_root,
                request,
                ToolExecutionResult(
                    status="error",
                    output=f"工具执行出错: {exc}",
                    final_arguments=dict(current_arguments),
                    extra_messages=extra_messages,
                    pre_hook_trace=pre_trace,
                    post_hook_trace=post_trace,
                ),
            )

        try:
            # 2. V2_REMOVAL(tool-hooks)：保留旧改参/deny 顺序直到插件迁完。
            denied_reason, current_arguments = await self._run_pre_hooks(
                hooks=legacy_hooks,
                request=request,
                current_arguments=current_arguments,
                extra_messages=extra_messages,
                traces=pre_trace,
            )
        except HookExecutionError as exc:
            return await self._settle(
                composition_root,
                request,
                ToolExecutionResult(
                    status="error",
                    output=f"工具执行出错: {exc}",
                    final_arguments=dict(current_arguments),
                    extra_messages=extra_messages,
                    pre_hook_trace=pre_trace,
                    post_hook_trace=post_trace,
                ),
            )
        final_arguments = dict(current_arguments)
        if denied_reason:
            return await self._settle(
                composition_root,
                request,
                ToolExecutionResult(
                    status="denied",
                    output=denied_reason,
                    final_arguments=final_arguments,
                    extra_messages=extra_messages,
                    pre_hook_trace=pre_trace,
                    post_hook_trace=post_trace,
                ),
            )

        try:
            # 3. v3 authorization sees the final prepared/legacy arguments.
            denied_reason = await self._run_execution_authorize(
                composition_root,
                request,
                final_arguments,
            )
        except Exception as exc:
            return await self._settle(
                composition_root,
                request,
                ToolExecutionResult(
                    status="error",
                    output=f"工具执行出错: {exc}",
                    final_arguments=final_arguments,
                    extra_messages=extra_messages,
                    pre_hook_trace=pre_trace,
                    post_hook_trace=post_trace,
                ),
            )
        if denied_reason:
            return await self._settle(
                composition_root,
                request,
                ToolExecutionResult(
                    status="denied",
                    output=denied_reason,
                    final_arguments=final_arguments,
                    extra_messages=extra_messages,
                    pre_hook_trace=pre_trace,
                    post_hook_trace=post_trace,
                ),
            )

        try:
            # 4. Only the invoker owns real tool execution.
            output = await invoker(request.tool_name, final_arguments)
        except Exception as exc:
            error_text = str(exc)
            try:
                # 工具自身报错后，允许 post_tool_error 做记录型处理。
                await self._run_post_hooks(
                    HookContext(
                        event="post_tool_error",
                        request=request,
                        current_arguments=final_arguments,
                        error=error_text,
                    ),
                    hooks=legacy_hooks,
                    extra_messages=extra_messages,
                    traces=post_trace,
                )
            except HookExecutionError as hook_exc:
                return await self._settle(
                    composition_root,
                    request,
                    ToolExecutionResult(
                        status="error",
                        output=f"工具执行出错: {hook_exc}",
                        final_arguments=final_arguments,
                        extra_messages=extra_messages,
                        pre_hook_trace=pre_trace,
                        post_hook_trace=post_trace,
                    ),
                )
            return await self._settle(
                composition_root,
                request,
                ToolExecutionResult(
                    status="error",
                    output=f"工具执行出错: {error_text}",
                    final_arguments=final_arguments,
                    extra_messages=extra_messages,
                    pre_hook_trace=pre_trace,
                    post_hook_trace=post_trace,
                ),
            )

        try:
            # 5. V2_REMOVAL(tool-hooks)：legacy post 先完成，再发布 v3 result。
            await self._run_post_hooks(
                HookContext(
                    event="post_tool_use",
                    request=request,
                    current_arguments=final_arguments,
                    result=output,
                ),
                hooks=legacy_hooks,
                extra_messages=extra_messages,
                traces=post_trace,
                fail_open=True,
            )
        except HookExecutionError as exc:
            return await self._settle(
                composition_root,
                request,
                ToolExecutionResult(
                    status="error",
                    output=f"工具执行出错: {exc}",
                    final_arguments=final_arguments,
                    extra_messages=extra_messages,
                    pre_hook_trace=pre_trace,
                    post_hook_trace=post_trace,
                ),
            )
        return await self._settle(
            composition_root,
            request,
            ToolExecutionResult(
                status="success",
                output=output,
                final_arguments=final_arguments,
                extra_messages=extra_messages,
                pre_hook_trace=pre_trace,
                post_hook_trace=post_trace,
            ),
        )

    async def preflight(
        self,
        request: ToolExecutionRequest,
    ) -> ToolExecutionResult:
        legacy_hooks, composition_root = self._runtime_extensions()
        current_arguments = dict(request.arguments)
        extra_messages: list[str] = []
        pre_trace: list[HookTraceItem] = []
        try:
            current_arguments = await self._run_input_prepare(
                composition_root,
                request,
                current_arguments,
            )
        except Exception as exc:
            return ToolExecutionResult(
                status="error",
                output=f"工具执行出错: {exc}",
                final_arguments=dict(current_arguments),
                extra_messages=extra_messages,
                pre_hook_trace=pre_trace,
            )
        try:
            denied_reason, current_arguments = await self._run_pre_hooks(
                hooks=legacy_hooks,
                request=request,
                current_arguments=current_arguments,
                extra_messages=extra_messages,
                traces=pre_trace,
            )
        except HookExecutionError as exc:
            return ToolExecutionResult(
                status="error",
                output=f"工具执行出错: {exc}",
                final_arguments=dict(current_arguments),
                extra_messages=extra_messages,
                pre_hook_trace=pre_trace,
            )
        if denied_reason:
            return ToolExecutionResult(
                status="denied",
                output=denied_reason,
                final_arguments=dict(current_arguments),
                extra_messages=extra_messages,
                pre_hook_trace=pre_trace,
            )
        final_arguments = dict(current_arguments)
        try:
            denied_reason = await self._run_execution_authorize(
                composition_root,
                request,
                final_arguments,
            )
        except Exception as exc:
            return ToolExecutionResult(
                status="error",
                output=f"工具执行出错: {exc}",
                final_arguments=final_arguments,
                extra_messages=extra_messages,
                pre_hook_trace=pre_trace,
            )
        if denied_reason:
            return ToolExecutionResult(
                status="denied",
                output=denied_reason,
                final_arguments=final_arguments,
                extra_messages=extra_messages,
                pre_hook_trace=pre_trace,
            )
        return ToolExecutionResult(
            status="success",
            output="",
            final_arguments=final_arguments,
            extra_messages=extra_messages,
            pre_hook_trace=pre_trace,
        )

    async def _run_pre_hooks(
        self,
        *,
        hooks: Sequence[ToolHook],
        request: ToolExecutionRequest,
        current_arguments: dict[str, Any],
        extra_messages: list[str],
        traces: list[HookTraceItem],
    ) -> tuple[str, dict[str, Any]]:
        for hook in hooks:
            if hook.event != "pre_tool_use":
                continue
            ctx = HookContext(
                event="pre_tool_use",
                request=request,
                current_arguments=dict(current_arguments),
            )
            try:
                matched = hook.matches(ctx)
            except Exception as exc:
                raise HookExecutionError(hook.name, hook.event, exc) from exc
            if not matched:
                traces.append(
                    HookTraceItem(
                        hook_name=hook.name,
                        event=hook.event,
                        matched=False,
                    )
                )
                continue
            try:
                outcome = await hook.run(ctx)
            except Exception as exc:
                raise HookExecutionError(hook.name, hook.event, exc) from exc
            if outcome.updated_input is not None:
                try:
                    current_arguments = ToolInput.from_request(
                        request,
                        outcome.updated_input,
                    ).mutable_arguments()
                except (TypeError, ValueError) as exc:
                    raise HookExecutionError(hook.name, hook.event, exc) from exc
            if outcome.extra_message:
                extra_messages.append(outcome.extra_message)
            traces.append(
                HookTraceItem(
                    hook_name=hook.name,
                    event=hook.event,
                    matched=True,
                    decision=outcome.decision,
                    reason=outcome.reason,
                    extra_message=outcome.extra_message,
                )
            )
            if outcome.decision == "deny":
                reason = outcome.reason.strip() or "工具调用被拦截"
                return reason, current_arguments
        return "", current_arguments

    async def _run_post_hooks(
        self,
        ctx: HookContext,
        *,
        hooks: Sequence[ToolHook],
        extra_messages: list[str],
        traces: list[HookTraceItem],
        fail_open: bool = False,
    ) -> None:
        for hook in hooks:
            if hook.event != ctx.event:
                continue
            try:
                matched = hook.matches(ctx)
            except Exception as exc:
                if fail_open:
                    traces.append(
                        HookTraceItem(
                            hook_name=hook.name,
                            event=hook.event,
                            matched=False,
                            reason=f"hook failed: {exc}",
                        )
                    )
                    continue
                raise HookExecutionError(hook.name, hook.event, exc) from exc
            if not matched:
                traces.append(
                    HookTraceItem(
                        hook_name=hook.name,
                        event=hook.event,
                        matched=False,
                    )
                )
                continue
            try:
                outcome = await hook.run(ctx)
            except Exception as exc:
                if fail_open:
                    traces.append(
                        HookTraceItem(
                            hook_name=hook.name,
                            event=hook.event,
                            matched=True,
                            reason=f"hook failed: {exc}",
                        )
                    )
                    continue
                raise HookExecutionError(hook.name, hook.event, exc) from exc
            if outcome.extra_message:
                extra_messages.append(outcome.extra_message)
            traces.append(
                HookTraceItem(
                    hook_name=hook.name,
                    event=hook.event,
                    matched=True,
                    decision=outcome.decision,
                    reason=outcome.reason,
                    extra_message=outcome.extra_message,
                )
            )

    def _runtime_extensions(
        self,
    ) -> tuple[list[ToolHook], CompositionRoot | None]:
        from agent.plugins.snapshot import get_current_runtime_snapshot

        snapshot = get_current_runtime_snapshot()
        if snapshot is None:
            return self._hooks, None
        fixed = [
            hook
            for hook in self._hooks
            if not getattr(hook, "snapshot_managed", False)
        ]
        return fixed, snapshot.composition_root

    async def _run_input_prepare(
        self,
        root: CompositionRoot | None,
        request: ToolExecutionRequest,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        if root is None:
            return arguments
        original = ToolInput.from_request(request, arguments)
        prepared = await root.context.transform(TOOL_INPUT_PREPARE, original)
        if not prepared.same_call(original):
            raise CompositionError(
                "TOOL_INPUT_IDENTITY_CHANGED",
                "tool.input.prepare 只能通过 with_arguments() 修改参数",
            )
        return prepared.mutable_arguments()

    async def _run_execution_authorize(
        self,
        root: CompositionRoot | None,
        request: ToolExecutionRequest,
        arguments: dict[str, Any],
    ) -> str:
        if root is None:
            return ""
        tool_input = ToolInput.from_request(request, arguments)
        decision = await root.context.serial(
            TOOL_EXECUTION_AUTHORIZE,
            tool_input,
        )
        if decision is None:
            return ""
        return decision.value.strip() or "工具调用被拦截"

    async def _settle(
        self,
        root: CompositionRoot | None,
        request: ToolExecutionRequest,
        result: ToolExecutionResult,
    ) -> ToolExecutionResult:
        if root is not None:
            await root.context.observe(
                TOOL_RESULT,
                ToolResult.from_execution(request, result),
            )
        return result
