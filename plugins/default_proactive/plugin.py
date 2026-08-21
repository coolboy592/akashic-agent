from __future__ import annotations

from plugins.default_proactive.runtime import (
    ProactiveFlowRuntime,
    build_default_proactive_modules,
)
from plugins.default_proactive.factory import AgentTickFactory
from proactive_v2.lifecycle import ProactiveLifecycleSpec
from proactive_v2.runtime_scope import ProactiveRuntimeScope


api_version = 3
name = "default_proactive"
version = "3.0.0"
desc = "Default proactive runtime private island"
author = "Akashic Core"
inject = ()
skill_roots = ()
drift_skill_roots = ()
workspace_roots = ()


def apply(ctx: object, config: object) -> None:
    """Keep the private runtime descriptor side-effect free during composition."""

    _ = ctx, config


class DefaultRuntimeFactory:
    lifecycle_id = "default"

    def __call__(self, scope: ProactiveRuntimeScope) -> object:
        return AgentTickFactory(scope).build_runtime()


class DefaultModuleFactory:
    lifecycle_id = "default"

    def __call__(self, runtime: object) -> list[object]:
        if not isinstance(runtime, ProactiveFlowRuntime):
            raise RuntimeError("default proactive 收到未知 Runtime")
        return build_default_proactive_modules(runtime)


def build_default_lifecycle() -> ProactiveLifecycleSpec:
    """Build the mature Default lifecycle without publishing a v2 plugin method."""

    return ProactiveLifecycleSpec(
        id="default",
        initial_slots=(
            "proactive:cfg",
            "proactive:session_key",
            "proactive:started_at",
            "proactive:last_user_at",
            "proactive:base_judge_send_threshold",
        ),
        terminal_slots=("run:next_wakeup",),
    )
