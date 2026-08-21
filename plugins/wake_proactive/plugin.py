from __future__ import annotations

from typing import cast

from plugins.wake_proactive.runtime import WakeRuntime
from proactive_v2.lifecycle import ProactiveLifecycleSpec
from proactive_v2.runtime_scope import ProactiveRuntimeScope


api_version = 3
name = "wake_proactive"
version = "3.0.0"
desc = "Wake proactive runtime private island"
author = "Akashic Core"
inject = ()
skill_roots = ()
drift_skill_roots = ()
workspace_roots = ()


def apply(ctx: object, config: object) -> None:
    """Keep the private runtime descriptor side-effect free during composition."""

    _ = ctx, config


class WakeRuntimeFactory:
    lifecycle_id = "wake"

    def __call__(self, scope: ProactiveRuntimeScope) -> object:
        return WakeRuntime(scope)


class WakeProactiveModuleFactory:
    lifecycle_id = "wake"

    def __call__(self, runtime: object) -> list[object]:
        from plugins.wake_proactive.modules import build_wake_runtime_modules
        return cast(list[object], build_wake_runtime_modules(runtime))


def build_wake_lifecycle() -> ProactiveLifecycleSpec:
    """Build the mature Wake lifecycle without publishing a v2 plugin method."""

    return ProactiveLifecycleSpec(
        id="wake",
        initial_slots=(
            "proactive:cfg",
            "proactive:session_key",
            "proactive:started_at",
            "proactive:last_user_at",
        ),
        terminal_slots=("run:next_wakeup",),
    )
