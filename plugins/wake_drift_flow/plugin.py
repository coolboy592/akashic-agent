from __future__ import annotations

from plugins.wake_proactive.modules import build_wake_drift_modules
from plugins.wake_proactive.runtime import WakeRuntime


api_version = 3
name = "wake_drift_flow"
version = "3.0.0"
desc = "Wake drift flow private island"
author = "Akashic Core"
inject = ()
skill_roots = ()
drift_skill_roots = ()
workspace_roots = ()


def apply(ctx: object, config: object) -> None:
    """Keep the private drift descriptor side-effect free during composition."""

    _ = ctx, config


class WakeDriftModuleFactory:
    lifecycle_id = "wake"

    def __call__(self, runtime: object) -> list[object]:
        if not isinstance(runtime, WakeRuntime):
            raise RuntimeError("wake drift flow 收到未知 Runtime")
        return build_wake_drift_modules(runtime)
