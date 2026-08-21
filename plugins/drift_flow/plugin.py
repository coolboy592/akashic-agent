from __future__ import annotations

from plugins.default_proactive.runtime import ProactiveFlowRuntime
from plugins.drift_flow.modules import build_drift_flow_modules


api_version = 3
name = "drift_flow"
version = "3.0.0"
desc = "Default drift flow private island"
author = "Akashic Core"
inject = ()
skill_roots = ()
drift_skill_roots = ()
workspace_roots = ()


def apply(ctx: object, config: object) -> None:
    """Keep the private drift descriptor side-effect free during composition."""

    _ = ctx, config


class DriftModuleFactory:
    lifecycle_id = "default"

    def __call__(self, runtime: object) -> list[object]:
        if not isinstance(runtime, ProactiveFlowRuntime):
            raise RuntimeError("drift flow 收到未知 Runtime")
        return build_drift_flow_modules(runtime)
