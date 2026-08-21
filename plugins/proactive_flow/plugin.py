from __future__ import annotations

from plugins.default_proactive.runtime import ProactiveFlowRuntime
from plugins.proactive_flow.modules import build_proactive_flow_modules


api_version = 3
name = "proactive_flow"
version = "3.0.0"
desc = "Default proactive flow private island"
author = "Akashic Core"
inject = ()
skill_roots = ()
drift_skill_roots = ()
workspace_roots = ()


def apply(ctx: object, config: object) -> None:
    """Keep the private flow descriptor side-effect free during composition."""

    _ = ctx, config


class ProactiveModuleFactory:
    lifecycle_id = "default"

    def __call__(self, runtime: object) -> list[object]:
        if not isinstance(runtime, ProactiveFlowRuntime):
            raise RuntimeError("proactive flow 收到未知 Runtime")
        return build_proactive_flow_modules(runtime)
