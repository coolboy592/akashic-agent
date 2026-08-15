from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field

from agent.plugin_composition import Context, ServiceKey

PROBE_SIGNAL = ServiceKey["ProbeSignal"]("probe.signal")
PROBE_FORMATTER = ServiceKey[Callable[[str], str]]("probe.formatter")


@dataclass(frozen=True, slots=True)
class ProbeSignal:
    value: str


@dataclass(slots=True)
class ProbeTrace:
    events: list[str] = field(default_factory=list)


class ProbeProvider:
    name = "probe-provider"
    inject = ()

    def __init__(self, value: str, trace: ProbeTrace) -> None:
        self.value = value
        self.trace = trace

    async def apply(self, ctx: Context) -> None:
        """Persist plugin-owned state and provide the probe signal."""

        # 1. Core assigns the generation-scoped root; the plugin owns its schema.
        _ = (ctx.data_root / "state.json").write_text(
            json.dumps(
                {"generation": ctx.generation_id, "value": self.value},
                ensure_ascii=False,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        # 2. Service and cleanup are both owned by this Fiber.
        self.trace.events.append(f"provider:load:{self.value}")
        _ = await ctx.provide(PROBE_SIGNAL, ProbeSignal(self.value))
        _ = await ctx.effect(
            lambda: lambda: self.trace.events.append(f"provider:cleanup:{self.value}"),
            label="probe-provider-trace",
        )


class ProbeConsumer:
    name = "probe-consumer"
    inject = (PROBE_SIGNAL,)

    def __init__(self, trace: ProbeTrace) -> None:
        self.trace = trace

    async def apply(self, ctx: Context) -> None:
        """Consume the required signal and mount one optional enhancement."""

        signal = ctx.require(PROBE_SIGNAL)
        self.trace.events.append(f"consumer:load:{signal.value}")

        async def apply_formatter(inner: Context) -> None:
            formatter = inner.require(PROBE_FORMATTER)
            self.trace.events.append(f"consumer:formatted:{formatter(signal.value)}")
            _ = await inner.effect(
                lambda: lambda: self.trace.events.append(
                    f"consumer:formatter-cleanup:{signal.value}"
                ),
                label="probe-formatter-consumer",
            )

        _ = await ctx.inject(
            (PROBE_FORMATTER,),
            apply_formatter,
            name="probe-formatter-consumer",
        )
        _ = await ctx.effect(
            lambda: lambda: self.trace.events.append(
                f"consumer:cleanup:{signal.value}"
            ),
            label="probe-consumer-trace",
        )


class ProbeFormatterProvider:
    name = "probe-formatter-provider"
    inject = ()

    async def apply(self, ctx: Context) -> None:
        _ = await ctx.provide(PROBE_FORMATTER, str.upper)
