from __future__ import annotations

from typing import Any, cast

from agent.plugin_composition.channels import (
    ChannelAdapter,
    ChannelCapability,
    ChannelFactoryContext,
    CoreChannelDefinition,
)


def build_core_channel_definition(channel: object) -> CoreChannelDefinition:
    """Project one Core-owned native channel into the committed v3 catalog."""

    # 1. Validate the native factory at the Core integration boundary.
    name = getattr(channel, "name", None)
    if not isinstance(name, str) or not name:
        raise ValueError("Core channel name 无效")
    build_adapter = getattr(channel, "build_v3_adapter", None)
    if not callable(build_adapter):
        raise TypeError(f"Core channel {name} 缺少 build_v3_adapter(context)")
    native_factory = cast(Any, build_adapter)

    # 2. Freeze one stable definition; the channel owns provider-specific delivery.
    def factory(context: ChannelFactoryContext) -> ChannelAdapter:
        adapter = native_factory(context)
        if adapter is None:
            raise TypeError(f"Core channel {name} build_v3_adapter 返回空值")
        return cast(ChannelAdapter, adapter)

    return CoreChannelDefinition(
        name=name,
        capabilities=frozenset({ChannelCapability.OUTBOUND}),
        factory=factory,
        inbound_identity=None,
        source_revision="core-native-v3",
        config_revision="core-native-v3",
        generation_id="core-native-v3",
        config={"channel": name},
    )


__all__ = ["build_core_channel_definition"]
