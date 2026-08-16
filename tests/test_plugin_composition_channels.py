from __future__ import annotations

from pathlib import Path

import pytest

from agent.plugin_composition import (
    CHANNELS,
    ChannelCapability,
    ChannelDefinition,
    CompositionError,
    CompositionRoot,
    CredentialRef,
    InboundIdentity,
    PluginChannels,
    PluginRuntime,
)
from agent.plugin_composition.channels import (
    ChannelDescriptor,
    ChannelFactoryFreezeInput,
    ChannelFactoryProvenance,
    ChannelRegistrySnapshot,
    _freeze_plugin_channels,
    _registry_identity,
)


def _runtime(plugin_id: str, root: Path, *, generation: str = "plugin-generation") -> PluginRuntime:
    plugin_dir = root / plugin_id
    plugin_dir.mkdir(parents=True, exist_ok=True)
    return PluginRuntime(
        plugin_id=plugin_id,
        plugin_dir=plugin_dir,
        data_dir=plugin_dir / "data",
        workspace=plugin_dir / "workspace",
        config=None,
    )


def _definition(name: str = "feishu") -> ChannelDefinition:
    return ChannelDefinition(
        name=name,
        capabilities=frozenset(ChannelCapability),
        factory_export=f"{name}:build_channel",
        inbound_identity=InboundIdentity.PROVIDER_MESSAGE_ID,
        credential_paths=("app_id", "app_secret"),
    )


def _provenance(name: str, *, generation: str = "plugin-generation") -> ChannelFactoryProvenance:
    definition = _definition(name)
    return ChannelFactoryProvenance(
        plugin_id="plugin",
        generation_id=generation,
        channel_name=name,
        source_revision="source-1",
        config_revision="config-1",
        factory_export=definition.factory_export,
    )


@pytest.mark.asyncio
async def test_channel_registry_registration_health_freeze_and_effect_cleanup(
    tmp_path: Path,
) -> None:
    root = CompositionRoot("root-generation")
    channels = PluginChannels(root.instance_token)
    await root.context.provide(CHANNELS, channels)

    async def apply(ctx) -> None:
        await ctx.require(CHANNELS).register(ctx, _definition())

    fiber = await root.mount(
        apply,
        name="plugin",
        runtime=_runtime("plugin", tmp_path),
        inject=(CHANNELS,),
    )
    snapshot = _freeze_plugin_channels(
        channels,
        root.instance_token,
        factory_provenance={
            "feishu": ChannelFactoryFreezeInput(
                "plugin-generation",
                source_revision="source-1",
                config_revision="config-1",
            )
        },
    )
    assert snapshot.descriptors[0].owner == "plugin"
    assert snapshot.descriptors[0].capabilities == tuple(
        sorted(ChannelCapability, key=lambda item: item.value)
    )
    assert snapshot.factories[0].source_revision == "source-1"
    assert root.receipt().health[0].required is True
    assert root.receipt().effects == (
        "root:service:core.channels",
        "plugin:channel:feishu",
        "plugin:health:channel:feishu",
    )

    await fiber.dispose()
    assert _freeze_plugin_channels(channels, root.instance_token) is snapshot
    assert root.receipt().effects == ("root:service:core.channels",)
    await root.dispose()


@pytest.mark.asyncio
async def test_channel_registry_rejects_duplicate_frozen_and_wrong_root(
    tmp_path: Path,
) -> None:
    root = CompositionRoot("root-generation")
    channels = PluginChannels(root.instance_token)
    await root.context.provide(CHANNELS, channels)
    captured = None

    async def duplicate(ctx) -> None:
        nonlocal captured
        captured = ctx
        service = ctx.require(CHANNELS)
        await service.register(ctx, _definition())
        await service.register(ctx, _definition())

    _ = await root.mount(
        duplicate,
        name="plugin",
        runtime=_runtime("plugin", tmp_path),
        inject=(CHANNELS,),
    )
    assert not root.receipt().ready
    assert root.receipt().health == ()
    assert len(_freeze_plugin_channels(channels, root.instance_token).descriptors) == 0
    assert captured is not None

    other = CompositionRoot("other-generation")
    other_channels = PluginChannels(other.instance_token)
    await other.context.provide(CHANNELS, other_channels)
    with pytest.raises(CompositionError, match="不属于当前 Root"):
        await channels.register(other.context, _definition())

    await other.dispose()
    await root.dispose()


@pytest.mark.asyncio
async def test_channel_registry_identity_is_root_independent_and_ordered(
    tmp_path: Path,
) -> None:
    identities: list[str] = []
    for suffix, names in (("candidate", ("qqbot", "feishu")), ("formal", ("feishu", "qqbot"))):
        root = CompositionRoot(f"{suffix}-root")
        channels = PluginChannels(root.instance_token)
        await root.context.provide(CHANNELS, channels)

        async def apply(ctx) -> None:
            service = ctx.require(CHANNELS)
            for name in names:
                await service.register(ctx, _definition(name))

        _ = await root.mount(
            apply,
            name="plugin",
            runtime=_runtime("plugin", tmp_path / suffix),
            inject=(CHANNELS,),
        )
        snapshot = _freeze_plugin_channels(
            channels,
            root.instance_token,
            factory_provenance={
                name: ChannelFactoryProvenance(
                    plugin_id="plugin",
                    generation_id="same-generation",
                    channel_name=name,
                    source_revision="same-source",
                    config_revision="same-config",
                    factory_export=_definition(name).factory_export,
                )
                for name in names
            },
        )
        identities.append(snapshot.identity)
        assert snapshot.root_instance_token is root.instance_token
        await root.dispose()
    assert identities[0] == identities[1]


def test_channel_declarations_and_provenance_reject_invalid_values() -> None:
    with pytest.raises(ValueError):
        _ = _definition("BadName")
    with pytest.raises(ValueError):
        _ = ChannelDefinition(
            name="feishu",
            capabilities=frozenset({"inbound"}),  # type: ignore[arg-type]
            factory_export=lambda: None,  # type: ignore[arg-type]
            inbound_identity=InboundIdentity.PROVIDER_MESSAGE_ID,
            credential_paths=("app_id", "app_id"),
        )
    with pytest.raises(ValueError):
        _ = CredentialRef(("app_secret", ".."))


def test_channel_snapshot_identity_is_content_addressed() -> None:
    descriptor = _definition()
    frozen_descriptor = ChannelDescriptor(
        owner="plugin",
        name=descriptor.name,
        capabilities=tuple(sorted(descriptor.capabilities, key=lambda item: item.value)),
        factory_export=descriptor.factory_export,
        inbound_identity=descriptor.inbound_identity,
        credential_paths=descriptor.credential_paths,
    )
    provenance = _provenance("feishu")
    snapshot = ChannelRegistrySnapshot(
        descriptors=(frozen_descriptor,),
        factories=(provenance,),
        identity=_registry_identity((frozen_descriptor,), (provenance,)),
        root_instance_token=object(),
    )
    assert snapshot.identity
    with pytest.raises(ValueError, match="identity"):
        _ = ChannelRegistrySnapshot(
            descriptors=snapshot.descriptors,
            factories=snapshot.factories,
            identity="not-the-digest",
            root_instance_token=object(),
        )

    with pytest.raises(ValueError, match="名称重复"):
        _ = ChannelRegistrySnapshot(
            descriptors=(frozen_descriptor, frozen_descriptor),
            factories=(provenance, provenance),
            identity="unused",
            root_instance_token=object(),
        )
