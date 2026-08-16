from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol

from agent.plugin_composition.context import Context, FiberHandle, HealthHandle
from agent.plugin_composition.model import CompositionError, IncidentView, ServiceKey


_NAME = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_FACTORY_EXPORT = re.compile(r"^[A-Za-z_][A-Za-z0-9_.:]*$")


class ChannelCapability(StrEnum):
    INBOUND = "inbound"
    OUTBOUND = "outbound"
    CONTROL = "control"
    TURN_STREAM = "turn_stream"


class InboundIdentity(StrEnum):
    PROVIDER_MESSAGE_ID = "provider_message_id"


class DeliveryStatus(StrEnum):
    DELIVERED = "delivered"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class CredentialRef:
    """Opaque credential path; it never contains or resolves secret bytes."""

    path: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.path, tuple) or not self.path:
            raise ValueError("CredentialRef.path 必须是非空 tuple")
        for segment in self.path:
            if (
                not isinstance(segment, str)
                or not segment
                or segment.strip() != segment
                or segment in {".", ".."}
                or "/" in segment
                or "\\" in segment
                or "\x00" in segment
            ):
                raise ValueError("CredentialRef.path 包含非法段")


class ProviderClient(Protocol):
    async def aclose(self) -> None: ...


class ProviderClientFactory(Protocol):
    async def create(
        self,
        credentials: Mapping[str, CredentialRef],
    ) -> ProviderClient: ...

    async def aclose(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ChannelFactoryContext:
    snapshot_id: str
    generation_id: str
    binding_token: str
    config: Mapping[str, object]
    credentials: Mapping[str, CredentialRef]
    provider_client_factory: ProviderClientFactory

    def __post_init__(self) -> None:
        _text(self.snapshot_id, "snapshot_id")
        _text(self.generation_id, "generation_id")
        _text(self.binding_token, "binding_token")
        config = _freeze_channel_config(self.config)
        if not isinstance(config, Mapping):
            raise TypeError("channel factory config 必须是 mapping")
        credentials = _credential_refs(self.credentials)
        object.__setattr__(self, "config", config)
        object.__setattr__(self, "credentials", credentials)


@dataclass(frozen=True, slots=True)
class ChannelReady:
    binding_token: str
    subscriptions: tuple[str, ...] = ()
    admission_open: bool = False

    def __post_init__(self) -> None:
        _text(self.binding_token, "binding_token")
        object.__setattr__(self, "subscriptions", _text_tuple(self.subscriptions, "subscriptions"))
        if not isinstance(self.admission_open, bool):
            raise TypeError("admission_open 必须是 bool")


@dataclass(frozen=True, slots=True)
class ChannelCleanupFailure:
    stage: str
    plugin_id: str
    generation_id: str
    binding_token: str
    resource: str
    error_type: str
    message: str
    retry_action: str

    def __post_init__(self) -> None:
        for field_name in (
            "stage",
            "plugin_id",
            "generation_id",
            "binding_token",
            "resource",
            "error_type",
            "message",
            "retry_action",
        ):
            _text(getattr(self, field_name), field_name)


@dataclass(frozen=True, slots=True)
class StopReceipt:
    binding_token: str
    resources_closed: bool
    failures: tuple[ChannelCleanupFailure, ...] = ()

    def __post_init__(self) -> None:
        _text(self.binding_token, "binding_token")
        if not isinstance(self.resources_closed, bool):
            raise TypeError("resources_closed 必须是 bool")
        if not isinstance(self.failures, tuple) or any(
            not isinstance(item, ChannelCleanupFailure) for item in self.failures
        ):
            raise TypeError("failures 必须是 ChannelCleanupFailure tuple")


@dataclass(frozen=True, slots=True)
class ProviderDeliveryRequest:
    binding_token: str
    delivery_id: str
    recipient: str
    body: str

    def __post_init__(self) -> None:
        _text(self.binding_token, "binding_token")
        _text(self.delivery_id, "delivery_id")
        _text(self.recipient, "recipient")
        if not isinstance(self.body, str):
            raise TypeError("body 必须是 str")


@dataclass(frozen=True, slots=True)
class ProviderDeliveryReceipt:
    delivery_id: str
    status: DeliveryStatus
    provider_ids: tuple[str, ...] = ()
    error: str | None = None

    def __post_init__(self) -> None:
        _text(self.delivery_id, "delivery_id")
        if not isinstance(self.status, DeliveryStatus):
            raise TypeError("status 必须是 DeliveryStatus")
        object.__setattr__(self, "provider_ids", _text_tuple(self.provider_ids, "provider_ids"))
        if self.error is not None:
            _text(self.error, "error")


class ChannelAdapter(Protocol):
    async def start(self) -> ChannelReady: ...

    async def deliver(self, request: ProviderDeliveryRequest) -> ProviderDeliveryReceipt: ...

    async def stop(self) -> StopReceipt: ...


@dataclass(frozen=True, slots=True)
class ChannelDefinition:
    """Describe one plugin-owned channel factory without opening it."""

    name: str
    capabilities: frozenset[ChannelCapability]
    factory_export: str
    inbound_identity: InboundIdentity | None
    credential_paths: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or _NAME.fullmatch(self.name) is None:
            raise ValueError(f"channel name 无效: {self.name}")
        if not isinstance(self.capabilities, frozenset) or not self.capabilities:
            raise ValueError("channel capabilities 必须是非空 frozenset")
        if any(not isinstance(item, ChannelCapability) for item in self.capabilities):
            raise ValueError("channel capabilities 必须只包含 ChannelCapability")
        if (
            not isinstance(self.factory_export, str)
            or _FACTORY_EXPORT.fullmatch(self.factory_export) is None
            or ".." in self.factory_export
            or self.factory_export.endswith((".", ":"))
        ):
            raise ValueError(f"channel factory_export 无效: {self.factory_export}")
        has_inbound = ChannelCapability.INBOUND in self.capabilities
        if has_inbound and not isinstance(self.inbound_identity, InboundIdentity):
            raise ValueError("inbound channel 必须声明 inbound_identity")
        if not has_inbound and self.inbound_identity is not None:
            raise ValueError("非 inbound channel 不得声明 inbound_identity")
        object.__setattr__(self, "credential_paths", _credential_paths(self.credential_paths))


@dataclass(frozen=True, slots=True)
class ChannelDescriptor:
    owner: str
    name: str
    capabilities: tuple[ChannelCapability, ...]
    factory_export: str
    inbound_identity: InboundIdentity | None
    credential_paths: tuple[str, ...]

    def __post_init__(self) -> None:
        _text(self.owner, "owner")
        if not isinstance(self.name, str) or _NAME.fullmatch(self.name) is None:
            raise ValueError(f"channel descriptor name 无效: {self.name}")
        if not self.capabilities or any(
            not isinstance(item, ChannelCapability) for item in self.capabilities
        ):
            raise ValueError("channel descriptor capabilities 类型无效")
        if tuple(sorted(self.capabilities, key=lambda item: item.value)) != self.capabilities:
            raise ValueError("channel descriptor capabilities 顺序必须 canonical")
        if (
            not isinstance(self.factory_export, str)
            or _FACTORY_EXPORT.fullmatch(self.factory_export) is None
            or ".." in self.factory_export
            or self.factory_export.endswith((".", ":"))
        ):
            raise ValueError("channel descriptor factory_export 无效")
        has_inbound = ChannelCapability.INBOUND in self.capabilities
        if has_inbound and not isinstance(self.inbound_identity, InboundIdentity):
            raise ValueError("inbound channel descriptor 必须声明 inbound_identity")
        if not has_inbound and self.inbound_identity is not None:
            raise ValueError("非 inbound channel descriptor 不得声明 inbound_identity")
        object.__setattr__(self, "credential_paths", _credential_paths(self.credential_paths))


@dataclass(frozen=True, slots=True)
class ChannelFactoryProvenance:
    plugin_id: str
    generation_id: str
    channel_name: str
    source_revision: str
    config_revision: str
    factory_export: str

    def __post_init__(self) -> None:
        _text(self.plugin_id, "plugin_id")
        _text(self.generation_id, "generation_id")
        if not isinstance(self.channel_name, str) or _NAME.fullmatch(self.channel_name) is None:
            raise ValueError(f"factory provenance channel_name 无效: {self.channel_name}")
        if not isinstance(self.source_revision, str):
            raise ValueError("source_revision 必须是字符串")
        if not isinstance(self.config_revision, str):
            raise ValueError("config_revision 必须是字符串")
        if not isinstance(self.factory_export, str) or _FACTORY_EXPORT.fullmatch(self.factory_export) is None:
            raise ValueError("factory provenance factory_export 无效")


@dataclass(frozen=True, slots=True)
class ChannelFactoryFreezeInput:
    """Core-only input carrying source/config provenance into a freeze."""

    generation_id: str
    source_revision: str = ""
    config_revision: str = ""

    def __post_init__(self) -> None:
        _text(self.generation_id, "generation_id")
        if not isinstance(self.source_revision, str):
            raise ValueError("source_revision 必须是字符串")
        if not isinstance(self.config_revision, str):
            raise ValueError("config_revision 必须是字符串")


@dataclass(frozen=True, slots=True)
class ChannelRegistrySnapshot:
    descriptors: tuple[ChannelDescriptor, ...]
    factories: tuple[ChannelFactoryProvenance, ...]
    identity: str
    root_instance_token: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        descriptors = tuple(self.descriptors)
        factories = tuple(self.factories)
        if any(not isinstance(item, ChannelDescriptor) for item in descriptors):
            raise TypeError("channel registry descriptor 类型无效")
        if any(not isinstance(item, ChannelFactoryProvenance) for item in factories):
            raise TypeError("channel registry factory provenance 类型无效")
        if len({item.name for item in descriptors}) != len(descriptors):
            raise ValueError("channel registry descriptor 名称重复")
        factory_keys = tuple(_factory_sort_key(item) for item in factories)
        if len(set(factory_keys)) != len(factory_keys):
            raise ValueError("channel registry factory provenance 重复")
        if tuple(sorted(descriptors, key=lambda item: item.name)) != descriptors:
            raise ValueError("channel registry descriptors 必须按 name 排序")
        if tuple(sorted(factories, key=_factory_sort_key)) != factories:
            raise ValueError("channel registry factories 必须按 provenance 排序")
        if self.identity != _registry_identity(descriptors, factories):
            raise ValueError("channel registry identity 与内容不匹配")
        object.__setattr__(self, "descriptors", descriptors)
        object.__setattr__(self, "factories", factories)


CHANNELS = ServiceKey["PluginChannels"]("core.channels")


@dataclass(slots=True)
class _ChannelRegistration:
    owner: str
    definition: ChannelDefinition
    descriptor: ChannelDescriptor
    owner_fiber: FiberHandle
    activation_token: object
    generation_id: str
    incident_reporter: Callable[[str, str], IncidentView]
    health: HealthHandle | None = None


class _ChannelDeclarations:
    """Own one Root-local declaration set until Core freezes it."""

    def __init__(self) -> None:
        self._registrations: dict[str, _ChannelRegistration] = {}
        self._frozen: ChannelRegistrySnapshot | None = None

    async def register(self, ctx: Context, definition: ChannelDefinition) -> None:
        """Validate and register one blueprint as Fiber-owned Effects."""

        normalized = _normalize_definition(definition)
        owner_fiber = ctx.fiber
        activation_token = owner_fiber.activation_token
        if activation_token is None:
            raise CompositionError(
                "INACTIVE_FIBER",
                f"{ctx.runtime.plugin_id} 当前 Fiber 没有 active activation",
            )
        registration: _ChannelRegistration | None = None

        def setup() -> Callable[[], None]:
            nonlocal registration
            registration, cleanup = self._register(
                ctx.runtime.plugin_id,
                normalized,
                owner_fiber,
                activation_token,
                ctx.generation_id,
                ctx.report_incident,
            )
            return cleanup

        registration_effect = await ctx.effect(
            setup,
            label=f"channel:{normalized.name}",
        )
        try:
            health = await ctx.health(f"channel:{normalized.name}", required=True)
        except BaseException:
            await registration_effect.aclose()
            raise
        assert registration is not None
        registration.health = health

    def freeze(
        self,
        root_instance_token: object,
        *,
        factory_provenance_by_owner: Mapping[
            str,
            ChannelFactoryFreezeInput | tuple[str, str, str],
        ]
        | None = None,
    ) -> ChannelRegistrySnapshot:
        """Freeze declarations with Core-supplied factory provenance."""

        if self._frozen is not None:
            if self._frozen.root_instance_token is not root_instance_token:
                raise RuntimeError("channel declaration registry 属于另一棵 Root")
            return self._frozen
        provenance = factory_provenance_by_owner or {}
        registrations = tuple(
            sorted(self._registrations.values(), key=lambda item: item.definition.name)
        )
        descriptors = tuple(item.descriptor for item in registrations)
        factories = tuple(
            sorted(
                (
                    _make_provenance(item, provenance.get(item.owner))
                    for item in registrations
                ),
                key=_factory_sort_key,
            )
        )
        snapshot = ChannelRegistrySnapshot(
            descriptors=descriptors,
            factories=factories,
            identity=_registry_identity(descriptors, factories),
            root_instance_token=root_instance_token,
        )
        self._frozen = snapshot
        return snapshot

    def _register(
        self,
        owner: str,
        definition: ChannelDefinition,
        owner_fiber: FiberHandle,
        activation_token: object,
        generation_id: str,
        incident_reporter: Callable[[str, str], IncidentView],
    ) -> tuple[_ChannelRegistration, Callable[[], None]]:
        if self._frozen is not None:
            raise CompositionError(
                "PLUGIN_CHANNELS_FROZEN",
                "插件 channel 声明已冻结，不能新增",
            )
        if definition.name in self._registrations:
            raise CompositionError(
                "DUPLICATE_PLUGIN_CHANNEL",
                f"插件 channel 名称重复: {definition.name}",
            )
        descriptor = ChannelDescriptor(
            owner=owner,
            name=definition.name,
            capabilities=tuple(sorted(definition.capabilities, key=lambda item: item.value)),
            factory_export=definition.factory_export,
            inbound_identity=definition.inbound_identity,
            credential_paths=definition.credential_paths,
        )
        registration = _ChannelRegistration(
            owner=owner,
            definition=definition,
            descriptor=descriptor,
            owner_fiber=owner_fiber,
            activation_token=activation_token,
            generation_id=generation_id,
            incident_reporter=incident_reporter,
        )
        self._registrations[definition.name] = registration

        def cleanup() -> None:
            if self._registrations.get(definition.name) is registration:
                del self._registrations[definition.name]

        return registration, cleanup


class PluginChannels:
    """Expose only Fiber-owned channel blueprint registration to plugins."""

    def __init__(self, root_instance_token: object) -> None:
        self._root_instance_token = root_instance_token
        self._declarations = _ChannelDeclarations()

    async def register(self, ctx: Context, definition: ChannelDefinition) -> None:
        """Register one channel blueprint through the Core-owned collector."""

        if (
            ctx._root_instance_token() is not self._root_instance_token
            or ctx.require(CHANNELS) is not self
        ):
            raise CompositionError(
                "CHANNEL_SERVICE_ROOT_MISMATCH",
                "插件 channel Service 不属于当前 Root",
            )
        await self._declarations.register(ctx, definition)


def _freeze_plugin_channels(
    value: object,
    root_instance_token: object,
    *,
    factory_provenance_by_owner: Mapping[
        str,
        ChannelFactoryFreezeInput | tuple[str, str, str],
    ]
    | None = None,
) -> ChannelRegistrySnapshot:
    """Freeze the exact Core-created channel declaration facade."""

    if not isinstance(value, PluginChannels):
        raise RuntimeError("RuntimeSnapshot channel Service 类型无效")
    if value._root_instance_token is not root_instance_token:
        raise RuntimeError("RuntimeSnapshot channel Service 不属于 exact Root")
    return value._declarations.freeze(
        root_instance_token,
        factory_provenance_by_owner=factory_provenance_by_owner,
    )


def _normalize_definition(definition: ChannelDefinition) -> ChannelDefinition:
    if not isinstance(definition, ChannelDefinition):
        raise TypeError("PluginChannels.register 只接受 ChannelDefinition")
    return ChannelDefinition(
        name=definition.name,
        capabilities=frozenset(definition.capabilities),
        factory_export=definition.factory_export,
        inbound_identity=definition.inbound_identity,
        credential_paths=tuple(definition.credential_paths),
    )


def _make_provenance(
    registration: _ChannelRegistration,
    supplied: ChannelFactoryFreezeInput | tuple[str, str, str] | None,
) -> ChannelFactoryProvenance:
    if supplied is None:
        source = ChannelFactoryFreezeInput(registration.generation_id)
        return ChannelFactoryProvenance(
            plugin_id=registration.owner,
            generation_id=source.generation_id,
            channel_name=registration.definition.name,
            source_revision=source.source_revision,
            config_revision=source.config_revision,
            factory_export=registration.definition.factory_export,
        )
    if isinstance(supplied, ChannelFactoryFreezeInput):
        result = ChannelFactoryProvenance(
            plugin_id=registration.owner,
            generation_id=supplied.generation_id,
            channel_name=registration.definition.name,
            source_revision=supplied.source_revision,
            config_revision=supplied.config_revision,
            factory_export=registration.definition.factory_export,
        )
    elif isinstance(supplied, tuple) and len(supplied) == 3:
        result = ChannelFactoryProvenance(
            plugin_id=registration.owner,
            generation_id=supplied[0],
            channel_name=registration.definition.name,
            source_revision=supplied[1],
            config_revision=supplied[2],
            factory_export=registration.definition.factory_export,
        )
    else:
        raise TypeError("channel factory provenance 输入类型无效")
    return result


def _factory_sort_key(item: ChannelFactoryProvenance) -> tuple[str, str, str, str, str, str]:
    return (
        item.plugin_id,
        item.generation_id,
        item.channel_name,
        item.source_revision,
        item.config_revision,
        item.factory_export,
    )


def _registry_identity(
    descriptors: tuple[ChannelDescriptor, ...],
    factories: tuple[ChannelFactoryProvenance, ...],
) -> str:
    payload = {
        "descriptors": [
            {
                "owner": item.owner,
                "name": item.name,
                "capabilities": [capability.value for capability in item.capabilities],
                "factory_export": item.factory_export,
                "inbound_identity": (
                    None
                    if item.inbound_identity is None
                    else item.inbound_identity.value
                ),
                "credential_paths": list(item.credential_paths),
            }
            for item in descriptors
        ],
        "factories": [
            {
                "plugin_id": item.plugin_id,
                "generation_id": item.generation_id,
                "channel_name": item.channel_name,
                "source_revision": item.source_revision,
                "config_revision": item.config_revision,
                "factory_export": item.factory_export,
            }
            for item in factories
        ],
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _credential_paths(value: object) -> tuple[str, ...]:
    if not isinstance(value, tuple) or not value:
        raise ValueError("credential_paths 必须是非空 tuple")
    result: list[str] = []
    for path in value:
        if not isinstance(path, str) or not path or path.strip() != path:
            raise ValueError("credential_paths 必须是非空字符串")
        if any(not part or part in {".", ".."} for part in path.split(".")):
            raise ValueError(f"credential path 无效: {path}")
        if path in result:
            raise ValueError(f"credential_paths 重复: {path}")
        result.append(path)
    return tuple(result)


def _credential_refs(
    value: Mapping[str, CredentialRef],
) -> Mapping[str, CredentialRef]:
    if not isinstance(value, Mapping):
        raise TypeError("credentials 必须是 mapping")
    result: dict[str, CredentialRef] = {}
    for path in sorted(value):
        ref = value[path]
        if not isinstance(path, str) or not isinstance(ref, CredentialRef):
            raise TypeError("credentials 必须映射到 CredentialRef")
        if path != ".".join(ref.path):
            raise ValueError(f"credential path 与 ref 不一致: {path}")
        result[path] = ref
    return MappingProxyType(result)


def _freeze_channel_config(value: object, *, seen: frozenset[int] = frozenset()) -> object:
    if value is None or isinstance(value, (bool, int, str, CredentialRef)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("channel factory config 不接受非有限 float")
        return value
    if isinstance(value, Mapping):
        marker = id(value)
        if marker in seen:
            raise ValueError("channel factory config 不接受 cycle")
        next_seen = seen | {marker}
        result: dict[str, object] = {}
        for key in sorted(value):
            if not isinstance(key, str):
                raise TypeError("channel factory config mapping key 必须是 str")
            result[key] = _freeze_channel_config(value[key], seen=next_seen)
        return MappingProxyType(result)
    if isinstance(value, (list, tuple)):
        marker = id(value)
        if marker in seen:
            raise ValueError("channel factory config 不接受 cycle")
        next_seen = seen | {marker}
        return tuple(_freeze_channel_config(item, seen=next_seen) for item in value)
    raise TypeError(f"channel factory config 值类型无效: {type(value).__name__}")


def _text_tuple(value: object, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise TypeError(f"{field_name} 必须是 tuple")
    result = tuple(_text(item, field_name) for item in value)
    if len(set(result)) != len(result):
        raise ValueError(f"{field_name} 不能重复")
    return result


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError(f"{field_name} 必须是非空且无首尾空白的字符串")
    if any(ord(char) < 32 for char in value):
        raise ValueError(f"{field_name} 不能包含控制字符")
    return value


__all__ = [
    "CHANNELS",
    "ChannelAdapter",
    "ChannelCapability",
    "ChannelCleanupFailure",
    "ChannelFactoryContext",
    "ChannelReady",
    "CredentialRef",
    "DeliveryStatus",
    "ChannelDefinition",
    "ChannelDescriptor",
    "ChannelFactoryFreezeInput",
    "ChannelFactoryProvenance",
    "ChannelRegistrySnapshot",
    "InboundIdentity",
    "PluginChannels",
    "ProviderClient",
    "ProviderClientFactory",
    "ProviderDeliveryReceipt",
    "ProviderDeliveryRequest",
    "StopReceipt",
    "_freeze_plugin_channels",
]
