from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import TypeAlias

from agent.plugin_composition.context import Context, FiberHandle, HealthHandle
from agent.plugin_composition.model import CompositionError, FiberState, ServiceKey


_NAME = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_EXPORT = re.compile(r"^[A-Za-z_][A-Za-z0-9_.:]*$")
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_.:-]{0,127}$")
_CHANNELS = frozenset({"alert", "content", "context"})


def _text(value: object, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} 必须是字符串")
    if not allow_empty and not value:
        raise ValueError(f"{field} 不能为空")
    if value.strip() != value:
        raise ValueError(f"{field} 不能有首尾空白")
    return value


def _tuple(value: object, field: str) -> tuple[object, ...]:
    if not isinstance(value, tuple):
        raise TypeError(f"{field} 必须是 tuple")
    return value


def _text_tuple(value: object, field: str, *, allow_empty: bool = True) -> tuple[str, ...]:
    items = _tuple(value, field)
    result = tuple(_text(item, f"{field}[]") for item in items)
    if not allow_empty and not result:
        raise ValueError(f"{field} 不能为空")
    if len(set(result)) != len(result):
        raise ValueError(f"{field} 不能包含重复值")
    return result


def _identifier(value: object, field: str) -> str:
    text = _text(value, field)
    if _IDENTIFIER.fullmatch(text) is None:
        raise ValueError(f"{field} 无效: {text}")
    return text


def _export(value: object, field: str = "handler_export") -> str:
    text = _text(value, field)
    if _EXPORT.fullmatch(text) is None or ".." in text or text.endswith((".", ":")):
        raise ValueError(f"{field} 无效: {text}")
    return text


@dataclass(frozen=True, slots=True)
class ProactiveSourceDefinition:
    """Describe one source without retaining a callable or runtime client."""

    name: str
    channels: tuple[str, ...]
    mcp_server: str
    fetch_tool: str
    ack_tool: str = ""
    fetch_page_size: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or _NAME.fullmatch(self.name) is None:
            raise ValueError(f"proactive source name 无效: {self.name}")
        channels = _text_tuple(self.channels, "channels", allow_empty=False)
        if any(channel not in _CHANNELS for channel in channels):
            raise ValueError(f"proactive source channel 无效: {channels}")
        _identifier(self.mcp_server, "mcp_server")
        _identifier(self.fetch_tool, "fetch_tool")
        if not isinstance(self.ack_tool, str):
            raise TypeError("ack_tool 必须是字符串")
        if self.ack_tool:
            _identifier(self.ack_tool, "ack_tool")
        if (
            isinstance(self.fetch_page_size, bool)
            or not isinstance(self.fetch_page_size, int)
            or self.fetch_page_size < 0
        ):
            raise ValueError("fetch_page_size 必须是非负整数")


@dataclass(frozen=True, slots=True)
class ProactiveModuleDefinition:
    """Describe one proactive DAG module and its exported handler name."""

    slot: str
    lifecycle_id: str
    requires: tuple[str, ...] = ()
    produces: tuple[str, ...] = ()
    collects: tuple[str, ...] = ()
    handler_export: str = ""
    domain_effect: str | None = None
    domain_effect_lookup_export: str | None = None

    def __post_init__(self) -> None:
        slot = _identifier(self.slot, "slot")
        if not slot.startswith("proactive."):
            raise ValueError(f"proactive module slot 必须以 proactive. 开头: {slot}")
        _identifier(self.lifecycle_id, "lifecycle_id")
        for field, value in (
            ("requires", self.requires),
            ("produces", self.produces),
            ("collects", self.collects),
        ):
            items = _text_tuple(value, field)
            if any(_IDENTIFIER.fullmatch(item) is None for item in items):
                raise ValueError(f"{field} 包含无效 capability")
        _export(self.handler_export)
        if (self.domain_effect is None) != (
            self.domain_effect_lookup_export is None
        ):
            raise ValueError(
                "domain_effect 与 domain_effect_lookup_export 必须同时提供"
            )
        if self.domain_effect is not None:
            _identifier(self.domain_effect, "domain_effect")
            _export(self.domain_effect_lookup_export, "domain_effect_lookup_export")


@dataclass(frozen=True, slots=True)
class FetchItems:
    """Report a source page containing items and its opaque next cursor."""

    items: tuple[object, ...]
    cursor: str | None = None

    def __post_init__(self) -> None:
        items = _tuple(self.items, "items")
        object.__setattr__(self, "items", tuple(_freeze_value(item) for item in items))
        _cursor(self.cursor)


@dataclass(frozen=True, slots=True)
class FetchEmpty:
    """Report an empty source page without treating it as a failure."""

    cursor: str | None = None

    def __post_init__(self) -> None:
        _cursor(self.cursor)


@dataclass(frozen=True, slots=True)
class FetchSkip:
    """Report a source that was intentionally skipped until an optional time."""

    reason: str
    retry_at: datetime | None = None

    def __post_init__(self) -> None:
        _text(self.reason, "reason")
        _aware_datetime(self.retry_at, "retry_at")


@dataclass(frozen=True, slots=True)
class FetchFailure:
    """Report a source failure without collapsing it into an empty result."""

    error: str
    retryable: bool

    def __post_init__(self) -> None:
        _text(self.error, "error")
        if not isinstance(self.retryable, bool):
            raise TypeError("retryable 必须是 bool")


FetchResult: TypeAlias = FetchItems | FetchEmpty | FetchSkip | FetchFailure


@dataclass(frozen=True, slots=True)
class AckCommitted:
    """Report IDs durably acknowledged by the source."""

    ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _text_tuple(self.ids, "ids")


@dataclass(frozen=True, slots=True)
class AckSkipped:
    """Report an acknowledgement intentionally skipped by the source."""

    reason: str

    def __post_init__(self) -> None:
        _text(self.reason, "reason")


@dataclass(frozen=True, slots=True)
class AckFailure:
    """Report an acknowledgement failure separately from fetch failures."""

    error: str
    retryable: bool

    def __post_init__(self) -> None:
        _text(self.error, "error")
        if not isinstance(self.retryable, bool):
            raise TypeError("retryable 必须是 bool")


AckResult: TypeAlias = AckCommitted | AckSkipped | AckFailure


@dataclass(frozen=True, slots=True)
class ProactiveSourceDescriptor:
    owner: str
    name: str
    channels: tuple[str, ...]
    mcp_server: str
    fetch_tool: str
    ack_tool: str
    fetch_page_size: int

    def __post_init__(self) -> None:
        _text(self.owner, "owner")
        definition = ProactiveSourceDefinition(
            name=self.name,
            channels=self.channels,
            mcp_server=self.mcp_server,
            fetch_tool=self.fetch_tool,
            ack_tool=self.ack_tool,
            fetch_page_size=self.fetch_page_size,
        )
        object.__setattr__(self, "channels", definition.channels)


@dataclass(frozen=True, slots=True)
class ProactiveModuleDescriptor:
    owner: str
    lifecycle_id: str
    slot: str
    requires: tuple[str, ...]
    produces: tuple[str, ...]
    collects: tuple[str, ...]
    handler_export: str
    domain_effect: str | None
    domain_effect_lookup_export: str | None = None

    def __post_init__(self) -> None:
        _text(self.owner, "owner")
        definition = ProactiveModuleDefinition(
            slot=self.slot,
            lifecycle_id=self.lifecycle_id,
            requires=self.requires,
            produces=self.produces,
            collects=self.collects,
            handler_export=self.handler_export,
            domain_effect=self.domain_effect,
            domain_effect_lookup_export=self.domain_effect_lookup_export,
        )
        for field in ("requires", "produces", "collects"):
            object.__setattr__(self, field, getattr(definition, field))


@dataclass(frozen=True, slots=True)
class ProactiveSourceBinding:
    """Bind one source descriptor to an exact Fiber activation."""

    descriptor: ProactiveSourceDescriptor
    generation_id: str
    definition: ProactiveSourceDefinition
    owner_fiber: FiberHandle
    activation_token: object
    health: HealthHandle

    @property
    def owner(self) -> str:
        return self.descriptor.owner

    def is_owned(self) -> bool:
        return (
            self.owner_fiber.state is FiberState.ACTIVE
            and self.owner_fiber.activation_token is self.activation_token
        )

    def is_live(self) -> bool:
        return self.is_owned() and self.health.healthy


@dataclass(frozen=True, slots=True)
class ProactiveModuleBinding:
    """Bind one module descriptor to an exact Fiber activation."""

    descriptor: ProactiveModuleDescriptor
    generation_id: str
    definition: ProactiveModuleDefinition
    owner_fiber: FiberHandle
    activation_token: object
    health: HealthHandle

    @property
    def owner(self) -> str:
        return self.descriptor.owner

    def is_owned(self) -> bool:
        return (
            self.owner_fiber.state is FiberState.ACTIVE
            and self.owner_fiber.activation_token is self.activation_token
        )

    def is_live(self) -> bool:
        return self.is_owned() and self.health.healthy


class ProactiveCatalog(
    Mapping[str, ProactiveSourceBinding | ProactiveModuleBinding]
):
    """Expose immutable source/module descriptors and live exact bindings."""

    __slots__ = (
        "_root_instance_token",
        "_sources",
        "_modules",
        "_source_descriptors",
        "_module_descriptors",
        "_identity",
    )

    def __init__(
        self,
        sources: Mapping[str, ProactiveSourceBinding],
        modules: Mapping[str, ProactiveModuleBinding],
        *,
        root_instance_token: object,
    ) -> None:
        self._root_instance_token = root_instance_token
        self._sources = MappingProxyType(dict(sorted(sources.items())))
        self._modules = MappingProxyType(dict(sorted(modules.items())))
        self._source_descriptors = tuple(
            sorted(
                (item.descriptor for item in self._sources.values()),
                key=lambda item: (item.owner, item.name),
            )
        )
        self._module_descriptors = tuple(
            sorted(
                (item.descriptor for item in self._modules.values()),
                key=lambda item: (item.owner, item.slot),
            )
        )
        payload = {
            "sources": [_source_identity(item) for item in self._source_descriptors],
            "modules": [_module_identity(item) for item in self._module_descriptors],
        }
        self._identity = _digest(payload)

    @property
    def root_instance_token(self) -> object:
        return self._root_instance_token

    @property
    def sources(self) -> Mapping[str, ProactiveSourceBinding]:
        return self._sources

    @property
    def modules(self) -> Mapping[str, ProactiveModuleBinding]:
        return self._modules

    @property
    def source_descriptors(self) -> tuple[ProactiveSourceDescriptor, ...]:
        return self._source_descriptors

    @property
    def module_descriptors(self) -> tuple[ProactiveModuleDescriptor, ...]:
        return self._module_descriptors

    @property
    def identity(self) -> str:
        return self._identity

    @property
    def catalog_digest(self) -> str:
        return self._identity

    def source(self, name: str) -> ProactiveSourceBinding | None:
        """Resolve a canonical owner/name key or a unique source name."""

        binding = self._sources.get(name)
        if binding is not None:
            return binding
        matches = tuple(item for item in self._sources.values() if item.descriptor.name == name)
        if len(matches) == 1:
            return matches[0]
        return None

    def module(self, slot: str) -> ProactiveModuleBinding | None:
        """Resolve a canonical owner/slot key or a unique module slot."""

        binding = self._modules.get(slot)
        if binding is not None:
            return binding
        matches = tuple(item for item in self._modules.values() if item.descriptor.slot == slot)
        if len(matches) == 1:
            return matches[0]
        return None

    @property
    def descriptors(self) -> tuple[ProactiveSourceDescriptor | ProactiveModuleDescriptor, ...]:
        return self._source_descriptors + self._module_descriptors

    def __getitem__(
        self,
        key: str,
    ) -> ProactiveSourceBinding | ProactiveModuleBinding:
        source = self._sources.get(key)
        if source is not None:
            return source
        module = self._modules.get(key)
        if module is not None:
            return module
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        yield from self._sources
        yield from self._modules

    def __len__(self) -> int:
        return len(self._sources) + len(self._modules)


PROACTIVE_COMPONENTS = ServiceKey["PluginProactiveComponents"](
    "core.proactive_components"
)


@dataclass(slots=True)
class _Registration:
    token: int
    owner: str
    generation_id: str
    definition: ProactiveSourceDefinition | ProactiveModuleDefinition
    descriptor: ProactiveSourceDescriptor | ProactiveModuleDescriptor
    owner_fiber: FiberHandle
    activation_token: object
    health: HealthHandle | None = None


class _ProactiveDeclarations:
    def __init__(self) -> None:
        self._next_token = 1
        self._registrations: dict[int, _Registration] = {}
        self._source_names: dict[tuple[str, str], int] = {}
        self._module_slots: dict[tuple[str, str], int] = {}
        self._frozen: ProactiveCatalog | None = None

    async def register(
        self,
        ctx: Context,
        definition: ProactiveSourceDefinition | ProactiveModuleDefinition,
    ) -> None:
        normalized = _normalize_definition(definition)
        activation_token = ctx.fiber.activation_token
        if activation_token is None:
            raise CompositionError("INACTIVE_FIBER", "当前 Fiber 没有 active activation")
        registration: _Registration | None = None

        def setup() -> object:
            nonlocal registration
            registration, cleanup = self._register(
                ctx.runtime.plugin_id,
                ctx.generation_id,
                normalized,
                ctx.fiber,
                activation_token,
            )
            return cleanup

        effect = await ctx.effect(
            setup,
            label=f"proactive:{_registration_label(normalized)}",
        )
        try:
            health = await ctx.health(
                f"proactive:{_registration_label(normalized)}",
                required=True,
            )
        except BaseException:
            await effect.aclose()
            raise
        assert registration is not None
        registration.health = health

    def _register(
        self,
        owner: str,
        generation_id: str,
        definition: ProactiveSourceDefinition | ProactiveModuleDefinition,
        owner_fiber: FiberHandle,
        activation_token: object,
    ) -> tuple[_Registration, object]:
        if self._frozen is not None:
            raise CompositionError(
                "PROACTIVE_COMPONENTS_FROZEN",
                "插件 proactive component 声明已冻结，不能新增",
            )
        token = self._next_token
        self._next_token += 1
        if isinstance(definition, ProactiveSourceDefinition):
            key = (owner, definition.name)
            if key in self._source_names:
                raise CompositionError(
                    "DUPLICATE_PROACTIVE_SOURCE",
                    f"proactive source 名称重复: {definition.name}",
                )
            descriptor: ProactiveSourceDescriptor | ProactiveModuleDescriptor = ProactiveSourceDescriptor(
                owner=owner,
                name=definition.name,
                channels=definition.channels,
                mcp_server=definition.mcp_server,
                fetch_tool=definition.fetch_tool,
                ack_tool=definition.ack_tool,
                fetch_page_size=definition.fetch_page_size,
            )
            self._source_names[key] = token
        else:
            key = (owner, definition.slot)
            if key in self._module_slots:
                raise CompositionError(
                    "DUPLICATE_PROACTIVE_MODULE",
                    f"proactive module slot 重复: {definition.slot}",
                )
            descriptor = ProactiveModuleDescriptor(
                owner=owner,
                lifecycle_id=definition.lifecycle_id,
                slot=definition.slot,
                requires=definition.requires,
                produces=definition.produces,
                collects=definition.collects,
                handler_export=definition.handler_export,
                domain_effect=definition.domain_effect,
                domain_effect_lookup_export=definition.domain_effect_lookup_export,
            )
            self._module_slots[key] = token
        registration = _Registration(
            token=token,
            owner=owner,
            generation_id=generation_id,
            definition=definition,
            descriptor=descriptor,
            owner_fiber=owner_fiber,
            activation_token=activation_token,
        )
        self._registrations[token] = registration

        def cleanup() -> None:
            self._registrations.pop(token, None)
            if isinstance(definition, ProactiveSourceDefinition):
                key = (owner, definition.name)
                if self._source_names.get(key) == token:
                    self._source_names.pop(key, None)
            else:
                key = (owner, definition.slot)
                if self._module_slots.get(key) == token:
                    self._module_slots.pop(key, None)

        return registration, cleanup

    def freeze(
        self,
        root_instance_token: object,
        generation_ids: Mapping[str, str] | None = None,
    ) -> ProactiveCatalog:
        if self._frozen is not None:
            if self._frozen.root_instance_token is not root_instance_token:
                raise RuntimeError("proactive catalog 属于另一棵 Root")
            if generation_ids is not None and any(
                generation_ids.get(binding.owner) != binding.generation_id
                for binding in (
                    *self._frozen.sources.values(),
                    *self._frozen.modules.values(),
                )
            ):
                raise RuntimeError("proactive catalog generation identity 已冻结")
            return self._frozen
        sources: dict[str, ProactiveSourceBinding] = {}
        modules: dict[str, ProactiveModuleBinding] = {}
        for registration in sorted(self._registrations.values(), key=lambda item: item.token):
            if registration.health is None:
                raise RuntimeError("proactive component 缺少 required Health")
            generation_id = (
                registration.generation_id
                if generation_ids is None
                else generation_ids.get(registration.owner)
            )
            if generation_id is None:
                raise RuntimeError(
                    f"proactive component owner 不属于 generations: {registration.owner}"
                )
            if isinstance(registration.definition, ProactiveSourceDefinition):
                assert isinstance(registration.descriptor, ProactiveSourceDescriptor)
                binding = ProactiveSourceBinding(
                    descriptor=registration.descriptor,
                    generation_id=generation_id,
                    definition=registration.definition,
                    owner_fiber=registration.owner_fiber,
                    activation_token=registration.activation_token,
                    health=registration.health,
                )
                sources[_source_key(binding.descriptor)] = binding
            else:
                assert isinstance(registration.descriptor, ProactiveModuleDescriptor)
                binding = ProactiveModuleBinding(
                    descriptor=registration.descriptor,
                    generation_id=generation_id,
                    definition=registration.definition,
                    owner_fiber=registration.owner_fiber,
                    activation_token=registration.activation_token,
                    health=registration.health,
                )
                modules[_module_key(binding.descriptor)] = binding
        self._frozen = ProactiveCatalog(
            sources,
            modules,
            root_instance_token=root_instance_token,
        )
        return self._frozen


class PluginProactiveComponents:
    """Expose only Root-token-bound proactive registration to plugins."""

    def __init__(self, root_instance_token: object) -> None:
        self._root_instance_token = root_instance_token
        self._declarations = _ProactiveDeclarations()

    async def register(
        self,
        ctx: Context,
        definition: ProactiveSourceDefinition | ProactiveModuleDefinition,
    ) -> None:
        if (
            ctx._root_instance_token() is not self._root_instance_token
            or ctx.require(PROACTIVE_COMPONENTS) is not self
        ):
            raise CompositionError(
                "PROACTIVE_SERVICE_ROOT_MISMATCH",
                "插件 proactive component Service 不属于当前 Root",
            )
        await self._declarations.register(ctx, definition)


def _freeze_plugin_proactive_components(
    value: object,
    root_instance_token: object,
    generation_ids: Mapping[str, str] | None = None,
) -> ProactiveCatalog:
    """Freeze the exact Core-created proactive registration facade."""

    if not isinstance(value, PluginProactiveComponents):
        raise RuntimeError("RuntimeSnapshot proactive Service 类型无效")
    if value._root_instance_token is not root_instance_token:
        raise RuntimeError("RuntimeSnapshot proactive Service 不属于 exact Root")
    return value._declarations.freeze(root_instance_token, generation_ids)


def _normalize_definition(
    definition: ProactiveSourceDefinition | ProactiveModuleDefinition,
) -> ProactiveSourceDefinition | ProactiveModuleDefinition:
    if isinstance(definition, ProactiveSourceDefinition):
        return ProactiveSourceDefinition(
            name=definition.name,
            channels=tuple(definition.channels),
            mcp_server=definition.mcp_server,
            fetch_tool=definition.fetch_tool,
            ack_tool=definition.ack_tool,
            fetch_page_size=definition.fetch_page_size,
        )
    if isinstance(definition, ProactiveModuleDefinition):
        return ProactiveModuleDefinition(
            slot=definition.slot,
            lifecycle_id=definition.lifecycle_id,
            requires=tuple(definition.requires),
            produces=tuple(definition.produces),
            collects=tuple(definition.collects),
            handler_export=definition.handler_export,
            domain_effect=definition.domain_effect,
            domain_effect_lookup_export=definition.domain_effect_lookup_export,
        )
    raise TypeError(
        "PluginProactiveComponents.register 只接受 ProactiveSourceDefinition 或 ProactiveModuleDefinition"
    )


def _registration_label(
    definition: ProactiveSourceDefinition | ProactiveModuleDefinition,
) -> str:
    return definition.name if isinstance(definition, ProactiveSourceDefinition) else definition.slot


def _source_key(descriptor: ProactiveSourceDescriptor) -> str:
    return f"{descriptor.owner}:{descriptor.name}"


def _module_key(descriptor: ProactiveModuleDescriptor) -> str:
    return f"{descriptor.owner}:{descriptor.slot}"


def _source_identity(descriptor: ProactiveSourceDescriptor) -> dict[str, object]:
    return {
        "owner": descriptor.owner,
        "name": descriptor.name,
        "channels": list(descriptor.channels),
        "mcp_server": descriptor.mcp_server,
        "fetch_tool": descriptor.fetch_tool,
        "ack_tool": descriptor.ack_tool,
        "fetch_page_size": descriptor.fetch_page_size,
    }


def _module_identity(descriptor: ProactiveModuleDescriptor) -> dict[str, object]:
    return {
        "owner": descriptor.owner,
        "lifecycle_id": descriptor.lifecycle_id,
        "slot": descriptor.slot,
        "requires": list(descriptor.requires),
        "produces": list(descriptor.produces),
        "collects": list(descriptor.collects),
        "handler_export": descriptor.handler_export,
        "domain_effect": descriptor.domain_effect,
        "domain_effect_lookup_export": descriptor.domain_effect_lookup_export,
    }


def _digest(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _cursor(value: object) -> None:
    if value is not None:
        _text(value, "cursor")


def _aware_datetime(value: object, field: str) -> None:
    if value is None:
        return
    if not isinstance(value, datetime):
        raise TypeError(f"{field} 必须是 timezone-aware datetime 或 None")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} 必须是 timezone-aware datetime")


def _freeze_value(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {
                _text(key, "item key"): _freeze_value(item)
                for key, item in value.items()
            }
        )
    if isinstance(value, list):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze_value(item) for item in value)
    return value


__all__ = [
    "AckCommitted",
    "AckFailure",
    "AckResult",
    "AckSkipped",
    "FetchEmpty",
    "FetchFailure",
    "FetchItems",
    "FetchResult",
    "FetchSkip",
    "PROACTIVE_COMPONENTS",
    "PluginProactiveComponents",
    "ProactiveCatalog",
    "ProactiveModuleBinding",
    "ProactiveModuleDefinition",
    "ProactiveModuleDescriptor",
    "ProactiveSourceBinding",
    "ProactiveSourceDefinition",
    "ProactiveSourceDescriptor",
    "_freeze_plugin_proactive_components",
]
