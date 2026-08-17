"""Core-private admission and descriptors for the Default/Wake proactive island.

The public composition namespace deliberately does not expose this registry.  It
is an in-tree compatibility seam until every proactive consumer has migrated.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import tomllib
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType, ModuleType
from typing import Literal, cast


PrivateFamily = Literal["default", "wake"]


@dataclass(frozen=True, slots=True)
class PrivateProactiveDefinition:
    """Describe one admitted in-tree module without retaining runtime state."""

    family: PrivateFamily
    order: int
    package_id: str
    member: str
    exports: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.family not in {"default", "wake"}:
            raise ValueError(f"private proactive family 无效: {self.family!r}")
        if not isinstance(self.order, int) or self.order < 0:
            raise ValueError("private proactive order 必须是非负整数")
        if not self.package_id or self.package_id.strip() != self.package_id:
            raise ValueError("private proactive package_id 无效")
        if not self.member or self.member.strip() != self.member:
            raise ValueError("private proactive member 无效")
        if not self.exports or len(set(self.exports)) != len(self.exports):
            raise ValueError("private proactive exports 必须非空且不得重复")
        if any(not item or item.strip() != item for item in self.exports):
            raise ValueError("private proactive exports 必须是非空字符串")


PRIVATE_PROACTIVE_DEFINITIONS: tuple[PrivateProactiveDefinition, ...] = (
    PrivateProactiveDefinition(
        family="default",
        order=0,
        package_id="default-proactive",
        member="default_proactive",
        exports=("DefaultRuntimeFactory", "DefaultModuleFactory", "build_default_lifecycle"),
    ),
    PrivateProactiveDefinition(
        family="default",
        order=1,
        package_id="default-proactive",
        member="proactive_flow",
        exports=("ProactiveModuleFactory",),
    ),
    PrivateProactiveDefinition(
        family="default",
        order=2,
        package_id="default-proactive",
        member="drift_flow",
        exports=("DriftModuleFactory",),
    ),
    PrivateProactiveDefinition(
        family="wake",
        order=0,
        package_id="wake-proactive",
        member="wake_proactive",
        exports=(
            "WakeRuntimeFactory",
            "WakeProactiveModuleFactory",
            "build_wake_lifecycle",
        ),
    ),
    PrivateProactiveDefinition(
        family="wake",
        order=1,
        package_id="wake-proactive",
        member="wake_proactive_flow",
        exports=("WakeContentModuleFactory",),
    ),
    PrivateProactiveDefinition(
        family="wake",
        order=2,
        package_id="wake-proactive",
        member="wake_drift_flow",
        exports=("WakeDriftModuleFactory",),
    ),
)

_DEFINITION_BY_MEMBER = {item.member: item for item in PRIVATE_PROACTIVE_DEFINITIONS}


def core_project_root() -> Path:
    """Resolve the repository root from this executing Core package."""

    source = Path(__file__).resolve(strict=True)
    root = source.parents[2]
    if not (root / "agent" / "plugins" / "private_proactive.py").is_file():
        raise RuntimeError(f"Core source root 无效: {root}")
    return root


def private_proactive_root(member: str) -> Path:
    """Return the one canonical source root admitted for a member."""

    definition = _definition(member)
    root = core_project_root() / "plugins" / definition.member
    _require_plain_path(root, core_project_root())
    return root


@dataclass(frozen=True, slots=True)
class PrivateProactiveMember:
    """Freeze one source-relative member identity for candidate/formal use."""

    definition: PrivateProactiveDefinition
    root: Path
    entry: Path
    module_name: str
    source_revision: str
    generation_id: str
    module: ModuleType = field(repr=False, compare=False)

    @property
    def member(self) -> str:
        return self.definition.member

    @property
    def family(self) -> PrivateFamily:
        return self.definition.family

    @property
    def order(self) -> int:
        return self.definition.order

    @property
    def export_names(self) -> tuple[str, ...]:
        return self.definition.exports

    def resolve_export(self, name: str) -> object:
        """Resolve an already-admitted export without accepting arbitrary names."""

        if name not in self.definition.exports:
            raise KeyError(f"private proactive export 未声明: {self.member}.{name}")
        value = getattr(self.module, name, None)
        if not callable(value):
            raise RuntimeError(f"private proactive export 不可调用: {self.member}.{name}")
        if getattr(value, "__module__", None) != self.module_name:
            raise RuntimeError(f"private proactive export 来源不匹配: {self.member}.{name}")
        return value


class PrivateProactiveCatalog(Mapping[str, PrivateProactiveMember]):
    """Immutable Core-private descriptor catalog for one snapshot."""

    __slots__ = ("_members", "_identity", "_root_instance_token")

    def __init__(
        self,
        members: Iterable[PrivateProactiveMember],
        *,
        root_instance_token: object | None = None,
    ) -> None:
        ordered = tuple(sorted(members, key=lambda item: (item.family, item.order)))
        seen: set[str] = set()
        for item in ordered:
            if item.member in seen:
                raise RuntimeError(f"private proactive member 重复: {item.member}")
            seen.add(item.member)
        self._members = MappingProxyType({item.member: item for item in ordered})
        payload = [
            {
                "family": item.family,
                "order": item.order,
                "package_id": item.definition.package_id,
                "member": item.member,
                "exports": item.export_names,
                "root": item.root.as_posix(),
                "entry": item.entry.as_posix(),
                "source_revision": item.source_revision,
                "generation_id": item.generation_id,
            }
            for item in ordered
        ]
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()
        self._identity = hashlib.sha256(encoded).hexdigest()[:16]
        self._root_instance_token = root_instance_token

    @property
    def members(self) -> tuple[PrivateProactiveMember, ...]:
        return tuple(self._members.values())

    @property
    def identity(self) -> str:
        return self._identity

    @property
    def catalog_digest(self) -> str:
        return self._identity

    @property
    def root_instance_token(self) -> object | None:
        return self._root_instance_token

    def family(self, family: PrivateFamily) -> tuple[PrivateProactiveMember, ...]:
        """Return members in the contract's fixed family/order sequence."""

        if family not in {"default", "wake"}:
            raise ValueError(f"private proactive family 无效: {family!r}")
        return tuple(item for item in self.members if item.family == family)

    def member(self, member: str) -> PrivateProactiveMember:
        return self._members[member]

    def __getitem__(self, key: str) -> PrivateProactiveMember:
        return self._members[key]

    def __iter__(self):
        return iter(self._members)

    def __len__(self) -> int:
        return len(self._members)


class PrivateProactiveRegistry:
    """Collect explicit definitions and freeze one private catalog."""

    __slots__ = ("_members", "_frozen")

    def __init__(self) -> None:
        self._members: dict[str, PrivateProactiveMember] = {}
        self._frozen: PrivateProactiveCatalog | None = None

    def register(
        self,
        module: ModuleType,
        *,
        source_revision: str,
        generation_id: str,
    ) -> PrivateProactiveMember:
        """Admit one exact module and retain only its immutable identity."""

        if self._frozen is not None:
            raise RuntimeError("private proactive registry 已冻结")
        member = admit_private_proactive_module(module)
        if not source_revision or source_revision.strip() != source_revision:
            raise ValueError("private proactive source_revision 无效")
        if not generation_id or generation_id.strip() != generation_id:
            raise ValueError("private proactive generation_id 无效")
        root = private_proactive_root(member.member)
        entry = root / "plugin.py"
        binding = PrivateProactiveMember(
            definition=member.definition,
            root=root,
            entry=entry,
            module_name=module.__name__,
            source_revision=source_revision,
            generation_id=generation_id,
            module=module,
        )
        previous = self._members.get(binding.member)
        if previous is not None:
            raise RuntimeError(f"private proactive member 重复: {binding.member}")
        self._members[binding.member] = binding
        return binding

    def freeze(self, *, root_instance_token: object | None = None) -> PrivateProactiveCatalog:
        """Seal descriptors so later registration cannot mutate the snapshot view."""

        if self._frozen is None:
            self._frozen = PrivateProactiveCatalog(
                self._members.values(),
                root_instance_token=root_instance_token,
            )
        elif self._frozen.root_instance_token is not root_instance_token:
            raise RuntimeError("private proactive catalog 属于另一棵 Root")
        return self._frozen


def admit_private_proactive_module(module: ModuleType) -> PrivateProactiveMember:
    """Fail-loudly unless a module is exactly one of the six in-tree entries."""

    name = getattr(module, "name", None)
    if not isinstance(name, str) or name not in _DEFINITION_BY_MEMBER:
        raise ValueError(f"module 不是 private proactive allowlist member: {name!r}")
    definition = _DEFINITION_BY_MEMBER[name]
    if getattr(module, "api_version", None) != 3:
        raise ValueError(f"private proactive 必须声明 api_version=3: {name}")
    apply = getattr(module, "apply", None)
    if not callable(apply) or not _is_apply_signature(apply):
        raise ValueError(f"private proactive apply(ctx, config) 无效: {name}")
    root = private_proactive_root(name)
    entry = root / "plugin.py"
    imported = getattr(module, "__file__", None)
    if not isinstance(imported, str):
        raise ValueError(f"private proactive entry 来源不存在: {name}")
    _require_exact_entry_path(imported, entry, core_project_root())
    for export in definition.exports:
        value = getattr(module, export, None)
        if not callable(value) or getattr(value, "__module__", None) != module.__name__:
            raise ValueError(f"private proactive export 来源不匹配: {name}.{export}")
    _validate_package_manifest(definition, root)
    return PrivateProactiveMember(
        definition=definition,
        root=root,
        entry=entry,
        module_name=module.__name__,
        source_revision="",
        generation_id="",
        module=module,
    )


def build_private_proactive_catalog(
    generations: Iterable[object],
    *,
    root_instance_token: object | None = None,
) -> PrivateProactiveCatalog | None:
    """Build a catalog from loaded generations without invoking any export."""

    registry = PrivateProactiveRegistry()
    found = False
    for generation in generations:
        plugin_id = getattr(generation, "plugin_id", None)
        if not isinstance(plugin_id, str) or plugin_id not in _DEFINITION_BY_MEMBER:
            continue
        instance = getattr(generation, "instance", None)
        module = getattr(instance, "module", None)
        if not isinstance(module, ModuleType):
            raise RuntimeError(f"private proactive generation 缺少 module: {plugin_id}")
        registry.register(
            module,
            source_revision=str(getattr(generation, "source_revision", "")),
            generation_id=str(getattr(generation, "generation_id", "")),
        )
        found = True
    if not found:
        return None
    return registry.freeze(root_instance_token=root_instance_token)


def _definition(member: str) -> PrivateProactiveDefinition:
    try:
        return _DEFINITION_BY_MEMBER[member]
    except KeyError as error:
        raise ValueError(f"private proactive member 未知: {member}") from error


def _require_plain_path(path: Path, project_root: Path) -> None:
    """Reject symlink components between Core root and an admitted module root."""

    try:
        relative = path.relative_to(project_root)
    except ValueError as error:
        raise ValueError(f"private proactive root 越过 Core source root: {path}") from error
    current = project_root
    if current.is_symlink():
        raise ValueError(f"Core source root 不得是 symlink: {current}")
    for component in relative.parts:
        current /= component
        if current.is_symlink():
            raise ValueError(f"private proactive path component 不得是 symlink: {current}")
    resolved = path.resolve(strict=True)
    if resolved != path:
        raise ValueError(f"private proactive root 不是 exact canonical path: {path}")
    if not path.is_dir():
        raise ValueError(f"private proactive root 不是目录: {path}")


def _require_exact_entry_path(
    imported: str,
    entry: Path,
    project_root: Path,
) -> None:
    """Reject lexical aliases and symlink components for an admitted entry."""

    imported_entry = Path(os.path.abspath(imported))
    if imported_entry != entry:
        raise ValueError(f"private proactive entry 来源不匹配: {imported}")
    try:
        relative = imported_entry.relative_to(project_root)
    except ValueError as error:
        raise ValueError(
            f"private proactive entry 越过 Core source root: {imported_entry}"
        ) from error
    current = project_root
    if current.is_symlink():
        raise ValueError(f"Core source root 不得是 symlink: {current}")
    for component in relative.parts:
        current /= component
        if current.is_symlink():
            raise ValueError(f"private proactive entry path component 不得是 symlink: {current}")
    if not imported_entry.is_file():
        raise ValueError(f"private proactive entry 来源不存在: {imported_entry}")


def _validate_package_manifest(
    definition: PrivateProactiveDefinition,
    root: Path,
) -> None:
    project_root = core_project_root()
    path = project_root / "plugin_packages" / definition.package_id / "package.toml"
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"private proactive package manifest 缺失: {path}")
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ValueError(f"private proactive package manifest 无法解析: {path}") from error
    package = raw.get("package")
    if not isinstance(package, dict):
        raise ValueError(f"private proactive package manifest 缺少 [package]: {path}")
    if package.get("id") != definition.package_id:
        raise ValueError(f"private proactive package id 不匹配: {definition.package_id}")
    members = package.get("members")
    expected = tuple(
        item.member for item in PRIVATE_PROACTIVE_DEFINITIONS if item.package_id == definition.package_id
    )
    if not isinstance(members, list) or tuple(members) != expected:
        raise ValueError(
            f"private proactive package members 不匹配: {definition.package_id}"
        )
    if root != project_root / "plugins" / definition.member:
        raise ValueError(f"private proactive member root 不匹配: {definition.member}")


def _is_apply_signature(apply: object) -> bool:
    try:
        parameters = tuple(
            inspect.signature(cast(Callable[..., object], apply)).parameters.values()
        )
    except (TypeError, ValueError):
        return False
    positional = {
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    }
    return (
        tuple(item.name for item in parameters) == ("ctx", "config")
        and all(item.kind in positional for item in parameters)
        and all(item.default is inspect.Parameter.empty for item in parameters)
    )


__all__ = (
    "PRIVATE_PROACTIVE_DEFINITIONS",
    "PrivateFamily",
    "PrivateProactiveCatalog",
    "PrivateProactiveDefinition",
    "PrivateProactiveMember",
    "PrivateProactiveRegistry",
    "admit_private_proactive_module",
    "build_private_proactive_catalog",
    "core_project_root",
    "private_proactive_root",
)
