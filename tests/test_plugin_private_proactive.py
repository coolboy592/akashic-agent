from __future__ import annotations

import importlib
import importlib.util
from types import ModuleType

import pytest

from agent.plugins.private_proactive import (
    PRIVATE_PROACTIVE_DEFINITIONS,
    PrivateProactiveRegistry,
    admit_private_proactive_module,
)


def _module(member: str) -> ModuleType:
    return importlib.import_module(f"plugins.{member}.plugin")


def test_six_exact_private_members_freeze_in_family_order() -> None:
    registry = PrivateProactiveRegistry()
    for definition in PRIVATE_PROACTIVE_DEFINITIONS:
        registry.register(
            _module(definition.member),
            source_revision=f"source:{definition.member}",
            generation_id=f"generation:{definition.member}",
        )

    catalog = registry.freeze(root_instance_token=None)

    assert [item.member for item in catalog.family("default")] == [
        "default_proactive",
        "proactive_flow",
        "drift_flow",
    ]
    assert [item.member for item in catalog.family("wake")] == [
        "wake_proactive",
        "wake_proactive_flow",
        "wake_drift_flow",
    ]
    assert all(
        item.resolve_export(name) is getattr(item.module, name)
        for item in catalog.members
        for name in item.export_names
    )


def test_private_catalog_identity_includes_generation_and_source_revision() -> None:
    module = _module("default_proactive")
    first = PrivateProactiveRegistry()
    first.register(module, source_revision="source-1", generation_id="generation-1")
    second = PrivateProactiveRegistry()
    second.register(module, source_revision="source-2", generation_id="generation-1")
    third = PrivateProactiveRegistry()
    third.register(module, source_revision="source-1", generation_id="generation-2")

    identities = {
        first.freeze().identity,
        second.freeze().identity,
        third.freeze().identity,
    }

    assert len(identities) == 3


def test_external_same_name_and_reexport_are_rejected_before_registration() -> None:
    fake = ModuleType("external.default_proactive")
    fake.__file__ = "/tmp/external/default_proactive/plugin.py"
    fake.api_version = 3
    fake.name = "default_proactive"
    fake.apply = lambda ctx, config: None
    actual = _module("default_proactive")
    for name in (
        "DefaultRuntimeFactory",
        "DefaultModuleFactory",
        "build_default_lifecycle",
    ):
        setattr(fake, name, getattr(actual, name))

    with pytest.raises(ValueError, match="entry 来源|export 来源"):
        admit_private_proactive_module(fake)


def test_symlink_entry_is_rejected_before_export_admission(tmp_path) -> None:
    actual = _module("default_proactive")
    target = tmp_path / "external" / "default_proactive" / "plugin.py"
    target.parent.mkdir(parents=True)
    target.symlink_to(actual.__file__)
    spec = importlib.util.spec_from_file_location(
        "external.default_proactive.plugin",
        target,
    )
    assert spec is not None and spec.loader is not None
    external = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(external)

    with pytest.raises(ValueError, match="entry 来源不匹配|symlink"):
        admit_private_proactive_module(external)


@pytest.mark.parametrize("member", [item.member for item in PRIVATE_PROACTIVE_DEFINITIONS])
def test_private_entries_are_pure_v3_without_plugin_class(member: str) -> None:
    module = _module(member)

    assert module.api_version == 3
    assert callable(module.apply)
    assert not any(
        name.endswith("Plugin") and isinstance(value, type)
        for name, value in vars(module).items()
    )
