from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def test_v2_decorator_module_and_exports_are_removed() -> None:
    """Keep the removed decorator module out of the production import surface."""

    decorators = REPOSITORY_ROOT / "agent" / "plugins" / "decorators.py"
    public_api = (REPOSITORY_ROOT / "agent" / "plugins" / "__init__.py").read_text(
        encoding="utf-8"
    )

    assert not decorators.exists()
    assert "agent.plugins.decorators" not in public_api
    assert "on_tool_pre" not in public_api
    assert "on_tool_call" not in public_api
    assert "on_tool_result" not in public_api
    assert '"tool"' not in public_api


def test_registry_drops_tool_hook_metadata_and_dead_lookups() -> None:
    """Keep the superseded global plugin registry physically absent."""

    registry = REPOSITORY_ROOT / "agent" / "plugins" / "registry.py"
    public_api = (REPOSITORY_ROOT / "agent" / "plugins" / "__init__.py").read_text(
        encoding="utf-8"
    )

    assert not registry.exists()
    assert "agent.plugins.registry" not in public_api
    assert "plugin_registry" not in public_api
