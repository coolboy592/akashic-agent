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
    """Keep only metadata still required by the current Manager transition path."""

    registry = (
        REPOSITORY_ROOT / "agent" / "plugins" / "registry.py"
    ).read_text(encoding="utf-8")

    for removed_name in (
        "HandlerType",
        "TOOL_HOOK",
        "PRE_TOOL",
        "hook_tool_name",
        "get_by_name",
        "get_by_event_type",
        "get_handlers_by_event_type",
    ):
        assert removed_name not in registry
