from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

from agent.plugin_composition.channels import DeliveryStatus


_GATE_PATH = (
    Path(__file__).resolve().parents[1] / "docker/debug/plugin_v3_e3_gate.py"
)
_SPEC = importlib.util.spec_from_file_location("plugin_v3_e3_gate", _GATE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)


def test_gate_scope_pins_exact_scenarios_and_explicit_webui_boundary() -> None:
    assert gate.CHANNEL_PLUGIN_IDS == ("feishu", "qqbot")
    assert gate.PASSIVE_PLUGIN_IDS == ("citation", "meme")
    assert gate.GATE_SCOPE["exact_plugins"] == (
        "feishu",
        "qqbot",
        "citation",
        "meme",
    )
    assert gate.GATE_SCOPE["provider"] == (
        "synthetic recording HTTP/WS adapter; no external delivery"
    )
    assert gate.GATE_SCOPE["public_webui_docker_e2e"] == (
        "separate plugin_v3_webui_e2e Gate"
    )
    assert tuple(item["id"] for item in gate.SCENARIO_CATALOG) == (
        "channel",
        "message_push",
        "passive_webui",
    )
    assert len(gate._scenario_catalog_sha256()) == 64


def test_tree_digest_is_stable_for_missing_and_changes_with_content(
    tmp_path: Path,
) -> None:
    missing = gate._tree_digest(tmp_path / "missing")
    assert missing == gate._tree_digest(tmp_path / "missing")

    tree = tmp_path / "tree"
    tree.mkdir()
    payload = tree / "payload.txt"
    payload.write_text("one", encoding="utf-8")
    before = gate._tree_digest(tree)
    payload.write_text("two", encoding="utf-8")
    assert gate._tree_digest(tree) != before


def test_protected_digest_excludes_unrelated_runtime_files(tmp_path: Path) -> None:
    gate._write_channel_config(tmp_path)
    before = gate._protected_digest(tmp_path)
    (tmp_path / "runtime.log").write_text("diagnostic", encoding="utf-8")
    assert gate._protected_digest(tmp_path) == before
    config = tmp_path / "plugin-data" / "feishu-builtin" / "config.local.toml"
    config.write_text(config.read_text(encoding="utf-8") + "allowFrom = [\"x\"]\n", encoding="utf-8")
    assert gate._protected_digest(tmp_path) != before


def test_recording_factory_preserves_delivery_identity_and_unknown_status() -> None:
    factory = gate._RecordingFactory("feishu", {"appId": "synthetic"})
    delivered = factory.record(
        recipient="chat",
        delivery_id="delivery-1",
        kind="feishu.send",
    )
    factory.fail_after_effect = True
    unknown = factory.record(
        recipient="chat",
        delivery_id="delivery-2",
        kind="feishu.send",
    )

    assert delivered is DeliveryStatus.DELIVERED
    assert unknown is DeliveryStatus.UNKNOWN
    assert [effect.delivery_id for effect in factory.effects] == [
        "delivery-1",
        "delivery-2",
    ]


def test_prepare_channel_checkouts_adds_only_temp_python_marker(
    tmp_path: Path,
) -> None:
    providers = tmp_path / "providers"
    (providers / "feishu" / ".venv" / "bin").mkdir(parents=True)
    (providers / "qqbot").mkdir()

    gate._prepare_channel_checkouts(providers)

    marker = providers / "feishu" / ".venv" / "bin" / "python"
    assert marker.is_symlink()
    assert marker.resolve() == Path(sys.executable).resolve()
    assert {
        path.relative_to(providers).as_posix() for path in providers.rglob("*")
    } == {
        "feishu",
        "feishu/.venv",
        "feishu/.venv/bin",
        "feishu/.venv/bin/python",
        "qqbot",
    }


def test_write_json_round_trips_gate_evidence(tmp_path: Path) -> None:
    report = tmp_path / "reports" / "gate.json"
    payload = {
        "status": "passed",
        "scope": {"exact_plugins": list(gate.GATE_SCOPE["exact_plugins"])},
        "cleanup": {"sandbox_removed": True, "residuals": []},
    }

    gate._write_json(report, payload)

    assert json.loads(report.read_text(encoding="utf-8")) == payload
