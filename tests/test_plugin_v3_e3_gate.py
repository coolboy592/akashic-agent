from __future__ import annotations

import asyncio
import importlib.util
import inspect
import json
import sys
from pathlib import Path

import pytest

from agent.plugin_composition.channels import DeliveryStatus


_GATE_PATH = (
    Path(__file__).resolve().parents[1] / "docker/debug/plugin_v3_e3_gate.py"
)
_SPEC = importlib.util.spec_from_file_location("plugin_v3_e3_gate", _GATE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)


def test_gate_scope_pins_full_fleet_and_explicit_webui_boundary() -> None:
    assert gate.CHANNEL_PLUGIN_IDS == ("feishu", "qqbot")
    assert gate.PASSIVE_PLUGIN_IDS == ("citation", "meme")
    assert gate.FLEET_EXTERNAL_PLUGIN_IDS == (
        "setup_helper",
        "status_commands",
        "daynight_gate",
        "emotion",
        "calendar-mcp",
        "feed-mcp",
        "fitbit-mcp",
        "steam-mcp",
        "huayue-skills",
        "github_watch",
    )
    assert gate.FLEET_LOCK_PLUGIN_IDS == (
        *gate.FLEET_EXTERNAL_PLUGIN_IDS,
        "feishu",
        "qqbot",
    )
    assert gate.PRIVATE_PROACTIVE_PLUGIN_IDS == (
        "default_proactive",
        "proactive_flow",
        "drift_flow",
        "wake_proactive",
        "wake_proactive_flow",
        "wake_drift_flow",
    )
    assert gate.GATE_SCOPE["exact_plugins"] == (
        *gate.FLEET_LOCK_PLUGIN_IDS,
        *gate.PRIVATE_PROACTIVE_PLUGIN_IDS,
    )
    assert gate.GATE_SCOPE["lock_plugins"] == gate.FLEET_LOCK_PLUGIN_IDS
    assert gate.GATE_SCOPE["private_proactive_entries"] == (
        *gate.PRIVATE_PROACTIVE_PLUGIN_IDS,
    )
    assert gate.GATE_SCOPE["passive_plugins"] == gate.PASSIVE_PLUGIN_IDS
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
        "full_boot_catalog",
        "candidate_lifecycle",
        "proactive_fixed_clock",
        "process_restart",
        "private_default_wake",
        "github_watch_controlled_remote",
    )
    assert len(gate._scenario_catalog_sha256()) == 64


def test_fleet_lock_and_coverage_oracles_reject_missing_or_unexpected_ids() -> None:
    class LockItem:
        def __init__(self, plugin_id: str) -> None:
            self.id = plugin_id

    lock = [LockItem(plugin_id) for plugin_id in gate.FLEET_LOCK_PLUGIN_IDS]
    assert gate._validate_fleet_lock(lock)["status"] == "complete"
    try:
        gate._validate_fleet_lock(lock[:-1])
    except gate.GateFailure as error:
        assert "missing" in str(error)
    else:
        raise AssertionError("缺失 fleet lock id 未被拒绝")
    try:
        gate._validate_fleet_lock(
            [*lock, LockItem("unexpected-e3-plugin")]
        )
    except gate.GateFailure as error:
        assert "extra" in str(error)
    else:
        raise AssertionError("未知 fleet lock id 未被拒绝")

    coverage = gate._fleet_coverage_contract(
        active_external_ids=gate.FLEET_LOCK_PLUGIN_IDS,
        active_private_ids=gate.PRIVATE_PROACTIVE_PLUGIN_IDS,
    )
    assert coverage["status"] == "complete"
    assert coverage["families"]["default"] == [
        "default_proactive",
        "proactive_flow",
        "drift_flow",
    ]
    try:
        gate._fleet_coverage_contract(
            active_external_ids=gate.FLEET_LOCK_PLUGIN_IDS[:-1],
            active_private_ids=gate.PRIVATE_PROACTIVE_PLUGIN_IDS,
        )
    except gate.GateFailure as error:
        assert "missing_external" in str(error)
    else:
        raise AssertionError("缺失 external fleet id 未被拒绝")


def test_proactive_and_github_policy_oracles_are_explicit() -> None:
    evidence = gate._proactive_coverage_oracle(gate.PROACTIVE_ORACLE_KINDS)
    assert evidence["status"] == "complete"
    assert evidence["required"] == [
        "empty",
        "skip",
        "source",
        "model",
        "delivery",
        "restart",
    ]
    try:
        gate._proactive_coverage_oracle(("empty", "skip"))
    except gate.GateFailure as error:
        assert "model" in str(error)
    else:
        raise AssertionError("缺失 proactive outcome 未被拒绝")

    blocked = gate._github_watch_policy(None)
    assert blocked == {
        "status": "blocked",
        "reason": "no dedicated controlled GitHub remote configured",
        "credentials_read": False,
        "network_effects": 0,
        "external_sends": 0,
    }
    controlled = gate._github_watch_policy("file:///tmp/e3-controlled-remote")
    assert controlled["status"] == "controlled_remote"
    assert controlled["credentials_read"] is False
    try:
        gate._github_watch_policy("recording://not-supported")
    except gate.GateFailure as error:
        assert "scheme" in str(error)
    else:
        raise AssertionError("未知 GitHub controlled remote scheme 未被拒绝")


def test_runtime_evidence_mutant_missing_full_boot_is_blocked() -> None:
    evidence = {
        "lock": {"status": "complete"},
        "full_boot_catalog": {"status": "complete"},
        "candidate_lifecycle": {"status": "complete"},
        "fleet_coverage": {"status": "complete"},
        "proactive": {"status": "complete"},
        "github_watch": {"status": "blocked"},
    }
    mutant = dict(evidence)
    del mutant["full_boot_catalog"]
    try:
        gate._require_runtime_evidence(mutant)
    except gate.GateBlocked as error:
        assert "full_boot_catalog" in str(error)
    else:
        raise AssertionError("删除 full_boot_catalog runtime evidence 后仍允许通过")

    blocked_proactive = dict(evidence)
    blocked_proactive["proactive"] = {"status": "blocked"}
    try:
        gate._require_runtime_evidence(blocked_proactive)
    except gate.GateBlocked as error:
        assert "proactive" in str(error)
    else:
        raise AssertionError("blocked proactive runtime evidence 未阻断 Gate")

    blocked_github = dict(evidence)
    try:
        gate._require_runtime_evidence(blocked_github)
    except gate.GateBlocked as error:
        assert "controlled remote" in str(error)
    else:
        raise AssertionError("blocked GitHub Watch runtime evidence 未阻断 Gate")


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


def test_recording_sink_uses_typed_v3_dispatcher() -> None:
    delivered: list[dict[str, str]] = []
    push = gate.MessagePushTool()
    push.bind_v3_channel_dispatcher(gate._build_recording_dispatcher(delivered))

    result = json.loads(
        asyncio.run(
            push.execute(
                target_channel="recording",
                target_chat_id="operator",
                message="deterministic",
            )
        )
    )

    assert result == {
        "delivery_id": "e3-proactive-delivery-1",
        "status": "delivered",
        "retryable": False,
        "provider_ids": ["recording"],
        "error": None,
    }
    assert delivered == [
        {
            "channel": "recording",
            "chat_id": "operator",
            "content": "deterministic",
        }
    ]
    source = inspect.getsource(gate._run_proactive_tick)
    assert "bind_v3_channel_dispatcher" in source
    assert "register_channel" not in source


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


def test_tmp_root_is_optional_and_caller_owned(tmp_path: Path) -> None:
    assert gate._resolve_tmp_root(None) is None
    assert gate._resolve_tmp_root(tmp_path) == tmp_path.resolve()

    invalid = tmp_path / "missing"
    with pytest.raises(gate.GateFailure, match="tmp root"):
        gate._resolve_tmp_root(invalid)
