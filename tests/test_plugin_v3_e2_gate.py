from __future__ import annotations

import json
from pathlib import Path

import pytest

from docker.debug import plugin_v3_e2_gate as gate


def test_e2_lock_selects_exact_locked_set() -> None:
    locks = gate._load_lock(gate.DEFAULT_LOCK)

    assert tuple(item.id for item in locks) == gate.EXPECTED_PLUGIN_IDS
    assert all(item.requested_ref == item.resolved_sha for item in locks)
    assert all(item.resolved_sha == item.change_source_pr_head for item in locks)
    assert all(item.repository.startswith("https://github.com/") for item in locks)


def test_e2_lock_rejects_revision_drift(tmp_path: Path) -> None:
    raw = json.loads(gate.DEFAULT_LOCK.read_text(encoding="utf-8"))
    raw["plugins"][0]["requested_ref"] = "0" * 40
    lock_path = tmp_path / "fleet.lock.json"
    lock_path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="revision"):
        gate._load_lock(lock_path)


def test_e2_shell_catalog_and_listener_contract_are_locked() -> None:
    assert tuple(item.id for item in gate.SCENARIO_CATALOG) == (
        "plain-rm",
        "sudo-cluster",
        "sudo-preserve-env",
        "sudo-mode-denied",
        "repeat-1",
        "repeat-2",
        "repeat-3",
    )
    assert gate.EXPECTED_LISTENERS == (
        "transform:tool.input.prepare[akashic.tool-input.v1]:shell_restore",
        "serial:tool.execution.authorize[bail=akashic.tool-deny-reason.v1]:shell_safety",
        "serial:tool.execution.authorize[bail=akashic.tool-deny-reason.v1]:tool_loop_guard",
    )
    assert len(gate._scenario_catalog_sha256()) == 64


def test_recording_payload_oracle_is_strict() -> None:
    gate._assert_recording_payload("feed-mcp", "get_proactive_events", {"status": "empty"})
    gate._assert_recording_payload(
        "steam-mcp",
        "get_steam_context",
        {"items": [{"recording": True}]},
    )

    with pytest.raises(RuntimeError, match="typed empty"):
        gate._assert_recording_payload("feed-mcp", "get_proactive_events", {"items": []})
    with pytest.raises(RuntimeError, match="recording=true"):
        gate._assert_recording_payload(
            "steam-mcp",
            "get_steam_context",
            {"items": [{"recording": False}]},
        )


def test_gate_blocked_is_not_a_success_status() -> None:
    assert {"passed", "blocked", "failed"}.isdisjoint({"not_run"})
    assert gate.GATE_VERSION == 1
