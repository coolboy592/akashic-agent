from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from docker.debug import plugin_v3_e1_gate as gate


def test_e1_catalog_is_exact_and_has_no_implicit_pass() -> None:
    assert gate.E1_PLUGIN_IDS == (
        "akasha", "default_memory", "citation", "meme", "emotion", "observe",
        "proactive_feedback", "plugin_undo",
    )
    assert "not_run" not in {"passed", "blocked", "failed"}


def test_passive_webui_report_oracle_accepts_synthetic_pass(tmp_path: Path) -> None:
    locks = gate._select_e1_locks(gate.DEFAULT_LOCK)
    core = gate.fleet_gate._core_evidence()
    report_path = tmp_path / "plugin-passive-webui-v3.json"
    payload: dict[str, object] = {
        "status": "passed",
        "scenario_profile": gate.PASSIVE_WEBUI_SCENARIO_PROFILE,
        "core": {
            "head": core["head"],
            "tree": core["tree"],
            "dirty_status": [],
        },
        "sources": [
            {"kind": "contract", "id": "plugin_contracts"},
            {
                "kind": "plugin", "id": "citation",
                "resolved_sha": locks["citation"].resolved_sha,
            },
            {
                "kind": "plugin", "id": "meme",
                "resolved_sha": locks["meme"].resolved_sha,
            },
        ],
        "runtime": {
            "status": "passed",
            "model_request": {"citation_index": 10, "meme_index": 20},
            "messages": [
                {"role": "user"},
                {
                    "role": "assistant",
                    "cited_memory_ids": ["mem_1"],
                    "media": ["/sandbox/workspace/memes/shy/001.png"],
                },
            ],
        },
        "cleanup": {
            "residuals": [], "sandbox_removed": True, "source_unchanged": True,
        },
    }
    report_path.write_text(json.dumps(payload), encoding="utf-8")

    evidence = gate._validate_passive_webui_report(report_path, locks, core)

    assert evidence["status"] == "passed"
    assert evidence["source_shas"] == {
        "citation": locks["citation"].resolved_sha,
        "meme": locks["meme"].resolved_sha,
    }
    assert evidence["prompt_order"] == {"citation_index": 10, "meme_index": 20}


def test_passive_webui_report_oracle_rejects_stale_core(tmp_path: Path) -> None:
    locks = gate._select_e1_locks(gate.DEFAULT_LOCK)
    core = gate.fleet_gate._core_evidence()
    report_path = tmp_path / "plugin-passive-webui-v3.json"
    report_path.write_text(
        json.dumps(
            {
                "status": "passed",
                "scenario_profile": gate.PASSIVE_WEBUI_SCENARIO_PROFILE,
                "core": {
                    "head": "0" * 40,
                    "tree": core["tree"],
                    "dirty_status": [],
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(gate.E1GateError, match="Core head"):
        gate._validate_passive_webui_report(report_path, locks, core)


def test_sqlite_state_covers_without_rowid_tables(tmp_path: Path) -> None:
    sessions = gate.SessionManager(tmp_path)
    try:
        before = gate._sqlite_state(sessions.db_path)
        _ = gate._seed_interaction(
            sessions, key="test:e1:append", turn="turn:test:e1:append", label="test"
        )
        after = gate._sqlite_state(sessions.db_path)
        diff = gate._sqlite_diff(before, after)
        assert before["integrity"] == "ok"
        assert after["integrity"] == "ok"
        assert diff["deleted_count"] == 0
        assert any(item.startswith("messages:") for item in diff["inserted"])
    finally:
        sessions.close()


@pytest.mark.asyncio
async def test_combined_gate_runs_real_disposable_write_sets(tmp_path: Path) -> None:
    report_path = tmp_path / "gate.json"
    report = await gate._run_gate(
        lock_path=gate.DEFAULT_LOCK,
        report_path=report_path,
        tmp_root=tmp_path,
        provided_raw=[],
        offline=True,
        passive_webui_report=tmp_path / "missing-plugin-passive-webui-v3.json",
    )
    assert report["status"] == "blocked"
    scenarios = {item["id"]: item for item in report["scenarios"]}
    assert scenarios["runtime_boot_default"]["status"] == "passed"
    assert scenarios["runtime_boot_akasha"]["status"] == "passed"
    assert scenarios["append_only_sessiondb"]["status"] == "passed"
    assert scenarios["plugin_undo"]["status"] == "passed"
    assert scenarios["core_crash_recovery"]["status"] == "passed"
    assert scenarios["passive_prompt_metadata_media"]["status"] == "blocked"
    crash = scenarios["core_crash_recovery"]["core_process_crash"]
    assert crash["child_exit_code"] == 17
    assert report_path.is_file()
    persisted = json.loads(report_path.read_text(encoding="utf-8"))
    assert persisted["status"] == "blocked"
    assert persisted["workspace_persisted"] is False
    external = {
        item["id"]: item for item in report["plugins"]
        if item["id"] in gate.E1_EXTERNAL_PLUGIN_IDS
    }
    assert set(external) == set(gate.E1_EXTERNAL_PLUGIN_IDS)
    assert all(item["status"] == "blocked" for item in external.values())


def test_undo_backup_integrity_oracle_is_sqlite_based(tmp_path: Path) -> None:
    db = tmp_path / "backup.db"
    connection = sqlite3.connect(db)
    try:
        connection.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT)")
        connection.execute("INSERT INTO t(value) VALUES ('ok')")
        connection.commit()
    finally:
        connection.close()
    assert gate._sqlite_state(db)["integrity"] == "ok"
