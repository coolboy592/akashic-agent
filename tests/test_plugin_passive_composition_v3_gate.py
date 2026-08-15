from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys

import pytest

_GATE_PATH = (
    Path(__file__).resolve().parents[1]
    / "docker/debug/plugin_passive_composition_v3_gate.py"
)
_SPEC = importlib.util.spec_from_file_location(
    "plugin_passive_composition_v3_gate",
    _GATE_PATH,
)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)


def test_lock_pins_exact_pure_v3_source_set() -> None:
    lock = gate._load_lock(gate.DEFAULT_LOCK)

    assert lock.contract.id == "plugin_contracts"
    assert tuple(plugin.id for plugin in lock.plugins) == gate.EXPECTED_PLUGIN_IDS
    assert all(item.requested_ref == item.resolved_sha for item in lock.plugins)
    assert all(item.change_source_pr_head == item.resolved_sha for item in lock.plugins)
    assert lock.contract.requested_ref == lock.contract.resolved_sha
    assert lock.contract.change_source_pr_head == lock.contract.resolved_sha


def test_lock_rejects_floating_revision(tmp_path: Path) -> None:
    raw = json.loads(gate.DEFAULT_LOCK.read_text(encoding="utf-8"))
    raw["plugins"][0]["requested_ref"] = "main"
    lock = tmp_path / "lock.json"
    lock.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="完整 SHA"):
        gate._load_lock(lock)


def test_lock_rejects_plugin_order_drift(tmp_path: Path) -> None:
    raw = json.loads(gate.DEFAULT_LOCK.read_text(encoding="utf-8"))
    raw["plugins"].reverse()
    lock = tmp_path / "lock.json"
    lock.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="集合或顺序"):
        gate._load_lock(lock)


def test_gate_freezes_listener_order_and_scenario_digest() -> None:
    assert gate.EXPECTED_LISTENERS == (
        "serial:turn.prompt_render:citation",
        "serial:turn.prompt_render:meme",
        "serial:turn.after_reasoning.preprocess:citation",
        "serial:turn.after_reasoning.preprocess:meme",
        "serial:turn.after_reasoning.cleanup:citation",
    )
    assert gate.SCENARIO_PROFILE == "citation-meme-passive-v3-v1"
    assert len(gate._scenario_catalog_sha256()) == 64


def test_gate_pins_protocol_source_and_version() -> None:
    assert gate.GATE_VERSION == 1
    assert gate.PROTOCOL_SOURCE_COMMIT == ("a047470a39d4f7d2e6be1d2a8e2824916d52fad1")
    evidence = gate._protocol_source_evidence()
    assert evidence["commit"] == gate.PROTOCOL_SOURCE_COMMIT
    assert [item["path"] for item in evidence["files"]] == list(
        gate.PROTOCOL_SOURCE_PATHS
    )
    assert all(len(item["sha256"]) == 64 for item in evidence["files"])


def test_report_schema_has_reconstructible_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lock_path = tmp_path / "lock.json"
    lock_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(gate, "ROOT", tmp_path)
    monkeypatch.setattr(
        gate,
        "_protocol_source_evidence",
        lambda: {"commit": "protocol"},
    )
    monkeypatch.setattr(gate, "_git_output", lambda *_args: "git-identity")
    source = gate.SourceEvidence(
        id="source",
        repository="https://github.com/example/source.git",
        requested_ref="a" * 40,
        resolved_sha="a" * 40,
        change_source_pr_head="a" * 40,
        tree="tree",
    )
    contract = gate.ContractEvidence(
        contract="akashic-plugin-api-v3",
        plugin_ids=("citation", "meme"),
        source_sha256=("b" * 64, "c" * 64),
        plugin_classes=((), ()),
    )

    report = gate._build_report(
        core_status=[],
        lock_path=lock_path,
        contract_evidence=source,
        contract_report=contract,
        plugin_evidence=(source, source),
        runtime={"cleanup": {"listeners": []}},
    )

    assert report["status"] == "passed"
    assert report["gate_version"] == gate.GATE_VERSION
    assert report["protocol_source"] == {"commit": "protocol"}
    assert report["scenario_catalog_sha256"] == gate._scenario_catalog_sha256()
    assert report["cleanup"] == {"listeners": []}


def test_ci_runs_real_gate_with_full_history_and_clean_core() -> None:
    workflow = (gate.ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "  workflow_dispatch:\n" in workflow
    job = workflow.split("  plugin-passive-composition-v3-gate:\n", 1)[1].split(
        "\n  check-and-test:",
        1,
    )[0]

    assert "fetch-depth: 0" in job
    assert (
        "python docker/debug/plugin_passive_composition_v3_gate.py "
        "--require-clean-core"
    ) in job
    assert "continue-on-error" not in job
    assert "pytest.skip" not in _GATE_PATH.read_text(encoding="utf-8")
