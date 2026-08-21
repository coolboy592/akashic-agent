from __future__ import annotations

import json
import subprocess
from dataclasses import fields
from pathlib import Path

import pytest

from docker.debug import plugin_v3_mobile_gate as gate


def test_lock_is_exact_pure_v3_mobile_fleet() -> None:
    contracts = gate._load_lock(gate.DEFAULT_LOCK)

    assert tuple(item.id for item in contracts) == gate.EXPECTED_PLUGIN_IDS
    assert contracts[0].source == "in-tree"
    assert tuple(item.id for item in contracts[1:]) == gate.EXTERNAL_PLUGIN_IDS
    for item in contracts:
        assert not gate.FORBIDDEN_LOCK_FIELDS.intersection(asdict_keys(item))
    assert all(
        item.resolved_sha == item.requested_ref == item.change_source_pr_head
        for item in contracts[1:]
    )


def test_lock_rejects_v2_fields(tmp_path: Path) -> None:
    raw = json.loads(gate.DEFAULT_LOCK.read_text(encoding="utf-8"))
    raw["plugins"][0]["plugin_class"] = "AkashaPlugin"
    path = tmp_path / "v2-lock.json"
    path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="v2 字段"):
        gate._load_lock(path)


def test_ast_mobile_contract_covers_all_sources() -> None:
    for contract in gate._load_lock(gate.DEFAULT_LOCK):
        if contract.source != "in-tree":
            continue
        root = gate.ROOT / contract.path
        evidence = gate._inspect_static_source(
            contract,
            root,
            root / contract.entrypoint,
        )
        assert evidence["status"] == "passed", evidence
        namespace = evidence["namespace"]
        assert isinstance(namespace, dict)
        inject = namespace["inject"]
        apply_signature = namespace["apply_signature"]
        assert isinstance(inject, list)
        assert isinstance(apply_signature, str)
        assert inject.count("UI_SLOTS") == 1
        assert apply_signature == "apply(ctx, config)"


def test_assets_and_core_runner_pass_for_in_tree_akasha() -> None:
    contract = gate._load_lock(gate.DEFAULT_LOCK)[0]
    root = gate.ROOT / contract.path

    assets = gate._asset_evidence(root, contract)
    module = assets["module"]
    stylesheet = assets["stylesheet"]
    assert isinstance(module, dict)
    assert isinstance(stylesheet, dict)
    assert module["sha256"]
    assert stylesheet["sha256"]
    runner = (
        "node",
        str(gate.UI_CONTRACT_RUNNER),
        str(root / contract.module),
        str(contract.navigation).lower(),
        json.dumps(contract.slots, ensure_ascii=False),
    )
    result = subprocess.run(runner, cwd=root, check=False)
    assert result.returncode == 0


def test_static_contract_rejects_missing_ui_registration(tmp_path: Path) -> None:
    root = tmp_path / "plugin"
    root.mkdir()
    source = (
        "from agent.plugin_composition import UI_SLOTS\n"
        "api_version = 3\nname = 'fixture'\n"
        "inject = (UI_SLOTS,)\n"
        "async def apply(ctx, config):\n"
        "    return None\n"
    )
    entrypoint = root / "plugin.py"
    entrypoint.write_text(source, encoding="utf-8")
    contract = gate.PluginContract(
        id="fixture",
        source="in-tree",
        path=".",
        entrypoint="plugin.py",
        module="mobile.js",
        stylesheet="mobile.css",
        navigation=False,
        slots=(),
        node_test="test.mjs",
        node_setup="none",
    )

    evidence = gate._inspect_static_source(contract, root, entrypoint)

    assert evidence["status"] == "failed"
    errors = evidence["errors"]
    assert isinstance(errors, list)
    assert any("register_mobile" in error for error in errors if isinstance(error, str))


def test_python_contract_runner_is_external_process() -> None:
    source = Path(gate.__file__).read_text(encoding="utf-8")
    assert "subprocess.run(" in source
    assert "akashic_plugin_contracts" in source
    assert "importlib" not in source


def test_offline_source_resolution_reports_every_missing_external() -> None:
    contracts = gate._load_lock(gate.DEFAULT_LOCK)
    roots, evidence, errors = gate._resolve_sources(
        contracts,
        Path("/tmp/v3-mobile-test-sandbox"),
        {},
        offline=True,
    )

    assert tuple(roots) == ("akasha",)
    assert tuple(item["id"] for item in evidence) == gate.EXPECTED_PLUGIN_IDS
    assert tuple(item["id"] for item in evidence if item["status"] == "failed") == gate.EXTERNAL_PLUGIN_IDS
    assert len(errors) == len(gate.EXTERNAL_PLUGIN_IDS)


def asdict_keys(item: gate.PluginContract) -> set[str]:
    return {field.name for field in fields(item)}
