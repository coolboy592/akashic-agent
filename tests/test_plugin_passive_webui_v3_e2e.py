from __future__ import annotations

import json
import inspect
import subprocess
from pathlib import Path

import pytest

from docker.debug import plugin_passive_webui_v3_e2e as gate


def test_gate_freezes_exact_pure_v3_scenario() -> None:
    lock = gate.composition_gate._load_lock(  # pyright: ignore[reportPrivateUsage]
        gate.composition_gate.DEFAULT_LOCK
    )

    assert gate.GATE_VERSION == 2
    assert gate.SCENARIO_PROFILE == "citation-meme-webui-v3-v1"
    assert gate.EXPECTED_PLUGIN_IDS == ("citation@webui", "meme@webui")
    assert tuple(item.id for item in lock.plugins) == ("citation", "meme")
    assert all(item.requested_ref == item.resolved_sha for item in lock.plugins)
    assert len(gate._scenario_sha256()) == 64  # pyright: ignore[reportPrivateUsage]


def test_capability_oracle_distinguishes_builtin_and_plugin_skills() -> None:
    payload: dict[str, object] = {
        "plugins": [
            {"id": "citation@webui"},
            {"id": "meme@webui"},
        ],
        "skills": [
            {"name": "plugin-system", "source": "builtin"},
            {"name": "meme-manage", "source": "workspace"},
        ],
    }

    gate._assert_capabilities(payload)  # pyright: ignore[reportPrivateUsage]
    cast_skills = payload["skills"]
    assert isinstance(cast_skills, list)
    cast_skills.append({"name": "unexpected", "source": "workspace"})
    with pytest.raises(gate.GateFailure, match="插件 Skill 投影错误"):
        gate._assert_capabilities(payload)  # pyright: ignore[reportPrivateUsage]


def test_message_oracle_requires_citation_and_meme_persistence() -> None:
    session_id = "web:test"
    payload: dict[str, object] = {
        "total": 2,
        "items": [
            {
                "session_key": session_id,
                "role": "user",
                "content": gate.USER_INPUT,
            },
            {
                "session_key": session_id,
                "role": "assistant",
                "content": "答复正文",
                "cited_memory_ids": ["mem_1"],
                "media": ["/sandbox/workspace/memes/shy/001.png"],
            },
        ],
    }

    assert gate._assert_messages(  # pyright: ignore[reportPrivateUsage]
        payload,
        session_id,
    ) == payload["items"]
    assistant = payload["items"]
    assert isinstance(assistant, list)
    assert isinstance(assistant[1], dict)
    assistant[1]["cited_memory_ids"] = []
    with pytest.raises(gate.GateFailure, match="citation metadata"):
        gate._assert_messages(payload, session_id)  # pyright: ignore[reportPrivateUsage]


def test_message_oracle_rejects_cross_session_assistant() -> None:
    session_id = "web:test"
    payload: dict[str, object] = {
        "total": 2,
        "items": [
            {"session_key": session_id, "role": "user", "content": gate.USER_INPUT},
            {
                "session_key": "web:other",
                "role": "assistant",
                "content": "答复正文",
                "cited_memory_ids": ["mem_1"],
                "media": ["/sandbox/workspace/memes/shy/001.png"],
            },
        ],
    }

    with pytest.raises(gate.GateFailure, match="assistant session"):
        gate._assert_messages(payload, session_id)  # pyright: ignore[reportPrivateUsage]


def test_final_frame_must_belong_to_created_session() -> None:
    class WebSocket:
        def recv(self, *, timeout: float) -> str:
            _ = timeout
            return json.dumps(
                {
                    "type": "message.final",
                    "session_id": "web:other",
                    "content": "答复正文",
                    "media": ["/sandbox/workspace/memes/shy/001.png"],
                }
            )

    frames: list[dict[str, object]] = []
    with pytest.raises(gate.GateFailure, match="final session"):
        gate._receive_final(  # pyright: ignore[reportPrivateUsage]
            WebSocket(),
            frames,
            "web:expected",
        )
    assert frames[0]["session_id"] == "web:other"


def test_webui_only_config_rejects_another_enabled_channel() -> None:
    config: dict[str, object] = {
        "channels": {
            "chat": {"enabled": True, "channel_name": "web"},
            "telegram": {"enabled": False},
            "qq": {"enabled": False},
        },
        "mobile_realtime": {"enabled": False},
        "proactive": {"enabled": False},
    }

    gate._assert_webui_only(config)  # pyright: ignore[reportPrivateUsage]
    channels = config["channels"]
    assert isinstance(channels, dict)
    telegram = channels["telegram"]
    assert isinstance(telegram, dict)
    telegram["enabled"] = True
    with pytest.raises(gate.GateFailure, match="Telegram"):
        gate._assert_webui_only(config)  # pyright: ignore[reportPrivateUsage]


def test_chat_health_must_be_ready_even_when_http_succeeds() -> None:
    gate._assert_ready(  # pyright: ignore[reportPrivateUsage]
        {"status": "ready"},
        "/api/chat/health",
    )
    with pytest.raises(gate.GateFailure, match="未 ready"):
        gate._assert_ready(  # pyright: ignore[reportPrivateUsage]
            {"status": "degraded"},
            "/api/chat/health",
        )


def test_final_immutability_rejects_shutdown_publication_or_asset_changes(
    tmp_path: Path,
) -> None:
    sandbox = tmp_path
    plugin_base = (
        sandbox / "home/.akashic-plugin/cache" / gate.MARKETPLACE / "citation"
    )
    first = plugin_base / ".artifacts/first"
    second = plugin_base / ".artifacts/second"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    (first / "plugin.py").write_text("FIRST = True\n", encoding="utf-8")
    (second / "plugin.py").write_text("SECOND = True\n", encoding="utf-8")
    memes = sandbox / "workspace/memes"
    memes.mkdir(parents=True)
    (memes / "manifest.json").write_text("{}\n", encoding="utf-8")
    pointer = gate.ArtifactPointer(".artifacts/first")
    gate.write_pointers(plugin_base, stable=pointer, latest=pointer)
    installed: list[dict[str, object]] = [
        {
            "plugin_id": f"citation@{gate.MARKETPLACE}",
            "pointer": ".artifacts/first",
            "artifact_sha256_before": gate._tree_sha256(first),  # pyright: ignore[reportPrivateUsage]
            "pointers_before": gate._pointer_paths(plugin_base),  # pyright: ignore[reportPrivateUsage]
            "artifact_inventory_before": gate._artifact_inventory(plugin_base),  # pyright: ignore[reportPrivateUsage]
        }
    ]
    replacement = gate.ArtifactPointer(".artifacts/second")
    gate.write_pointers(plugin_base, stable=replacement, latest=replacement)

    with pytest.raises(gate.GateFailure, match="installed pointers"):
        gate._verify_runtime_immutability(  # pyright: ignore[reportPrivateUsage]
            sandbox,
            installed,
            gate._tree_sha256(memes),  # pyright: ignore[reportPrivateUsage]
            phase="after_stop",
        )

    gate.write_pointers(plugin_base, stable=pointer, latest=pointer)
    third = plugin_base / ".artifacts/third"
    third.mkdir()
    (third / "plugin.py").write_text("THIRD = True\n", encoding="utf-8")
    with pytest.raises(gate.GateFailure, match="重发布 installed artifact"):
        gate._verify_runtime_immutability(  # pyright: ignore[reportPrivateUsage]
            sandbox,
            installed,
            gate._tree_sha256(memes),  # pyright: ignore[reportPrivateUsage]
            phase="after_stop",
        )

    third.joinpath("plugin.py").unlink()
    third.rmdir()
    before_memes = gate._tree_sha256(memes)  # pyright: ignore[reportPrivateUsage]
    (memes / "shutdown.txt").write_text("mutated\n", encoding="utf-8")
    with pytest.raises(gate.GateFailure, match="workspace/memes"):
        gate._verify_runtime_immutability(  # pyright: ignore[reportPrivateUsage]
            sandbox,
            installed,
            before_memes,
            phase="after_stop",
        )


def test_host_rechecks_immutability_after_graceful_stop() -> None:
    source = inspect.getsource(gate._run_host)  # pyright: ignore[reportPrivateUsage]

    stop_index = source.index('[*compose, "stop"')
    final_check_index = source.index('phase="after_stop"')
    assert stop_index < final_check_index


def test_cleanup_oracle_requires_zero_compose_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_residuals(
        command: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        _ = command, cwd, env
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(gate, "ROOT", tmp_path)
    monkeypatch.setattr(gate, "_run", no_residuals)

    assert gate._cleanup_evidence(  # pyright: ignore[reportPrivateUsage]
        ["docker", "compose"],
        "project",
        {},
        0,
    ) == {"compose_down_returncode": 0, "residuals": []}


def test_ci_runs_real_webui_gate_and_uploads_evidence() -> None:
    workflow = (gate.ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    job = workflow.split("  plugin-passive-composition-v3-gate:\n", 1)[1].split(
        "\n  check-and-test:",
        1,
    )[0]

    assert (
        "python docker/debug/plugin_passive_webui_v3_e2e.py --require-clean-core"
        in job
    )
    assert "docker/debug/reports/plugin-passive-webui-v3/" in job
    assert "continue-on-error" not in job
    assert "pytest.skip" not in Path(gate.__file__).read_text(encoding="utf-8")
