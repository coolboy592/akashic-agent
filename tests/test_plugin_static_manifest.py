from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import agent.plugins.install as install_module
from agent.plugins.install import install_git_plugin
from agent.plugins.static_manifest import load_static_plugin_manifest


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _commit(root: Path) -> None:
    _git(root, "init", "--quiet")
    _git(root, "config", "user.email", "test@example.invalid")
    _git(root, "config", "user.name", "test")
    _git(root, "add", ".")
    _git(root, "commit", "--quiet", "-m", "fixture")


def _manifest(
    *,
    name: str = "calendar",
    version: str = "3.0.0",
    entrypoint: str = "plugin.py",
    requirements: str = "mcp/requirements.txt",
) -> str:
    return (
        "schema_version = 1\n"
        f'name = "{name}"\n'
        f'version = "{version}"\n'
        "api_version = 3\n"
        f'entrypoint = "{entrypoint}"\n\n'
        "[[python]]\n"
        f'requirements = "{requirements}"\n\n'
        "[validation]\n"
        'exclude_data_paths = [".env", "token.json"]\n'
    )


def test_static_manifest_is_import_free_and_exposes_runtime_policy(
    tmp_path: Path,
) -> None:
    root = tmp_path / "calendar"
    (root / "mcp").mkdir(parents=True)
    (root / "plugin.py").write_text(
        "raise RuntimeError('must not import during static parse')\n",
        encoding="utf-8",
    )
    (root / "mcp" / "requirements.txt").write_text("requests\n", encoding="utf-8")
    (root / "akashic.plugin.toml").write_text(_manifest(), encoding="utf-8")

    manifest = load_static_plugin_manifest(root)

    assert manifest.name == "calendar"
    assert manifest.version == "3.0.0"
    assert manifest.entrypoint == "plugin.py"
    assert manifest.requirements == ("mcp/requirements.txt",)
    assert manifest.python[0].runtime_root == "mcp"
    assert manifest.exclude_data_paths == (".env", "token.json")
    assert len(manifest.identity_digest) == 64


def test_static_manifest_validates_mcp_and_process_declarations(
    tmp_path: Path,
) -> None:
    root = tmp_path / "calendar"
    (root / "mcp").mkdir(parents=True)
    (root / "plugin.py").write_text("", encoding="utf-8")
    (root / "mcp" / "run_mcp.py").write_text("", encoding="utf-8")
    (root / "mcp" / "run_server.py").write_text("", encoding="utf-8")
    (root / "mcp" / "requirements.txt").write_text("", encoding="utf-8")
    (root / "akashic.plugin.toml").write_text(
        _manifest()
        + """
[[processes]]
name = "calendar_api"
command = ["python", "mcp/run_server.py"]
cwd = "mcp"
port_env = "PORT"
formal_port = 18000
readiness_path = "/health"

[[mcp]]
name = "calendar"
command = ["python", "mcp/run_mcp.py"]
cwd = "mcp"
required_tools = ["get_proactive_events"]
candidate_read_only_tools = ["get_proactive_events"]
endpoint_env = [{env = "PORT", process = "calendar_api"}]
candidate_env = {CALENDAR_BACKEND = "recording"}
""",
        encoding="utf-8",
    )

    manifest = load_static_plugin_manifest(root)

    assert manifest.managed_processes[0].name == "calendar_api"
    assert manifest.managed_processes[0].formal_port == 18000
    assert manifest.mcp_servers[0].endpoint_env == (("PORT", "calendar_api"),)
    assert manifest.mcp_servers[0].candidate_env == (("CALENDAR_BACKEND", "recording"),)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("entrypoint", "/plugin.py", "entrypoint"),
        ("requirements", "../requirements.txt", "requirements"),
        ("requirements", "missing.txt", "requirements"),
    ],
)
def test_static_manifest_rejects_unsafe_or_missing_runtime_paths(
    tmp_path: Path,
    field: str,
    value: str,
    message: str,
) -> None:
    root = tmp_path / "calendar"
    (root / "mcp").mkdir(parents=True)
    (root / "plugin.py").write_text("", encoding="utf-8")
    (root / "mcp" / "requirements.txt").write_text("", encoding="utf-8")
    manifest_text = _manifest(
        entrypoint=value if field == "entrypoint" else "plugin.py",
        requirements=value if field == "requirements" else "mcp/requirements.txt",
    )
    (root / "akashic.plugin.toml").write_text(manifest_text, encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_static_plugin_manifest(root)


def test_static_manifest_rejects_manifest_symlink(tmp_path: Path) -> None:
    root = tmp_path / "calendar"
    root.mkdir()
    (root / "plugin.py").write_text("", encoding="utf-8")
    outside = tmp_path / "outside.toml"
    outside.write_text(_manifest(requirements="requirements.txt"), encoding="utf-8")
    (root / "akashic.plugin.toml").symlink_to(outside)

    with pytest.raises(ValueError, match="缺少静态 manifest"):
        load_static_plugin_manifest(root)


def test_v3_static_install_stages_before_importing_plugin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "calendar-source"
    (repo / "mcp").mkdir(parents=True)
    (repo / "plugin.py").write_text(
        "from pathlib import Path\n"
        "Path(__file__).with_name('imported').write_text('bad')\n"
        "raise RuntimeError('static install must not import')\n",
        encoding="utf-8",
    )
    (repo / "mcp" / "requirements.txt").write_text("requests\n", encoding="utf-8")
    (repo / "akashic.plugin.toml").write_text(_manifest(), encoding="utf-8")
    _commit(repo)
    calls: list[tuple[str, Path]] = []

    def fake_run(args: list[str], *, cwd: Path, label: str) -> None:
        calls.append((label, cwd))
        if label.endswith("venv"):
            python_path = install_module._venv_python_path(cwd / ".venv")
            python_path.parent.mkdir(parents=True, exist_ok=True)
            python_path.write_text("", encoding="utf-8")

    monkeypatch.setattr(install_module, "_run_command", fake_run)
    result = install_git_plugin(
        workspace=tmp_path / "workspace",
        source=str(repo),
        marketplace="lab",
        plugins_home=tmp_path / "plugins-home",
    )

    assert result.plugin_name == "calendar"
    assert result.plugin_version == "3.0.0"
    assert [label for label, _ in calls] == [
        "calendar python[0] venv",
        "calendar python[0] pip install",
    ]
    assert not (result.installed_path / "imported").exists()
    assert (result.installed_path / "akashic.plugin.toml").is_file()
    assert (result.installed_path / "mcp" / ".venv").is_dir()

