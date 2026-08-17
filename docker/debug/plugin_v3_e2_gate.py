"""Concentrated disposable-workspace E2 Gate for the locked v3 plugin fleet.

The gate deliberately keeps the production ``PluginManager`` and its typed
runtime hosts in the loop. It never changes the checkout, never uses formal
credentials, and records ``blocked`` when the supplied Python runtime cannot
start the locked recording backends.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import textwrap
import urllib.request
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agent.plugins.artifacts import ArtifactPointer, read_pointers, write_pointers  # noqa: E402
from agent.plugins.manager import PluginManager  # noqa: E402
from agent.plugins.manifest import write_plugin_manifest  # noqa: E402
from agent.plugins.snapshot import (  # noqa: E402
    bind_runtime_snapshot,
    reset_runtime_snapshot,
)
from agent.plugins.static_manifest import load_static_plugin_manifest  # noqa: E402
from agent.tool_hooks.executor import ToolExecutor  # noqa: E402
from agent.tool_hooks.types import (  # noqa: E402
    ToolExecutionRequest,
    ToolExecutionResult,
)
from bus.event_bus import EventBus  # noqa: E402

DEFAULT_LOCK = ROOT / "docker" / "debug" / "plugin-v3-fleet.lock.json"
DEFAULT_REPORT = (
    ROOT / "docker" / "debug" / "reports" / "plugin-v3-e2" / "gate.json"
)
GATE_VERSION = 1
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")

EXPECTED_PLUGIN_IDS = (
    "shell_restore",
    "shell_safety",
    "tool_loop_guard",
    "calendar-mcp",
    "feed-mcp",
    "fitbit-mcp",
    "steam-mcp",
)
SHELL_PLUGIN_IDS = EXPECTED_PLUGIN_IDS[:3]
MCP_PLUGIN_IDS = EXPECTED_PLUGIN_IDS[3:]
INSTALLED_NAMES = {
    "calendar-mcp": "calendar",
    "feed-mcp": "feed",
    "fitbit-mcp": "fitbit",
    "steam-mcp": "steam",
}
EXPECTED_LISTENERS = (
    "transform:tool.input.prepare[akashic.tool-input.v1]:shell_restore",
    "serial:tool.execution.authorize[bail=akashic.tool-deny-reason.v1]:shell_safety",
    "serial:tool.execution.authorize[bail=akashic.tool-deny-reason.v1]:tool_loop_guard",
)
SCENARIO_PROFILE = "plugin-v3-e2-shell-v1"
READONLY_PROBES: dict[str, tuple[str, ...]] = {
    "calendar-mcp": ("get_proactive_events",),
    "feed-mcp": ("get_proactive_events",),
    "fitbit-mcp": ("get_proactive_events", "get_sleep_context"),
    "steam-mcp": ("get_steam_context",),
}
FORMAL_PORTS = {"calendar-mcp": 18000, "fitbit-mcp": 18765}
REQUIRED_IMPORTS = {
    "calendar-mcp": (
        "mcp",
        "fastapi",
        "uvicorn",
        "dotenv",
        "dateutil",
        "google.oauth2.credentials",
        "google.auth.transport.requests",
        "googleapiclient.discovery",
        "google_auth_oauthlib.flow",
    ),
    "feed-mcp": ("mcp",),
    "fitbit-mcp": ("mcp", "fastapi", "uvicorn", "requests"),
    "steam-mcp": ("mcp",),
}

ToolInvoker = Callable[[str, dict[str, Any]], Awaitable[Any]]


class GateBlocked(RuntimeError):
    """Indicate an environment or runtime prerequisite that was not met."""


@dataclass(frozen=True, slots=True)
class PluginLock:
    id: str
    repository: str
    requested_ref: str
    resolved_sha: str
    change_source_pr_head: str


@dataclass(frozen=True, slots=True)
class PluginEvidence:
    id: str
    repository: str
    requested_ref: str
    resolved_sha: str
    change_source_pr_head: str
    tree: str


@dataclass(frozen=True, slots=True)
class ScenarioCase:
    id: str
    session: str
    command: str
    expected_status: str
    expected_invoked: bool


@dataclass(frozen=True, slots=True)
class ScenarioEvidence:
    id: str
    status: str
    final_command: str
    invoked: bool
    exit_code: int | None


@dataclass(frozen=True, slots=True)
class RuntimeEvidence:
    id: str
    plugin_id: str
    generation_id: str
    mode: str
    state: str
    mcp_tools: tuple[str, ...]
    probes: tuple[dict[str, object], ...]
    process_endpoints: tuple[dict[str, object], ...]
    candidate_workspace: str
    formal_data_before: str
    formal_data_after: str
    cleanup: dict[str, object]


@dataclass(frozen=True, slots=True)
class CleanupEvidence:
    shell_generation_ids: tuple[str, ...]
    retained_runtime_failures: tuple[str, ...]
    cleanup_failures: tuple[str, ...]
    listeners: tuple[str, ...]
    effects: tuple[str, ...]


SCENARIO_CATALOG = (
    ScenarioCase("plain-rm", "plain", "rm /tmp/plain.txt", "success", True),
    ScenarioCase(
        "sudo-cluster",
        "cluster",
        "sudo -nE rm /tmp/cluster.txt",
        "success",
        True,
    ),
    ScenarioCase(
        "sudo-preserve-env",
        "env",
        "sudo -n --preserve-env=HOME rm /tmp/env.txt",
        "success",
        True,
    ),
    ScenarioCase(
        "sudo-mode-denied",
        "mode",
        "sudo -n -s rm /tmp/mode.txt",
        "denied",
        False,
    ),
    ScenarioCase("repeat-1", "repeat", "rm /tmp/repeat.txt", "success", True),
    ScenarioCase("repeat-2", "repeat", "rm /tmp/repeat.txt", "success", True),
    ScenarioCase("repeat-3", "repeat", "rm /tmp/repeat.txt", "denied", False),
)


def _required_string(item: Mapping[str, object], name: str) -> str:
    value = item.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"锁字段必须是非空字符串: {name}")
    return value


def _parse_plugin_lock(raw: object) -> PluginLock:
    """Parse one lock entry and require one immutable commit identity."""

    # 1. Reject fields that are not part of the fleet lock contract.
    expected = {
        "id",
        "repository",
        "requested_ref",
        "resolved_sha",
        "change_source_pr_head",
    }
    if not isinstance(raw, dict) or set(raw) != expected:
        raise ValueError(f"v3 fleet lock entry 字段无效: {raw!r}")

    # 2. Freeze the repository and all three revision claims.
    item = cast(dict[str, object], raw)
    values = {key: _required_string(item, key) for key in expected}
    repository = values["repository"]
    if not repository.startswith("https://github.com/"):
        raise ValueError(f"插件仓库必须是 GitHub HTTPS 地址: {repository}")
    revisions = tuple(
        values[key]
        for key in ("requested_ref", "resolved_sha", "change_source_pr_head")
    )
    if any(COMMIT_PATTERN.fullmatch(value) is None for value in revisions):
        raise ValueError(f"插件 revision 必须是完整 40-hex SHA: {values['id']}")
    if len(set(revisions)) != 1:
        raise ValueError(f"插件三个 revision 必须完全一致: {values['id']}")
    return PluginLock(
        id=values["id"],
        repository=repository,
        requested_ref=values["requested_ref"],
        resolved_sha=values["resolved_sha"],
        change_source_pr_head=values["change_source_pr_head"],
    )


def _load_lock(path: Path) -> tuple[PluginLock, ...]:
    """Load the seven E2 entries from the shared fleet lock."""

    # 1. Validate the shared document without accepting a missing or duplicate id.
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or set(raw) != {"schema_version", "plugins"}:
        raise ValueError("v3 fleet lock 根结构无效")
    if raw["schema_version"] != 1:
        raise ValueError(f"不支持的 v3 fleet lock 版本: {raw['schema_version']!r}")
    entries = raw["plugins"]
    if not isinstance(entries, list):
        raise ValueError("v3 fleet lock plugins 必须是列表")
    parsed = tuple(_parse_plugin_lock(item) for item in entries)
    by_id: dict[str, PluginLock] = {}
    for item in parsed:
        if item.id in by_id:
            raise ValueError(f"v3 fleet lock 存在重复插件: {item.id}")
        by_id[item.id] = item

    # 2. Select the exact E2 contract in its contract order.
    missing = tuple(
        plugin_id for plugin_id in EXPECTED_PLUGIN_IDS if plugin_id not in by_id
    )
    if missing:
        raise ValueError(f"v3 E2 lock 缺少插件: {', '.join(missing)}")
    return tuple(by_id[plugin_id] for plugin_id in EXPECTED_PLUGIN_IDS)


def _run(command: tuple[str, ...], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    """Run one checked local command and preserve stderr for the caller."""

    return subprocess.run(
        command,
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _git_output(cwd: Path, *args: str) -> str:
    return _run(("git", *args), cwd=cwd).stdout.strip()


def _checkout_locked_plugin(lock: PluginLock, checkout: Path) -> PluginEvidence:
    """Fetch one exact public Git object into a new disposable checkout."""

    # 1. Create an isolated repository and fetch only the locked object.
    checkout.parent.mkdir(parents=True, exist_ok=True)
    _run(("git", "init", "--quiet", str(checkout)), cwd=ROOT)
    _run(("git", "remote", "add", "origin", lock.repository), cwd=checkout)
    _run(
        ("git", "fetch", "--quiet", "--depth=1", "origin", lock.resolved_sha),
        cwd=checkout,
    )
    _run(("git", "checkout", "--quiet", "--detach", "FETCH_HEAD"), cwd=checkout)

    # 2. Verify both commit and working-tree identity before handing it to Core.
    if _git_output(checkout, "rev-parse", "HEAD") != lock.resolved_sha:
        raise RuntimeError(f"插件检出提交与锁不一致: {lock.id}")
    if _git_output(checkout, "status", "--porcelain"):
        raise RuntimeError(f"插件检出后工作树不干净: {lock.id}")
    return PluginEvidence(
        id=lock.id,
        repository=lock.repository,
        requested_ref=lock.requested_ref,
        resolved_sha=lock.resolved_sha,
        change_source_pr_head=lock.change_source_pr_head,
        tree=_git_output(checkout, "rev-parse", "HEAD^{tree}"),
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _scenario_catalog_sha256() -> str:
    encoded = json.dumps(
        [asdict(item) for item in SCENARIO_CATALOG],
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _runtime_interpreter(path: Path) -> Path:
    """Validate one supplied interpreter without mutating its environment."""

    candidate = path.expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    if not candidate.is_file() or not os.access(candidate, os.X_OK):
        raise GateBlocked(f"runtime Python 不可执行: {candidate}")
    return candidate


def _check_imports(runtime_python: Path, plugin_ids: tuple[str, ...]) -> None:
    """Check imports required by the locked candidate recording processes."""

    # 1. Query the supplied interpreter, rather than this Gate's interpreter.
    modules = tuple(
        dict.fromkeys(
            module for plugin_id in plugin_ids for module in REQUIRED_IMPORTS[plugin_id]
        )
    )
    script = textwrap.dedent(
        """
        import importlib.util
        import json
        import sys
        missing = [name for name in sys.argv[1:] if importlib.util.find_spec(name) is None]
        print(json.dumps(missing))
        """
    )
    result = subprocess.run(
        (str(runtime_python), "-c", script, *modules),
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    missing = json.loads(result.stdout)
    if missing:
        raise GateBlocked(
            "recording runtime 依赖未安装；请使用声明 requirements staging 后重试: "
            + ", ".join(str(item) for item in missing)
        )


def _create_runtime_stage(runtime_python: Path, sandbox: Path) -> Path:
    """Create a disposable venv-shaped runtime link for static manifests."""

    # 1. Keep plugin artifact trees immutable while exposing the supplied Python.
    stage = sandbox / "runtime-python"
    binary = stage / "bin" / "python"
    binary.parent.mkdir(parents=True, exist_ok=False)
    binary.symlink_to(runtime_python)
    return stage


def _copy_source_to_artifact(source: Path, target: Path) -> None:
    """Copy a locked source tree while excluding VCS and generated files."""

    shutil.copytree(
        source,
        target,
        ignore=shutil.ignore_patterns(
            ".git",
            ".venv",
            "__pycache__",
            ".pytest_cache",
            ".mypy_cache",
            ".ruff_cache",
            "node_modules",
        ),
    )


def _stage_candidate_artifact(
    source: Path,
    plugin_id: str,
    cache_root: Path,
    runtime_stage: Path,
) -> Path:
    """Stage stable and latest copies for one installed candidate transaction."""

    # 1. Materialize exact source copies under the disposable installed cache.
    manifest = load_static_plugin_manifest(source)
    plugin_base = cache_root / "github" / manifest.name
    stable = plugin_base / ".artifacts" / "stable"
    latest = plugin_base / ".artifacts" / "latest"
    _copy_source_to_artifact(source, stable)
    _copy_source_to_artifact(source, latest)

    # 2. Bind every declared Python runtime to the caller-supplied interpreter.
    for artifact in (stable, latest):
        for runtime in manifest.python:
            runtime_root = (artifact / runtime.runtime_root).resolve(strict=False)
            runtime_root.mkdir(parents=True, exist_ok=True)
            link = runtime_root / ".venv"
            if link.exists() or link.is_symlink():
                raise RuntimeError(f"artifact runtime staging target 已存在: {link}")
            link.symlink_to(runtime_stage, target_is_directory=True)
    plugin_base.mkdir(parents=True, exist_ok=True)
    _ = write_pointers(
        plugin_base,
        stable=ArtifactPointer(".artifacts/stable"),
        latest=ArtifactPointer(".artifacts/latest"),
    )
    if not plugin_id.endswith("-mcp"):
        raise ValueError(f"installed candidate id 与 E2 plugin id 不一致: {plugin_id}")
    return plugin_base


def _prepare_external_candidates(
    checkouts: Mapping[str, Path],
    cache_root: Path,
    runtime_stage: Path,
) -> dict[str, Path]:
    """Stage all four MCP candidates without loading their formal generations."""

    cache_root.mkdir(parents=True, exist_ok=True)
    bases: dict[str, Path] = {}
    for plugin_id in MCP_PLUGIN_IDS:
        bases[plugin_id] = _stage_candidate_artifact(
            checkouts[plugin_id],
            plugin_id,
            cache_root,
            runtime_stage,
        )
    entries = {f"{INSTALLED_NAMES[item]}@github": True for item in MCP_PLUGIN_IDS}
    _ = write_plugin_manifest(entries, plugins_home=cache_root.parent)
    return bases


def _make_fake_sudo(bin_dir: Path) -> None:
    """Install a local non-privileged sudo shim for disposable shell commands."""

    # 1. The shim only strips the tested non-interactive flags.
    script = """#!/bin/sh
set -eu
while [ "$#" -gt 0 ]; do
  case "$1" in
    -n|-nE|-E|--non-interactive|--preserve-env|--preserve-env=*) shift ;;
    *) break ;;
  esac
done
exec "$@"
"""
    bin_dir.mkdir(parents=True, exist_ok=True)
    path = bin_dir / "sudo"
    path.write_text(script, encoding="utf-8")
    path.chmod(0o755)


def _shell_command_for_case(case: ScenarioCase, target_root: Path) -> tuple[str, Path]:
    stem = case.command.rsplit("/", 1)[-1]
    target = target_root / stem
    return case.command.replace(f"/tmp/{stem}", str(target)), target


def _assert_shell_scenario(
    case: ScenarioCase,
    result: ToolExecutionResult,
    final_command: str,
    restore_dir: Path,
) -> None:
    """Assert locked status, transformed command and real file movement."""

    if result.status != case.expected_status:
        raise RuntimeError(f"场景 {case.id} 状态错误: {result.status} {result.output}")
    if case.expected_status == "success":
        tokens = shlex.split(final_command)
        if "mv" not in tuple(Path(item).name for item in tokens):
            raise RuntimeError(f"场景 {case.id} 未执行 rm -> mv: {final_command}")
        if str(restore_dir) not in tokens:
            raise RuntimeError(f"场景 {case.id} 未指向还原目录: {final_command}")
    elif case.id == "sudo-mode-denied":
        if "普通命令执行" not in str(result.output):
            raise RuntimeError(f"场景 {case.id} 未由 Safety 拒绝: {result.output}")
    elif case.id == "repeat-3":
        if not str(result.output).startswith("tool_loop_guard:"):
            raise RuntimeError(f"场景 {case.id} 未由 Loop Guard 拒绝: {result.output}")


async def _run_shell_scenarios(
    manager: PluginManager,
    sandbox: Path,
) -> tuple[tuple[str, ...], tuple[ScenarioEvidence, ...], list[dict[str, object]]]:
    """Run the exact Shell Restore/Safety/Loop Guard catalog through ToolExecutor."""

    # 1. Freeze the formal Root topology and bind one real runtime lease.
    snapshot = manager.current_snapshot
    if snapshot is None or snapshot.composition_root is None:
        raise RuntimeError("正式 snapshot 缺少 v3 CompositionRoot")
    listeners = snapshot.composition_root.topology_view().listeners
    if listeners != EXPECTED_LISTENERS:
        raise RuntimeError(f"v3 Shell listener 顺序不符合锁定合同: {listeners}")
    restore = manager.generation("shell_restore")
    if restore is None:
        raise RuntimeError("正式 snapshot 缺少 shell_restore generation")
    restore_dir = restore.data_dir / "restore"
    target_root = sandbox / "shell-targets"
    fake_bin = sandbox / "fake-bin"
    _make_fake_sudo(fake_bin)
    invocations: list[dict[str, object]] = []
    executor = ToolExecutor()

    async def invoke(tool_name: str, arguments: dict[str, Any]) -> object:
        if tool_name != "shell":
            raise RuntimeError(f"E2 shell invoker 收到未知工具: {tool_name}")
        command = str(arguments.get("command", ""))
        environment = dict(os.environ)
        environment["PATH"] = f"{fake_bin}:{environment.get('PATH', '')}"
        completed = await asyncio.to_thread(
            subprocess.run,
            command,
            shell=True,
            cwd=target_root,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,
            check=False,
        )
        record = {
            "tool_name": tool_name,
            "arguments": dict(arguments),
            "returncode": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
        invocations.append(record)
        if completed.returncode != 0:
            raise RuntimeError(f"真实 shell 返回 {completed.returncode}: {completed.stderr}")
        return completed.stdout

    target_root.mkdir(parents=True, exist_ok=True)
    lease = manager.snapshot_store.lease()
    token = bind_runtime_snapshot(lease)
    evidence: list[ScenarioEvidence] = []
    try:
        for index, case in enumerate(SCENARIO_CATALOG):
            command, target = _shell_command_for_case(case, target_root)
            if case.expected_invoked or case.id == "repeat-3":
                target.write_text(case.id, encoding="utf-8")
            before = len(invocations)
            result = await executor.execute(
                ToolExecutionRequest(
                    call_id=f"e2-shell-{index}",
                    tool_name="shell",
                    arguments={"command": command},
                    source="passive",
                    session_key=case.session,
                ),
                invoke,
            )
            invoked = len(invocations) == before + 1
            if invoked != case.expected_invoked:
                raise RuntimeError(
                    f"场景 {case.id} invoker 状态错误: expected={case.expected_invoked} actual={invoked}"
                )
            final_command = str(result.final_arguments.get("command", ""))
            _assert_shell_scenario(case, result, final_command, restore_dir)
            if case.expected_status == "success" and not (restore_dir / target.name).is_file():
                raise RuntimeError(f"场景 {case.id} 未观察到真实文件进入 restore: {target}")
            exit_code: int | None = None
            if invoked:
                raw_exit_code = invocations[-1].get("returncode")
                if not isinstance(raw_exit_code, int):
                    raise RuntimeError(f"场景 {case.id} 缺少真实进程返回码")
                exit_code = raw_exit_code
            evidence.append(
                ScenarioEvidence(
                    id=case.id,
                    status=result.status,
                    final_command=final_command,
                    invoked=invoked,
                    exit_code=exit_code,
                )
            )
    finally:
        reset_runtime_snapshot(token)
        await lease.release()
    if len(invocations) != 5:
        raise RuntimeError(f"Shell 真实 invoker 调用次数错误: {len(invocations)}")
    return listeners, tuple(evidence), invocations


def _json_output(output: str) -> object:
    try:
        return json.loads(output)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"recording MCP 返回非 JSON typed payload: {output!r}") from error


def _assert_recording_payload(plugin_id: str, tool_name: str, payload: object) -> None:
    """Require the known recording payload shape for each read-only tool."""

    if not isinstance(payload, dict):
        raise RuntimeError(f"{plugin_id}:{tool_name} payload 不是 object: {payload!r}")
    if plugin_id == "steam-mcp":
        items = payload.get("items")
        if not isinstance(items, list) or not items or not isinstance(items[0], dict):
            raise RuntimeError(f"{plugin_id}:{tool_name} recording items 缺失")
        if items[0].get("recording") is not True:
            raise RuntimeError(f"{plugin_id}:{tool_name} 未返回 recording=true")
        return
    if payload.get("status") != "empty":
        raise RuntimeError(f"{plugin_id}:{tool_name} 未返回 typed empty: {payload!r}")


def _readiness(endpoint_url: str) -> int:
    request = urllib.request.Request(endpoint_url, method="GET")
    with urllib.request.urlopen(request, timeout=5) as response:
        return int(response.status)


async def _probe_candidate(
    manager: PluginManager,
    plugin_id: str,
    bases: Mapping[str, Path],
) -> RuntimeEvidence:
    """Publish one latest candidate, exercise recording routes, then discard it."""

    installed_id = f"{INSTALLED_NAMES[plugin_id]}@github"
    formal_data = manager._workspace / "plugin-data" / f"{INSTALLED_NAMES[plugin_id]}-github"
    before = _sha256_tree(formal_data)
    prepared = await manager.prepare_candidate(installed_id)
    if prepared is None or prepared.runtime_snapshot is None:
        raise GateBlocked(f"{plugin_id} prepare_candidate 未生成 typed snapshot")
    generation_id = prepared.generation_id
    candidate_workspace = prepared.validation_workspace
    if candidate_workspace is None:
        raise RuntimeError(f"{plugin_id} candidate 缺少 validation workspace")
    result = await manager.publish_prepared(installed_id)
    if result.get("publication_state") != "latest_ready":
        raise RuntimeError(f"{plugin_id} candidate 未进入 latest_ready: {result}")
    runtime = manager.composition_generation_host.get(generation_id)
    if runtime is None or runtime.mode != "candidate":
        raise RuntimeError(f"{plugin_id} 未观察到 candidate CompositionRuntime")
    if runtime.mcp is None:
        raise RuntimeError(f"{plugin_id} candidate 缺少 MCP generation")
    server_name = INSTALLED_NAMES[plugin_id]
    server = runtime.mcp.server(server_name)
    expected_tools = READONLY_PROBES[plugin_id]
    if tuple(server.tool_names) != expected_tools:
        raise RuntimeError(
            f"{plugin_id} candidate tool allowlist 漂移: {server.tool_names} != {expected_tools}"
        )
    route = server.route()
    probes: list[dict[str, object]] = []
    for tool_name in expected_tools:
        call = await route.call(tool_name, {})
        if not call.success:
            raise RuntimeError(f"{plugin_id}:{tool_name} MCP tool_error: {call.output}")
        payload = _json_output(call.output)
        _assert_recording_payload(plugin_id, tool_name, payload)
        probes.append({"tool": tool_name, "status": call.status, "payload": payload})
    if plugin_id != "steam-mcp":
        try:
            await route.call("acknowledge_events", {"event_ids": []})
        except PermissionError as error:
            probes.append({"tool": "acknowledge_events", "status": "denied", "error": str(error)})
        else:
            raise RuntimeError(f"{plugin_id} candidate 暴露了写入 ack 路径")

    process_endpoints: list[dict[str, object]] = []
    if runtime.processes is not None:
        for name, endpoint in runtime.processes.endpoints.items():
            status = _readiness(endpoint.readiness_url)
            if status != 200:
                raise RuntimeError(f"{plugin_id}:{name} readiness 非 200: {status}")
            formal_port = FORMAL_PORTS.get(plugin_id)
            if formal_port is not None and endpoint.port == formal_port:
                raise RuntimeError(f"{plugin_id}:{name} candidate 占用 formal port {formal_port}")
            process_endpoints.append(
                {
                    "name": name,
                    "port": endpoint.port,
                    "readiness_url": endpoint.readiness_url,
                    "status": status,
                    "epoch": endpoint.epoch,
                }
            )
    after_probe = _sha256_tree(formal_data)
    if before != after_probe:
        raise RuntimeError(f"{plugin_id} candidate recording 改写 formal plugin-data")

    cleanup = await manager.drop_candidate(installed_id)
    pointers = read_pointers(bases[plugin_id])
    if pointers is None or pointers.stable != pointers.latest:
        raise RuntimeError(f"{plugin_id} discard 后 stable/latest pointer 未收敛")
    retained_runtime = manager.composition_generation_host.get(generation_id)
    retained_failure = manager.composition_generation_host.failure(generation_id)
    if retained_runtime is not None or retained_failure is not None:
        raise RuntimeError(f"{plugin_id} discard 后仍保留 runtime owner")
    if candidate_workspace.parent.exists():
        raise RuntimeError(f"{plugin_id} candidate workspace 未清理: {candidate_workspace.parent}")
    return RuntimeEvidence(
        id=plugin_id,
        plugin_id=installed_id,
        generation_id=generation_id,
        mode=runtime.mode,
        state=runtime.mcp.state,
        mcp_tools=tuple(server.tool_names),
        probes=tuple(probes),
        process_endpoints=tuple(process_endpoints),
        candidate_workspace=str(candidate_workspace),
        formal_data_before=before,
        formal_data_after=after_probe,
        cleanup=cast(dict[str, object], cleanup),
    )


def _sha256_tree(path: Path) -> str:
    """Hash one disposable data tree without inventing missing state."""

    digest = hashlib.sha256()
    if not path.exists():
        return digest.hexdigest()
    if not path.is_dir():
        raise RuntimeError(f"plugin-data 不是目录: {path}")
    for item in sorted(path.rglob("*")):
        relative = item.relative_to(path).as_posix()
        digest.update(relative.encode())
        if item.is_file():
            digest.update(item.read_bytes())
    return digest.hexdigest()


async def _run_in_process_failure(
    manager: PluginManager,
    bases: Mapping[str, Path],
) -> dict[str, object]:
    """Inject one invariant failure and verify pointer/runtime rollback in-process."""

    plugin_id = "steam-mcp"
    installed_id = f"{INSTALLED_NAMES[plugin_id]}@github"
    prepared = await manager.prepare_candidate(installed_id)
    if prepared is None:
        raise GateBlocked("in-process failure probe 无法准备 Steam candidate")
    generation_id = prepared.generation_id
    manager_any = cast(Any, manager)
    original = manager_any._post_publish_invariants

    async def fail_invariant(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("e2 forced in-process invariant failure")

    manager_any._post_publish_invariants = fail_invariant
    observed: str | None = None
    try:
        try:
            await manager.publish_prepared(installed_id)
        except RuntimeError as error:
            observed = str(error)
        else:
            raise RuntimeError("in-process failure injection 未暴露异常")
    finally:
        manager_any._post_publish_invariants = original
    if observed is None or (
        "post-publish" not in observed and "invariant" not in observed
    ):
        raise RuntimeError(f"in-process failure 语义不明确: {observed}")
    if manager.prepared_generation(installed_id) is not None:
        raise RuntimeError("in-process failure 后 prepared generation 未清理")
    if manager.composition_generation_host.get(generation_id) is not None:
        raise RuntimeError("in-process failure 后 candidate runtime 未清理")
    pointers = read_pointers(bases[plugin_id])
    if pointers is None or pointers.stable != pointers.latest:
        raise RuntimeError("in-process failure 后 artifact pointer 未恢复 stable")
    return {
        "id": "in-process-failure",
        "status": "passed",
        "plugin_id": installed_id,
        "generation_id": generation_id,
        "error": observed,
        "pointer": {"stable": pointers.stable.path, "latest": pointers.latest.path},
    }


def _boot_process_ids(boot_id: str) -> tuple[int, ...]:
    """Return Linux process identities carrying one exact Core boot token."""

    expected = f"AKASHIC_BOOT_ID={boot_id}".encode()
    process_ids: list[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            environ = (entry / "environ").read_bytes().split(b"\0")
        except OSError:
            continue
        if expected in environ:
            process_ids.append(int(entry.name))
    return tuple(sorted(process_ids))


async def _run_core_process_crash(
    checkouts: Mapping[str, Path],
    sandbox: Path,
    runtime_python: Path,
) -> dict[str, object]:
    """SIGKILL a child Core after candidate start and verify durable recovery."""

    # 1. Use a separate disposable manager/cache so the active Gate manager is untouched.
    crash_root = sandbox / "core-crash"
    providers = crash_root / "providers"
    cache_root = crash_root / "plugin-home" / "cache"
    workspace = crash_root / "workspace"
    evidence_path = crash_root / "child-evidence.json"
    providers.mkdir(parents=True)
    crash_root.mkdir(exist_ok=True)
    for plugin_id in SHELL_PLUGIN_IDS:
        _copy_source_to_artifact(checkouts[plugin_id], providers / plugin_id)
    crash_root.mkdir(parents=True, exist_ok=True)
    old_boot = f"e2-old-{os.getpid()}-{os.urandom(4).hex()}"
    new_boot = f"e2-new-{os.getpid()}-{os.urandom(4).hex()}"
    stage = sandbox / "runtime-python"
    steam_source = checkouts["steam-mcp"]
    child_code = textwrap.dedent(
        """
        import asyncio
        import json
        import shutil
        import sys
        from pathlib import Path
        from agent.plugins.artifacts import ArtifactPointer, write_pointers
        from agent.plugins.manager import PluginManager
        from agent.plugins.manifest import write_plugin_manifest
        from agent.plugins.static_manifest import load_static_plugin_manifest
        from bus.event_bus import EventBus

        async def main() -> None:
            source = Path(sys.argv[1])
            providers = Path(sys.argv[2])
            cache = Path(sys.argv[3])
            workspace = Path(sys.argv[4])
            stage = Path(sys.argv[5])
            evidence = Path(sys.argv[6])
            manager = PluginManager([providers], event_bus=EventBus(), workspace=workspace, installed_cache_root=cache)
            await manager.load_all()
            data_root = workspace / "plugin-data" / "steam-github"
            data_root.mkdir(parents=True, exist_ok=True)
            (data_root / "steam_mcp_config.json").write_text(json.dumps({"steam_api_key": "test-only", "steam_id": "76561198000000000", "snapshot_interval_seconds": 3600}), encoding="utf-8")
            base = cache / "github" / "steam"
            stable = base / ".artifacts" / "stable"
            latest = base / ".artifacts" / "latest"
            shutil.copytree(source, stable, ignore=shutil.ignore_patterns(".git", ".venv", "__pycache__"))
            shutil.copytree(source, latest, ignore=shutil.ignore_patterns(".git", ".venv", "__pycache__"))
            (stable / ".e2-stable-baseline").write_text("synthetic stable baseline", encoding="utf-8")
            manifest = load_static_plugin_manifest(source)
            for artifact in (stable, latest):
                for runtime in manifest.python:
                    root = artifact / runtime.runtime_root
                    root.mkdir(parents=True, exist_ok=True)
                    (root / ".venv").symlink_to(stage, target_is_directory=True)
            write_pointers(base, stable=ArtifactPointer(".artifacts/stable"), latest=ArtifactPointer(".artifacts/latest"))
            write_plugin_manifest({"steam@github": True}, plugins_home=cache.parent)
            candidate = await manager.prepare_candidate("steam@github")
            if candidate is None:
                raise RuntimeError("child Core 未准备 Steam candidate")
            publication = await manager.publish_prepared("steam@github")
            if publication.get("publication_state") != "latest_ready":
                raise RuntimeError(f"child Core candidate 未 latest_ready: {publication}")
            evidence.write_text(json.dumps({"generation_id": candidate.generation_id, "tx_id": candidate.reload_tx_id}), encoding="utf-8")
            await asyncio.sleep(60)

        asyncio.run(main())
        """
    )
    child_env = dict(os.environ)
    child_env["AKASHIC_BOOT_ID"] = old_boot
    child_env["AKASHIC_SUPERVISED"] = "1"
    child_env["PYTHONPATH"] = str(ROOT) + os.pathsep + child_env.get("PYTHONPATH", "")
    child_log = crash_root / "child.log"
    process = subprocess.Popen(
        (str(runtime_python), "-c", child_code, str(steam_source), str(providers), str(cache_root), str(workspace), str(stage), str(evidence_path)),
        cwd=ROOT,
        env=child_env,
        stdout=child_log.open("w", encoding="utf-8"),
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired as error:
        process.kill()
        _ = process.wait(timeout=5)
        raise RuntimeError("Core crash child 未进入 SIGKILL probe 终点") from error
    if process.returncode != -9:
        log = child_log.read_text(encoding="utf-8", errors="replace")
        raise RuntimeError(f"Core crash child 非预期退出 {process.returncode}: {log[-2000:]}")
    if not evidence_path.is_file():
        raise RuntimeError("Core crash child 未写入 candidate transaction evidence")
    child_evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    stale_before = _boot_process_ids(old_boot)

    # 2. A fresh supervised Core must normalize the exact pointer and journal.
    previous_boot = os.environ.get("AKASHIC_BOOT_ID")
    previous_supervised = os.environ.get("AKASHIC_SUPERVISED")
    manager: PluginManager | None = None
    recovery_error: str | None = None
    pending: tuple[object, ...] = ()
    pointers = None
    try:
        os.environ["AKASHIC_BOOT_ID"] = new_boot
        os.environ["AKASHIC_SUPERVISED"] = "1"
        manager = PluginManager([providers], event_bus=EventBus(), workspace=workspace, installed_cache_root=cache_root)
        await manager.load_all()
        pending = manager.reload_journal.pending_recovery()
        pointers = read_pointers(cache_root / "github" / "steam")
    except (OSError, RuntimeError, ValueError) as error:
        recovery_error = str(error) or type(error).__name__
    finally:
        if manager is not None:
            await manager.terminate_all()
        if previous_boot is None:
            os.environ.pop("AKASHIC_BOOT_ID", None)
        else:
            os.environ["AKASHIC_BOOT_ID"] = previous_boot
        if previous_supervised is None:
            os.environ.pop("AKASHIC_SUPERVISED", None)
        else:
            os.environ["AKASHIC_SUPERVISED"] = previous_supervised

    # 3. Never leak the killed Core's child runtime; retain a blocked receipt if cleanup was manual.
    stale_after_manager = _boot_process_ids(old_boot)
    manual_cleanup = False
    if stale_after_manager:
        from agent.background.boot_guardian import _cleanup_boot_processes

        await asyncio.to_thread(_cleanup_boot_processes, boot_id=old_boot, gateway_group_id=None)
        manual_cleanup = True
    stale_after_cleanup = _boot_process_ids(old_boot)
    pointer_ok = pointers is not None and pointers.stable == pointers.latest
    recovery_ok = recovery_error is None and not pending and pointer_ok and not stale_after_manager
    status = "passed" if recovery_ok else "blocked"
    return {
        "id": "core-process-crash",
        "status": status,
        "old_boot_id": old_boot,
        "new_boot_id": new_boot,
        "child": child_evidence,
        "stale_processes_before_restart": list(stale_before),
        "stale_processes_after_manager": list(stale_after_manager),
        "stale_processes_after_cleanup": list(stale_after_cleanup),
        "manual_cleanup": manual_cleanup,
        "pending_recovery_count": len(pending),
        "pointer_normalized": pointer_ok,
        "recovery_error": recovery_error,
        "reason": (
            "Core startup did not clean the old boot owner automatically"
            if stale_after_manager
            else recovery_error
        ),
    }


async def _run_gate(
    checkouts: Mapping[str, Path],
    sandbox: Path,
    runtime_python: Path,
) -> dict[str, object]:
    """Run Shell, MCP candidate, and failure-cleanup checks in one Manager."""

    # 1. Load only the three locked Shell providers as formal stable plugins.
    workspace = sandbox / "workspace"
    cache_root = sandbox / "plugin-home" / "cache"
    manager = PluginManager(
        plugin_dirs=[sandbox / "providers"],
        event_bus=EventBus(),
        workspace=workspace,
        installed_cache_root=cache_root,
    )
    runtime_stage = _create_runtime_stage(runtime_python, sandbox)
    bases: dict[str, Path] = {}
    root = None
    shell_result: tuple[
        tuple[str, ...], tuple[ScenarioEvidence, ...], list[dict[str, object]]
    ] | None = None
    runtime_evidence: list[RuntimeEvidence] = []
    in_process_failure: dict[str, object] | None = None
    core_crash: dict[str, object] | None = None
    try:
        await manager.load_all()
        shell_result = await _run_shell_scenarios(manager, sandbox)
        bases = _prepare_external_candidates(checkouts, cache_root, runtime_stage)
        for plugin_id in MCP_PLUGIN_IDS:
            runtime_evidence.append(await _probe_candidate(manager, plugin_id, bases))
        in_process_failure = await _run_in_process_failure(manager, bases)
        core_crash = await _run_core_process_crash(checkouts, sandbox, runtime_python)
        root = (
            manager.current_snapshot.composition_root
            if manager.current_snapshot is not None
            else None
        )
    finally:
        await manager.terminate_all()

    if shell_result is None or root is None:
        raise RuntimeError("E2 Gate 成功路径未保留稳定 Root 证据")
    retained_failures: list[str] = []
    for evidence in runtime_evidence:
        failure = manager.composition_generation_host.failure(evidence.generation_id)
        if failure is not None:
            retained_failures.append(f"{failure.generation_id}:{failure.error}")
    cleanup = CleanupEvidence(
        shell_generation_ids=tuple(
            generation.generation_id
            for generation in (
                manager.generation(plugin_id) for plugin_id in SHELL_PLUGIN_IDS
            )
            if generation is not None
        ),
        retained_runtime_failures=tuple(retained_failures),
        cleanup_failures=tuple(str(item) for item in manager.cleanup_failures),
        listeners=root.topology_view().listeners,
        effects=root.receipt().effects,
    )
    if cleanup.retained_runtime_failures or cleanup.cleanup_failures or cleanup.effects:
        raise RuntimeError(f"E2 cleanup evidence 未清零: {cleanup}")
    return {
        "shell": {
            "listeners": list(shell_result[0]),
            "scenarios": [asdict(item) for item in shell_result[1]],
            "invocations": shell_result[2],
        },
        "runtime": [asdict(item) for item in runtime_evidence],
        "in_process_failure": in_process_failure,
        "core_process_crash": core_crash,
        "cleanup": asdict(cleanup),
    }


def _write_report(path: Path, report: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="运行集中 v3 插件 E2 Gate")
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--runtime-python", type=Path, default=Path(sys.executable))
    parser.add_argument("--require-clean-core", action="store_true")
    return parser.parse_args()


def main() -> int:
    """Run the Gate and return zero only after every required oracle passes."""

    args = _parse_args()
    report_path = args.report.resolve()
    checked_at = datetime.now(UTC).isoformat()
    core_status = _git_output(ROOT, "status", "--porcelain").splitlines()
    base_report: dict[str, object] = {
        "status": "failed",
        "gate_version": GATE_VERSION,
        "checked_at": checked_at,
        "core": {
            "head": _git_output(ROOT, "rev-parse", "HEAD"),
            "tree": _git_output(ROOT, "rev-parse", "HEAD^{tree}"),
            "dirty_status": core_status,
        },
        "lock": str(args.lock.resolve()),
        "lock_sha256": None,
        "scenario_profile": SCENARIO_PROFILE,
        "scenario_catalog_sha256": _scenario_catalog_sha256(),
        "scenario_catalog": [asdict(item) for item in SCENARIO_CATALOG],
        "plugins": [],
        "cases": [],
        "blockers": [],
        "failures": [],
    }
    try:
        if args.require_clean_core and core_status:
            raise GateBlocked(f"核心工作树不干净: {core_status}")
        lock_path = args.lock.resolve()
        base_report["lock_sha256"] = _sha256(lock_path)
        locks = _load_lock(lock_path)
        base_report["lock_plugins"] = [asdict(item) for item in locks]
        runtime_python = _runtime_interpreter(args.runtime_python)
        _check_imports(runtime_python, MCP_PLUGIN_IDS)
        with tempfile.TemporaryDirectory(prefix="akashic-plugin-v3-e2-") as raw:
            sandbox = Path(raw)
            providers = sandbox / "providers"
            providers.mkdir()
            checkouts: dict[str, Path] = {}
            evidences: list[PluginEvidence] = []
            for lock in locks:
                checkout = (
                    providers / lock.id
                    if lock.id in SHELL_PLUGIN_IDS
                    else sandbox / "sources" / lock.id
                )
                checkouts[lock.id] = checkout
                evidences.append(_checkout_locked_plugin(lock, checkout))
            base_report["plugins"] = [asdict(item) for item in evidences]
            base_report["runtime_python"] = str(runtime_python)
            gate_result = asyncio.run(_run_gate(checkouts, sandbox, runtime_python))
        base_report.update(gate_result)
        core_case = cast(dict[str, object], gate_result.get("core_process_crash", {}))
        if core_case.get("status") == "blocked":
            base_report["status"] = "blocked"
            base_report["blockers"] = [str(core_case.get("reason") or "Core process crash recovery blocked")]
            print(f"plugin v3 concentrated E2 gate blocked: {report_path}", file=sys.stderr)
            status = 2
        elif core_case.get("status") == "failed":
            base_report["status"] = "failed"
            base_report["failures"] = [str(core_case.get("reason") or "Core process crash recovery failed")]
            print(f"plugin v3 concentrated E2 gate failed: {report_path}", file=sys.stderr)
            status = 1
        else:
            base_report["status"] = "passed"
            print(f"plugin v3 concentrated E2 gate passed: {report_path}")
            status = 0
    except GateBlocked as error:
        message = str(error) or type(error).__name__
        base_report["blockers"] = [message]
        base_report["status"] = "blocked"
        print(f"plugin v3 concentrated E2 gate blocked: {message}", file=sys.stderr)
        status = 2
    except (
        OSError,
        RuntimeError,
        ValueError,
        json.JSONDecodeError,
        subprocess.CalledProcessError,
    ) as error:
        message = str(error) or type(error).__name__
        base_report["failures"] = [message]
        print(f"plugin v3 concentrated E2 gate failed: {message}", file=sys.stderr)
        status = 1
    finally:
        _write_report(report_path, base_report)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
