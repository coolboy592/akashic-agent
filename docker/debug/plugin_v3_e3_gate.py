from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from contextlib import ExitStack
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any, cast
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agent.plugins import channel_generation_host  # noqa: E402
from agent.plugins.dashboard_host import PluginDashboardHost  # noqa: E402
from agent.plugins.generation_activity_host import ActivityHost  # noqa: E402
from agent.plugins.generation_job_host import (  # noqa: E402
    BackgroundJobActivityAdapter,
)
from agent.plugins.generation_private_proactive_host import PrivateProactiveHost  # noqa: E402
from agent.plugins.generation_proactive_host import ProactiveActivityAdapter  # noqa: E402
from agent.plugins.manager import PluginManager  # noqa: E402
from agent.plugin_composition.channels import (  # noqa: E402
    ChannelDeliveryReceipt,
    ChannelInboundMessage,
    DeliveryStatus,
    InboundOwner,
    JsonValue,
    OutboundEnvelope,
    RawInbound,
)
from agent.plugin_composition.proactive import (  # noqa: E402
    FetchEmpty,
    FetchFailure,
)
from agent.tools.message_push import MessagePushTool  # noqa: E402
from agent.tools.registry import ToolRegistry  # noqa: E402
from bus.event_bus import EventBus  # noqa: E402
from docker.debug import plugin_passive_composition_v3_gate as passive_gate  # noqa: E402
from docker.debug import plugin_v3_e2_gate as e2_gate  # noqa: E402
from docker.debug import plugin_v3_fleet_gate as fleet_gate  # noqa: E402
from docker.debug.proactive_sandbox import SandboxProvider  # noqa: E402
from proactive_v2.config import ProactiveConfig  # noqa: E402
from proactive_v2.loop import ProactiveLoop  # noqa: E402
from proactive_v2.state import ProactiveStateStore  # noqa: E402
from agent.plugins.generation_proactive_bridge import (  # noqa: E402
    CommittedProactiveBridge,
)
from bus.events import (  # noqa: E402
    ChannelMessage as BusChannelMessage,
)
from session.manager import SessionManager  # noqa: E402


DEFAULT_REPORT = ROOT / "docker/debug/reports/plugin-v3-e3" / "gate.json"
GATE_VERSION = 1
SCENARIO_PROFILE = "plugin-v3-e3-fleet-channel-proactive-v3"
CHANNEL_PLUGIN_IDS = ("feishu", "qqbot")
PASSIVE_PLUGIN_IDS = ("citation", "meme")
FLEET_EXTERNAL_PLUGIN_IDS = (
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
FLEET_LOCK_PLUGIN_IDS = FLEET_EXTERNAL_PLUGIN_IDS + CHANNEL_PLUGIN_IDS
PRIVATE_PROACTIVE_PLUGIN_IDS = (
    "default_proactive",
    "proactive_flow",
    "drift_flow",
    "wake_proactive",
    "wake_proactive_flow",
    "wake_drift_flow",
)
E3_EXACT_PLUGIN_IDS = FLEET_LOCK_PLUGIN_IDS + PRIVATE_PROACTIVE_PLUGIN_IDS
FLEET_MODULE_IDS = {
    "setup_helper": "setup_helper",
    "status_commands": "status_commands",
    "daynight_gate": "daynight_gate",
    "emotion": "emotion",
    "calendar-mcp": "calendar",
    "feed-mcp": "feed",
    "fitbit-mcp": "fitbit",
    "steam-mcp": "steam",
    "huayue-skills": "huayue-skills",
    "github_watch": "github-watch",
    "feishu": "feishu",
    "qqbot": "qqbot",
}
PRIVATE_PROACTIVE_FAMILIES = {
    "default": ("default_proactive", "proactive_flow", "drift_flow"),
    "wake": ("wake_proactive", "wake_proactive_flow", "wake_drift_flow"),
}
PROACTIVE_ORACLE_KINDS = (
    "empty",
    "skip",
    "source",
    "model",
    "delivery",
    "restart",
)
GATE_SCOPE = {
    "exact_plugins": E3_EXACT_PLUGIN_IDS,
    "lock_plugins": FLEET_LOCK_PLUGIN_IDS,
    "private_proactive_entries": PRIVATE_PROACTIVE_PLUGIN_IDS,
    "passive_plugins": PASSIVE_PLUGIN_IDS,
    "provider": "synthetic recording HTTP/WS adapter; no external delivery",
    "proactive_provider": "fixed-clock deterministic model and recording sink",
    "github_watch": "dedicated controlled remote or strict fail-loud blocked",
    "passive_webui_surface": "PluginDashboardHost programmatic projection",
    "public_webui_docker_e2e": "separate plugin_v3_webui_e2e Gate",
}
SCENARIO_CATALOG = (
    {
        "id": "channel",
        "title": "Feishu/QQ recording channel",
        "oracle": (
            "exact snapshot/generation binding",
            "candidate discard and promotion",
            "loopback inbound lease and duplicate suppression",
            "recording provider delivery",
        ),
    },
    {
        "id": "message_push",
        "title": "MessagePush awaited v3 delivery",
        "oracle": (
            "stable delivery identity",
            "UNKNOWN after provider effect",
            "no blind retry",
            "retryable=false tool result",
        ),
    },
    {
        "id": "passive_webui",
        "title": "Citation/Meme passive Dashboard chain",
        "oracle": (
            "prompt protocol and media projection",
            "Dashboard reads disposable workspace",
            "workspace asset tree unchanged",
            "Root cleanup leaves zero composition resources",
        ),
    },
    {
        "id": "full_boot_catalog",
        "title": "Fleet full boot and exact catalog",
        "oracle": (
            "all locked fleet sources are exact",
            "commands/status/channel/MCP/proactive catalogs are committed",
            "Default and Wake private thin entries are both present",
        ),
    },
    {
        "id": "candidate_lifecycle",
        "title": "Fleet candidate discard/promote/reload",
        "oracle": (
            "candidate preparation has no provider/source/model/delivery effect",
            "discard leaves no validation residual",
            "promote/reload publishes a new snapshot identity",
        ),
    },
    {
        "id": "proactive_fixed_clock",
        "title": "Deterministic proactive empty/skip/source/model/delivery",
        "oracle": PROACTIVE_ORACLE_KINDS[:-1],
    },
    {
        "id": "process_restart",
        "title": "In-process failure and SIGKILL process restart",
        "oracle": ("in-process failure", "SIGKILL", "durable restart cleanup"),
    },
    {
        "id": "private_default_wake",
        "title": "Default/Wake private proactive thin entrypoints",
        "oracle": ("default family", "wake family", "no private external sender"),
    },
    {
        "id": "github_watch_controlled_remote",
        "title": "GitHub Watch controlled remote policy",
        "oracle": (
            "dedicated controlled remote probe",
            "or strict fail-loud blocked",
            "no formal credentials or send",
        ),
    },
)


class GateFailure(RuntimeError):
    """Represent an E3 evidence or invariant failure."""


class GateBlocked(GateFailure):
    """Represent an explicitly unavailable external or runtime prerequisite."""


class _NamedPrivateProactiveHost(PrivateProactiveHost):
    """Give the Default and Wake private children distinct ActivityHost names."""

    def __init__(self, family: str, name: str) -> None:
        super().__init__(family)
        self.name = name


class _RecordingTurnHandle:
    """Complete one synthetic Turn immediately while retaining its identity."""

    def __init__(self, turn_id: str) -> None:
        self.id = turn_id

    async def result(self) -> None:
        return None


class _RecordingConversationRuntime:
    """Record formal programmatic Turn admission without running a model."""

    def __init__(self) -> None:
        self.requests: list[dict[str, object]] = []

    async def start_turn(
        self,
        request: object,
        *,
        runtime_snapshot_lease: object,
    ) -> _RecordingTurnHandle:
        turn_id = f"e3-recording-turn-{len(self.requests) + 1}"
        self.requests.append(
            {
                "turn_id": turn_id,
                "thread_id": str(getattr(request, "thread_id", "")),
                "input": str(getattr(request, "input", "")),
                "snapshot_id": str(
                    getattr(getattr(runtime_snapshot_lease, "snapshot", None), "snapshot_id", "")
                ),
            }
        )
        return _RecordingTurnHandle(turn_id)


def _validate_fleet_lock(lock_items: Iterable[object]) -> dict[str, object]:
    """Validate the exact E3 lock projection without checking out or importing code."""

    # 1. Read only the already parsed lock identities and reject duplicates.
    ids = tuple(str(getattr(item, "id", "")) for item in lock_items)
    if len(ids) != len(set(ids)):
        raise GateFailure(f"E3 fleet lock 含重复 plugin id: {ids}")
    expected: set[str] = set(FLEET_LOCK_PLUGIN_IDS)
    known: set[str] = set(fleet_gate.EXPECTED_PLUGIN_IDS)
    actual = set(ids)
    missing = tuple(sorted(expected - actual))
    extra = tuple(sorted(actual - known))
    ignored = tuple(sorted(actual - expected))
    if missing or extra:
        raise GateFailure(
            f"E3 fleet lock 集合错误: missing={missing}, extra={extra}"
        )
    return {
        "status": "complete",
        "plugin_ids": list(FLEET_LOCK_PLUGIN_IDS),
        "missing": [],
        "extra": list(ignored),
    }


def _fleet_coverage_contract(
    *,
    active_external_ids: Iterable[str],
    active_private_ids: Iterable[str],
) -> dict[str, object]:
    """Return the exact full-fleet coverage contract for a committed snapshot."""

    # 1. Compare stable external and private identities independently.
    external = set(active_external_ids)
    private = set(active_private_ids)
    expected_external: set[str] = set(FLEET_LOCK_PLUGIN_IDS)
    expected_private: set[str] = set(PRIVATE_PROACTIVE_PLUGIN_IDS)
    missing_external = tuple(sorted(expected_external - external))
    missing_private = tuple(sorted(expected_private - private))
    unexpected_external = tuple(sorted(external - expected_external))
    unexpected_private = tuple(sorted(private - expected_private))
    if missing_external or missing_private:
        raise GateFailure(
            "E3 full-fleet coverage incomplete: "
            f"missing_external={missing_external}, missing_private={missing_private}"
        )
    if unexpected_external or unexpected_private:
        raise GateFailure(
            "E3 full-fleet coverage contains unexpected ids: "
            f"external={unexpected_external}, private={unexpected_private}"
        )
    return {
        "status": "complete",
        "external": list(FLEET_LOCK_PLUGIN_IDS),
        "private": list(PRIVATE_PROACTIVE_PLUGIN_IDS),
        "families": {
            family: list(plugin_ids)
            for family, plugin_ids in PRIVATE_PROACTIVE_FAMILIES.items()
        },
    }


def _proactive_coverage_oracle(observed_kinds: Iterable[str]) -> dict[str, object]:
    """Require every deterministic proactive outcome and restart observation."""

    # 1. Preserve the declared order while making duplicate observations visible.
    observed = tuple(str(kind) for kind in observed_kinds)
    observed_set = set(observed)
    missing = tuple(kind for kind in PROACTIVE_ORACLE_KINDS if kind not in observed_set)
    if missing:
        raise GateFailure(f"proactive outcome coverage incomplete: missing={missing}")
    return {
        "status": "complete",
        "required": list(PROACTIVE_ORACLE_KINDS),
        "observed": list(observed),
        "duplicate_observations": len(observed) - len(observed_set),
    }


def _github_watch_policy(controlled_remote: str | None) -> dict[str, object]:
    """Describe a controlled GitHub Watch probe or an explicit fail-loud block."""

    # 1. A Gate never invents a remote or turns the formal App credentials on.
    remote = "" if controlled_remote is None else controlled_remote.strip()
    if not remote:
        return {
            "status": "blocked",
            "reason": "no dedicated controlled GitHub remote configured",
            "credentials_read": False,
            "network_effects": 0,
            "external_sends": 0,
        }
    if not remote.startswith(("file://", "ssh://", "https://")):
        raise GateFailure(f"GitHub controlled remote scheme 不受支持: {remote!r}")
    return {
        "status": "controlled_remote",
        "remote": remote,
        "credentials_read": False,
        "network_effects": 0,
        "external_sends": 0,
    }


@dataclass(frozen=True, slots=True)
class RecordingEffect:
    channel: str
    recipient: str
    delivery_id: str
    kind: str
    status: str


class _RecordingClient:
    def __init__(self, credentials: Mapping[str, str]) -> None:
        self._credentials = dict(credentials)
        self.close_calls = 0

    def credential(self, ref: object) -> str:
        path = getattr(ref, "path", ())
        if not isinstance(path, tuple) or not path:
            raise KeyError(path)
        key = path[0]
        if key not in self._credentials:
            raise KeyError(path)
        return self._credentials[key]

    async def aclose(self) -> None:
        self.close_calls += 1


class _RecordingFactory:
    def __init__(self, channel: str, credentials: Mapping[str, str]) -> None:
        self.channel = channel
        self.client = _RecordingClient(credentials)
        self.create_calls = 0
        self.close_calls = 0
        self.fail_after_effect = False
        self.effects: list[RecordingEffect] = []

    async def create(self, credentials: object) -> _RecordingClient:
        del credentials
        self.create_calls += 1
        return self.client

    async def aclose(self) -> None:
        self.close_calls += 1

    def record(
        self,
        *,
        recipient: str,
        delivery_id: str,
        kind: str,
    ) -> DeliveryStatus:
        status = (
            DeliveryStatus.UNKNOWN
            if self.fail_after_effect
            else DeliveryStatus.DELIVERED
        )
        self.effects.append(
            RecordingEffect(
                channel=self.channel,
                recipient=recipient,
                delivery_id=delivery_id,
                kind=kind,
                status=status.value,
            )
        )
        return status


class _FailingProvider:
    """Raise one deterministic model failure at the provider boundary."""

    def __init__(self) -> None:
        self.calls = 0

    async def chat(self, **_kwargs: object) -> object:
        self.calls += 1
        raise RuntimeError("e3 deterministic model failure")


class _FailingMcpRoute:
    """Raise one deterministic source failure behind a committed MCP route."""

    async def call(self, *_args: object, **_kwargs: object) -> object:
        raise RuntimeError("e3 deterministic source failure")


_FIXED_CLOCK = datetime(2026, 8, 18, 7, 0, tzinfo=UTC)


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz: object = None) -> datetime:
        if tz is None:
            return _FIXED_CLOCK.replace(tzinfo=None)
        return _FIXED_CLOCK.astimezone(cast(Any, tz))


def main() -> int:
    """Run one disposable E3 over exact channel and passive plugin sources."""

    args = _parse_args()
    report_path = args.report.resolve()
    core_status = _git_output("status", "--porcelain").splitlines()
    report: dict[str, object] = {
        "status": "failed",
        "gate_version": GATE_VERSION,
        "checked_at": datetime.now(UTC).isoformat(),
        "scenario_profile": SCENARIO_PROFILE,
        "scope": GATE_SCOPE,
        "scenario_catalog_sha256": _scenario_catalog_sha256(),
        "scenario_catalog": list(SCENARIO_CATALOG),
        "core": {
            "head": _git_output("rev-parse", "HEAD"),
            "tree": _git_output("rev-parse", "HEAD^{tree}"),
            "dirty_status": core_status,
        },
        "lock": str(fleet_gate.DEFAULT_LOCK.relative_to(ROOT)),
        "lock_sha256": _sha256(fleet_gate.DEFAULT_LOCK),
        "runtime": {},
        "cleanup": {"sandbox_removed": False, "residuals": ["not_started"]},
    }
    error_text = ""
    sandbox_path: Path | None = None
    try:
        if args.require_clean_core and core_status:
            raise GateFailure(f"核心工作树不干净: {core_status}")
        with tempfile.TemporaryDirectory(
            prefix="akashic-plugin-v3-e3-",
            dir="/home/huashen/.cache/akashic-gate-tmp",
        ) as raw:
            sandbox_path = Path(raw)
            report["runtime"] = asyncio.run(_run_runtime(sandbox_path))
            _require_runtime_evidence(cast(dict[str, object], report["runtime"]))
        report["cleanup"] = {
            "sandbox_removed": sandbox_path is not None and not sandbox_path.exists(),
            "residuals": [],
        }
        cleanup = cast(dict[str, object], report["cleanup"])
        if cleanup["sandbox_removed"] is not True:
            raise GateFailure("E3 sandbox cleanup 未完成")
        report["status"] = "passed"
    except GateBlocked as error:
        report["status"] = "blocked"
        error_text = f"{type(error).__name__}: {error}"
        report["error"] = error_text
    except BaseException as error:
        error_text = f"{type(error).__name__}: {error}"
        report["error"] = error_text
    finally:
        if sandbox_path is not None and sandbox_path.exists():
            shutil.rmtree(sandbox_path, ignore_errors=True)
        cleanup = cast(dict[str, object], report["cleanup"])
        cleanup["sandbox_removed"] = sandbox_path is None or not sandbox_path.exists()
        if cleanup["sandbox_removed"] and cleanup.get("residuals") == ["not_started"]:
            cleanup["residuals"] = []
        _write_json(report_path, report)

    if report["status"] != "passed":
        print(
            f"plugin v3 E3 gate {report['status']}: {error_text}",
            file=sys.stderr,
        )
        print(f"evidence: {report_path}", file=sys.stderr)
        return 1
    print(f"plugin v3 E3 gate passed: {report_path}")
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="验证 pure-v3 Channel/MessagePush/Passive WebUI 集中 E3"
    )
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--require-clean-core", action="store_true")
    return parser.parse_args()


def _require_runtime_evidence(runtime: Mapping[str, object]) -> None:
    """Reject a report whose new E3 scenarios were declared but not executed."""

    # 1. Require every new scenario result before allowing the Gate to pass.
    required = (
        "lock",
        "full_boot_catalog",
        "candidate_lifecycle",
        "fleet_coverage",
        "proactive",
        "github_watch",
    )
    missing = tuple(key for key in required if key not in runtime)
    if missing:
        raise GateBlocked(f"E3 runtime evidence 缺少 required scenarios: {missing}")
    for key in ("full_boot_catalog", "candidate_lifecycle", "fleet_coverage"):
        evidence = runtime[key]
        if not isinstance(evidence, Mapping) or evidence.get("status") != "complete":
            raise GateBlocked(f"E3 scenario {key} 未完成: {evidence!r}")
    proactive = runtime["proactive"]
    if not isinstance(proactive, Mapping) or proactive.get("status") != "complete":
        raise GateBlocked(f"E3 proactive scenario 未执行: {proactive!r}")
    github_watch = runtime["github_watch"]
    if (
        not isinstance(github_watch, Mapping)
        or github_watch.get("status") != "controlled_remote"
    ):
        raise GateBlocked(
            "E3 GitHub Watch controlled remote scenario 未完成: "
            f"{github_watch!r}"
        )


async def _run_runtime(sandbox: Path) -> dict[str, object]:
    """Checkout exact sources and execute every E3 coverage contract in one sandbox."""

    # 1. Freeze and checkout the complete external lock projection.
    lock_items = fleet_gate._load_lock(fleet_gate.DEFAULT_LOCK)
    lock_contract = _validate_fleet_lock(lock_items)
    locked = {item.id: item for item in lock_items}
    providers = sandbox / "providers"
    fleet_providers = providers / "fleet"
    channel_providers = providers / "channels"
    passive_providers = providers / "passive"
    fleet_providers.mkdir(parents=True)
    channel_providers.mkdir(parents=True)
    passive_providers.mkdir(parents=True)
    source_evidence: list[dict[str, object]] = []
    for plugin_id in FLEET_LOCK_PLUGIN_IDS:
        checkout = fleet_providers / plugin_id
        evidence = fleet_gate._checkout_locked_plugin(locked[plugin_id], checkout)
        source_evidence.append(asdict(evidence))
    for plugin_id in CHANNEL_PLUGIN_IDS:
        _copy_source_tree(fleet_providers / plugin_id, channel_providers / plugin_id)
    passive_locks = {
        item.id: item
        for item in lock_items
        if item.id in PASSIVE_PLUGIN_IDS
    }
    if set(passive_locks) != set(PASSIVE_PLUGIN_IDS):
        raise GateBlocked("E3 passive lock 缺少 Citation/Meme")
    for plugin_id in PASSIVE_PLUGIN_IDS:
        checkout = passive_providers / plugin_id
        evidence = fleet_gate._checkout_locked_plugin(passive_locks[plugin_id], checkout)
        source_evidence.append(asdict(evidence))
    _prepare_channel_checkouts(fleet_providers)
    _prepare_channel_checkouts(channel_providers)

    # 2. Stage one synthetic Python wrapper so formal MCP processes stay recording-only.
    runtime_stage = _stage_fleet_runtime(sandbox, fleet_providers)

    # 3. Keep the existing channel/passive evidence while running the new fleet manager.
    channel = await _run_channel_scenario(channel_providers, sandbox)
    passive = await _run_passive_webui_scenario(passive_providers, sandbox)
    full_fleet = await _run_full_fleet_scenario(
        fleet_providers,
        sandbox,
        runtime_stage,
    )
    return {
        "sources": source_evidence,
        "lock": lock_contract,
        "channel": channel["channel"],
        "message_push": channel["message_push"],
        "channel_cleanup": channel["cleanup"],
        "passive_webui": passive,
        "full_boot_catalog": full_fleet["full_boot_catalog"],
        "candidate_lifecycle": full_fleet["candidate_lifecycle"],
        "fleet_coverage": full_fleet["fleet_coverage"],
        "github_watch": full_fleet["github_watch"],
        "proactive": full_fleet["proactive"],
    }


def _copy_source_tree(source: Path, target: Path) -> None:
    """Copy one checked-out source into a disposable provider set."""

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


def _stage_fleet_runtime(sandbox: Path, providers: Path) -> dict[str, object]:
    """Stage recording-only Python entrypoints and exact MCP runtime roots."""

    # 1. Reuse E2's exact manifest requirements staging for all four MCPs.
    checkouts = {
        plugin_id: providers / plugin_id for plugin_id in e2_gate.MCP_PLUGIN_IDS
    }
    pip_tmp = sandbox / "pip-tmp"
    pip_tmp.mkdir(parents=True, exist_ok=True)
    previous_tmp = os.environ.get("TMPDIR")
    os.environ["TMPDIR"] = str(pip_tmp)
    try:
        runtime, runtime_python, requirement_evidence = e2_gate._create_runtime_stage(
            Path(sys.executable),
            sandbox,
            checkouts,
        )
    except e2_gate.GateBlocked as error:
        raise GateBlocked(str(error)) from error
    finally:
        if previous_tmp is None:
            os.environ.pop("TMPDIR", None)
        else:
            os.environ["TMPDIR"] = previous_tmp
        shutil.rmtree(pip_tmp, ignore_errors=True)

    # 2. Preserve the staged interpreter and make its declared entrypoint recording-only.
    real_interpreter = runtime_python.with_name("python.real")
    runtime_python.rename(real_interpreter)
    interpreter = runtime_python
    interpreter.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        "export STEAM_BACKEND=recording\n"
        "export FEED_BACKEND=recording\n"
        "export CALENDAR_BACKEND=recording\n"
        "export FITBIT_BACKEND=recording\n"
        f"exec {shlex.quote(str(real_interpreter))} \"$@\"\n",
        encoding="utf-8",
    )
    interpreter.chmod(0o755)

    # 3. Bind every declared MCP runtime to that exact staged wrapper.
    from agent.plugins.static_manifest import load_static_plugin_manifest

    for plugin_id in e2_gate.MCP_PLUGIN_IDS:
        manifest = load_static_plugin_manifest(providers / plugin_id)
        for declaration in manifest.python:
            runtime_root = providers / plugin_id / declaration.runtime_root
            link = runtime_root / ".venv"
            if link.exists() or link.is_symlink():
                if link.is_dir() and not link.is_symlink():
                    shutil.rmtree(link)
                else:
                    link.unlink()
            link.symlink_to(runtime, target_is_directory=True)
    return {
        "runtime_root": str(runtime),
        "python": str(interpreter),
        "requirements": [dict(item) for item in requirement_evidence],
        "recording_env": {
            "STEAM_BACKEND": "recording",
            "FEED_BACKEND": "recording",
            "CALENDAR_BACKEND": "recording",
            "FITBIT_BACKEND": "recording",
        },
    }


def _prepare_channel_checkouts(providers: Path) -> None:
    """Add only the installed-runtime marker required by the channel manifests."""

    marker = providers / "feishu" / ".venv" / "bin" / "python"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.symlink_to(sys.executable)


async def _run_full_fleet_scenario(
    providers: Path,
    sandbox: Path,
    runtime_stage: Mapping[str, object],
) -> dict[str, object]:
    """Boot the exact fleet, assert catalogs, and exercise one real candidate reload."""

    # 1. Build one disposable Core manager over every checked-out external and private root.
    workspace = sandbox / "fleet-workspace"
    _write_fleet_config(workspace)
    event_bus = EventBus()
    tools = ToolRegistry()
    sessions = SessionManager(workspace)
    private_roots = tuple(
        ROOT / "plugins" / plugin_id for plugin_id in PRIVATE_PROACTIVE_PLUGIN_IDS
    )
    manager = PluginManager(
        plugin_dirs=[providers, *private_roots],
        event_bus=event_bus,
        tool_registry=tools,
        workspace=workspace,
        session_manager=sessions,
        installed_cache_root=sandbox / "fleet-plugin-home" / "cache",
    )
    conversation_runtime = _RecordingConversationRuntime()
    background_adapter = BackgroundJobActivityAdapter(
        event_bus,
        manager.snapshot_store,
        ledger_path=str(workspace / "runtime" / "background-jobs.sqlite"),
        workspace=str(workspace),
        interval_poll_seconds=3600,
    )
    background_adapter.bind_conversation_runtime(
        conversation_runtime,
        programmatic_session_creator=sessions.control_store.create_session,
        programmatic_session_reader=sessions.control_store.get_session_meta,
    )
    activity_adapter = ProactiveActivityAdapter(
        manager.composition_generation_host
    )
    private_default = _NamedPrivateProactiveHost("default", "private_proactive")
    private_wake = _NamedPrivateProactiveHost("wake", "private_proactive_wake")
    activity_host = ActivityHost(
        (background_adapter, activity_adapter, private_default, private_wake)
    )
    manager.bind_activity_host(activity_host)
    factories = {
        "feishu": _RecordingFactory(
            "feishu",
            {"appId": "synthetic-feishu-id", "appSecret": "synthetic-feishu-secret"},
        ),
        "qqbot": _RecordingFactory(
            "qqbot",
            {"appId": "synthetic-qq-id", "clientSecret": "synthetic-qq-secret"},
        ),
    }
    manager.bind_channel_provider_factory_resolver(lambda _snapshot: factories)
    original_resolver = _bind_recording_channel_adapters()
    stable: Any | None = None
    try:
        await manager.load_all()
        stable = manager.current_snapshot
        if stable is None:
            raise GateFailure("full-fleet boot 缺少 committed snapshot")
        full_boot = _assert_full_fleet_catalog(manager, stable, runtime_stage)
        counts_before_candidate = {
            "source_fetch": activity_adapter.source_fetch_invocations,
            "module": activity_adapter.module_invocations,
            "handler_resolution": activity_adapter.handler_resolution_count,
            "background_handler_resolution": (
                background_adapter.handler_resolution_count
            ),
        }

        # 2. Prepare and discard a candidate without changing stable publication or executing activity.
        candidate = await manager.prepare_candidate("daynight_gate")
        if candidate is None or candidate.runtime_snapshot is None:
            raise GateFailure("daynight_gate candidate snapshot 缺失")
        candidate_identity = {
            "snapshot_id": candidate.runtime_snapshot.snapshot_id,
            "generation_id": candidate.generation_id,
        }
        if manager.current_snapshot is not stable:
            raise GateFailure("candidate preparation 改变了 stable snapshot")
        await manager.discard_prepared("daynight_gate")
        if manager.current_snapshot is not stable:
            raise GateFailure("candidate discard 未恢复 stable snapshot")
        counts_after_discard = {
            "source_fetch": activity_adapter.source_fetch_invocations,
            "module": activity_adapter.module_invocations,
            "handler_resolution": activity_adapter.handler_resolution_count,
            "background_handler_resolution": (
                background_adapter.handler_resolution_count
            ),
        }
        if counts_after_discard != counts_before_candidate:
            raise GateFailure(
                "candidate discard 意外执行 proactive/background activity: "
                f"before={counts_before_candidate} after={counts_after_discard}"
            )
        validation_root = workspace / "runtime" / "plugin-validation"
        validation_residuals = (
            tuple(sorted(path.name for path in validation_root.iterdir()))
            if validation_root.exists()
            else ()
        )
        if validation_residuals:
            raise GateFailure(
                f"daynight_gate candidate discard 留下 validation residuals: {validation_residuals}"
            )

        # 3. Promote the same exact candidate and require a new committed snapshot identity.
        promoted_candidate = await manager.prepare_candidate("daynight_gate")
        if promoted_candidate is None:
            raise GateFailure("daynight_gate promote candidate 缺失")
        publication = await manager.publish_prepared("daynight_gate")
        promoted = manager.current_snapshot
        if promoted is None or promoted is stable:
            raise GateFailure("candidate promote 未产生新 stable snapshot")
        if promoted.snapshot_id == stable.snapshot_id:
            raise GateFailure("candidate promote snapshot identity 未变化")
        candidate_lifecycle = {
            "status": "complete",
            "candidate": candidate_identity,
            "discard": {
                "stable_snapshot_id": stable.snapshot_id,
                "validation_residuals": list(validation_residuals),
                "activity_before": counts_before_candidate,
                "activity_after_discard": counts_after_discard,
            },
            "promote": {
                "publication": publication,
                "snapshot_id": promoted.snapshot_id,
                "generation_id": promoted_candidate.generation_id,
            },
        }
        coverage = _fleet_coverage_from_snapshot(promoted)
        github_watch = await _run_github_watch_controlled_remote(
            manager=manager,
            background_adapter=background_adapter,
            sandbox=sandbox,
        )
        proactive = await _run_deterministic_proactive(
            manager=manager,
            activity_host=activity_host,
            sessions=sessions,
            event_bus=event_bus,
            workspace=workspace,
        )
        return {
            "full_boot_catalog": full_boot,
            "candidate_lifecycle": candidate_lifecycle,
            "fleet_coverage": coverage,
            "github_watch": github_watch,
            "proactive": proactive,
        }
    finally:
        try:
            await manager.terminate_all()
        finally:
            channel_generation_host._resolve_sync_factory = original_resolver
            await event_bus.aclose()
            sessions._store.close()


async def _run_github_watch_controlled_remote(
    *,
    manager: PluginManager,
    background_adapter: BackgroundJobActivityAdapter,
    sandbox: Path,
) -> dict[str, object]:
    """Run the locked GitHub Watch job against a local bare remote and Core Turn port."""

    # 1. Create a disposable bare repository whose commit is the only checkout input.
    remote, seed_commit = _create_controlled_git_remote(sandbox)
    remote_uri = remote.as_uri()
    binding = background_adapter.active_binding
    if binding is None:
        raise GateFailure("GitHub Watch 缺少 committed BackgroundJob binding")
    poll_keys = tuple(key for key in binding.jobs if key.endswith(":poll"))
    if poll_keys != ("github-watch:poll",):
        raise GateFailure(f"GitHub Watch job catalog 错误: {poll_keys}")
    generation = manager.generation("github-watch")
    if generation is None:
        raise GateFailure("GitHub Watch generation 缺失")
    module = cast(Any, generation.instance).module
    original_client = module.GitHubClient
    original_checkout = module.CheckoutManager
    clients: list[Any] = []
    reactions: list[dict[str, object]] = []

    class ControlledGitHubClient:
        """Expose deterministic GitHub API reads while retaining no credentials."""

        def __init__(
            self,
            *,
            app_id: int,
            installation_id: int,
            pem_path: Path,
        ) -> None:
            del app_id, installation_id, pem_path
            self.phase = 0
            self.credentials_read = False
            clients.append(self)

        def installation_token(self) -> str:
            raise AssertionError("controlled GitHub remote attempted credential access")

        def repository(self, _repo: str) -> dict[str, object]:
            return {
                "owner": {"login": "recording-owner"},
                "default_branch": "main",
            }

        def issues(self, _repo: str) -> list[dict[str, object]]:
            return [self._issue()]

        def pulls(self, _repo: str) -> list[dict[str, object]]:
            return []

        def issue(self, _repo: str, _number: int) -> dict[str, object]:
            return self._issue()

        def comments(self, _repo: str, _number: int) -> list[dict[str, object]]:
            if self.phase == 0:
                return []
            return [
                {
                    "id": 1,
                    "body": "@akashic-review-bot inspect this local issue",
                    "user": {"login": "recording-owner"},
                    "html_url": "file:///controlled/comment/1",
                }
            ]

        def timeline(self, _repo: str, _number: int) -> list[dict[str, object]]:
            return []

        def add_reaction(
            self,
            _repo: str,
            _number: int,
            content: str,
        ) -> dict[str, object]:
            reactions.append(
                {
                    "content": content,
                    "remote": remote_uri,
                    "external": False,
                }
            )
            return {
                "content": content,
                "html_url": "file:///controlled/reaction/1",
                "id": 1,
            }

        def _issue(self) -> dict[str, object]:
            return {
                "number": 1,
                "updated_at": (
                    "2026-08-20T00:00:00Z"
                    if self.phase == 0
                    else "2026-08-20T00:01:00Z"
                ),
                "title": "Controlled E3 issue",
                "body": "Inspect the deterministic local checkout.",
                "user": {"login": "recording-owner"},
                "state": "open",
                "draft": False,
                "html_url": "file:///controlled/issue/1",
            }

    class ControlledCheckoutManager(original_checkout):
        """Reuse the real checkout/worktree flow with a file remote and no token."""

        def __init__(self, client: object, **kwargs: object) -> None:
            super().__init__(client, **kwargs)
            self._controlled_remote_uri = remote_uri

        def _ensure_mirror(self, repo: str, operation_dir: Path) -> Path:
            mirror = self._mirror_path(repo)
            if (mirror / "HEAD").is_file() and (mirror / "objects").is_dir():
                return mirror
            mirror.parent.mkdir(parents=True, exist_ok=True)
            self._run(
                [
                    "git",
                    "-c",
                    "credential.helper=",
                    "clone",
                    "--mirror",
                    self._controlled_remote_uri,
                    str(mirror),
                ]
            )
            return mirror

        def _refresh_mirror(self, mirror: Path, operation_dir: Path) -> None:
            del operation_dir
            self._run(
                [
                    "git",
                    "-C",
                    str(mirror),
                    "worktree",
                    "prune",
                    "--expire",
                    "now",
                ]
            )
            self._run(
                [
                    "git",
                    "-C",
                    str(mirror),
                    "fetch",
                    "origin",
                    "--prune",
                    "+refs/heads/*:refs/heads/*",
                ]
            )

        def _run_authenticated(self, operation_dir: Path, command: list[str]) -> None:
            del operation_dir
            self._run(command)

    setattr(module, "GitHubClient", ControlledGitHubClient)
    setattr(module, "CheckoutManager", ControlledCheckoutManager)
    try:
        # 2. Admit one baseline poll and one owner-mention poll via the real adapter.
        await background_adapter.enqueue_interval(
            binding,
            "github-watch:poll",
            interval_bucket="e3-github-baseline",
        )
        await _wait_background_job_settled(binding)
        if len(clients) != 1:
            raise GateFailure(f"GitHub Watch formal runtime 创建次数错误: {len(clients)}")
        clients[0].phase = 1
        await background_adapter.enqueue_interval(
            binding,
            "github-watch:poll",
            interval_bucket="e3-github-owner-mention",
        )
        await _wait_background_job_settled(binding)

        # 3. Verify durable job, event, and programmatic Turn identities.
        first = background_adapter.ledger
        if first is None:
            raise GateFailure("GitHub Watch 缺少 BackgroundJob outcome ledger")
        outcomes = []
        for bucket in ("e3-github-baseline", "e3-github-owner-mention"):
            outcome = first.find_by_event(
                plugin_id="github-watch",
                job_name="poll",
                interval_bucket=bucket,
            )
            if outcome is None or str(outcome.state.value) != "succeeded":
                raise GateFailure(f"GitHub Watch job outcome 未成功: {bucket} {outcome}")
            outcomes.append(
                {
                    "interval_bucket": bucket,
                    "invocation_id": outcome.invocation_id,
                    "state": outcome.state.value,
                    "snapshot_id": outcome.snapshot_id,
                    "plugin_generation_id": outcome.plugin_generation_id,
                }
            )
        bound = getattr(module, "_bound", None)
        ledger = None if bound is None else getattr(bound, "ledger", None)
        if ledger is None:
            raise GateFailure("GitHub Watch formal EventLedger 未初始化")
        event_key = "recording-owner/recording-repo:issue:1:comment:1"
        event = ledger.get_event(event_key)
        if event.status != "dispatched":
            raise GateFailure(f"GitHub Watch event 未 dispatched: {event}")
        if not isinstance(event.thread_id, str) or not event.thread_id:
            raise GateFailure("GitHub Watch event 缺少 programmatic Session identity")
        if not isinstance(event.turn_id, str) or not event.turn_id:
            raise GateFailure("GitHub Watch event 缺少 programmatic Turn identity")
        # The synthetic runtime is owned by the adapter through the job invocation port.
        runtime = background_adapter.conversation_runtime
        requests = [] if runtime is None else list(getattr(runtime, "requests", ()))
        if len(requests) != 1:
            raise GateFailure(f"GitHub Watch programmatic Turn 次数错误: {len(requests)}")
        if requests[0]["turn_id"] != event.turn_id or requests[0]["thread_id"] != event.thread_id:
            raise GateFailure(
                "GitHub Watch job/Turn identity 不一致: "
                f"request={requests[0]} event={event}"
            )
        if not all(not bool(client.credentials_read) for client in clients):
            raise GateFailure("GitHub Watch 读取了 formal credentials")
        policy = _github_watch_policy(remote_uri)
        policy.update(
            {
                "bare_remote": remote.is_dir() and (remote / "HEAD").is_file(),
                "seed_commit": seed_commit,
                "job": {
                    "key": "github-watch:poll",
                    "binding_snapshot_id": binding.snapshot_id,
                    "outcomes": outcomes,
                },
                "event": {
                    "event_key": event.event_key,
                    "status": event.status,
                    "thread_id": event.thread_id,
                    "turn_id": event.turn_id,
                    "trigger_kind": event.trigger_kind,
                },
                "programmatic_turns": requests,
                "reactions": reactions,
            }
        )
        if policy["bare_remote"] is not True:
            raise GateFailure("GitHub controlled remote 不是有效 bare file:// remote")
        return policy
    finally:
        setattr(module, "GitHubClient", original_client)
        setattr(module, "CheckoutManager", original_checkout)


async def _wait_background_job_settled(binding: object) -> None:
    """Wait for explicit job admission without consuming the long-lived interval loop."""

    # 1. Poll only the accepted invocation state; ActivityHost owns interval shutdown.
    runtime = cast(Any, binding)
    for _ in range(500):
        if not runtime.pending_admission and not runtime.queued and not runtime.running:
            await asyncio.sleep(0)
            if not runtime.pending_admission and not runtime.queued and not runtime.running:
                return
        await asyncio.sleep(0.01)
    raise GateFailure("GitHub Watch explicit job 未在 deterministic window 内结算")


def _create_controlled_git_remote(sandbox: Path) -> tuple[Path, str]:
    """Create one local bare remote and return its path plus seed commit."""

    # 1. Seed an ordinary repository with one deterministic commit.
    source = sandbox / "github-controlled-source"
    remote = sandbox / "github-controlled-remote.git"
    source.mkdir(parents=True, exist_ok=False)
    remote.mkdir(parents=True, exist_ok=False)
    _run_local_git(remote, "init", "--bare")
    _run_local_git(source, "init")
    _run_local_git(source, "config", "user.name", "E3 recording")
    _run_local_git(source, "config", "user.email", "e3-recording@example.invalid")
    (source / "README.md").write_text("E3 controlled remote\n", encoding="utf-8")
    _run_local_git(source, "add", "README.md")
    _run_local_git(source, "commit", "-m", "seed controlled E3 remote")
    _run_local_git(source, "branch", "-M", "main")
    _run_local_git(source, "remote", "add", "origin", remote.as_uri())
    _run_local_git(source, "push", "origin", "main")
    _run_local_git(remote, "symbolic-ref", "HEAD", "refs/heads/main")
    seed_commit = _run_local_git(source, "rev-parse", "HEAD")
    return remote, seed_commit


def _run_local_git(cwd: Path, *args: str) -> str:
    """Run one explicit local Git command in the disposable Gate sandbox."""

    completed = subprocess.run(
        ("git", *args),
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return completed.stdout.strip()


async def _run_deterministic_proactive(
    *,
    manager: PluginManager,
    activity_host: ActivityHost,
    sessions: SessionManager,
    event_bus: EventBus,
    workspace: Path,
) -> dict[str, object]:
    """Execute deterministic source, model, skip, delivery, and restart probes."""

    # 1. Seed one workspace-only Drift skill; no external model or channel is used.
    from agent.persona import read_default_veda

    memory_dir = workspace / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    (memory_dir / "VEDA.md").write_text(read_default_veda() + "\n", encoding="utf-8")
    skill_dir = workspace / "drift" / "skills" / "sandbox_probe"
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(
        "---\n"
        "name: sandbox_probe\n"
        "description: E3 deterministic proactive probe\n"
        "---\n\n"
        "完成一次固定的主动链路探测。\n",
        encoding="utf-8",
    )
    snapshot = manager.current_snapshot
    if snapshot is None or snapshot.tool_registry is None:
        raise GateFailure("proactive probe 缺少 stable snapshot/tool registry")

    # 2. Probe one successful empty source and one typed source failure through the exact bridge.
    bridge = CommittedProactiveBridge(activity_host)
    source_empty = await _probe_proactive_source(
        manager=manager,
        activity_host=activity_host,
        owner="feed",
        fail=False,
    )
    source_failure = await _probe_proactive_source(
        manager=manager,
        activity_host=activity_host,
        owner="calendar",
        fail=True,
    )
    observed: list[str] = []
    if isinstance(source_empty, FetchEmpty):
        observed.append("empty")
    else:
        raise GateFailure(f"recording feed source 未返回 FetchEmpty: {source_empty!r}")
    if isinstance(source_failure, FetchFailure):
        observed.append("source")
    else:
        raise GateFailure(
            f"受控 calendar route 未返回 FetchFailure: {source_failure!r}"
        )

    # 3. Run the empty-source path with Drift disabled; stable state must settle as skip.
    skip_path = workspace / "proactive-skip.db"
    skip_run = await _run_proactive_tick(
        manager=manager,
        activity_host=activity_host,
        sessions=sessions,
        event_bus=event_bus,
        workspace=workspace,
        snapshot=snapshot,
        state_path=skip_path,
        config=_deterministic_proactive_config(drift=False),
        provider=SandboxProvider("drift"),
        expected_failure=False,
        label="skip",
    )
    if skip_run["tick"].get("terminal_action") != "skip":
        raise GateFailure(f"empty proactive tick 未 settle skip: {skip_run}")
    observed.append("skip")

    # 4. Force one model error, then restart from the same state path and deliver via recording sink.
    restart_path = workspace / "proactive-restart.db"
    failed_run = await _run_proactive_tick(
        manager=manager,
        activity_host=activity_host,
        sessions=sessions,
        event_bus=event_bus,
        workspace=workspace,
        snapshot=snapshot,
        state_path=restart_path,
        config=_deterministic_proactive_config(drift=True),
        provider=_FailingProvider(),
        expected_failure=True,
        label="model-failure",
    )
    if "deterministic model failure" not in str(failed_run.get("error")):
        raise GateFailure(f"model failure probe 未保留原始错误: {failed_run}")
    observed.append("model")
    restarted_run = await _run_proactive_tick(
        manager=manager,
        activity_host=activity_host,
        sessions=sessions,
        event_bus=event_bus,
        workspace=workspace,
        snapshot=snapshot,
        state_path=restart_path,
        config=_deterministic_proactive_config(drift=True),
        provider=SandboxProvider("drift"),
        expected_failure=False,
        label="restart",
    )
    if not restarted_run["delivered"]:
        raise GateFailure(f"model failure 后 proactive restart 未 delivery: {restarted_run}")
    observed.extend(("delivery", "restart"))
    return {
        "status": "complete",
        "clock": _FIXED_CLOCK.isoformat(),
        "oracle": _proactive_coverage_oracle(observed),
        "empty_source": _source_result_evidence(source_empty),
        "source_failure": _source_result_evidence(source_failure),
        "skip": skip_run,
        "model_failure": failed_run,
        "restart": restarted_run,
    }


async def _probe_proactive_source(
    *,
    manager: PluginManager,
    activity_host: ActivityHost,
    owner: str,
    fail: bool,
) -> object:
    """Fetch one exact committed source, optionally through a failing route."""

    host = manager.composition_generation_host
    original_route_for = host.route_for
    if fail:
        def route_for(generation_id: str, server_name: str) -> object:
            if server_name == "calendar":
                return _FailingMcpRoute()
            return original_route_for(generation_id, server_name)

        host.route_for = route_for  # type: ignore[method-assign]
    try:
        lease = await manager.snapshot_store.acquire()
        from agent.plugins.snapshot import bind_runtime_snapshot, reset_runtime_snapshot

        async with lease:
            token = bind_runtime_snapshot(lease)
            try:
                runtime = CommittedProactiveBridge(activity_host).runtime_for(
                    lease.snapshot
                )
                source = next(
                    facade
                    for facade in runtime.sources.values()
                    if facade.descriptor.owner == owner
                )
                return await source.fetch(lease)
            finally:
                reset_runtime_snapshot(token)
    finally:
        if fail:
            host.route_for = original_route_for  # type: ignore[method-assign]


def _source_result_evidence(result: object) -> dict[str, object]:
    """Serialize one typed source result without losing its status."""

    payload: dict[str, object] = {"type": type(result).__name__}
    if isinstance(result, FetchFailure):
        payload.update({"error": result.error, "retryable": result.retryable})
    if isinstance(result, FetchEmpty):
        payload["cursor"] = result.cursor
    return payload


def _deterministic_proactive_config(*, drift: bool) -> ProactiveConfig:
    """Build one fixed-target proactive config with no probabilistic waits."""

    return ProactiveConfig(
        enabled=True,
        lifecycle="default",
        default_channel="recording",
        default_chat_id="operator",
        model="e3-deterministic-model",
        feed_poller_interval_seconds=3600,
        anyaction_daily_max_actions=999,
        anyaction_min_interval_seconds=0,
        anyaction_probability_min=1.0,
        anyaction_probability_max=1.0,
        agent_tick_max_steps=8,
        agent_tick_content_limit=5,
        agent_tick_context_prob=0.0,
        agent_tick_delivery_cooldown_hours=0,
        message_dedupe_enabled=False,
        delivery_dedupe_hours=0,
        drift_enabled=drift,
        drift_max_steps=5,
        drift_min_interval_hours=0,
    )


async def _run_proactive_tick(
    *,
    manager: PluginManager,
    activity_host: ActivityHost,
    sessions: SessionManager,
    event_bus: EventBus,
    workspace: Path,
    snapshot: object,
    state_path: Path,
    config: ProactiveConfig,
    provider: object,
    expected_failure: bool,
    label: str,
) -> dict[str, object]:
    """Run one fixed-clock proactive tick against the manager's committed snapshot."""

    delivered: list[dict[str, str]] = []
    push = MessagePushTool()
    push.bind_v3_channel_dispatcher(_build_recording_dispatcher(delivered))
    state = ProactiveStateStore(state_path)
    loop = ProactiveLoop(
        session_manager=sessions,
        provider=cast(Any, provider),
        push_tool=push,
        config=config,
        model=config.model,
        max_tokens=1024,
        state_store=state,
        rng=__import__("random").Random(0),
        shared_tools=cast(Any, snapshot).tool_registry,
        event_bus=event_bus,
        runtime_snapshot_store=manager.snapshot_store,
        activity_host=activity_host,
    )
    modules = (
        __import__("proactive_v2.frame", fromlist=["datetime"]),
        __import__("proactive_v2.loop", fromlist=["datetime"]),
        __import__("plugins.default_proactive.context", fromlist=["datetime"]),
        __import__("plugins.default_proactive.runtime", fromlist=["datetime"]),
        __import__("plugins.drift_flow.runtime", fromlist=["datetime"]),
    )
    result: dict[str, object] = {"label": label, "delivered": delivered}
    try:
        with ExitStack() as stack:
            for module in modules:
                stack.enter_context(patch.object(module, "datetime", _FixedDateTime))
            try:
                await loop._start_current_snapshot()
                await loop._tick()
            except BaseException as error:
                if not expected_failure:
                    raise
                result["error"] = f"{type(error).__name__}: {error}"
            result["tick"] = _read_proactive_tick(state_path)
    finally:
        try:
            if getattr(loop, "_kernel_started", False):
                await loop._stop_active_kernel()
        finally:
            loop.close()
            state.close()
    return result


def _build_recording_dispatcher(
    delivered: list[dict[str, str]],
) -> Callable[[BusChannelMessage, bool], Awaitable[ChannelDeliveryReceipt]]:
    """为私有 proactive probe 绑定 typed recording sink。"""

    async def dispatch(
        message: BusChannelMessage,
        _passive: bool,
    ) -> ChannelDeliveryReceipt:
        if message.channel != "recording":
            raise GateFailure(
                f"proactive recording 收到未知 channel: {message.channel!r}"
            )
        delivery_id = f"e3-proactive-delivery-{len(delivered) + 1}"
        delivered.append(
            {
                "channel": message.channel,
                "chat_id": message.chat_id,
                "content": message.content,
            }
        )
        return ChannelDeliveryReceipt(
            delivery_id=delivery_id,
            status=DeliveryStatus.DELIVERED,
            provider_ids=("recording",),
        )

    return dispatch


def _read_proactive_tick(path: Path) -> dict[str, object]:
    """Read the durable terminal evidence for one disposable proactive tick."""

    if not path.exists():
        return {}
    with sqlite3.connect(path) as database:
        row = database.execute(
            """
            SELECT terminal_action, skip_reason, steps_taken, content_count,
                   drift_entered, final_message
            FROM tick_log ORDER BY rowid DESC LIMIT 1
            """
        ).fetchone()
    if row is None:
        return {}
    return {
        "terminal_action": row[0],
        "skip_reason": row[1],
        "steps_taken": row[2],
        "content_count": row[3],
        "drift_entered": bool(row[4]),
        "final_message": row[5],
    }


def _bind_recording_channel_adapters() -> object:
    """Patch only the disposable channel adapter factory to stop network loops."""

    original = channel_generation_host._resolve_sync_factory

    def resolve_factory(module: object, export: str) -> object:
        factory = original(cast(ModuleType, module), export)

        def wrapped(context: object) -> object:
            adapter = cast(Any, cast(Any, factory)(context))
            channel = str(getattr(adapter, "name", ""))
            if channel == "feishu":
                adapter._run_ws_client = lambda: adapter._ws_stopped.wait()
            elif channel == "qqbot":
                adapter._gateway_loop = lambda: adapter._stopped.wait()
            return adapter

        return wrapped

    channel_generation_host._resolve_sync_factory = resolve_factory
    return original


def _assert_full_fleet_catalog(
    manager: PluginManager,
    snapshot: object,
    runtime_stage: Mapping[str, object],
) -> dict[str, object]:
    """Assert commands, channels, MCP, proactive, jobs, and private catalogs from one snapshot."""

    # 1. Resolve exact plugin generation identities and lock-to-module mapping.
    current = cast(Any, snapshot)
    generation_ids = set(current.generations)
    expected_modules = set(FLEET_MODULE_IDS.values()) | set(PRIVATE_PROACTIVE_PLUGIN_IDS)
    if generation_ids != expected_modules:
        raise GateFailure(
            "full-fleet generation 集合错误: "
            f"missing={sorted(expected_modules - generation_ids)}, "
            f"extra={sorted(generation_ids - expected_modules)}"
        )
    coverage = _fleet_coverage_from_snapshot(current)

    # 2. Assert the stable command/channel/MCP/proactive/job/private catalogs.
    commands = current.command_registry
    command_names = set() if commands is None else {item.name for item in commands.descriptors}
    expected_commands = {"chatid", "memorystatus"}
    if not expected_commands.issubset(command_names):
        raise GateFailure(f"full-fleet command catalog 缺失: {expected_commands - command_names}")
    channels = current.channel_registry
    channel_names = (
        set() if channels is None else {item.name for item in channels.descriptors}
    )
    if channel_names != set(CHANNEL_PLUGIN_IDS):
        raise GateFailure(f"full-fleet channel catalog 错误: {sorted(channel_names)}")
    mcp = current.mcp_server_registry
    mcp_names = set() if mcp is None else {item.name for item in mcp.descriptors}
    expected_mcp = {"calendar", "feed", "fitbit", "steam"}
    if mcp_names != expected_mcp:
        raise GateFailure(f"full-fleet MCP catalog 错误: {sorted(mcp_names)}")
    proactive = current.proactive_component_catalog
    if proactive is None:
        raise GateFailure("full-fleet proactive catalog 缺失")
    sources = {(item.owner, item.name) for item in proactive.source_descriptors}
    expected_sources = {
        ("calendar", "upcoming_events"),
        ("feed", "subscriptions"),
        ("fitbit", "health_alerts"),
        ("fitbit", "sleep_context"),
        ("steam", "presence"),
    }
    if not expected_sources.issubset(sources):
        raise GateFailure(f"full-fleet proactive sources 缺失: {expected_sources - sources}")
    modules = {(item.owner, item.slot) for item in proactive.module_descriptors}
    expected_modules_catalog = {
        ("daynight_gate", "proactive.gate.daynight"),
        ("emotion", "proactive.prompt.emotion"),
    }
    if not expected_modules_catalog.issubset(modules):
        raise GateFailure(
            "full-fleet proactive modules 缺失: "
            f"{expected_modules_catalog - modules}"
        )
    private = current.private_proactive_catalog
    private_names = set() if private is None else {item.member for item in private.members}
    if private_names != set(PRIVATE_PROACTIVE_PLUGIN_IDS):
        raise GateFailure(f"private Default/Wake catalog 错误: {sorted(private_names)}")
    jobs = current.background_job_catalog
    job_names = set() if jobs is None else {item.name for item in jobs.descriptors}
    if not {"merge_proactive_pending", "poll"}.issubset(job_names):
        raise GateFailure(f"full-fleet background jobs 缺失: {job_names}")
    return {
        "status": "complete",
        "snapshot_id": current.snapshot_id,
        "generations": sorted(generation_ids),
        "commands": sorted(command_names),
        "channels": sorted(channel_names),
        "mcp": sorted(mcp_names),
        "proactive_sources": sorted(f"{owner}:{name}" for owner, name in sources),
        "proactive_modules": sorted(f"{owner}:{slot}" for owner, slot in modules),
        "private": sorted(private_names),
        "background_jobs": sorted(job_names),
        "runtime_stage": dict(runtime_stage),
    }


def _fleet_coverage_from_snapshot(snapshot: object) -> dict[str, object]:
    """Convert snapshot module identities back to the canonical lock IDs."""

    current = cast(Any, snapshot)
    active_modules = set(current.generations)
    reverse = {module: plugin_id for plugin_id, module in FLEET_MODULE_IDS.items()}
    active_external = tuple(
        reverse[module]
        for module in sorted(active_modules)
        if module in reverse
    )
    active_private = tuple(
        plugin_id for plugin_id in PRIVATE_PROACTIVE_PLUGIN_IDS if plugin_id in active_modules
    )
    return _fleet_coverage_contract(
        active_external_ids=active_external,
        active_private_ids=active_private,
    )


def _write_fleet_config(workspace: Path) -> None:
    """Write only synthetic fleet configuration into the disposable workspace."""

    _write_channel_config(workspace)
    configs = {
        "daynight_gate-builtin": (
            "enabled = true\n"
            'timezone = "UTC"\n'
            'start = "00:00"\n'
            'end = "06:00"\n'
            "pass_probability = 0.0\n"
        ),
        "calendar-builtin": "[proactive]\nenabled = true\n",
        "feed-builtin": "[proactive]\nenabled = true\n",
        "fitbit-builtin": "[proactive]\nenabled = true\n",
        "steam-builtin": "[proactive]\nenabled = true\n",
        "github-watch-builtin": (
            "app_id = 1\n"
            "installation_id = 1\n"
            'pem_path = "/nonexistent/e3-synthetic-github.pem"\n'
            'repositories = ["recording-owner/recording-repo"]\n'
        ),
    }
    roots = workspace / "plugin-data"
    for plugin_id in (
        "setup_helper-builtin",
        "status_commands-builtin",
        "emotion-builtin",
        "huayue-skills-builtin",
    ):
        (roots / plugin_id).mkdir(parents=True, exist_ok=True)
    for plugin_id, content in configs.items():
        data = roots / plugin_id
        data.mkdir(parents=True, exist_ok=True)
        (data / "config.local.toml").write_text(content, encoding="utf-8")


async def _run_channel_scenario(
    providers: Path,
    sandbox: Path,
) -> dict[str, object]:
    """Run channel publication, loopback admission, and awaited MessagePush."""

    workspace = sandbox / "channel-workspace"
    _write_channel_config(workspace)
    protected_before = _protected_digest(workspace)
    session_manager = SessionManager(workspace)
    factories = {
        "feishu": _RecordingFactory(
            "feishu",
            {"appId": "synthetic-feishu-id", "appSecret": "synthetic-feishu-secret"},
        ),
        "qqbot": _RecordingFactory(
            "qqbot",
            {
                "appId": "synthetic-qq-id",
                "clientSecret": "synthetic-qq-secret",
            },
        ),
    }
    received: list[dict[str, str]] = []
    inbound_owned: list[Any] = []
    original_resolver = channel_generation_host._resolve_sync_factory
    manager = PluginManager(
        plugin_dirs=[providers],
        event_bus=EventBus(),
        tool_registry=None,
        session_manager=session_manager,
        workspace=workspace,
        installed_cache_root=sandbox / "plugin-home" / "cache",
    )
    manager.bind_channel_provider_factory_resolver(lambda _snapshot: factories)

    async def publish_inbound(envelope: object) -> None:
        owned = cast(Any, envelope)
        received.append(
            {
                "channel": str(
                    getattr(getattr(envelope, "message", None), "channel", "")
                ),
                "message_id": str(getattr(envelope, "message_id")),
                "snapshot_id": str(getattr(envelope, "snapshot_id")),
                "generation_id": str(getattr(envelope, "generation_id")),
                "binding_token": str(getattr(envelope, "binding_token")),
            }
        )
        owned.handoff(InboundOwner.INGRESS, InboundOwner.BUS)
        inbound_owned.append(owned)

    manager.channel_generation_host.bind_inbound_publisher(publish_inbound)

    def resolve_factory(module: object, export: str) -> object:
        factory = original_resolver(cast(ModuleType, module), export)

        def wrapped(context: object) -> object:
            adapter = cast(Any, cast(Any, factory)(context))
            channel = str(getattr(adapter, "name", ""))
            if channel == "feishu":
                adapter._run_ws_client = lambda: adapter._ws_stopped.wait()

                async def send_one(
                    recipient: str,
                    _kind: str,
                    _content: str,
                ) -> tuple[DeliveryStatus, str | None, str | None]:
                    status = factories["feishu"].record(
                        recipient=recipient,
                        delivery_id=_current_delivery_id(),
                        kind="feishu.send",
                    )
                    return (
                        status,
                        "recording-feishu" if status is DeliveryStatus.DELIVERED else None,
                        None if status is DeliveryStatus.DELIVERED else "recording after-effect failure",
                    )

                adapter._send_one = send_one
            elif channel == "qqbot":
                adapter._gateway_loop = lambda: adapter._stopped.wait()

                async def send_text(
                    recipient: str,
                    _body: str,
                ) -> tuple[DeliveryStatus, str | None, str | None]:
                    status = factories["qqbot"].record(
                        recipient=recipient,
                        delivery_id=_current_delivery_id(),
                        kind="qqbot.send",
                    )
                    return (
                        status,
                        "recording-qqbot" if status is DeliveryStatus.DELIVERED else None,
                        None if status is DeliveryStatus.DELIVERED else "recording after-effect failure",
                    )

                adapter._send_text = send_text
            return adapter

        return wrapped

    delivery_context: dict[str, str] = {}
    promoted: Any | None = None

    def _current_delivery_id() -> str:
        return delivery_context.get("delivery_id", "recording-unknown")

    channel_generation_host._resolve_sync_factory = resolve_factory
    try:
        await manager.load_all()
        stable = manager.current_snapshot
        active = manager.active_channel_generation
        if stable is None or active is None:
            raise GateFailure("formal channel snapshot/runtime 缺失")
        if tuple(sorted(active.channels)) != CHANNEL_PLUGIN_IDS:
            raise GateFailure(f"formal channel 集合错误: {tuple(active.channels)}")
        stable_snapshot_id = stable.snapshot_id
        stable_generations = {
            plugin_id: _generation_identity(manager, plugin_id)
            for plugin_id in CHANNEL_PLUGIN_IDS
        }
        if any(factory.create_calls != 1 for factory in factories.values()):
            raise GateFailure("formal provider factory 调用次数不是 1")

        # Candidate preparation must not start provider clients or change stable publication.
        candidate = await manager.prepare_candidate("feishu")
        if candidate is None or candidate.runtime_snapshot is None:
            raise GateFailure("Feishu candidate snapshot 缺失")
        candidate_identity = {
            "snapshot_id": candidate.runtime_snapshot.snapshot_id,
            "generation_id": candidate.generation_id,
        }
        if manager.current_snapshot is not stable:
            raise GateFailure("candidate 在 promote 前改变了 stable snapshot")
        if factories["feishu"].create_calls != 1:
            raise GateFailure("candidate 意外创建 formal provider client")
        await manager.discard_prepared("feishu")
        if manager.current_snapshot is not stable:
            raise GateFailure("candidate discard 没有恢复 stable snapshot")
        validation_root = workspace / "runtime" / "plugin-validation"
        validation_residuals = (
            tuple(sorted(path.name for path in validation_root.iterdir()))
            if validation_root.exists()
            else ()
        )
        discard_state = {
            "stable_snapshot_id": manager.current_snapshot.snapshot_id,
            "candidate_validation_removed": not validation_residuals,
            "candidate_validation_root_exists": validation_root.exists(),
            "candidate_validation_residuals": list(validation_residuals),
        }
        if validation_residuals:
            raise GateFailure(
                "candidate discard 留下 validation data: "
                f"{validation_residuals}"
            )

        candidate = await manager.prepare_candidate("feishu")
        if candidate is None or candidate.runtime_snapshot is None:
            raise GateFailure("Feishu promote candidate snapshot 缺失")
        publication = await manager.publish_prepared("feishu")
        promoted = manager.current_snapshot
        promoted_active = manager.active_channel_generation
        if promoted is None or promoted_active is None:
            raise GateFailure("promote 后 channel runtime 缺失")
        if promoted.snapshot_id == stable_snapshot_id:
            raise GateFailure("promote 没有产生新 snapshot identity")
        if promoted_active.snapshot_id != promoted.snapshot_id:
            raise GateFailure("active Channel generation 未绑定 promoted snapshot")
        if manager.generation("feishu") is None:
            raise GateFailure("promote 后 Feishu generation 缺失")
        if any(factory.create_calls != 2 for factory in factories.values()):
            raise GateFailure("promote 后每个 formal provider factory 应重建一次")

        # Loopback ingress uses the adapter's real Core ingress port and closes its lease at ingress owner.
        loopback: dict[str, dict[str, object]] = {}
        for channel in CHANNEL_PLUGIN_IDS:
            active_state = manager.channel_generation_host._bindings[
                (promoted.snapshot_id, channel)
            ]
            adapter = active_state.adapter
            ingress = None if adapter is None else getattr(adapter, "_ingress", None)
            if adapter is None or not callable(getattr(ingress, "admit", None)):
                raise GateFailure(f"{channel} exact adapter ingress 未绑定")
            provider_identity = (
                "ou_recording" if channel == "feishu" else "qq_recording"
            )
            message = ChannelInboundMessage(
                channel=channel,
                sender=provider_identity,
                chat_id="recording-chat",
                content="loopback",
                timestamp=datetime.now(UTC),
                metadata={},
            )
            raw = RawInbound(
                message_id=f"e3-inbound-{channel}",
                message=message,
                provider_identity=provider_identity,
                recipient="recording-chat",
            )
            accepted = await cast(Any, ingress).admit(raw)
            owned = inbound_owned.pop(0)
            await owned.close(InboundOwner.BUS)
            duplicate = await cast(Any, ingress).admit(raw)
            channel_received = [
                item for item in received if item["channel"] == channel
            ]
            if not accepted or duplicate or len(channel_received) != 1:
                raise GateFailure(
                    f"{channel} loopback inbound 去重错误: "
                    f"accepted={accepted} duplicate={duplicate} received={received}"
                )
            inbound = channel_received[0]
            if (
                inbound["snapshot_id"] != promoted.snapshot_id
                or inbound["generation_id"] != active_state.generation_id
            ):
                raise GateFailure(f"{channel} inbound lease 没有绑定 promoted generation")
            loopback[channel] = {
                "accepted": accepted,
                "duplicate": duplicate,
                "received": channel_received,
            }

        # MessagePush takes the same exact binding lease and returns the settled v3 receipt.
        push = MessagePushTool()
        push.bind_v3_channel_dispatcher(
            _build_push_dispatcher(manager, None, delivery_context)
        )
        delivery_context["delivery_id"] = "e3-feishu-delivery-success"
        success = json.loads(
            await push.execute(
                target_channel="feishu",
                target_chat_id="recording-chat",
                message="recording success",
            )
        )
        if success["status"] != "delivered" or success["retryable"] is not False:
            raise GateFailure(f"MessagePush success receipt 错误: {success}")
        if success["delivery_id"] != delivery_context["delivery_id"]:
            raise GateFailure("MessagePush delivery identity 未保留")

        delivery_context["delivery_id"] = "e3-qqbot-delivery-success"
        qqbot_success = json.loads(
            await push.execute(
                target_channel="qqbot",
                target_chat_id="recording-chat",
                message="recording qqbot success",
            )
        )
        if (
            qqbot_success["status"] != "delivered"
            or qqbot_success["retryable"] is not False
            or qqbot_success["delivery_id"] != delivery_context["delivery_id"]
        ):
            raise GateFailure(f"QQBot MessagePush receipt 错误: {qqbot_success}")
        if len(factories["qqbot"].effects) != 1:
            raise GateFailure("QQBot provider effect 没有恰好一次提交")

        effects_before_failure = len(factories["feishu"].effects)
        factories["feishu"].fail_after_effect = True
        delivery_context["delivery_id"] = "e3-feishu-delivery-unknown"
        unknown = json.loads(
            await push.execute(
                target_channel="feishu",
                target_chat_id="recording-chat",
                message="recording after-effect failure",
            )
        )
        effects_after_failure = len(factories["feishu"].effects)
        if unknown["status"] != "unknown" or unknown["retryable"] is not False:
            raise GateFailure(f"MessagePush UNKNOWN receipt 错误: {unknown}")
        if effects_after_failure != effects_before_failure + 1:
            raise GateFailure("UNKNOWN provider effect 被盲重试")

        protected_after = _protected_digest(workspace)
        if protected_after != protected_before:
            raise GateFailure("channel candidate/promotion 改写了受保护 workspace 树")
        session_digest_after = _tree_digest(workspace / "sessions.db")
        channel_evidence = {
            "stable": {
                "snapshot_id": stable_snapshot_id,
                "generations": stable_generations,
            },
            "candidate": candidate_identity,
            "discard": discard_state,
            "promoted": {
                "publication": publication,
                "snapshot_id": promoted.snapshot_id,
                "generation": _generation_identity(manager, "feishu"),
                "channel_catalog_identity": promoted.channel_registry.identity
                if promoted.channel_registry is not None
                else None,
            },
            "loopback": loopback,
            "protected_workspace_digest": protected_after,
            "session_db_digest_after_inbound": session_digest_after,
        }
        message_push_evidence = {
            "success": success,
            "qqbot_success": qqbot_success,
            "unknown": unknown,
            "recorded_effects": [
                asdict(effect)
                for factory in factories.values()
                for effect in factory.effects
            ],
        }
    finally:
        try:
            for envelope in tuple(inbound_owned):
                if envelope.owner is InboundOwner.BUS:
                    await envelope.close(InboundOwner.BUS)
            await manager.terminate_all()
        finally:
            channel_generation_host._resolve_sync_factory = original_resolver

    host = manager.channel_generation_host
    cleanup = {
        "active_channel_generation": manager.active_channel_generation is None,
        "channel_tombstones": bool(host.failure(promoted.snapshot_id))
        if promoted is not None
        else False,
        "manager_cleanup_failures": [str(item) for item in manager.cleanup_failures],
        "workspace_protected_digest_unchanged": protected_after == protected_before,
    }
    if (
        not cleanup["active_channel_generation"]
        or cleanup["channel_tombstones"]
        or cleanup["manager_cleanup_failures"]
    ):
        raise GateFailure(f"channel terminate cleanup 失败: {cleanup}")
    return {
        "channel": channel_evidence,
        "message_push": message_push_evidence,
        "cleanup": cleanup,
    }


def _build_push_dispatcher(
    manager: PluginManager,
    channel: str | None,
    delivery_context: dict[str, str],
) -> Callable[[object, bool], Awaitable[ChannelDeliveryReceipt]]:
    """Convert MessagePush into the exact stable Host binding and awaited receipt."""

    async def dispatch(message: object, _passive: bool) -> ChannelDeliveryReceipt:
        source = manager.snapshot_store.lease()
        selected_channel = channel or str(getattr(message, "channel", ""))
        if selected_channel not in CHANNEL_PLUGIN_IDS:
            raise GateFailure(f"MessagePush Gate 渠道不在 exact catalog: {selected_channel}")
        binding = manager.channel_generation_host.acquire_binding(
            source,
            selected_channel,
        )
        delivery_id = delivery_context.get("delivery_id")
        if not delivery_id:
            raise GateFailure("MessagePush Gate 缺少 deterministic delivery id")
        try:
            if getattr(message, "attachments", ()):
                return ChannelDeliveryReceipt(
                    delivery_id,
                    DeliveryStatus.REJECTED,
                    error="E3 recording text-only channel rejects attachments",
                )
            envelope = OutboundEnvelope(
                logical_delivery_id=delivery_id,
                delivery_id=delivery_id,
                attempt_sequence=1,
                snapshot_id=binding.snapshot_id,
                generation_id=binding.generation_id,
                binding_token=binding.binding_token,
                channel=selected_channel,
                recipient=str(getattr(message, "chat_id")),
                body=str(getattr(message, "content")),
                metadata=cast(
                    Mapping[str, JsonValue],
                    getattr(message, "metadata", {}),
                ),
            )
            return await binding.deliver(envelope)
        finally:
            await binding.aclose()
            await source.release()

    return dispatch


async def _run_passive_webui_scenario(
    providers: Path,
    sandbox: Path,
) -> dict[str, object]:
    """Run Citation/Meme through prompt, reply, media, and Dashboard projections."""

    workspace = sandbox / "passive-workspace"
    image = passive_gate._write_meme_fixture(workspace)
    meme_before = passive_gate._tree_digest(workspace / "memes")
    manager = PluginManager(
        plugin_dirs=[providers],
        event_bus=EventBus(),
        tool_registry=None,
        workspace=workspace,
        installed_cache_root=sandbox / "passive-plugin-home" / "cache",
    )
    root: Any | None = None
    dashboard_host = PluginDashboardHost(core_routes=())
    try:
        await manager.load_all()
        snapshot = manager.current_snapshot
        if snapshot is None or snapshot.composition_root is None:
            raise GateFailure("passive WebUI snapshot/CompositionRoot 缺失")
        root = cast(Any, snapshot.composition_root)
        topology = root.topology_view()
        expected_listeners = passive_gate.EXPECTED_LISTENERS
        if topology.listeners != expected_listeners:
            raise GateFailure(f"passive listener 顺序错误: {topology.listeners}")
        if "citation.protocol" not in topology.services:
            raise GateFailure(f"Citation service 缺失: {topology.services}")
        dashboard_host.prepare_snapshot(snapshot)
        dashboard = passive_gate._assert_dashboard(snapshot, workspace)
        scenario = await passive_gate._run_passive_scenario(root, image)
        meme_after = passive_gate._tree_digest(workspace / "memes")
        if meme_after != meme_before:
            raise GateFailure("passive WebUI 改写了 workspace/memes")
        evidence = {
            "snapshot_id": snapshot.snapshot_id,
            "generation_ids": {
                plugin_id: _generation_identity(manager, plugin_id)
                for plugin_id in PASSIVE_PLUGIN_IDS
            },
            "topology": {
                "identity": topology.identity,
                "revision": topology.composition_revision,
                "listeners": list(topology.listeners),
                "services": list(topology.services),
            },
            "dashboard": asdict(dashboard),
            "scenario": asdict(scenario),
            "memes_before_sha256": meme_before,
            "memes_after_sha256": meme_after,
        }
    finally:
        await manager.terminate_all()
    if root is None:
        raise GateFailure("passive WebUI 没有正式 Root 证据")
    cleanup = {
        "topology_after_dispose": asdict(root.topology_view()),
        "receipt_after_dispose": asdict(root.receipt()),
        "dashboard_bindings_after_dispose": len(dashboard_host._bindings),
    }
    if (
        cleanup["topology_after_dispose"]["fibers"]
        or cleanup["topology_after_dispose"]["listeners"]
        or cleanup["topology_after_dispose"]["effects"]
        or cleanup["dashboard_bindings_after_dispose"]
    ):
        raise GateFailure(f"passive WebUI cleanup 失败: {cleanup}")
    evidence["cleanup"] = cleanup
    return evidence


def _write_channel_config(workspace: Path) -> None:
    """Create synthetic credentials in the disposable workspace only."""

    for plugin_id, content in {
        "feishu": 'appId = "synthetic-feishu-id"\nappSecret = "synthetic-feishu-secret"\n',
        "qqbot": (
            'appId = "synthetic-qq-id"\n'
            'clientSecret = "synthetic-qq-secret"\n'
            'allowFrom = ["recording"]\n'
        ),
    }.items():
        data = workspace / "plugin-data" / f"{plugin_id}-builtin"
        data.mkdir(parents=True, exist_ok=True)
        (data / "config.local.toml").write_text(content, encoding="utf-8")


def _generation_identity(manager: PluginManager, plugin_id: str) -> dict[str, str]:
    generation = manager.generation(plugin_id)
    if generation is None:
        raise GateFailure(f"generation 缺失: {plugin_id}")
    return {
        "generation_id": generation.generation_id,
        "source_revision": generation.source_revision,
    }


def _protected_digest(workspace: Path) -> dict[str, str]:
    return {
        "feishu_config": _tree_digest(
            workspace / "plugin-data" / "feishu-builtin" / "config.local.toml"
        ),
        "qqbot_config": _tree_digest(
            workspace / "plugin-data" / "qqbot-builtin" / "config.local.toml"
        ),
    }


def _scenario_catalog_sha256() -> str:
    encoded = json.dumps(
        SCENARIO_CATALOG,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _tree_digest(path: Path) -> str:
    """Hash one file or directory with path names and file content."""

    digest = hashlib.sha256()
    if not path.exists():
        digest.update(b"<missing>")
        return digest.hexdigest()
    if path.is_file():
        digest.update(b"f:")
        digest.update(path.name.encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        return digest.hexdigest()
    for item in sorted(path.rglob("*")):
        relative = item.relative_to(path).as_posix()
        kind = "d" if item.is_dir() else "f"
        digest.update(f"{kind}:{relative}\0".encode())
        if item.is_file():
            digest.update(item.read_bytes())
    return digest.hexdigest()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_output(*args: str) -> str:
    completed = subprocess.run(
        ("git", *args),
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return completed.stdout.strip()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    raise SystemExit(main())
