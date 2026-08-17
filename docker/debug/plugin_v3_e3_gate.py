from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agent.plugins import channel_generation_host  # noqa: E402
from agent.plugins.dashboard_host import PluginDashboardHost  # noqa: E402
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
from agent.tools.message_push import MessagePushTool  # noqa: E402
from bus.event_bus import EventBus  # noqa: E402
from docker.debug import plugin_passive_composition_v3_gate as passive_gate  # noqa: E402
from docker.debug import plugin_v3_fleet_gate as fleet_gate  # noqa: E402
from session.manager import SessionManager  # noqa: E402


DEFAULT_REPORT = ROOT / "docker/debug/reports/plugin-v3-e3" / "gate.json"
GATE_VERSION = 1
SCENARIO_PROFILE = "plugin-v3-e3-channel-message-push-passive-webui-v1"
CHANNEL_PLUGIN_IDS = ("feishu", "qqbot")
PASSIVE_PLUGIN_IDS = ("citation", "meme")
GATE_SCOPE = {
    "exact_plugins": CHANNEL_PLUGIN_IDS + PASSIVE_PLUGIN_IDS,
    "provider": "synthetic recording HTTP/WS adapter; no external delivery",
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
)


class GateFailure(RuntimeError):
    """Represent an E3 evidence or invariant failure."""


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
        with tempfile.TemporaryDirectory(prefix="akashic-plugin-v3-e3-") as raw:
            sandbox_path = Path(raw)
            report["runtime"] = asyncio.run(_run_runtime(sandbox_path))
        report["cleanup"] = {
            "sandbox_removed": sandbox_path is not None and not sandbox_path.exists(),
            "residuals": [],
        }
        cleanup = cast(dict[str, object], report["cleanup"])
        if cleanup["sandbox_removed"] is not True:
            raise GateFailure("E3 sandbox cleanup 未完成")
        report["status"] = "passed"
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
        print(f"plugin v3 E3 gate failed: {error_text}", file=sys.stderr)
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


async def _run_runtime(sandbox: Path) -> dict[str, object]:
    """Checkout exact sources and run the three E3 scenarios in one sandbox."""

    # 1. Freeze exact source identities before creating any runtime state.
    locked = {
        item.id: item
        for item in fleet_gate._load_lock(fleet_gate.DEFAULT_LOCK)
        if item.id in CHANNEL_PLUGIN_IDS + PASSIVE_PLUGIN_IDS
    }
    expected_ids = CHANNEL_PLUGIN_IDS + PASSIVE_PLUGIN_IDS
    if set(locked) != set(expected_ids):
        raise GateFailure(f"E3 exact plugin 集合错误: {tuple(locked)}")
    locks = {plugin_id: locked[plugin_id] for plugin_id in expected_ids}
    providers = sandbox / "providers"
    channel_providers = providers / "channels"
    passive_providers = providers / "passive"
    channel_providers.mkdir(parents=True)
    passive_providers.mkdir(parents=True)
    source_evidence: list[dict[str, object]] = []
    for plugin_id in locks:
        checkout_root = (
            channel_providers
            if plugin_id in CHANNEL_PLUGIN_IDS
            else passive_providers
        )
        checkout = checkout_root / plugin_id
        evidence = fleet_gate._checkout_locked_plugin(locks[plugin_id], checkout)
        source_evidence.append(asdict(evidence))
    _prepare_channel_checkouts(channel_providers)

    # 2. Exercise formal channel publication, MessagePush, and passive Dashboard.
    channel = await _run_channel_scenario(channel_providers, sandbox)
    passive = await _run_passive_webui_scenario(passive_providers, sandbox)
    return {
        "sources": source_evidence,
        "channel": channel["channel"],
        "message_push": channel["message_push"],
        "channel_cleanup": channel["cleanup"],
        "passive_webui": passive,
    }


def _prepare_channel_checkouts(providers: Path) -> None:
    """Add only the installed-runtime marker required by the channel manifests."""

    marker = providers / "feishu" / ".venv" / "bin" / "python"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.symlink_to(sys.executable)


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
        await cast(Any, envelope).close(InboundOwner.INGRESS)

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
