from __future__ import annotations

import asyncio
import copy
import functools
import hashlib
import importlib.util
import inspect
import json
import logging
import os
import secrets
import shutil
import socket
import sys
import tomllib
from dataclasses import dataclass, field, replace
from pathlib import Path, PurePosixPath, PureWindowsPath
from types import MappingProxyType, ModuleType, UnionType
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Literal, TypeVar, Union, cast, get_args, get_origin
from urllib.parse import urlsplit, urlunsplit

from pydantic import AliasChoices, AliasPath, BaseModel, ValidationError

from agent.plugin_composition import (
    CHANNELS,
    COMMANDS,
    MANAGED_PROCESSES,
    MCP_SERVERS,
    MEMORY_RUNTIME,
    MEMORY_TURN_RUNTIME,
    INTERACTION_UNDO,
    PROACTIVE_COMPONENTS,
    SESSION_READ,
    BACKGROUND_JOBS,
    UI_SLOTS,
    CommandRegistry,
    CompositionRoot,
    CredentialRef,
    FiberState,
    MemoryRuntimeInfo,
    MemoryTurnRuntime,
    InteractionUndoService,
    PluginChannels,
    PluginUiSlots,
    PluginCommands,
    PluginProactiveComponents,
    PluginBackgroundJobs,
    PluginRuntime,
    SessionReadService,
    resolve_mobile_ui_asset,
    ServiceView,
)
from core.memory.plugin import MemoryTurnRuntimeApi
from agent.plugin_composition.channels import (
    ChannelRegistrySnapshot,
    ProviderClientFactory,
)
from agent.plugin_composition.mcp_slots import PluginMcpServers
from agent.plugin_composition.process_slots import PluginManagedProcesses
from agent.plugin_composition.model import resolve_declared_workspace_root
from agent.plugins.composable import ComposablePlugin
from agent.plugins.interaction_undo import InteractionUndoCoordinator
from agent.plugins.composition_generation_host import (
    CompositionGenerationHost,
    CompositionRuntimeFailure,
    CompositionRuntimeGeneration,
)
from agent.plugins.channel_generation_host import (
    ChannelCleanupTombstone,
    ChannelGeneration,
    ChannelGenerationHost,
    ChannelStartRecord,
)
from agent.plugins.channel_credentials import CoreProviderClientFactory

from agent.plugins.manifest import (
    ensure_workspace_plugin_data_dir,
    load_package_manifest,
    load_plugin_manifest,
    plugins_root,
    validate_workspace_plugin_data_path,
    workspace_plugin_data_dir,
    write_package_manifest,
    write_plugin_manifest,
)
from agent.plugins.packages import (
    _select_enabled_plugin_packages,  # pyright: ignore[reportPrivateUsage]
    discover_plugin_packages,
)
from agent.plugins.specs import (
    ManagedServiceSpec,
    McpServerSpec,
    MobileUiContribution,
    ProactiveSourceSpec,
    RegisteredProactiveSource,
)
from agent.lifecycle.types import (
    AfterReasoningCtx,
    AfterStepCtx,
    AfterToolResultCtx,
    AfterTurnCtx,
    BeforeReasoningCtx,
    BeforeStepCtx,
    BeforeToolCallCtx,
    BeforeTurnCtx,
    PreToolCtx,
    PromptRenderCtx,
)
from infra.channels.base import SessionIdentityIndex
from infra.channels.artifacts import ChannelAttachmentArtifactStore
from agent.plugins.registry import MetadataKind, PluginEventType, plugin_registry
from agent.plugins.artifacts import (
    ArtifactPointer,
    ArtifactSelector,
    discard_latest_pointer,
    pointer_state_path,
    read_pointer,
    read_pointers,
    relative_artifact_pointer,
    resolve_pointer,
    write_pointers,
)
from agent.plugins.source_resolver import resolve_plugin_sources
from agent.plugins.jobs import (
    IntervalTrigger,
    PluginJobSpec,
    PluginLlmService,
    RegisteredPluginJob,
)
from agent.plugins.scope import CleanupFailure, PluginScope, ScopedEventBus
from agent.plugins.generation import (
    GateCheckResult,
    GateResult,
    MobileUiAsset,
    PluginContributions,
    PluginGeneration,
    PluginReadinessContext,
    PluginSemanticCheck,
)
from agent.plugins.importer import FreshPluginImporter
from agent.plugins.install import PluginInstallResult, install_git_plugin
from agent.plugins.static_manifest import (
    StaticPluginManifest,
    load_static_plugin_manifest,
    materialize_static_command,
    staged_python_interpreter,
    validate_module_exports,
)
from agent.plugins.reload_journal import (
    RecoveryActionName,
    RecoveryTarget,
    ReloadJournal,
    ReloadPhase,
    ReloadRecoveryAction,
)
from agent.plugins.skill_host import PluginSkillHost, PreparedSkillCatalog
from agent.mcp.generation import WorkspaceMcpGeneration
from agent.mcp.host import McpGenerationHost, PreparedMcpCatalog
from agent.plugins.activity_host import (
    PluginJobHost,
    PluginProactiveHost,
    PreparedJobCatalog,
    PreparedProactiveCatalog,
)
from agent.plugins.generation_activity_host import (
    ActivityCatalog,
    ActivityHost,
    ActivityTransaction,
)
from agent.plugins.private_proactive import (
    PRIVATE_PROACTIVE_DEFINITIONS,
    admit_private_proactive_module,
    build_private_proactive_catalog,
)
from agent.plugins.snapshot import (
    RuntimeSnapshot,
    RuntimeSnapshotCompiler,
    RuntimeSnapshotStore,
    SnapshotTransaction,
    get_current_runtime_snapshot,
    plugin_is_active,
)
from proactive_v2.lifecycle import ProactiveLifecycleSpec
from proactive_v2.lifecycle import ProactiveLifecycleBuilder
from agent.tool_hooks.base import ToolHook
from agent.tool_hooks.types import HookContext, HookOutcome
from bus.event_bus import EventBus
from infra.channels.contract import Channel
from infra.persistence.json_store import atomic_save_json

logger = logging.getLogger(__name__)
_UNRESOLVED_MEMORY_RUNTIME = object()
U = TypeVar("U")


def _generation_can_write(generation: PluginGeneration) -> bool:
    if generation.state in {"activating", "active"}:
        return True
    snapshot = get_current_runtime_snapshot()
    return (
        snapshot is not None
        and snapshot.generations.get(generation.plugin_id) is generation
    )


def _package_project_root(plugin_dirs: list[Path]) -> Path | None:
    for plugin_dir in plugin_dirs:
        root = plugin_dir.parent if plugin_dir.name == "plugins" else None
        if root is not None and (root / "plugin_packages").is_dir():
            return root
    return None


async def _complete_critical(awaitable: Awaitable[U]) -> tuple[U, bool]:
    """在外部取消后完成关键异步操作，并返回是否收到取消。"""

    # 1. 将关键操作放入独立任务，避免调用方取消传播进去
    task = asyncio.ensure_future(awaitable)
    cancelled = False

    # 2. 屏蔽等待并记录外部取消，直到操作本身结束
    while not task.done():
        try:
            _ = await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True

    # 3. 读取操作结果，保留其真实异常
    result = await task
    return result, cancelled


_EVENT_TYPE_MAP: dict[PluginEventType, type] = {
    PluginEventType.BEFORE_TURN: BeforeTurnCtx,
    PluginEventType.BEFORE_REASONING: BeforeReasoningCtx,
    PluginEventType.PROMPT_RENDER: PromptRenderCtx,
    PluginEventType.BEFORE_STEP: BeforeStepCtx,
    PluginEventType.AFTER_STEP: AfterStepCtx,
    PluginEventType.AFTER_REASONING: AfterReasoningCtx,
    PluginEventType.AFTER_TURN: AfterTurnCtx,
    PluginEventType.BEFORE_TOOL_CALL: BeforeToolCallCtx,
    PluginEventType.AFTER_TOOL_RESULT: AfterToolResultCtx,
}


@dataclass(frozen=True)
class ActivePluginInfo:
    plugin_id: str
    plugin_dir: Path
    manifest: dict[str, object]
    module_path: str
    skill_roots: tuple[Path, ...] = ()
    drift_skill_roots: tuple[Path, ...] = ()
    mcp_servers: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass(frozen=True)
class _ReadyPluginCandidate:
    plugin_id: str
    previous: PluginGeneration | None
    candidate: PluginGeneration
    snapshot: RuntimeSnapshot


class _PublicationParticipantSwitchError(RuntimeError):
    """Report a forward participant switch rejected before publication opened."""


class _PublicationParticipantRestoreError(RuntimeError):
    """Keep the old snapshot closed when an external owner cannot be restored."""

    def __init__(self, message: str, *, resources: tuple[str, ...]) -> None:
        super().__init__(message)
        self.resources = resources


@dataclass
class _ChannelPublicationState:
    previous: RuntimeSnapshot | None
    candidate: RuntimeSnapshot
    previous_identity: str | None
    candidate_identity: str | None
    old_runtime: ChannelGeneration | None
    old_factories: Mapping[str, ProviderClientFactory]
    new_factories: Mapping[str, ProviderClientFactory]
    changed: bool
    old_closed: bool = False
    old_stopped: bool = False
    new_runtime: ChannelGeneration | None = None


class PluginManager:
    POST_PUBLISH_TIMEOUT_SECONDS = 5.0

    def __init__(
        self,
        plugin_dirs: list[Path],
        *,
        event_bus: EventBus,
        workspace: Path,
        tool_registry: Any = None,
        session_manager: Any = None,
        memory_engine: Any = None,
        llm: PluginLlmService | None = None,
        installed_cache_root: Path | None = None,
        channel_attachment_store: ChannelAttachmentArtifactStore | None = None,
    ) -> None:
        self._dirs = plugin_dirs
        self._event_bus = event_bus
        self._tool_registry = tool_registry
        self._workspace = workspace
        self._session_manager = session_manager
        self._memory_engine = memory_engine
        self._interaction_undo = (
            InteractionUndoCoordinator(session_manager, memory_engine)
            if session_manager is not None and memory_engine is not None
            else None
        )
        self._composition_memory_runtime: MemoryRuntimeInfo | None | object = (
            _UNRESOLVED_MEMORY_RUNTIME if memory_engine is not None else None
        )
        self._llm = llm
        self._installed_cache_root = installed_cache_root
        self._channel_switcher: (
            Callable[
                [str, tuple[Channel, ...], tuple[Channel, ...]],
                Awaitable[None],
            ]
            | None
        ) = None
        self._dashboard_preparer: Callable[[RuntimeSnapshot], None] | None = None
        self._dashboard_validation_releaser: (
            Callable[[RuntimeSnapshot], Awaitable[None]] | None
        ) = None
        self._service_switcher: (
            Callable[
                [str, dict[str, dict[str, Any]], dict[str, dict[str, Any]]],
                Awaitable[None],
            ]
            | None
        ) = None
        self._candidate_service_starter: (
            Callable[[str, dict[str, dict[str, Any]]], Awaitable[None]] | None
        ) = None
        self._candidate_service_stopper: Callable[[str], Awaitable[None]] | None = None
        self._candidate_service_health_check: (
            Callable[[str], Awaitable[None]] | None
        ) = None
        self._endpoint_quiescer: Callable[[], Awaitable[None]] | None = None
        self._endpoint_resumer: Callable[[], Awaitable[None]] | None = None
        self._endpoint_switcher: (
            Callable[
                [
                    str,
                    dict[str, dict[str, Any]],
                    dict[str, dict[str, Any]],
                    tuple[Channel, ...],
                    tuple[Channel, ...],
                    tuple[tuple[str, str], ...],
                    tuple[tuple[str, str], ...],
                ],
                Awaitable[None],
            ]
            | None
        ) = None
        self._loaded: set[str] = set()
        self._channels: list[Channel] = []
        # V2_REMOVAL(tool-hooks)：迁移完外部 consumer 后删除可变 catalog。
        self._tool_hooks: list[ToolHook] = []
        self._before_turn_modules: list[object] = []
        self._before_reasoning_modules: list[object] = []
        self._prompt_render_modules: list[object] = []
        self._before_step_modules: list[object] = []
        self._after_step_modules: list[object] = []
        self._after_reasoning_modules: list[object] = []
        self._after_turn_modules: list[object] = []
        self._proactive_modules: list[object] = []
        self._proactive_lifecycles: list[object] = []
        self._proactive_module_factories: list[object] = []
        self._proactive_runtime_factories: list[object] = []
        self._proactive_sources: list[RegisteredProactiveSource] = []
        self._jobs: list[RegisteredPluginJob] = []
        self._active_plugins: dict[str, ActivePluginInfo] = {}
        self._scopes: dict[str, PluginScope] = {}
        self._cleanup_failures: list[CleanupFailure] = []
        self._active_generations: dict[str, PluginGeneration] = {}
        self._draining_generations: dict[str, list[PluginGeneration]] = {}
        self._prepared_generations: dict[str, PluginGeneration] = {}
        self._ready_candidate: _ReadyPluginCandidate | None = None
        self._gate_results: dict[str, GateResult] = {}
        self._stable_aliases: dict[str, str] = {}
        self._generation_sequence = 0
        self._composition_pending: tuple[str, ...] = ()
        self._candidate_prepare_lock = asyncio.Lock()
        self._fresh_importer = FreshPluginImporter()
        self._manager_namespace = secrets.token_hex(4)
        self._skill_host = PluginSkillHost(workspace)
        self._mcp_host = McpGenerationHost()
        self._composition_runtime_generations: dict[str, PluginGeneration] = {}
        self._composition_generation_host = CompositionGenerationHost(
            on_failure=self._on_composition_runtime_failure,
        )
        self._active_workspace_mcp: WorkspaceMcpGeneration | None = None
        self._prepared_workspace_mcp: WorkspaceMcpGeneration | None = None
        self._job_host = PluginJobHost()
        self._proactive_host = PluginProactiveHost()
        self._snapshot_compiler = RuntimeSnapshotCompiler()
        self._snapshot_store = RuntimeSnapshotStore(self._on_snapshot_drained)
        self._snapshot_skill_catalogs: dict[str, str] = {}
        self._reload_journal = ReloadJournal(workspace)
        self._channel_provider_factory_resolver: (
            Callable[
                [RuntimeSnapshot],
                Mapping[str, ProviderClientFactory],
            ]
            | None
        ) = self._default_channel_provider_factories
        self._channel_identity_indexes: dict[str, SessionIdentityIndex] = {}
        self._channel_generation_host = ChannelGenerationHost(
            on_before_start=self._reserve_channel_binding,
            config_revision_checker=self._check_channel_config_revision,
            on_failure=self._on_channel_cleanup_failure,
            snapshot_lease_acquirer=self._snapshot_store.lease,
            identity_resolver=self._resolve_channel_identity,
            identity_rememberer=self._remember_channel_identity,
            attachment_import=channel_attachment_store,
            attachment_read=channel_attachment_store,
        )
        self._active_channel_generation: ChannelGeneration | None = None
        self._active_channel_catalog_identity: str | None = None
        self._channel_boot_transactions: set[str] = set()
        self._activity_host: ActivityHost | None = None
        self._drain_transactions: dict[str, str] = {}
        self._drained_before_commit: set[str] = set()
        self._event_bus.bind_runtime_snapshot_store(self._snapshot_store)

    @property
    def loaded_count(self) -> int:
        return len(self._loaded)

    @property
    def tool_hooks(self) -> list[ToolHook]:
        if self.current_snapshot is not None:
            return list(self.current_snapshot.tool_hooks)
        return list(self._tool_hooks)

    # V2_REMOVAL(plugin-manager-projections)：channels/phase/proactive/jobs 是 v2 固定贡献的
    # Manager 投影。每个族群迁入 stable Root capability/catalog 且 bootstrap consumer 改读新
    # owner 后，删除下面的属性与 mutable fallback。
    @property
    def channels(self) -> list[Channel]:
        if self.current_snapshot is not None:
            return list(self.current_snapshot.channels.values())
        return list(self._channels)

    @property
    def before_turn_modules(self) -> list[object]:
        if self.current_snapshot is not None:
            return list(self.current_snapshot.before_turn_modules)
        return list(self._before_turn_modules)

    @property
    def before_reasoning_modules(self) -> list[object]:
        if self.current_snapshot is not None:
            return list(self.current_snapshot.before_reasoning_modules)
        return list(self._before_reasoning_modules)

    @property
    def prompt_render_modules(self) -> list[object]:
        if self.current_snapshot is not None:
            return list(self.current_snapshot.prompt_render_modules)
        return list(self._prompt_render_modules)

    @property
    def before_step_modules(self) -> list[object]:
        if self.current_snapshot is not None:
            return list(self.current_snapshot.before_step_modules)
        return list(self._before_step_modules)

    @property
    def after_step_modules(self) -> list[object]:
        if self.current_snapshot is not None:
            return list(self.current_snapshot.after_step_modules)
        return list(self._after_step_modules)

    @property
    def after_reasoning_modules(self) -> list[object]:
        if self.current_snapshot is not None:
            return list(self.current_snapshot.after_reasoning_modules)
        return list(self._after_reasoning_modules)

    @property
    def after_turn_modules(self) -> list[object]:
        if self.current_snapshot is not None:
            return list(self.current_snapshot.after_turn_modules)
        return list(self._after_turn_modules)

    @property
    def proactive_modules(self) -> list[object]:
        if self.current_snapshot is not None:
            return list(self.current_snapshot.proactive_modules)
        return list(self._proactive_modules)

    @property
    def proactive_lifecycles(self) -> list[object]:
        if self.current_snapshot is not None:
            return list(self.current_snapshot.proactive_lifecycles)
        return list(self._proactive_lifecycles)

    @property
    def proactive_module_factories(self) -> list[object]:
        if self.current_snapshot is not None:
            return list(self.current_snapshot.proactive_module_factories)
        return list(self._proactive_module_factories)

    @property
    def proactive_runtime_factories(self) -> list[object]:
        if self.current_snapshot is not None:
            return list(self.current_snapshot.proactive_runtime_factories)
        return list(self._proactive_runtime_factories)

    @property
    def proactive_sources(self) -> list[RegisteredProactiveSource]:
        if self.current_snapshot is not None:
            return list(self.current_snapshot.proactive_sources.values())
        return list(self._proactive_sources)

    @property
    def jobs(self) -> list[RegisteredPluginJob]:
        if self.current_snapshot is not None:
            return list(self.current_snapshot.jobs.values())
        return list(self._jobs)

    @property
    def llm(self) -> PluginLlmService | None:
        return self._llm

    @property
    def plugin_dirs(self) -> list[Path]:
        return list(self._dirs)

    @property
    def skill_projection_roots(self) -> list[Path]:
        roots = self.plugin_dirs
        if self._installed_cache_root is not None:
            roots.append(self._installed_cache_root)
        return roots

    def sync_skill_links(self):
        """Rebuild workspace links from the active stable plugin generations."""

        from agent.plugins.skill_links import PluginSkillLinker

        return PluginSkillLinker(
            workspace=self._workspace,
            plugin_roots=self.skill_projection_roots,
            memory_engine=self._memory_engine,
        ).sync(self.active_plugins())

    def _prepare_skill_links_for_promotion(
        self,
        generation: PluginGeneration,
        candidate_snapshot: RuntimeSnapshot,
    ) -> tuple[Any, list[ActivePluginInfo], list[ActivePluginInfo]]:
        """Build and validate both sides of the stable skill projection switch."""

        from agent.plugins.skill_links import PluginSkillLinker

        contributions = generation.production_contributions or generation.contributions
        plugin_dir = generation.plugin_dir.resolve(strict=False)
        target = ActivePluginInfo(
            plugin_id=generation.plugin_id,
            plugin_dir=plugin_dir,
            manifest=contributions.manifest,
            module_path=generation.module_path,
            skill_roots=contributions.skill_roots,
            drift_skill_roots=contributions.drift_skill_roots,
            mcp_servers=contributions.mcp_servers,
        )
        stable = self.active_plugins()
        post_promotion = [
            plugin
            for plugin in stable
            if plugin.plugin_id != generation.plugin_id
        ]
        if any(
            item is generation for item in candidate_snapshot.active_generations()
        ):
            post_promotion.append(target)
        linker = PluginSkillLinker(
            workspace=self._workspace,
            plugin_roots=self.skill_projection_roots,
            memory_engine=self._memory_engine,
        )
        linker.validate(post_promotion)
        return linker, stable, post_promotion

    def active_plugins(self) -> list[ActivePluginInfo]:
        return [
            self._active_plugins[generation.module_path]
            for generation in self._active_generations.values()
            if self._registry_active(generation.module_path)
        ]

    @property
    def cleanup_failures(self) -> list[CleanupFailure]:
        return list(self._cleanup_failures)

    def generation(self, plugin_id: str) -> PluginGeneration | None:
        return self._active_generations.get(plugin_id)

    def latest_gate(self, plugin_id: str) -> GateResult | None:
        return self._gate_results.get(plugin_id)

    def prepared_generation(self, plugin_id: str) -> PluginGeneration | None:
        return self._prepared_generations.get(plugin_id)

    def skill_catalog(self, generation_id: str) -> PreparedSkillCatalog | None:
        return self._skill_host.get(generation_id)

    def mcp_catalog(self, generation_id: str) -> PreparedMcpCatalog | None:
        return self._mcp_host.get(generation_id)

    @property
    def active_workspace_mcp(self) -> WorkspaceMcpGeneration | None:
        return self._active_workspace_mcp

    @property
    def prepared_workspace_mcp(self) -> WorkspaceMcpGeneration | None:
        return self._prepared_workspace_mcp

    def assert_no_workspace_mcp_plugin_conflicts(self) -> None:
        """拒绝启动扫描中发现的 workspace/plugin MCP 名称冲突。"""

        workspace = self._active_workspace_mcp
        if workspace is None:
            return
        names = set(workspace.catalog.servers)
        conflicts: list[str] = []
        for plugin_id, gate in self._gate_results.items():
            for check in gate.checks:
                if check.check_id != "mcp_servers" or check.status != "failed":
                    continue
                evidence = check.evidence
                if isinstance(evidence, list) and names.intersection(
                    item for item in evidence if isinstance(item, str)
                ):
                    conflicts.append(plugin_id)
        if conflicts:
            raise RuntimeError(
                "workspace MCP 与插件声明冲突: " + ", ".join(sorted(conflicts))
            )

    async def prepare_workspace_mcp(
        self,
        server_specs: dict[str, dict[str, Any]],
        *,
        revision: str,
    ) -> WorkspaceMcpGeneration:
        """准备 workspace MCP 候选，不改变当前运行快照。"""

        async with self._candidate_prepare_lock:
            await self._discard_workspace_mcp_candidate()
            self._check_workspace_mcp_name_conflicts(server_specs)

            # 1. 完整连接候选 catalog，任何失败都回收候选作用域
            self._generation_sequence += 1
            generation_id = (
                f"workspace-mcp:{self._generation_sequence}:{secrets.token_hex(4)}"
            )
            scope = PluginScope("workspace-mcp")
            try:
                catalog = await self._mcp_host.prepare(
                    generation_id,
                    server_specs=server_specs,
                    required_tools={},
                    scope=scope,
                )
                scope.defer(
                    "mcp_catalog",
                    lambda: self._mcp_host.close(generation_id),
                )
            except BaseException:
                cleanup_failures, _ = await _complete_critical(scope.aclose())
                self._cleanup_failures.extend(cleanup_failures)
                raise

            # 2. 基于锁内最新插件 generations 编译候选 snapshot
            generation = WorkspaceMcpGeneration(
                generation_id=generation_id,
                revision=revision,
                scope=scope,
                catalog=catalog,
            )
            try:
                generation.runtime_snapshot = self._compile_workspace_mcp_snapshot(
                    generation
                )
            except BaseException:
                await self._dispose_workspace_mcp(generation, state="rejected")
                raise
            self._prepared_workspace_mcp = generation
            return generation

    async def publish_workspace_mcp(self) -> WorkspaceMcpGeneration:
        """原子发布 workspace MCP 候选，并让旧代际随 lease 排空。"""

        async with self._candidate_prepare_lock:
            generation = self._prepared_workspace_mcp
            if generation is None or generation.runtime_snapshot is None:
                raise RuntimeError("workspace MCP 没有可发布候选")
            try:
                self._check_workspace_mcp_name_conflicts(generation.catalog.servers)
                snapshot = self._compile_workspace_mcp_snapshot(generation)
                generation.runtime_snapshot = snapshot
                self._validate_workspace_mcp_generation(generation)
            except BaseException:
                await self._discard_workspace_mcp_candidate()
                raise

            # 1. 首个快照直接安装；已有快照使用可回滚发布事务
            previous = self._active_workspace_mcp
            if self._snapshot_store.current is None:
                self._snapshot_store.install(snapshot)
            else:
                transaction = self._snapshot_store.begin_publish(snapshot)
                try:
                    await self._post_snapshot_invariants(snapshot)
                except BaseException:
                    self._prepared_workspace_mcp = None
                    await _complete_critical(self._snapshot_store.abort(transaction))
                    raise
                self._active_workspace_mcp = generation
                self._prepared_workspace_mcp = None
                generation.state = "active"
                _, commit_cancelled = await _complete_critical(
                    self._snapshot_store.commit(transaction)
                )
                self._mcp_host.mark_active(generation.generation_id)
                if previous is not None:
                    self._mcp_host.mark_draining(previous.generation_id)
                if commit_cancelled:
                    raise asyncio.CancelledError
                return generation

            self._active_workspace_mcp = generation
            self._prepared_workspace_mcp = None
            generation.state = "active"
            self._mcp_host.mark_active(generation.generation_id)
            return generation

    async def discard_workspace_mcp_candidate(self) -> None:
        async with self._candidate_prepare_lock:
            await self._discard_workspace_mcp_candidate()

    async def _discard_workspace_mcp_candidate(self) -> None:
        generation = self._prepared_workspace_mcp
        self._prepared_workspace_mcp = None
        if generation is None:
            return
        await self._dispose_workspace_mcp(generation, state="discarded")

    def _check_workspace_mcp_name_conflicts(
        self,
        server_specs: Mapping[str, object],
    ) -> None:
        occupied = {
            server_name
            for generation in self._active_generations.values()
            for server_name in generation.contributions.mcp_servers
        }
        occupied.update(
            server_name
            for generation in self._prepared_generations.values()
            for server_name in generation.contributions.mcp_servers
        )
        conflicts = sorted(occupied.intersection(server_specs))
        if conflicts:
            raise RuntimeError(
                f"workspace MCP 与插件 server 名称冲突: {', '.join(conflicts)}"
            )

    def bind_channel_switcher(
        self,
        switcher: Callable[
            [str, tuple[Channel, ...], tuple[Channel, ...]],
            Awaitable[None],
        ],
    ) -> None:
        self._channel_switcher = switcher

    def bind_dashboard_preparer(
        self,
        preparer: Callable[[RuntimeSnapshot], None],
        *,
        validation_releaser: Callable[[RuntimeSnapshot], Awaitable[None]],
    ) -> None:
        self._dashboard_preparer = preparer
        self._dashboard_validation_releaser = validation_releaser

    def bind_service_switcher(
        self,
        switcher: Callable[
            [str, dict[str, dict[str, Any]], dict[str, dict[str, Any]]],
            Awaitable[None],
        ],
    ) -> None:
        self._service_switcher = switcher

    def bind_candidate_service_host(
        self,
        *,
        start: Callable[[str, dict[str, dict[str, Any]]], Awaitable[None]],
        stop: Callable[[str], Awaitable[None]],
        assert_healthy: Callable[[str], Awaitable[None]],
    ) -> None:
        """Bind isolated managed-service ownership for validation candidates."""

        self._candidate_service_starter = start
        self._candidate_service_stopper = stop
        self._candidate_service_health_check = assert_healthy

    async def wait_mcp_fatal_failure(self) -> None:
        """Escalate exhausted active MCP recovery to the runtime owner."""

        await self._mcp_host.wait_fatal_failure()

    def bind_endpoint_admission(
        self,
        *,
        quiesce: Callable[[], Awaitable[None]],
        resume: Callable[[], Awaitable[None]],
    ) -> None:
        self._endpoint_quiescer = quiesce
        self._endpoint_resumer = resume

    def bind_endpoint_switcher(
        self,
        switcher: Callable[
            [
                str,
                dict[str, dict[str, Any]],
                dict[str, dict[str, Any]],
                tuple[Channel, ...],
                tuple[Channel, ...],
                tuple[tuple[str, str], ...],
                tuple[tuple[str, str], ...],
            ],
            Awaitable[None],
        ],
    ) -> None:
        self._endpoint_switcher = switcher

    def bind_channel_provider_factory_resolver(
        self,
        resolver: Callable[
            [RuntimeSnapshot],
            Mapping[str, ProviderClientFactory],
        ],
    ) -> None:
        """Bind Core's formal-only provider factory projection."""

        if not callable(resolver):
            raise TypeError("channel provider factory resolver 必须可调用")
        self._channel_provider_factory_resolver = resolver

    def bind_activity_host(self, host: ActivityHost) -> None:
        """Bind the single Core owner for proactive and background activity."""

        if self._activity_host is not None:
            raise RuntimeError("ActivityHost 已绑定")
        self._activity_host = host

    @staticmethod
    def _activity_catalog_identity(snapshot: RuntimeSnapshot | None) -> str | None:
        if snapshot is None:
            return None
        proactive = snapshot.proactive_component_catalog
        jobs = snapshot.background_job_catalog
        private = snapshot.private_proactive_catalog
        if proactive is None and jobs is None and private is None:
            return None
        descriptors = (
            () if proactive is None else proactive.descriptors
        ) + (
            () if jobs is None else jobs.descriptors
        )
        owners = sorted({descriptor.owner for descriptor in descriptors})
        bindings: list[str] = []
        for owner in owners:
            generation = snapshot.generations.get(owner)
            if generation is None:
                raise RuntimeError(f"Activity catalog owner generation 缺失: {owner}")
            bindings.append(
                f"{owner}:{generation.generation_id}:{generation.source_revision}"
            )
        if private is not None:
            for member in private.members:
                bindings.append(
                    f"{member.member}:{member.generation_id}:{member.source_revision}"
                )
        return "|".join(
            (
                "proactive:" + ("" if proactive is None else proactive.identity),
                "jobs:" + ("" if jobs is None else jobs.identity),
                "private-proactive:" + private.identity if private is not None else "private-proactive:",
                "bindings:" + ",".join(bindings),
            )
        )

    def _channel_identity_index(self, channel: str) -> SessionIdentityIndex:
        """Return the Core-owned durable identity index for one channel."""

        current = self._channel_identity_indexes.get(channel)
        if current is not None:
            return current
        if self._session_manager is None:
            raise RuntimeError("v3 Channel identity 需要 SessionManager")
        metadata_key = {
            "feishu": "feishu_open_id",
            "telegram": "username",
            "qq": "user_id",
        }.get(channel, "provider_identity")
        normalizer = str.lower if channel == "telegram" else None
        current = SessionIdentityIndex(
            self._session_manager,
            channel=channel,
            metadata_key=metadata_key,
            normalizer=normalizer,
        )
        _ = current.rebuild()
        self._channel_identity_indexes[channel] = current
        return current

    def _resolve_channel_identity(
        self,
        channel: str,
        provider_identity: str,
    ) -> str | None:
        """Resolve a proactive recipient without exposing SessionManager."""

        return self._channel_identity_index(channel).resolve(provider_identity)

    async def _remember_channel_identity(
        self,
        channel: str,
        provider_identity: str,
        recipient: str,
    ) -> None:
        """Persist identity mapping before accepting the inbound envelope."""

        await self._channel_identity_index(channel).remember(
            provider_identity,
            recipient,
        )

    @property
    def channel_generation_host(self) -> ChannelGenerationHost:
        return self._channel_generation_host

    @property
    def composition_generation_host(self) -> CompositionGenerationHost:
        return self._composition_generation_host

    @property
    def activity_host(self) -> ActivityHost | None:
        return self._activity_host

    @property
    def active_channel_generation(self) -> ChannelGeneration | None:
        """Return the exact committed channel runtime owned by this Manager."""

        return self._active_channel_generation

    @staticmethod
    def _channel_catalog_identity(snapshot: RuntimeSnapshot | None) -> str | None:
        registry = None if snapshot is None else snapshot.channel_registry
        return None if registry is None else registry.identity

    def _channel_provider_factories(
        self,
        snapshot: RuntimeSnapshot | None,
    ) -> Mapping[str, ProviderClientFactory]:
        """Resolve provider factories only for a non-empty frozen catalog."""

        registry = None if snapshot is None else snapshot.channel_registry
        if registry is None or not registry.descriptors:
            return {}
        resolver = self._channel_provider_factory_resolver
        if resolver is None:
            raise RuntimeError("v3 Channel provider factory resolver 尚未绑定")
        factories = resolver(cast(RuntimeSnapshot, snapshot))
        if not isinstance(factories, Mapping):
            raise TypeError("channel provider factory resolver 必须返回 mapping")
        return factories

    @staticmethod
    def _default_channel_provider_factories(
        snapshot: RuntimeSnapshot,
    ) -> Mapping[str, ProviderClientFactory]:
        """Build one formal credential owner for every frozen channel."""

        registry = snapshot.channel_registry
        if registry is None:
            return {}
        result: dict[str, ProviderClientFactory] = {}
        for descriptor in registry.descriptors:
            generation = snapshot.generations.get(descriptor.owner)
            if generation is None:
                raise RuntimeError(
                    f"channel owner generation 缺失: {descriptor.owner}"
                )
            result[descriptor.name] = CoreProviderClientFactory(
                generation.data_dir / "config.local.toml",
                descriptor.credential_paths,
                generation.config_revision,
            )
        return result

    def _prepare_channel_publication(
        self,
        previous: RuntimeSnapshot | None,
        candidate: RuntimeSnapshot,
    ) -> _ChannelPublicationState:
        """Freeze the exact old/new channel owners for one provisional switch."""

        previous_identity = self._channel_catalog_identity(previous)
        candidate_identity = self._channel_catalog_identity(candidate)
        changed = previous_identity != candidate_identity
        old_runtime = self._active_channel_generation
        if changed and previous_identity is not None:
            if (
                old_runtime is None
                or self._active_channel_catalog_identity != previous_identity
                or previous is None
            ):
                raise RuntimeError("旧 stable Channel runtime owner 不一致")
        return _ChannelPublicationState(
            previous=previous,
            candidate=candidate,
            previous_identity=previous_identity,
            candidate_identity=candidate_identity,
            old_runtime=old_runtime,
            old_factories=(
                self._channel_provider_factories(previous) if changed else {}
            ),
            new_factories=(
                self._channel_provider_factories(candidate) if changed else {}
            ),
            changed=changed,
        )

    async def _close_channel_publication(
        self,
        state: _ChannelPublicationState,
    ) -> None:
        """Close, drain, and stop the old runtime before switching endpoints."""

        if not state.changed:
            return
        if state.old_runtime is not None:
            state.old_runtime.close_admission()
            state.old_closed = True
            await state.old_runtime.drain()
            await state.old_runtime.stop()
            state.old_stopped = True
            self._active_channel_generation = None
            self._active_channel_catalog_identity = None

    async def _start_channel_publication(
        self,
        state: _ChannelPublicationState,
    ) -> None:
        """Start the new exact runtime with admission still closed."""

        if not state.changed:
            return
        if state.candidate_identity is not None:
            state.new_runtime = await self._channel_generation_host.start_formal(
                state.candidate,
                state.new_factories,
            )

    def _open_channel_publication(self, state: _ChannelPublicationState) -> None:
        """Publish and open the new exact runtime after the stable pointer moved."""

        if not state.changed:
            return
        self._active_channel_generation = state.new_runtime
        self._active_channel_catalog_identity = state.candidate_identity
        if state.new_runtime is not None:
            state.new_runtime.open_admission()
        self._finish_channel_boot_transactions(state.candidate)

    def _finish_channel_boot_transactions(
        self,
        snapshot: RuntimeSnapshot,
    ) -> None:
        """Finish only journal rows created for a fresh stable Channel boot."""

        registry = snapshot.channel_registry
        owners = set() if registry is None else {
            descriptor.owner for descriptor in registry.descriptors
        }
        for plugin_id in owners:
            generation = snapshot.generations[plugin_id]
            tx_id = generation.reload_tx_id
            if tx_id is None or tx_id not in self._channel_boot_transactions:
                continue
            self._advance_reload(generation, "committed")
            self._advance_reload(generation, "complete")
            self._channel_boot_transactions.remove(tx_id)

    def _abort_channel_boot_transactions(
        self,
        snapshot: RuntimeSnapshot,
        error: BaseException,
    ) -> None:
        """Abort clean fresh-boot rows while preserving cleanup tombstones."""

        for generation in snapshot.generations.values():
            tx_id = generation.reload_tx_id
            if tx_id is None or tx_id not in self._channel_boot_transactions:
                continue
            phase = self._reload_journal.get(tx_id).phase
            if phase not in {"cleanup_failed", "degraded"}:
                self._abort_reload(
                    generation,
                    error=str(error) or type(error).__name__,
                )
            self._channel_boot_transactions.remove(tx_id)

    async def _stop_staged_channel_publication(
        self,
        state: _ChannelPublicationState,
    ) -> None:
        """Stop the staged new runtime before restoring other participants."""

        if not state.changed or state.new_runtime is None:
            return
        await state.new_runtime.stop()

    async def _restore_old_channel_publication(
        self,
        state: _ChannelPublicationState,
    ) -> None:
        """Reconstruct the old runtime after all other owners rolled back."""

        if not state.changed:
            return
        restored = state.old_runtime
        if state.old_stopped and state.previous is not None:
            try:
                restored = await self._channel_generation_host.start_formal(
                    state.previous,
                    state.old_factories,
                    boot_owner="plugin-manager-rollback",
                )
            except BaseException:
                self._active_channel_generation = None
                self._active_channel_catalog_identity = None
                raise
        self._active_channel_generation = restored
        self._active_channel_catalog_identity = state.previous_identity

    def _reopen_restored_channel_publication(
        self,
        state: _ChannelPublicationState,
    ) -> None:
        """Reopen the restored runtime only after the old snapshot is restored."""

        if not state.changed:
            return
        runtime = self._active_channel_generation
        if runtime is not None:
            runtime.open_admission()
        if state.previous is not None:
            self._finish_channel_boot_transactions(state.previous)

    async def _reserve_channel_binding(self, record: ChannelStartRecord) -> None:
        """Persist an exact binding reservation before plugin code can run."""

        generation = self._channel_generation(record.plugin_id, record.generation_id)
        tx_id = self._ensure_runtime_recovery_transaction(generation)
        if self._reload_journal.get(tx_id).phase == "preparing":
            self._reload_journal.advance(tx_id, "prepared")
            self._reload_journal.advance(tx_id, "validating")
            self._reload_journal.advance(tx_id, "commit_started")
            self._channel_boot_transactions.add(tx_id)
        self._reload_journal.annotate(
            tx_id,
            {
                "event": "channel_binding_reserved",
                "snapshot_id": record.snapshot_id,
                "catalog_identity": record.catalog_identity,
                "plugin_id": record.plugin_id,
                "generation_id": record.generation_id,
                "channel_name": record.channel_name,
                "binding_token": record.binding_token,
                "artifact_pointer": record.artifact_pointer,
                "factory_export": record.factory_export,
                "source_revision": record.source_revision,
                "config_revision": record.config_revision,
                "raw_config_revision": record.raw_config_revision,
                "descriptor_digest": record.descriptor_digest,
                "target": record.target,
                "boot_owner": record.boot_owner,
                "attempt": record.attempt,
            },
        )

    async def _check_channel_config_revision(
        self,
        record: ChannelStartRecord,
    ) -> None:
        """Fence formal credential resolution to the frozen raw config bytes."""

        generation = self._channel_generation(record.plugin_id, record.generation_id)
        if str(generation.plugin_dir) != record.artifact_pointer:
            raise RuntimeError("channel artifact pointer 已漂移")
        current_revision = _file_revision(
            generation.data_dir / "config.local.toml"
        )
        if current_revision != record.raw_config_revision:
            raise RuntimeError("channel credential config revision 已漂移")

    async def _on_channel_cleanup_failure(
        self,
        failure: ChannelCleanupTombstone,
    ) -> None:
        """Persist one retained channel binding without touching plugin Fiber state."""

        try:
            generation = self._channel_generation(
                failure.plugin_id,
                failure.generation_id,
            )
        except RuntimeError:
            generation = None
        if generation is None:
            actions = tuple(
                action
                for action in self._reload_journal.pending_recovery()
                if action.plugin_id == failure.plugin_id
                and action.failure_resource
                == f"channel-binding:{failure.binding_token}"
            )
            if len(actions) != 1:
                raise RuntimeError(
                    "channel cleanup failure 缺少 durable exact owner"
                )
            tx_id = actions[0].tx_id
            recovery_target = actions[0].recovery_target
        else:
            tx_id = self._ensure_runtime_recovery_transaction(generation)
            recovery_target = self._composition_recovery_target(
                generation,
                tx_id=tx_id,
            )
        self._reload_journal.advance(
            tx_id,
            "cleanup_failed",
            error=failure.error,
            resource=f"channel-binding:{failure.binding_token}",
            formal_effects=("channel_binding_cleanup_pending",),
            recovery_action="retry_generation_cleanup",
            recovery_target=recovery_target,
            details={
                "event": "channel_binding_cleanup_failed",
                "snapshot_id": failure.snapshot_id,
                "catalog_identity": failure.catalog_identity,
                "channel_name": failure.channel_name,
                "binding_token": failure.binding_token,
                "artifact_pointer": failure.artifact_pointer,
                "factory_export": failure.factory_export,
                "source_revision": failure.source_revision,
                "config_revision": failure.config_revision,
                "raw_config_revision": failure.raw_config_revision,
                "descriptor_digest": failure.descriptor_digest,
                "target": failure.target,
                "boot_owner": failure.boot_owner,
                "attempt": failure.attempt_count,
            },
        )

    def _channel_generation(
        self,
        plugin_id: str,
        generation_id: str,
    ) -> PluginGeneration:
        """Find one exact retained generation without consulting a same-name replacement."""

        candidates: list[PluginGeneration] = []
        for snapshot in (self.current_snapshot, self.latest_snapshot):
            if snapshot is None:
                continue
            snapshot_generation = snapshot.generations.get(plugin_id)
            if snapshot_generation is not None:
                candidates.append(snapshot_generation)
        active = self._active_generations.get(plugin_id)
        if active is not None:
            candidates.append(active)
        prepared = self._prepared_generations.get(plugin_id)
        if prepared is not None:
            candidates.append(prepared)
        ready = self._ready_candidate
        if ready is not None and ready.plugin_id == plugin_id:
            candidates.append(ready.candidate)
            if ready.previous is not None:
                candidates.append(ready.previous)
        candidates.extend(self._draining_generations.get(plugin_id, ()))
        for generation in candidates:
            if generation.generation_id == generation_id:
                return generation
        raise RuntimeError(
            "channel binding 缺少 exact generation owner: "
            f"{plugin_id}/{generation_id}"
        )

    def job_catalog(self, generation_id: str) -> PreparedJobCatalog | None:
        return self._job_host.get(generation_id)

    def proactive_catalog(
        self,
        generation_id: str,
    ) -> PreparedProactiveCatalog | None:
        return self._proactive_host.get(generation_id)

    @property
    def current_snapshot(self) -> RuntimeSnapshot | None:
        return self._snapshot_store.current

    @property
    def latest_snapshot(self) -> RuntimeSnapshot | None:
        return self._snapshot_store.latest

    @property
    def ready_candidate(self) -> PluginGeneration | None:
        return (
            None if self._ready_candidate is None else self._ready_candidate.candidate
        )

    @property
    def installed_plugins_home(self) -> Path:
        return _plugins_home(self._installed_cache_root)

    @property
    def snapshot_store(self) -> RuntimeSnapshotStore:
        return self._snapshot_store

    @property
    def reload_journal(self) -> ReloadJournal:
        return self._reload_journal

    def sync_manifest(self, *, plugins_home: Path | None = None) -> Path:
        entries = load_plugin_manifest(plugins_home)
        project_root = _package_project_root(self._dirs)
        if project_root is not None:
            packages = discover_plugin_packages(project_root)
            package_entries = load_package_manifest(plugins_home)
            for package_id, package in packages.items():
                if package_id not in package_entries:
                    package_entries[package_id] = any(
                        entries.get(member, False) for member in package.members
                    )
                for member in package.members:
                    entries.pop(member, None)
            _ = write_package_manifest(package_entries, plugins_home=plugins_home)
        for mod in self.discover(installed_selector="latest"):
            if mod.get("package_id"):
                continue
            _ = entries.setdefault(_resolve_plugin_id(mod), True)
        return write_plugin_manifest(entries, plugins_home=plugins_home)

    def watch_revision(self) -> str:
        digest = hashlib.sha256()
        home = _plugins_home(self._installed_cache_root)
        digest.update(_path_metadata(home / "manifest.toml"))
        for mod in self.discover(installed_selector="latest"):
            plugin_id = _resolve_plugin_id(mod)
            plugin_dir = Path(mod["plugin_root"])
            data_dir = _resolve_plugin_data_dir(
                mod["name"],
                mod,
                self._workspace,
            )
            digest.update(plugin_id.encode())
            digest.update(_source_metadata_revision(plugin_dir))
            digest.update(_path_metadata(data_dir / "config.local.toml"))
        return digest.hexdigest()

    def _registry_active(self, module_path: str) -> bool:
        if module_path not in self._active_plugins:
            return False
        instance = plugin_registry.get_instance(module_path)
        if instance is None:
            return True
        if (
            isinstance(instance, ComposablePlugin)
            and self.current_snapshot is not None
        ):
            return any(
                generation.module_path == module_path
                for generation in self.current_snapshot.active_generations()
            )
        return plugin_is_active(instance, plugin_id=module_path)

    def stable_telegram_command_catalog(self) -> tuple[tuple[str, str], ...]:
        """Return discovery commands from the exact committed stable snapshot."""

        return self._snapshot_bot_commands(self.current_snapshot)

    def stable_mobile_command_catalog(self) -> tuple[tuple[str, str], ...]:
        """Return mobile commands from the exact committed stable snapshot."""

        return self._snapshot_bot_commands(self.current_snapshot)

    def stable_channel_catalog(self) -> ChannelRegistrySnapshot | None:
        """Return the exact committed stable channel declaration catalog."""

        snapshot = self.current_snapshot
        return None if snapshot is None else snapshot.channel_registry

    @staticmethod
    def _snapshot_bot_commands(
        snapshot: RuntimeSnapshot | None,
    ) -> tuple[tuple[str, str], ...]:
        """Project one immutable channel catalog from a snapshot."""

        if snapshot is None:
            return ()
        registry = snapshot.command_registry
        if registry is None:
            return ()
        return tuple(
            (descriptor.name, descriptor.description)
            for descriptor in registry.descriptors
        )

    # 扫描所有 plugin_dirs，返回可加载的插件描述列表
    def discover(
        self,
        *,
        installed_selector: ArtifactSelector = "stable",
    ) -> list[dict[str, str]]:
        mods: list[dict[str, str]] = []
        seen_names: set[str] = set()
        project_root = _package_project_root(self._dirs)
        packages = discover_plugin_packages(project_root) if project_root else {}
        enabled_packages = (
            _select_enabled_plugin_packages(
                packages,
                load_package_manifest(_plugins_home(self._installed_cache_root)),
            )
            if project_root
            else {}
        )
        member_packages = {
            member: package.id
            for package in packages.values()
            for member in package.members
        }
        enabled_members = {
            member
            for package in enabled_packages.values()
            for member in package.members
        }
        for source in resolve_plugin_sources(
            self._dirs,
            installed_cache_root=self._installed_cache_root,
            installed_selector=installed_selector,
        ):
            name = source.plugin_name or source.plugin_root.name
            package_id = member_packages.get(name, "")
            if package_id and name not in enabled_members:
                continue
            if name in seen_names and source.source_type == "builtin":
                logger.warning("插件名重复，跳过: %s (%s)", name, source.plugin_root)
                continue
            seen_names.add(name)
            import_suffix = name.replace("-", "_").replace("@", "_")
            import_source = source.marketplace or source.plugin_root.parent.name
            module_path = source.plugin_root / source.entrypoint
            mods.append(
                {
                    "name": name,
                    "plugin_root": str(source.plugin_root),
                    "module_path": str(module_path) if module_path is not None else "",
                    "entrypoint": source.entrypoint,
                    "manifest_digest": (
                        source.static_manifest.identity_digest
                        if source.static_manifest is not None
                        else ""
                    ),
                    "import_path": f"akasic_plugin_{import_source}_{import_suffix}",
                    "marketplace": source.marketplace,
                    "source_type": source.source_type,
                    "package_id": package_id,
                }
            )
        return mods

    async def load_all(self) -> None:
        """Load stable plugins and reconstruct any durable latest candidate."""

        # 1. 先收敛已提交的 interaction 删除，再开放任何插件命令。
        if self._interaction_undo is not None:
            await self._interaction_undo.recover_pending()

        # 2. 处理尚未进入 latest_ready 的残留事务，恢复磁盘 pointer。
        recovery = self._reload_journal.pending_recovery()
        self._require_unique_recovery_plugins(recovery)
        stable_by_id = self._discovered_by_id(installed_selector="stable")
        latest_by_id = self._discovered_by_id(installed_selector="latest")
        runtime_recovery = tuple(
            action
            for action in recovery
            if action.action in {
                "retry_generation_cleanup",
                "retry_runtime_recovery",
            }
        )
        runtime_receipts = await self._prepare_boot_runtime_recovery(
            runtime_recovery
        )
        recovery = tuple(
            action for action in recovery if action not in runtime_recovery
        )
        if runtime_recovery:
            stable_by_id = self._discovered_by_id(installed_selector="stable")
            latest_by_id = self._discovered_by_id(installed_selector="latest")
        for action in recovery:
            if action.action != "discard_candidate":
                continue
            self._discard_recovery_pointer(
                action.plugin_id,
                action.source_revision,
                stable_by_id=stable_by_id,
                latest_by_id=latest_by_id,
            )
            self._reload_journal.finish_recovery(action)
            self._write_startup_recovery_fact(action, committed=False)

        # 3. 根据 durable pointer 判定 promoting 崩溃发生在切换前还是切换后。
        stable_by_id = self._discovered_by_id(installed_selector="stable")
        latest_by_id = self._discovered_by_id(installed_selector="latest")
        restore_candidates, restore_committed, restore_discarded = (
            self._classify_reload_recovery(
                recovery,
                stable_by_id=stable_by_id,
                latest_by_id=latest_by_id,
            )
        )
        for action in restore_discarded:
            self._reload_journal.finish_recovery(action)
            self._write_startup_recovery_fact(action, committed=False)

        # 4. stable 在未发布事务中完整装配；latest 随后以新事务恢复。
        if self._active_generations:
            for mod in stable_by_id.values():
                _ = await self._load_one(mod)
        else:
            await self._load_stable_batch(tuple(stable_by_id.values()))
        self._finish_committed_recovery(restore_committed)
        self._finish_boot_runtime_recovery(
            runtime_recovery,
            runtime_receipts,
        )
        await self._restore_latest_candidates(restore_candidates, latest_by_id)

    async def _prepare_boot_runtime_recovery(
        self,
        actions: tuple[ReloadRecoveryAction, ...],
    ) -> dict[str, str]:
        """Clean exact previous boots and normalize their durable artifact targets."""

        if not actions:
            return {}
        current_boot_id = os.environ.get("AKASHIC_BOOT_ID", "").strip()
        if os.environ.get("AKASHIC_SUPERVISED") != "1" or not current_boot_id:
            raise RuntimeError(
                "v3 runtime recovery 需要 supervised boot identity"
            )
        from agent.background.boot_guardian import _cleanup_boot_processes

        cleaned_boots: set[str] = set()
        receipts: dict[str, str] = {}
        for action in actions:
            previous_boot_id = action.runtime_owner_boot_id
            if not previous_boot_id or previous_boot_id == current_boot_id:
                raise RuntimeError(
                    "v3 runtime recovery 缺少不同于当前进程的旧 boot identity"
                )
            if previous_boot_id not in cleaned_boots:
                await asyncio.to_thread(
                    _cleanup_boot_processes,
                    boot_id=previous_boot_id,
                    gateway_group_id=None,
                )
                cleaned_boots.add(previous_boot_id)
            self._normalize_runtime_recovery_pointer(action)
            receipts[action.tx_id] = (
                f"boot-reconcile:previous={previous_boot_id}:"
                f"current={current_boot_id}:cleanup=complete:"
                f"target={action.recovery_target}"
            )
        return receipts

    def _normalize_runtime_recovery_pointer(
        self,
        action: ReloadRecoveryAction,
    ) -> None:
        """Verify one exact pointer pair and select only its recorded target."""

        plugin_name, separator, marketplace = action.plugin_id.rpartition("@")
        if not separator:
            raise RuntimeError(
                "跨 boot runtime recovery 只接受带 exact pointer 的 installed plugin"
            )
        plugin_base = (
            _plugins_home(self._installed_cache_root)
            / "cache"
            / marketplace
            / plugin_name
        )
        pointers = read_pointers(plugin_base)
        if pointers is None or action.recovery_target is None:
            raise RuntimeError("runtime recovery 缺少 durable pointer/target evidence")
        base = ArtifactPointer(action.base_artifact_pointer)
        candidate_pointer = action.candidate_artifact_pointer
        pair = (pointers.stable, pointers.latest)
        if action.recovery_target == "base":
            accepted = {(base, base)}
            if candidate_pointer is not None:
                accepted.add((base, ArtifactPointer(candidate_pointer)))
            if pair not in accepted:
                raise RuntimeError(
                    f"runtime recovery base pointer 漂移: {plugin_base}: {pair}"
                )
            _ = write_pointers(plugin_base, stable=base, latest=base)
            return
        if candidate_pointer is None:
            raise RuntimeError("runtime recovery candidate target 缺少 exact pointer")
        candidate = ArtifactPointer(candidate_pointer)
        if pair != (candidate, candidate):
            raise RuntimeError(
                f"runtime recovery candidate pointer 未提交: {plugin_base}: {pair}"
            )

    def _finish_boot_runtime_recovery(
        self,
        actions: tuple[ReloadRecoveryAction, ...],
        receipts: Mapping[str, str],
    ) -> None:
        """Seal boot reconciliation only after the authoritative stable Root is live."""

        snapshot = self.current_snapshot
        for action in actions:
            generation = self._active_generations.get(action.plugin_id)
            expected_pointer = (
                action.candidate_artifact_pointer
                if action.recovery_target == "candidate"
                else action.base_artifact_pointer
            )
            if expected_pointer is not None:
                if generation is None:
                    raise RuntimeError(
                        "runtime recovery 未重建 exact stable generation"
                    )
                plugin_base = _installed_artifact_base(generation)
                if plugin_base is None or (
                    generation.plugin_dir.relative_to(plugin_base).as_posix()
                    != expected_pointer
                ):
                    raise RuntimeError(
                        "runtime recovery stable artifact identity 不一致"
                    )
            elif generation is not None:
                raise RuntimeError("runtime recovery 应恢复为无插件 base")
            if (
                action.recovery_target == "candidate"
                and generation is not None
                and generation.source_revision != action.source_revision
            ):
                raise RuntimeError(
                    "candidate runtime recovery source revision 不一致"
                )
            if generation is not None and snapshot is not None:
                if self._composition_runtime_declared(snapshot, action.plugin_id):
                    if self._composition_generation_host.get(
                        generation.generation_id
                    ) is None:
                        raise RuntimeError(
                            "boot runtime recovery stable Host 未就绪"
                        )
                registry = snapshot.channel_registry
                channel_declared = registry is not None and any(
                    descriptor.owner == action.plugin_id
                    for descriptor in registry.descriptors
                )
                if channel_declared:
                    channel_runtime = self._active_channel_generation
                    if (
                        channel_runtime is None
                        or channel_runtime.snapshot_id != snapshot.snapshot_id
                        or self._channel_generation_host.get(snapshot.snapshot_id)
                        is None
                        or self._active_channel_catalog_identity
                        != registry.identity
                    ):
                        raise RuntimeError(
                            "boot runtime recovery stable Channel Host 未就绪"
                        )
            if "activity-publication" in (action.failure_resource or ""):
                if snapshot is None or self._activity_host is None:
                    raise RuntimeError("boot runtime recovery 缺少 stable Activity owner")
                activity = self._activity_host.active
                expected_activity = ActivityCatalog(
                    proactive=snapshot.proactive_component_catalog,
                    background_jobs=snapshot.background_job_catalog,
                    private_proactive=snapshot.private_proactive_catalog,
                ).identity
                if (
                    activity is None
                    or activity.snapshot_id != snapshot.snapshot_id
                    or activity.catalog_identity != expected_activity
                    or not activity.admission_open
                ):
                    raise RuntimeError(
                        "boot runtime recovery stable Activity Host 未就绪"
                    )
            receipt = receipts.get(action.tx_id)
            if receipt is None:
                raise RuntimeError("boot runtime recovery receipt 缺失")
            stable_identity = (
                "none"
                if generation is None
                else f"{generation.generation_id}:{generation.source_revision}"
            )
            snapshot_id = "none" if snapshot is None else snapshot.snapshot_id
            self._reload_journal.finish_recovery(
                action,
                retry_receipt=(
                    f"{receipt}:snapshot={snapshot_id}:stable={stable_identity}"
                ),
            )
            self._write_startup_recovery_fact(
                action,
                committed=action.recovery_target == "candidate",
            )

    async def _load_stable_batch(
        self,
        mods: tuple[dict[str, str], ...],
    ) -> None:
        """暂存全部 stable 插件并发布一个完整运行时快照。"""

        staged: list[PluginGeneration] = []
        snapshot: RuntimeSnapshot | None = None
        catalog_id: str | None = None
        published_count = self._legacy_publication_counts()
        try:
            # 1. 只导入、校验并准备声明，不开放任何 stable snapshot。
            for mod in mods:
                generation = await self._load_one(mod, stage_stable=True)
                if generation is not None:
                    staged.append(generation)
            if not staged:
                return
            snapshot, catalog_id = await self._compile_stable_batch_snapshot(staged)
            for generation in staged:
                generation.runtime_snapshot = snapshot

            # 2. Root declarations become live only after the whole batch settled.
            for generation in staged:
                await self._start_composition_generation_runtime(
                    generation,
                    snapshot,
                    mode="formal",
                )

            # 3. legacy v2 只作为待迁移参与者在事务内 prepare/activate；
            #    v3 lifecycle 已由完整 CompositionRoot mount。
            await self._activate_stable_batch(staged)

            # 4. 全部准备成功后才登记 stable owner，并一次安装快照。
            assert catalog_id is not None
            await self._publish_stable_batch(staged, snapshot, catalog_id)
        except BaseException as error:
            # 5. 未发布事务失败时恢复所有进程内 owner，并反向释放资源。
            _, cleanup_cancelled = await _complete_critical(
                self._discard_stable_batch(
                    staged,
                    snapshot=snapshot,
                    catalog_id=catalog_id,
                    published_count=published_count,
                )
            )
            if cleanup_cancelled:
                raise asyncio.CancelledError
            if isinstance(error, _StablePluginFailed):
                await self._retry_stable_batch_without_failed(mods, error)
                return
            raise

    async def _compile_stable_batch_snapshot(
        self,
        staged: list[PluginGeneration],
    ) -> tuple[RuntimeSnapshot, str]:
        """为 stable 启动批次编译一个完整的未发布快照。"""

        try:
            return await self._compile_topology_snapshot(
                {item.plugin_id: item for item in staged}
            )
        except Exception as error:
            if len(staged) == 1 and "missing_services=" not in str(error):
                raise _StablePluginFailed(
                    staged[0], "runtime_snapshot", error
                ) from error
            raise

    async def _activate_stable_batch(
        self,
        staged: list[PluginGeneration],
    ) -> None:
        """准备 legacy 参与者，但不发布它们的注册项。"""

        for generation in staged:
            instance = cast(Any, generation.instance)
            try:
                await self._prepare_generation(generation)
                generation.state = "activating"
                if not isinstance(instance, ComposablePlugin):
                    instance.context.data_dir = generation.data_dir
                    instance.context.session_manager = self._session_manager
                    instance.context.memory_engine = self._memory_engine
                    instance.context.llm = self._llm
                    instance.activate()
            except Exception as error:
                raise _StablePluginFailed(generation, "prepare", error) from error

    async def _publish_stable_batch(
        self,
        staged: list[PluginGeneration],
        snapshot: RuntimeSnapshot,
        catalog_id: str,
    ) -> None:
        """登记全部 stable owner 并一次安装批次快照。"""

        for generation in staged:
            instance = cast(Any, generation.instance)
            try:
                self._register_tools(instance, generation.module_path, [])
                self._bind_tool_hooks(instance, generation.module_path)
                self._publish_contributions(generation.contributions)
                self._channels.extend(generation.contributions.channels)
                if generation.staged_event_bus is not None:
                    generation.staged_event_bus.publish()
                generation.minimum_resource_count = generation.scope.resource_count
                self._scopes[generation.module_path] = generation.scope
                self._loaded.add(generation.module_path)
                generation.state = "active"
                self._active_generations[generation.plugin_id] = generation
                self._activate_published_generation(generation, None)
            except Exception as error:
                raise _StablePluginFailed(generation, "publish", error) from error
        self._compile_snapshot_event_handlers(snapshot)
        self._commit_stable_kv(staged)
        self._snapshot_skill_catalogs[snapshot.snapshot_id] = catalog_id
        await self._publish_committed_snapshot(snapshot)
        for generation in staged:
            generation.boot_created_data_dir = False
            if generation.mcp_catalog is not None:
                self._mcp_host.mark_active(generation.generation_id)
            logger.info("插件已加载: %s", generation.plugin_id)

    async def _discard_stable_batch(
        self,
        staged: list[PluginGeneration],
        *,
        snapshot: RuntimeSnapshot | None,
        catalog_id: str | None,
        published_count: tuple[int, ...],
    ) -> None:
        """释放只归属于未发布启动批次的全部资源。"""

        self._restore_legacy_publication_counts(published_count)
        pending = self._snapshot_store.pending_transaction
        store_owned_pending = (
            snapshot is not None
            and pending is not None
            and pending.candidate is snapshot
        )
        if snapshot is not None and not store_owned_pending:
            _ = self._snapshot_skill_catalogs.pop(snapshot.snapshot_id, None)
        for generation in reversed(staged):
            _ = self._active_generations.pop(generation.plugin_id, None)
            if not store_owned_pending:
                generation.runtime_snapshot = None
                await self._dispose_generation(generation, state="discarded")
        if store_owned_pending:
            assert pending is not None
            await self._snapshot_store.abort(pending)
            for generation in staged:
                generation.runtime_snapshot = None
                if generation.boot_created_data_dir:
                    _remove_validation_data_dir(generation.data_dir)
                    generation.boot_created_data_dir = False
        else:
            for generation in staged:
                if generation.boot_created_data_dir:
                    _remove_validation_data_dir(generation.data_dir)
                    generation.boot_created_data_dir = False
        self._rollback_stable_kv(staged)
        if (
            not store_owned_pending
            and snapshot is not None
            and snapshot.composition_root is not None
        ):
            await snapshot.composition_root.dispose()
        if catalog_id is not None and not store_owned_pending:
            self._skill_host.close(catalog_id)

    async def _retry_stable_batch_without_failed(
        self,
        mods: tuple[dict[str, str], ...],
        failure: _StablePluginFailed,
    ) -> None:
        """记录被拒绝的 stable 参与者并重建剩余批次。"""

        generation = failure.generation
        self._record_failed_gate(
            plugin_id=generation.plugin_id,
            revision=generation.source_revision,
            check_id=failure.phase,
            reason=str(failure.cause) or type(failure.cause).__name__,
        )
        logger.warning(
            "插件 %s 加载失败，回滚整个未发布批次: %s",
            generation.plugin_id,
            failure.cause,
        )
        remaining = tuple(
            mod for mod in mods if _resolve_plugin_id(mod) != generation.plugin_id
        )
        await self._load_stable_batch(remaining)

    @staticmethod
    def _commit_stable_kv(staged: list[PluginGeneration]) -> None:
        """在快照安装前提交全部已准备的 v2 KV。"""

        from agent.plugins.context import PreparedPluginKVStore

        for generation in staged:
            if isinstance(generation.instance, ComposablePlugin):
                continue
            kv_store = cast(Any, generation.instance).context.kv_store
            try:
                if isinstance(kv_store, PreparedPluginKVStore):
                    kv_store.commit()
            except Exception as error:
                raise _StablePluginFailed(generation, "publish", error) from error

    @staticmethod
    def _rollback_stable_kv(staged: list[PluginGeneration]) -> None:
        """失败批次全部任务停止后恢复 v2 KV 文件。"""

        from agent.plugins.context import PreparedPluginKVStore

        for generation in reversed(staged):
            if isinstance(generation.instance, ComposablePlugin):
                continue
            kv_store = cast(Any, generation.instance).context.kv_store
            if isinstance(kv_store, PreparedPluginKVStore):
                kv_store.rollback_commit()

    def _legacy_publication_counts(self) -> tuple[int, ...]:
        """记录启动回滚可移除的 v2 发布尾部。"""

        return (
            len(self._tool_hooks),
            len(self._before_turn_modules),
            len(self._before_reasoning_modules),
            len(self._prompt_render_modules),
            len(self._before_step_modules),
            len(self._after_step_modules),
            len(self._after_reasoning_modules),
            len(self._after_turn_modules),
            len(self._proactive_modules),
            len(self._proactive_lifecycles),
            len(self._proactive_module_factories),
            len(self._proactive_runtime_factories),
            len(self._proactive_sources),
            len(self._jobs),
            len(self._channels),
        )

    def _restore_legacy_publication_counts(self, counts: tuple[int, ...]) -> None:
        """只移除失败启动批次发布的 v2 兼容尾部。"""

        collections = (
            self._tool_hooks,
            self._before_turn_modules,
            self._before_reasoning_modules,
            self._prompt_render_modules,
            self._before_step_modules,
            self._after_step_modules,
            self._after_reasoning_modules,
            self._after_turn_modules,
            self._proactive_modules,
            self._proactive_lifecycles,
            self._proactive_module_factories,
            self._proactive_runtime_factories,
            self._proactive_sources,
            self._jobs,
            self._channels,
        )
        for collection, count in zip(collections, counts, strict=True):
            del collection[count:]

    @staticmethod
    def _require_unique_recovery_plugins(
        recovery: tuple[ReloadRecoveryAction, ...],
    ) -> None:
        seen: set[str] = set()
        for action in recovery:
            if action.plugin_id in seen:
                raise RuntimeError(
                    f"同一插件存在多个未完成 ReloadTransaction: {action.plugin_id}"
                )
            seen.add(action.plugin_id)

    def _classify_reload_recovery(
        self,
        recovery: tuple[ReloadRecoveryAction, ...],
        *,
        stable_by_id: dict[str, dict[str, str]],
        latest_by_id: dict[str, dict[str, str]],
    ) -> tuple[
        list[ReloadRecoveryAction],
        list[ReloadRecoveryAction],
        list[ReloadRecoveryAction],
    ]:
        """Classify durable transactions by the pointer switch already on disk."""

        restore_candidates: list[ReloadRecoveryAction] = []
        restore_committed: list[ReloadRecoveryAction] = []
        restore_discarded: list[ReloadRecoveryAction] = []
        for action in recovery:
            if action.action == "discard_candidate":
                continue
            stable_revision = _mod_source_revision(stable_by_id.get(action.plugin_id))
            latest_revision = _mod_source_revision(latest_by_id.get(action.plugin_id))
            if action.action == "restore_candidate":
                if latest_revision != action.source_revision:
                    raise RuntimeError(
                        "ReloadTransaction latest 恢复源码不一致: "
                        f"{action.plugin_id} expected={action.source_revision} "
                        f"actual={latest_revision}"
                    )
                restore_candidates.append(action)
                continue
            if stable_revision == action.source_revision:
                restore_committed.append(action)
                continue
            if (
                action.phase in {"commit_started", "promoting"}
                and latest_revision == action.source_revision
            ):
                self._discard_recovery_pointer(
                    action.plugin_id,
                    action.source_revision,
                    stable_by_id=stable_by_id,
                    latest_by_id=latest_by_id,
                )
                restore_discarded.append(replace(action, action="discard_candidate"))
                continue
            if (
                action.phase in {"commit_started", "promoting"}
                and stable_revision == latest_revision
                and self._has_installed_pointer_state(action.plugin_id)
            ):
                restore_discarded.append(action)
                continue
            raise RuntimeError(
                "ReloadTransaction 恢复源码不一致: "
                f"{action.plugin_id} expected={action.source_revision} "
                f"stable={stable_revision} latest={latest_revision}"
            )
        return restore_candidates, restore_committed, restore_discarded

    def _has_installed_pointer_state(self, plugin_id: str) -> bool:
        plugin_name, separator, marketplace = plugin_id.rpartition("@")
        if not separator:
            return False
        plugin_base = (
            _plugins_home(self._installed_cache_root)
            / "cache"
            / marketplace
            / plugin_name
        )
        state_path = pointer_state_path(plugin_base)
        return state_path.exists() or state_path.is_symlink()

    def _finish_committed_recovery(
        self,
        recovery: list[ReloadRecoveryAction],
    ) -> None:
        """Confirm that every disk-committed generation became active stable."""

        for action in recovery:
            generation = self._active_generations.get(action.plugin_id)
            if generation is None:
                raise RuntimeError(
                    f"ReloadTransaction 恢复缺少插件: {action.plugin_id}"
                )
            assert generation.source_revision == action.source_revision
            self._reload_journal.finish_recovery(action)
            self._write_startup_recovery_fact(action, committed=True)

    def _write_startup_recovery_fact(
        self,
        action: ReloadRecoveryAction,
        *,
        committed: bool,
    ) -> None:
        message = (
            f"{action.plugin_id} 更新已在 Core 重启后确认提交；当前使用新版本。"
            if committed
            else f"{action.plugin_id} 更新在 Core 重启时没有完成；候选已丢弃，原版本保持可用。"
        )
        atomic_save_json(
            self._workspace / "runtime" / "plugin-rollout-fact.json",
            {"message": message},
            ensure_ascii=False,
            domain="plugin_rollout_fact",
        )

    async def _restore_latest_candidates(
        self,
        recovery: list[ReloadRecoveryAction],
        latest_by_id: dict[str, dict[str, str]],
    ) -> None:
        """Rebuild latest candidates; reject a bad candidate without losing stable."""

        for action in recovery:
            self._reload_journal.finish_recovery(action)
            mod = latest_by_id.get(action.plugin_id)
            if mod is None:
                raise RuntimeError(
                    f"ReloadTransaction latest 恢复缺少插件: {action.plugin_id}"
                )
            generation = await self._load_one(mod, activate=False)
            if generation is None:
                _discard_installed_candidate_mod(mod)
                logger.error(
                    "ReloadTransaction latest 候选恢复失败，保留 stable: %s",
                    action.plugin_id,
                )
                continue
            try:
                result = await self._publish_prepared(action.plugin_id)
            except Exception:
                await self.discard_prepared(action.plugin_id)
                _discard_installed_candidate_mod(mod)
                logger.exception(
                    "ReloadTransaction latest 候选发布失败，保留 stable: %s",
                    action.plugin_id,
                )
                continue
            if result["publication_state"] != "latest_ready":
                _discard_installed_candidate_mod(mod)
                logger.error(
                    "ReloadTransaction latest 候选被拒绝，保留 stable: %s",
                    action.plugin_id,
                )

    def _discovered_by_id(
        self,
        *,
        installed_selector: ArtifactSelector,
    ) -> dict[str, dict[str, str]]:
        return {
            _resolve_plugin_id(mod): mod
            for mod in self.discover(installed_selector=installed_selector)
        }

    @staticmethod
    def _discard_recovery_pointer(
        plugin_id: str,
        source_revision: str,
        *,
        stable_by_id: dict[str, dict[str, str]],
        latest_by_id: dict[str, dict[str, str]],
    ) -> None:
        latest = latest_by_id.get(plugin_id)
        stable = stable_by_id.get(plugin_id)
        if latest is None or latest.get("source_type") != "installed":
            return
        latest_revision = _mod_source_revision(latest)
        stable_revision = _mod_source_revision(stable)
        if latest_revision == stable_revision:
            return
        if latest_revision != source_revision:
            raise RuntimeError(
                "ReloadTransaction discard 源码不一致: "
                f"{plugin_id} expected={source_revision} actual={latest_revision}"
            )
        plugin_base = _installed_artifact_base_from_root(Path(latest["plugin_root"]))
        _ = discard_latest_pointer(plugin_base)

    async def prepare_candidate(self, plugin_id: str) -> PluginGeneration | None:
        if self._ready_candidate is not None:
            raise RuntimeError(
                f"已有 latest 等待 promote/discard: {self._ready_candidate.plugin_id}"
            )
        await self.discard_prepared(plugin_id, preserve_latest=True)
        for mod in self.discover(installed_selector="latest"):
            if _resolve_plugin_id(mod) == plugin_id:
                generation = await self._load_one(mod, activate=False)
                if generation is None:
                    _discard_installed_candidate_mod(mod)
                return generation
        raise KeyError(f"插件不存在: {plugin_id}")

    async def discard_prepared(
        self,
        plugin_id: str,
        *,
        preserve_latest: bool = False,
        error: str = "candidate discarded",
    ) -> None:
        generation = self._prepared_generations.pop(plugin_id, None)
        if generation is None:
            return
        if not preserve_latest:
            _discard_generation_candidate_pointer(generation)
        _, cancelled = await _complete_critical(
            self._dispose_generation(generation, state="discarded")
        )
        runtime_failure = self._composition_generation_host.failure(
            generation.generation_id
        )
        if runtime_failure is not None:
            raise RuntimeError(
                "候选 runtime cleanup 未完成，必须显式 retry"
            )
        self._abort_reload(generation, error=error)
        if cancelled:
            raise asyncio.CancelledError

    def _begin_reload_attempt(
        self,
        *,
        plugin_id: str,
        generation_id: str,
        source_revision: str,
        config_revision: str,
        plugin_dir: Path,
        source_type: str,
    ) -> str:
        base = self.current_snapshot
        base_generation = (
            None if base is None else base.generations.get(plugin_id)
        )
        base_pointer: str | None = None
        candidate_pointer: str | None = None
        if source_type == "installed":
            plugin_base = _installed_artifact_base_from_root(plugin_dir)
            pointers = read_pointers(plugin_base)
            if pointers is None:
                raise RuntimeError(
                    f"installed reload 缺少 artifact pointer state: {plugin_base}"
                )
            candidate_pointer = plugin_dir.relative_to(plugin_base).as_posix()
            if pointers.latest.path != candidate_pointer:
                raise RuntimeError(
                    "installed reload generation 与 latest pointer 不一致: "
                    f"generation={candidate_pointer} latest={pointers.latest.path}"
                )
            base_pointer = pointers.stable.path
        return self._reload_journal.begin(
            plugin_id=plugin_id,
            base_snapshot_id=base.snapshot_id if base is not None else None,
            base_generation_id=(
                None if base_generation is None else base_generation.generation_id
            ),
            generation_id=generation_id,
            source_revision=source_revision,
            config_revision=config_revision,
            base_artifact_pointer=base_pointer,
            candidate_artifact_pointer=candidate_pointer,
        )

    def _abort_reload_attempt(self, tx_id: str | None, *, error: str) -> None:
        if tx_id is not None:
            self._reload_journal.advance(tx_id, "aborted", error=error)

    async def _dispose_generation(
        self,
        generation: PluginGeneration,
        *,
        state: str,
        preserve_stable_alias: bool = False,
        skip_composition_runtime: bool = False,
    ) -> None:
        """完成插件终止、作用域清理和注册表卸载。"""

        from agent.plugins.context import allow_plugin_cleanup_writes

        # 1. Host 必须在 exact Root/Health observer 仍存活时先回收进程。
        externally_cancelled = False
        if not skip_composition_runtime:
            try:
                await self._stop_composition_generation_runtime(generation)
            except asyncio.CancelledError:
                externally_cancelled = True
            except Exception as error:
                self._cleanup_failures.append(
                    CleanupFailure(
                        resource=f"plugin:{generation.plugin_id}:composition-runtime",
                        error=str(error) or type(error).__name__,
                    )
                )

        # 2. 回收尚未交给 snapshot store 的组合 Root。
        if generation.runtime_snapshot is not None:
            await self._dispose_unreferenced_composition_root(
                generation.runtime_snapshot
            )

        # 3. 终止 lifecycle v2 对象，并在调用方取消后继续完成它
        if generation.prepare_started:
            terminator = getattr(generation.instance, "terminate", None)
            if callable(terminator):
                try:
                    with allow_plugin_cleanup_writes(generation.generation_id):
                        _, terminator_cancelled = await _complete_critical(
                            cast(Callable[[], Awaitable[None]], terminator)()
                        )
                    externally_cancelled = (
                        externally_cancelled or terminator_cancelled
                    )
                except (asyncio.CancelledError, Exception) as error:
                    current = asyncio.current_task()
                    externally_cancelled = (
                        current is not None and current.cancelling() > 0
                    )
                    self._cleanup_failures.append(
                        CleanupFailure(
                            resource=f"plugin:{generation.plugin_id}:terminate",
                            error=str(error) or type(error).__name__,
                        )
                    )

        # 4. 收集作用域失败，确保外部取消不会截断资源清理
        with allow_plugin_cleanup_writes(generation.generation_id):
            cleanup_failures, cleanup_cancelled = await _complete_critical(
                generation.scope.aclose()
            )
        self._cleanup_failures.extend(cleanup_failures)
        externally_cancelled = externally_cancelled or cleanup_cancelled
        if (
            not skip_composition_runtime
            and self._composition_generation_host.failure(generation.generation_id)
            is not None
        ):
            self._record_composition_runtime_failure(
                generation,
                RuntimeError("generation runtime cleanup 未完成"),
                formal_effects=("generation_runtime_cleanup_pending",),
            )

        # 5. 清理注册表和模块树
        _ = self._scopes.pop(generation.module_path, None)
        self._loaded.discard(generation.module_path)
        _ = self._active_plugins.pop(generation.module_path, None)
        for metadata in plugin_registry.get_handlers_by_module_path(
            generation.module_path
        ):
            if metadata.kind == MetadataKind.TOOL and self._tool_registry is not None:
                self._tool_registry.unregister(
                    metadata.tool_name or metadata.handler_name
                )
        self._remove_module_tree(generation.module_path)
        stable_alias = self._stable_aliases.get(generation.module_path)
        if stable_alias is not None and not preserve_stable_alias:
            _ = self._stable_aliases.pop(generation.module_path, None)
            if plugin_registry.get_instance(stable_alias) is generation.instance:
                self._remove_module_tree(stable_alias)
            else:
                self._fresh_importer.unregister(stable_alias)
        generation.state = state
        if externally_cancelled:
            raise asyncio.CancelledError

    async def _dispose_unreferenced_composition_root(
        self,
        snapshot: RuntimeSnapshot,
    ) -> None:
        root = snapshot.composition_root
        if root is None or self._snapshot_store.composition_is_referenced_elsewhere(
            root,
            excluding_snapshot_id="",
        ):
            return
        if self._dashboard_validation_releaser is not None:
            await self._dashboard_validation_releaser(snapshot)
        await root.dispose()

    def _retire_generation(self, generation: PluginGeneration) -> None:
        """通知已关闭 admission 的 generation 进入退役状态。"""

        if generation.retire_started:
            return
        generation.retire_started = True
        generation.state = "retired"
        if generation.mcp_catalog is not None:
            self._mcp_host.mark_draining(generation.generation_id)
        self._draining_generations.setdefault(generation.plugin_id, []).append(
            generation
        )
        if isinstance(generation.instance, ComposablePlugin):
            return
        try:
            cast(Any, generation.instance).retire()
        except Exception as error:
            error_text = str(error) or type(error).__name__
            logger.warning(
                "插件 retire 失败 (%s): %s",
                generation.plugin_id,
                error_text,
            )
            self._cleanup_failures.append(
                CleanupFailure(
                    resource=f"plugin:{generation.plugin_id}:retire",
                    error=error_text,
                )
            )

    def _forget_drained_generation(self, generation: PluginGeneration) -> None:
        tracked = self._draining_generations.get(generation.plugin_id)
        if tracked is None:
            return
        remaining = [item for item in tracked if item is not generation]
        if remaining:
            self._draining_generations[generation.plugin_id] = remaining
        else:
            _ = self._draining_generations.pop(generation.plugin_id, None)

    async def _on_snapshot_drained(self, snapshot: RuntimeSnapshot) -> None:
        unreferenced_generations = tuple(
            generation
            for generation in snapshot.generations.values()
            if not self._snapshot_store.generation_is_referenced_elsewhere(
                generation,
                excluding_snapshot_id=snapshot.snapshot_id,
            )
        )
        for generation in unreferenced_generations:
            try:
                await self._stop_composition_generation_runtime(generation)
            except Exception as error:
                self._record_drained_composition_runtime_failure(
                    snapshot,
                    generation,
                    error,
                )
                self._cleanup_failures.append(
                    CleanupFailure(
                        resource=(
                            f"plugin:{generation.plugin_id}:composition-runtime"
                        ),
                        error=str(error) or type(error).__name__,
                    )
                )
        composition_root = snapshot.composition_root
        if (
            composition_root is not None
            and not self._snapshot_store.composition_is_referenced_elsewhere(
                composition_root,
                excluding_snapshot_id=snapshot.snapshot_id,
            )
        ):
            if self._dashboard_validation_releaser is not None:
                await self._dashboard_validation_releaser(snapshot)
            await composition_root.dispose()
        catalog_id = self._snapshot_skill_catalogs.pop(snapshot.snapshot_id, None)
        if catalog_id is not None:
            self._skill_host.close(catalog_id)
        state = "aborted" if snapshot.state == "aborted" else "retired"
        current = self._snapshot_store.current
        for generation in unreferenced_generations:
            replacement = (
                current.generations.get(generation.plugin_id)
                if current is not None
                else None
            )
            await self._dispose_generation(
                generation,
                state=state,
                preserve_stable_alias=(
                    replacement is not None and replacement is not generation
                ),
                skip_composition_runtime=True,
            )
            self._forget_drained_generation(generation)
        workspace_mcp = snapshot.workspace_mcp_generation
        if (
            workspace_mcp is not None
            and not self._snapshot_store.workspace_mcp_is_referenced_elsewhere(
                workspace_mcp,
                excluding_snapshot_id=snapshot.snapshot_id,
            )
        ):
            await self._dispose_workspace_mcp(workspace_mcp, state=state)
        self._finish_drained_reload(snapshot.snapshot_id)

    async def _dispose_workspace_mcp(
        self,
        generation: WorkspaceMcpGeneration,
        *,
        state: str,
    ) -> None:
        cleanup_failures, _ = await _complete_critical(generation.scope.aclose())
        self._cleanup_failures.extend(cleanup_failures)
        generation.state = state

    async def prepare_changed(self) -> list[dict[str, object]]:
        async with self._candidate_prepare_lock:
            if self._ready_candidate is not None:
                return [self._ready_candidate_status()]
            discovered = {
                _resolve_plugin_id(mod): mod
                for mod in self.discover(installed_selector="latest")
            }
            return await self._prepare_changed(discovered=discovered)

    async def reconcile_changed(self) -> list[dict[str, object]]:
        async with self._candidate_prepare_lock:
            return await self._reconcile_changed_locked()

    async def install_candidate(
        self,
        *,
        source: str,
        marketplace: str,
        ref_name: str,
        sparse_paths: list[str],
    ) -> tuple[PluginInstallResult, dict[str, object]]:
        """Stage one immutable artifact and publish its latest runtime atomically."""

        # 1. 与 watcher 共用 candidate owner，写 cache 前拒绝未决候选。
        async with self._candidate_prepare_lock:
            _, preflight_cancelled = await _complete_critical(
                self._reconcile_changed_locked()
            )
            if preflight_cancelled:
                raise asyncio.CancelledError
            status = self.candidate_status()
            if status["candidate_state"] in {
                "preparing",
                "prepared",
                "validating",
                "commit_started",
                "latest_ready",
                "discarding",
                "promoting",
            }:
                raise RuntimeError(
                    "已有插件候选等待处理: "
                    f"plugin={status['candidate_plugin_id']} "
                    f"phase={status['candidate_state']} "
                    f"tx={status['candidate_reload_tx_id']}"
                )

            # 2. 持锁完成 artifact 发布与 runtime reconcile，不留 watcher 插入窗口。
            result, install_cancelled = await _complete_critical(
                asyncio.to_thread(
                    install_git_plugin,
                    workspace=self._workspace,
                    source=source,
                    marketplace=marketplace,
                    ref_name=ref_name,
                    sparse_paths=sparse_paths,
                    plugins_home=self.installed_plugins_home,
                    stage_candidate=True,
                )
            )
            _, reconcile_cancelled = await _complete_critical(
                self._reconcile_changed_locked()
            )
            plugin_id = f"{result.plugin_name}@{result.marketplace}"
            status = self.candidate_status()
            if result.staged_candidate and (
                status["candidate_plugin_id"] != plugin_id
                or status["candidate_state"] != "latest_ready"
            ):
                raise RuntimeError(
                    "插件候选未进入 latest_ready: "
                    f"requestedPlugin={plugin_id} "
                    f"installedGitRevision={result.source_revision} "
                    f"actualPlugin={status['candidate_plugin_id']} "
                    f"actualRuntimeRevision={status['candidate_source_revision']} "
                    f"phase={status['candidate_state']} "
                    f"tx={status['candidate_reload_tx_id']} "
                    f"error={status['candidate_error']}"
                )
            if install_cancelled or reconcile_cancelled:
                raise asyncio.CancelledError
            return result, status

    def annotate_reload(self, tx_id: str, details: dict[str, object]) -> None:
        """Append turn lineage evidence to an existing reload transaction."""

        self._reload_journal.annotate(tx_id, details)

    def require_installed_plugin(self, plugin_id: str) -> None:
        """Fail before registering uninstall when the plugin has no installed owner."""

        manifest = load_plugin_manifest(_plugins_home(self._installed_cache_root))
        if plugin_id not in manifest:
            raise RuntimeError(f"插件未安装: {plugin_id}")

    async def _reconcile_changed_locked(self) -> list[dict[str, object]]:
        """Reconcile discovered latest artifacts while candidate ownership is held."""

        await self._snapshot_store.retry_drains()
        results: list[dict[str, object]] = []
        ready = self._ready_candidate
        if ready is not None:
            manifest = load_plugin_manifest(_plugins_home(self._installed_cache_root))
            if manifest.get(ready.plugin_id, True):
                return [self._ready_candidate_status()]
            results.append(await self._drop_ready(ready.plugin_id))
        discovered = {
            _resolve_plugin_id(mod): mod
            for mod in self.discover(installed_selector="latest")
        }
        manifest = load_plugin_manifest(_plugins_home(self._installed_cache_root))
        desired = {
            plugin_id
            for plugin_id, mod in discovered.items()
            if mod.get("package_id") or manifest.get(plugin_id, True)
        }
        for plugin_id in sorted(set(self._active_generations) - desired):
            results.append(await self._deactivate_plugin(plugin_id))
        for plugin_id in sorted(desired.intersection(self._active_generations)):
            prepared = await self._prepare_changed(
                discovered=discovered,
                plugin_ids={plugin_id},
                force_reprepare=True,
            )
            if not prepared:
                continue
            result = prepared[0]
            if result.get("prepared_generation") is None:
                results.append(result)
                continue
            publication = await self._publish_prepared(plugin_id)
            results.append(publication)
            if publication.get("publication_state") == "latest_ready":
                return results
        for plugin_id in sorted(desired - set(self._active_generations)):
            generation = await self._load_one(discovered[plugin_id], activate=False)
            if generation is None:
                _discard_installed_candidate_mod(discovered[plugin_id])
                continue
            publication = await self._publish_prepared(plugin_id)
            results.append(publication)
            if publication.get("publication_state") == "latest_ready":
                return results
        return results

    async def reconcile_disabled_and_drain(self, plugin_id: str) -> None:
        async with self._candidate_prepare_lock:
            manifest = load_plugin_manifest(_plugins_home(self._installed_cache_root))
            if manifest.get(plugin_id, False):
                raise RuntimeError(f"插件尚未禁用: {plugin_id}")
            if self._ready_candidate is not None:
                if self._ready_candidate.plugin_id != plugin_id:
                    raise RuntimeError(
                        "存在其他插件 latest，必须先 promote/discard: "
                        f"{self._ready_candidate.plugin_id}"
                    )
                _ = await self._drop_ready(plugin_id)
            active = self._active_generations.get(plugin_id)
            draining = self._draining_generations.get(plugin_id, [])
            if active is None and not draining:
                return
            if active is not None:
                _ = await self._deactivate_plugin(plugin_id)
                draining = self._draining_generations[plugin_id]
            for generation in draining:
                await self._snapshot_store.wait_for_generation_drained(generation)
                if not generation.scope.closed:
                    raise RuntimeError(f"插件旧代资源尚未关闭: {plugin_id}")
            _ = self._draining_generations.pop(plugin_id, None)

    async def _deactivate_plugin(self, plugin_id: str) -> dict[str, object]:
        active = self._active_generations[plugin_id]
        generations = {
            key: generation
            for key, generation in self._active_generations.items()
            if key != plugin_id
        }
        snapshot, catalog_id = await self._compile_topology_snapshot(generations)
        try:
            self._compile_snapshot_event_handlers(snapshot)
            if self._dashboard_preparer is not None:
                self._dashboard_preparer(snapshot)
        except BaseException:
            self._skill_host.close(catalog_id)
            await self._dispose_unreferenced_composition_root(snapshot)
            raise

        old_services = active.contributions.managed_services
        old_channels = active.contributions.channels
        old_commands = self.stable_telegram_command_catalog()
        new_commands = self._snapshot_bot_commands(snapshot)
        exclusive_endpoint_changed = bool(old_services or old_channels)
        command_catalog_changed = old_commands != new_commands
        v3_channel_catalog_changed = (
            self._channel_catalog_identity(self.current_snapshot)
            != self._channel_catalog_identity(snapshot)
        )
        publication_gated = (
            exclusive_endpoint_changed
            or command_catalog_changed
            or v3_channel_catalog_changed
        )
        from agent.plugins.snapshot import get_current_runtime_lease

        if (
            exclusive_endpoint_changed or v3_channel_catalog_changed
        ) and get_current_runtime_lease() is not None:
            self._skill_host.close(catalog_id)
            await self._dispose_unreferenced_composition_root(snapshot)
            raise RuntimeError("持有 RuntimeSnapshot lease 时不能切换独占端点")
        quiesced = (
            self._snapshot_store.pause_admission()
            if publication_gated
            else None
        )
        transaction = None
        try:
            if quiesced is not None:
                if exclusive_endpoint_changed and self._endpoint_quiescer is not None:
                    await self._endpoint_quiescer()
                if exclusive_endpoint_changed or v3_channel_catalog_changed:
                    await self._snapshot_store.wait_for_no_leases(quiesced)
            self._snapshot_skill_catalogs[snapshot.snapshot_id] = catalog_id
            transaction = self._snapshot_store.begin_publish(
                snapshot,
                admission_gated=quiesced is not None,
            )
            await self._post_snapshot_invariants(snapshot)
        except BaseException:
            if transaction is not None:
                await self._snapshot_store.abort(transaction)
            else:
                await self._snapshot_store.resume(quiesced)
                _ = self._snapshot_skill_catalogs.pop(snapshot.snapshot_id, None)
                self._skill_host.close(catalog_id)
                await self._dispose_unreferenced_composition_root(snapshot)
            if self._endpoint_resumer is not None and exclusive_endpoint_changed:
                await self._endpoint_resumer()
            raise

        commit_error: BaseException | None = None
        commit_cancelled = False
        try:
            assert transaction is not None
            _, commit_cancelled = await _complete_critical(
                self._commit_snapshot_with_publication_participants(
                    transaction,
                    plugin_id=plugin_id,
                    old_services=old_services,
                    new_services={},
                    old_channels=old_channels,
                    new_channels=(),
                    old_commands=old_commands,
                    new_commands=new_commands,
                    promote_latest=False,
                    force_provisional=exclusive_endpoint_changed,
                    after_open=lambda: self._retire_generation(active),
                )
            )
        except BaseException as error:
            commit_error = error
        if commit_error is not None:
            if self._snapshot_store.pending_candidate is snapshot:
                await self._snapshot_store.abort(
                    transaction,
                    reopen_previous=not isinstance(
                        commit_error,
                        _PublicationParticipantRestoreError,
                    ),
                )
            if (
                self._endpoint_resumer is not None
                and exclusive_endpoint_changed
                and self.current_snapshot is not None
                and self.current_snapshot.accepting_leases
            ):
                await self._endpoint_resumer()
            raise commit_error

        _ = self._active_generations.pop(plugin_id)
        self._channels = [
            channel
            for generation in self._active_generations.values()
            for channel in generation.contributions.channels
        ]
        resume_cancelled = False
        if self._endpoint_resumer is not None and exclusive_endpoint_changed:
            _, resume_cancelled = await _complete_critical(self._endpoint_resumer())
        if commit_cancelled or resume_cancelled:
            raise asyncio.CancelledError
        result: dict[str, object] = {
            "plugin_id": plugin_id,
            "old_generation": active.generation_id,
            "new_generation": None,
            "snapshot_id": snapshot.snapshot_id,
            "publication_state": "disabled",
        }
        logger.info(
            "plugin_snapshot_status %s",
            json.dumps(result, ensure_ascii=False, sort_keys=True),
        )
        return result

    async def _switch_plugin_endpoints(
        self,
        plugin_id: str,
        old_services: dict[str, dict[str, Any]],
        new_services: dict[str, dict[str, Any]],
        old_channels: tuple[Channel, ...],
        new_channels: tuple[Channel, ...],
        old_commands: tuple[tuple[str, str], ...],
        new_commands: tuple[tuple[str, str], ...],
    ) -> None:
        services_changed = old_services != new_services
        channels_changed = old_channels != new_channels
        if self._endpoint_switcher is not None:
            await self._endpoint_switcher(
                plugin_id,
                old_services,
                new_services,
                old_channels,
                new_channels,
                old_commands,
                new_commands,
            )
            return
        if services_changed and channels_changed:
            raise RuntimeError("同时切换 managed service 与 Channel 需要统一端点宿主")
        if services_changed:
            if self._service_switcher is None:
                raise RuntimeError("managed service 宿主未绑定")
            await self._service_switcher(plugin_id, old_services, new_services)
        if channels_changed:
            if self._channel_switcher is None:
                raise RuntimeError("Channel 宿主未绑定")
            await self._channel_switcher(plugin_id, old_channels, new_channels)

    async def _commit_snapshot_with_publication_participants(
        self,
        transaction: SnapshotTransaction,
        *,
        plugin_id: str,
        old_services: dict[str, dict[str, Any]],
        new_services: dict[str, dict[str, Any]],
        old_channels: tuple[Channel, ...],
        new_channels: tuple[Channel, ...],
        old_commands: tuple[tuple[str, str], ...],
        new_commands: tuple[tuple[str, str], ...],
        promote_latest: bool,
        force_provisional: bool = False,
        provisional_started: bool = False,
        before_open: Callable[[], None] | None = None,
        after_open: Callable[[], None] | None = None,
    ) -> SnapshotTransaction:
        """Publish one snapshot around a single closed external-participant step."""

        # 1. Snapshots without external participants retain the one-step path.
        endpoints_changed = (
            old_services != new_services
            or old_channels != new_channels
            or old_commands != new_commands
        )
        channel_catalog_changed = (
            self._channel_catalog_identity(transaction.previous)
            != self._channel_catalog_identity(transaction.candidate)
        )
        activity_catalog_changed = (
            self._activity_catalog_identity(transaction.previous)
            != self._activity_catalog_identity(transaction.candidate)
        )
        if (
            not endpoints_changed
            and not channel_catalog_changed
            and not activity_catalog_changed
            and not force_provisional
            and not provisional_started
        ):
            if promote_latest:
                return await self._snapshot_store.promote_latest(
                    before_open=before_open,
                    after_open=after_open,
                )
            await self._snapshot_store.commit(
                transaction,
                before_open=before_open,
                after_open=after_open,
            )
            return transaction

        # 2. Close both snapshots before any service/channel/command side effect.
        provisional = transaction
        if not provisional_started:
            provisional = (
                await self._snapshot_store.promote_latest_provisional()
                if promote_latest
                else transaction
            )
            if not promote_latest:
                await self._snapshot_store.commit_provisional(provisional)

        channel_state: _ChannelPublicationState | None = None
        activity_transaction: ActivityTransaction | None = None
        participants_switch_attempted = False
        forward_error: BaseException | None = None
        try:
            if activity_catalog_changed:
                activity_host = self._activity_host
                if activity_host is None:
                    raise RuntimeError("v3 Activity catalog 已声明但 ActivityHost 尚未绑定")
                target_lease = self._snapshot_store.retain_publication_target(
                    provisional
                )
                activity_transaction = await activity_host.prepare_transaction(
                    target_lease
                )
                await activity_host.pause_and_drain(activity_transaction)
            channel_state = self._prepare_channel_publication(
                provisional.previous,
                provisional.candidate,
            )
            await self._close_channel_publication(channel_state)
            if endpoints_changed:
                participants_switch_attempted = True
                try:
                    await self._switch_plugin_endpoints(
                        plugin_id,
                        old_services,
                        new_services,
                        old_channels,
                        new_channels,
                        old_commands,
                        new_commands,
                    )
                except BaseException as error:
                    forward_error = error
                    raise
            await self._start_channel_publication(channel_state)
            if activity_transaction is not None:
                assert self._activity_host is not None
                await self._activity_host.materialize_closed(activity_transaction)

            def open_participants() -> None:
                if after_open is not None:
                    after_open()
                if activity_transaction is not None:
                    assert self._activity_host is not None
                    self._activity_host.finalize(activity_transaction)
                assert channel_state is not None
                self._open_channel_publication(channel_state)

            await self._snapshot_store.finalize_provisional(
                provisional,
                before_open=before_open,
                after_open=open_participants,
            )
            if activity_transaction is not None:
                assert self._activity_host is not None
                await self._activity_host.open(activity_transaction)
        except BaseException as publication_error:
            if (
                activity_transaction is not None
                and activity_transaction.finalized
                and not activity_transaction.settled
                and self.current_snapshot is provisional.candidate
            ):
                provisional.candidate.accepting_leases = False
                raise _PublicationParticipantRestoreError(
                    "Activity 新 owner 已提交，但旧 child cleanup 尚未完成",
                    resources=("activity-publication",),
                ) from publication_error
            rollback_errors: list[BaseException] = []
            channel_cleanup_failed = False
            activity_cleanup_failed = False
            endpoint_restore_failed = False
            if (
                activity_transaction is not None
                and not activity_transaction.settled
            ):
                assert self._activity_host is not None
                try:
                    await self._activity_host.rollback(activity_transaction)
                except BaseException as caught:
                    rollback_errors.append(caught)
                    activity_cleanup_failed = True
            if channel_state is not None:
                old_snapshot_id = (
                    None
                    if channel_state.previous is None
                    else channel_state.old_runtime.snapshot_id
                    if channel_state.old_runtime is not None
                    else None
                )
                channel_cleanup_failed = (
                    self._channel_generation_host.failure(
                        channel_state.candidate.snapshot_id
                    )
                    is not None
                    or (
                        old_snapshot_id is not None
                        and self._channel_generation_host.failure(old_snapshot_id)
                        is not None
                    )
                )
                try:
                    await self._stop_staged_channel_publication(channel_state)
                except BaseException as caught:
                    rollback_errors.append(caught)
                    channel_cleanup_failed = True
                if channel_cleanup_failed and not rollback_errors:
                    rollback_errors.append(publication_error)
            if participants_switch_attempted and not channel_cleanup_failed:
                try:
                    await self._switch_plugin_endpoints(
                        plugin_id,
                        new_services,
                        old_services,
                        new_channels,
                        old_channels,
                        new_commands,
                        old_commands,
                    )
                except BaseException as caught:
                    rollback_errors.append(caught)
                    endpoint_restore_failed = True
            if channel_state is not None and not channel_cleanup_failed:
                try:
                    await self._restore_old_channel_publication(channel_state)
                except BaseException as caught:
                    rollback_errors.append(caught)
                    channel_cleanup_failed = True
            await self._snapshot_store.rollback_provisional(
                provisional,
                keep_candidate_latest=promote_latest,
                reopen_previous=not rollback_errors,
            )
            if not rollback_errors and channel_state is not None:
                self._reopen_restored_channel_publication(channel_state)
            self._abort_channel_boot_transactions(
                provisional.candidate,
                publication_error,
            )
            if rollback_errors:
                resources: list[str] = []
                if activity_cleanup_failed:
                    resources.append("activity-publication")
                if channel_cleanup_failed:
                    resources.extend(("plugin-endpoint", "channel-publication"))
                elif endpoint_restore_failed:
                    resources.append("plugin-endpoint")
                raise _PublicationParticipantRestoreError(
                    "外部 publication participant 失败后旧 owner 恢复失败: "
                    + "; ".join(
                        str(error) or type(error).__name__
                        for error in rollback_errors
                    ),
                    resources=tuple(resources),
                ) from rollback_errors[0]
            if forward_error is not None:
                if isinstance(forward_error, asyncio.CancelledError):
                    raise forward_error
                raise _PublicationParticipantSwitchError(
                    "外部 publication participant 拒绝切换: "
                    f"{str(forward_error) or type(forward_error).__name__}"
                ) from forward_error
            raise publication_error
        return provisional

    async def _compile_topology_snapshot(
        self,
        generations: dict[str, PluginGeneration],
    ) -> tuple[RuntimeSnapshot, str]:
        self._generation_sequence += 1
        catalog_id = f"topology:{self._generation_sequence}:{secrets.token_hex(4)}"
        ordered = list(generations.values())
        active_ordered = self._static_active_generations(ordered)
        catalog = self._skill_host.prepare(
            catalog_id,
            normal_roots=PluginSkillHost.roots_for(active_ordered, drift=False),
            drift_roots=PluginSkillHost.roots_for(active_ordered, drift=True),
            ignored_normal_roots=tuple(
                root
                for generation in active_ordered
                for root in generation.contributions.skill_roots
            ),
            ignored_drift_roots=tuple(
                root
                for generation in active_ordered
                for root in generation.contributions.drift_skill_roots
            ),
        )
        composition_root, created_root = await self._resolve_composition_root(
            generations
        )
        try:
            snapshot = self._snapshot_compiler.compile(
                generations,
                snapshot_revision=catalog_id,
                workspace_mcp_generation=self._active_workspace_mcp,
                composition_root=composition_root,
                private_proactive_catalog=build_private_proactive_catalog(
                    generations.values(),
                    root_instance_token=(
                        None
                        if composition_root is None
                        else composition_root.instance_token
                    ),
                ),
            )
            _validate_static_manifest_runtime(snapshot, generations)
            snapshot.skill_catalog_generation_id = catalog_id
            snapshot.plugin_skill_index = catalog.normal_plugins
            snapshot.tool_registry = self._compile_snapshot_tools(
                generations,
                self._active_workspace_mcp,
            )
            snapshot.tool_hooks = self._compile_snapshot_tool_hooks(generations)
            self._validate_snapshot_command_claims(snapshot)
            return snapshot, catalog_id
        except BaseException:
            self._skill_host.close(catalog_id)
            if created_root and composition_root is not None:
                await composition_root.dispose()
            raise

    async def publish_prepared(self, plugin_id: str) -> dict[str, object]:
        async with self._candidate_prepare_lock:
            return await self._publish_prepared(plugin_id)

    async def switch_ready(self, plugin_id: str) -> dict[str, object]:
        """Promote the one ready installed candidate without rebuilding it."""

        async with self._candidate_prepare_lock:
            ready = self._require_ready_candidate(plugin_id)
            generation = ready.candidate
            tx_id = generation.reload_tx_id
            if tx_id is None:
                raise RuntimeError("latest candidate 缺少 reload transaction")
            if self._reload_journal.get(tx_id).phase != "latest_ready":
                raise RuntimeError("latest candidate 已被 runtime recovery 撤销准入")
            from agent.plugins.context import PreparedPluginKVStore

            context = (
                None
                if isinstance(generation.instance, ComposablePlugin)
                else cast(Any, generation.instance).context
            )
            kv_store = None if context is None else context.kv_store
            if isinstance(kv_store, PreparedPluginKVStore) and kv_store.dirty:
                raise RuntimeError("候选插件修改了 KV，read-only 验证不能 promote")

            old_services = (
                ready.previous.contributions.managed_services
                if ready.previous is not None
                else {}
            )
            target_contributions = (
                generation.production_contributions or generation.contributions
            )
            new_services = target_contributions.managed_services
            old_channels = (
                ready.previous.contributions.channels
                if ready.previous is not None
                else ()
            )
            new_channels = target_contributions.channels
            old_commands = self.stable_telegram_command_catalog()
            new_commands = self._snapshot_bot_commands(ready.snapshot)
            stable_snapshot = self.current_snapshot
            v3_runtime_handoff = self._composition_runtime_declared(
                ready.snapshot,
                plugin_id,
            ) or (
                stable_snapshot is not None
                and self._composition_runtime_declared(
                    stable_snapshot,
                    plugin_id,
                )
            )
            exclusive_endpoint_changed = (
                old_services != new_services
                or old_channels != new_channels
                or v3_runtime_handoff
            )
            command_catalog_changed = old_commands != new_commands
            v3_channel_catalog_changed = (
                self._channel_catalog_identity(stable_snapshot)
                != self._channel_catalog_identity(ready.snapshot)
            )
            publication_gated = (
                exclusive_endpoint_changed
                or command_catalog_changed
                or v3_channel_catalog_changed
            )
            from agent.plugins.snapshot import get_current_runtime_lease

            if (
                exclusive_endpoint_changed or v3_channel_catalog_changed
            ) and get_current_runtime_lease() is not None:
                raise RuntimeError(
                    "持有 RuntimeSnapshot lease 时不能切换 Channel runtime"
                )

            skill_linker, stable_skill_plugins, target_skill_plugins = (
                self._prepare_skill_links_for_promotion(generation, ready.snapshot)
            )

            # 1. Seal both stable and validation leases before touching ownership.
            candidate_snapshot = self._snapshot_store.pause_candidate_admission(
                ready.snapshot
            )
            quiesced_snapshot = (
                self._snapshot_store.pause_admission() if publication_gated else None
            )
            runtime_restore_started = False
            provisional_transaction: SnapshotTransaction | None = None
            provisional_cancelled = False
            if publication_gated:
                try:
                    if exclusive_endpoint_changed and self._endpoint_quiescer is not None:
                        await self._endpoint_quiescer()
                    if quiesced_snapshot is not None and (
                        exclusive_endpoint_changed or v3_channel_catalog_changed
                    ):
                        await self._snapshot_store.wait_for_no_leases(quiesced_snapshot)
                    await self._snapshot_store.wait_for_no_leases(ready.snapshot)
                    self._snapshot_store.seal_candidate_validation(ready.snapshot)
                    (
                        provisional_transaction,
                        provisional_cancelled,
                    ) = await _complete_critical(
                        self._snapshot_store.promote_latest_provisional()
                    )
                    runtime_restore_started = True
                    await self._restore_ready_runtime(ready)
                    generation = ready.candidate
                    kv_store = (
                        None
                        if isinstance(generation.instance, ComposablePlugin)
                        else cast(Any, generation.instance).context.kv_store
                    )
                    new_services = generation.contributions.managed_services
                    new_channels = generation.contributions.channels
                    new_commands = self._snapshot_bot_commands(ready.snapshot)
                except BaseException:
                    gated_runtime_error: BaseException | None = None
                    if runtime_restore_started:
                        try:
                            await self._rollback_composition_runtime_replacement(
                                generation
                            )
                        except BaseException as error:
                            gated_runtime_error = error
                    if provisional_transaction is not None:
                        _, rollback_cancelled = await _complete_critical(
                            self._snapshot_store.rollback_provisional(
                                provisional_transaction,
                                keep_candidate_latest=True,
                                reopen_previous=gated_runtime_error is None,
                            )
                        )
                        provisional_cancelled = (
                            provisional_cancelled or rollback_cancelled
                        )
                    if gated_runtime_error is None:
                        await self._snapshot_store.resume(quiesced_snapshot)
                    if (
                        gated_runtime_error is None
                        and exclusive_endpoint_changed
                        and self._endpoint_resumer is not None
                    ):
                        await self._endpoint_resumer()
                    if runtime_restore_started and self._ready_candidate is ready:
                        if gated_runtime_error is None:
                            _ = await self._drop_ready(plugin_id)
                    else:
                        await self._snapshot_store.resume(candidate_snapshot)
                    if gated_runtime_error is not None:
                        self._record_composition_runtime_failure(
                            generation,
                            gated_runtime_error,
                            formal_effects=(
                                "candidate_validation_stopped",
                                "old_runtime_restore_uncertain",
                            ),
                        )
                        raise RuntimeError(
                            "candidate formalization 失败后旧 v3 runtime 恢复失败"
                        ) from gated_runtime_error
                    raise
            else:
                try:
                    await self._snapshot_store.wait_for_no_leases(ready.snapshot)
                    self._snapshot_store.seal_candidate_validation(ready.snapshot)
                    runtime_restore_started = True
                    await self._restore_ready_runtime(ready)
                    generation = ready.candidate
                    kv_store = (
                        None
                        if isinstance(generation.instance, ComposablePlugin)
                        else cast(Any, generation.instance).context.kv_store
                    )
                except BaseException:
                    formalization_runtime_error: BaseException | None = None
                    if runtime_restore_started:
                        try:
                            await self._rollback_composition_runtime_replacement(
                                generation
                            )
                        except BaseException as error:
                            formalization_runtime_error = error
                    if runtime_restore_started and self._ready_candidate is ready:
                        if formalization_runtime_error is None:
                            _ = await self._drop_ready(plugin_id)
                    else:
                        await self._snapshot_store.resume(candidate_snapshot)
                    if formalization_runtime_error is not None:
                        self._record_composition_runtime_failure(
                            generation,
                            formalization_runtime_error,
                            formal_effects=(
                                "candidate_validation_stopped",
                                "old_runtime_restore_uncertain",
                            ),
                        )
                        raise RuntimeError(
                            "candidate formalization 失败后旧 v3 runtime 恢复失败"
                        ) from formalization_runtime_error
                    raise

            # 2. 先切可回滚的 Skill 投影，再提交持久 pointer；整个回调不跨 await。
            skill_links_switched = False
            link_result = None

            def before_open() -> None:
                nonlocal link_result, skill_links_switched
                try:
                    link_result = skill_linker.sync(target_skill_plugins)
                except BaseException:
                    skill_linker.sync(stable_skill_plugins)
                    raise
                skill_links_switched = True
                phase = self._reload_journal.get(tx_id).phase
                if phase != "latest_ready":
                    raise RuntimeError(
                        "candidate runtime recovery 已阻止 pointer commit"
                    )
                self._advance_reload(generation, "promoting")
                artifact_base = _installed_artifact_base(generation)
                if artifact_base is not None:
                    _switch_ready_pointer(ready, artifact_base)
                if isinstance(kv_store, PreparedPluginKVStore):
                    kv_store.commit()

            # 3. Snapshot pointer 切换后再替换 manager 的 stable generation owner。
            def after_open() -> None:
                self._activate_published_generation(generation, ready.previous)
                generation.state = "active"
                if generation.mcp_catalog is not None:
                    self._mcp_host.mark_active(generation.generation_id)
                self._scopes[generation.module_path] = generation.scope
                self._loaded.add(generation.module_path)
                self._active_generations[plugin_id] = generation
                if ready.previous is not None:
                    self._retire_generation(ready.previous)
                self._channels = [
                    channel
                    for item in self._active_generations.values()
                    for channel in item.contributions.channels
                ]

            previous_snapshot = self.current_snapshot
            if previous_snapshot is not None:
                self._drain_transactions[previous_snapshot.snapshot_id] = tx_id
            try:
                transaction, final_cancelled = await _complete_critical(
                    self._commit_snapshot_with_publication_participants(
                        provisional_transaction
                        or SnapshotTransaction(
                            previous=previous_snapshot,
                            candidate=ready.snapshot,
                        ),
                        plugin_id=plugin_id,
                        old_services=old_services,
                        new_services=new_services,
                        old_channels=old_channels,
                        new_channels=new_channels,
                        old_commands=old_commands,
                        new_commands=new_commands,
                        promote_latest=True,
                        force_provisional=exclusive_endpoint_changed,
                        provisional_started=provisional_transaction is not None,
                        before_open=before_open,
                        after_open=after_open,
                    )
                )
                cancelled = provisional_cancelled or final_cancelled
            except BaseException as publication_error:
                skill_error: BaseException | None = None
                runtime_error: BaseException | None = None
                participant_restore_error = (
                    publication_error
                    if isinstance(
                        publication_error,
                        _PublicationParticipantRestoreError,
                    )
                    else None
                )
                if skill_links_switched:
                    try:
                        skill_linker.sync(stable_skill_plugins)
                    except BaseException as error:
                        skill_error = error
                if runtime_restore_started:
                    try:
                        await self._rollback_composition_runtime_replacement(
                            generation
                        )
                    except BaseException as error:
                        runtime_error = error
                if (
                    previous_snapshot is not None
                    and self.current_snapshot is previous_snapshot
                ):
                    _ = self._drain_transactions.pop(
                        previous_snapshot.snapshot_id,
                        None,
                    )
                if (
                    runtime_error is None
                    and skill_error is None
                    and participant_restore_error is None
                ):
                    await self._snapshot_store.resume(quiesced_snapshot)
                if (
                    runtime_error is None
                    and skill_error is None
                    and participant_restore_error is None
                    and self._endpoint_resumer is not None
                    and exclusive_endpoint_changed
                ):
                    await self._endpoint_resumer()
                recovery_error = (
                    runtime_error or participant_restore_error or skill_error
                )
                if self._ready_candidate is ready and recovery_error is None:
                    _ = await self._drop_ready(plugin_id)
                if recovery_error is not None:
                    recovery_resources: list[str] = []
                    recovery_effects: list[str] = []
                    if runtime_error is not None:
                        recovery_resources.append("composition-runtime")
                        recovery_effects.extend(
                            (
                                "candidate_formal_started",
                                "old_runtime_restore_uncertain",
                            )
                        )
                    if participant_restore_error is not None:
                        recovery_resources.extend(participant_restore_error.resources)
                        if "plugin-endpoint" in participant_restore_error.resources:
                            recovery_effects.append("endpoint_restore_uncertain")
                        if "channel-publication" in participant_restore_error.resources:
                            recovery_effects.append("stable_channel_restore_uncertain")
                        if "activity-publication" in participant_restore_error.resources:
                            recovery_effects.append("stable_activity_restore_uncertain")
                    if skill_error is not None:
                        recovery_resources.append("plugin-skill-projection")
                        recovery_effects.append("stable_skill_restore_uncertain")
                    self._record_composition_runtime_failure(
                        generation,
                        recovery_error,
                        resource=",".join(recovery_resources),
                        formal_effects=tuple(recovery_effects),
                    )
                    raise RuntimeError(
                        "插件 promote 失败后存在未完成的 formal recovery: "
                        + ", ".join(recovery_resources)
                    ) from recovery_error
                raise
            self._ready_candidate = None
            generation.replaced_composition_runtime_generation = None
            self._track_reload_drain(generation, transaction.previous)
            if self._endpoint_resumer is not None and exclusive_endpoint_changed:
                await self._endpoint_resumer()
            assert link_result is not None
            logger.info(
                "插件 stable skill 投影同步完成: expected=%d created=%d repaired=%d removed=%d skipped=%d",
                link_result.expected,
                link_result.created,
                link_result.repaired,
                link_result.removed,
                link_result.skipped,
            )
            result = self._publication_status(
                plugin_id,
                active=ready.previous,
                candidate=generation,
                publication_state="promoted",
            )
            validation_data_dir = (
                self._workspace
                / "runtime"
                / "plugin-validation"
                / generation.generation_id
            )
            try:
                await asyncio.to_thread(
                    _remove_validation_data_dir, validation_data_dir
                )
            except Exception as error:
                logger.error(
                    "候选隔离 plugin-data 清理失败: %s: %s",
                    validation_data_dir,
                    error,
                )
            logger.info(
                "plugin_snapshot_status %s",
                json.dumps(result, ensure_ascii=False, sort_keys=True),
            )
            if cancelled:
                raise asyncio.CancelledError
            return result

    async def retry_runtime_recovery(self, plugin_id: str) -> dict[str, object]:
        """Retry one durable v3 runtime owner and reconcile its exact pointer target."""

        result, cancelled = await _complete_critical(
            self._retry_runtime_recovery_critical(plugin_id)
        )
        if cancelled:
            raise asyncio.CancelledError
        return result

    async def _retry_runtime_recovery_critical(
        self,
        plugin_id: str,
    ) -> dict[str, object]:
        """Complete one runtime recovery transaction before exposing cancellation."""

        async with self._candidate_prepare_lock:
            actions = tuple(
                action
                for action in self._reload_journal.pending_recovery()
                if action.plugin_id == plugin_id
                and action.action
                in {"retry_generation_cleanup", "retry_runtime_recovery"}
            )
            if len(actions) != 1:
                raise RuntimeError("插件没有待执行的 runtime recovery")
            action = actions[0]
            ready = self._ready_candidate
            if (
                ready is not None
                and (
                    ready.plugin_id != plugin_id
                    or ready.candidate.reload_tx_id != action.tx_id
                )
            ):
                ready = None
            prepared = self._prepared_generations.get(plugin_id)
            if (
                prepared is not None
                and prepared.reload_tx_id != action.tx_id
            ):
                prepared = None

            # 1. Retry every exact retained Host owner before changing pointers.
            receipts: list[str] = []
            resource = action.failure_resource or ""
            if "runtime-snapshot-drain" in resource:
                await self._snapshot_store.retry_drains()
                receipts.append("runtime-snapshot-drain-complete")
            if "activity-publication" in resource:
                activity_host = self._activity_host
                if activity_host is None:
                    raise RuntimeError("Activity recovery 缺少 ActivityHost owner")
                await activity_host.retry_recovery()
                receipts.append("stable-activity-runtime-restored")
            channel_tokens = tuple(
                item.removeprefix("channel-binding:")
                for item in resource.split(",")
                if item.startswith("channel-binding:")
            )
            for binding_token in channel_tokens:
                await self._channel_generation_host.retry_generation_cleanup(
                    binding_token
                )
                receipts.append(
                    f"channel-binding-cleanup-complete:{binding_token}"
                )
            retained_generation_ids = tuple(
                dict.fromkeys(
                    generation_id
                    for generation_id in (
                        action.generation_id,
                        action.base_generation_id,
                    )
                    if generation_id is not None
                    and self._composition_generation_host.failure(generation_id)
                    is not None
                )
            )
            for generation_id in retained_generation_ids:
                if action.action == "retry_runtime_recovery":
                    receipts.append(
                        await self._composition_generation_host.retry_runtime_recovery(
                            generation_id
                        )
                    )
                if self._composition_generation_host.failure(generation_id) is None:
                    _ = self._composition_runtime_generations.pop(
                        generation_id,
                        None,
                    )
                else:
                    receipts.append(
                        await self._composition_generation_host.retry_generation_cleanup(
                            generation_id
                        )
                    )

            # 2. Rebuild the exact committed stable runtime when rollback left it absent.
            stable = self._active_generations.get(plugin_id)
            current = self.current_snapshot
            if (
                action.action == "retry_runtime_recovery"
                and stable is not None
                and current is not None
                and self._composition_runtime_declared(current, plugin_id)
                and self._composition_generation_host.get(stable.generation_id) is None
                and (
                    ready is None
                    or ready.candidate.replaced_composition_runtime_generation is None
                )
            ):
                await self._start_composition_generation_runtime(
                    stable,
                    current,
                    mode="formal",
                )
                receipts.append("stable-composition-runtime-restored")

            # 3. Restore non-runtime formal effects only while their candidate owner exists.
            if (
                action.action == "retry_runtime_recovery"
                and ready is not None
                and ready.candidate.replaced_composition_runtime_generation is not None
            ):
                await self._restore_replaced_composition_runtime(ready.candidate)
                receipts.append("stable-composition-runtime-restored")
            if "plugin-endpoint" in resource:
                if ready is None:
                    raise RuntimeError("runtime recovery 缺少 endpoint candidate owner")
                old_services = (
                    {} if ready.previous is None else ready.previous.contributions.managed_services
                )
                old_channels = (
                    () if ready.previous is None else ready.previous.contributions.channels
                )
                new_contributions = (
                    ready.candidate.production_contributions
                    or ready.candidate.contributions
                )
                old_commands = self.stable_telegram_command_catalog()
                await self._switch_plugin_endpoints(
                    plugin_id,
                    new_contributions.managed_services,
                    old_services,
                    new_contributions.channels,
                    old_channels,
                    old_commands,
                    old_commands,
                )
                receipts.append("stable-plugin-endpoints-restored")
            if "plugin-skill-projection" in resource:
                if ready is None:
                    raise RuntimeError("runtime recovery 缺少 skill candidate owner")
                linker, stable_plugins, _target_plugins = (
                    self._prepare_skill_links_for_promotion(
                        ready.candidate,
                        ready.snapshot,
                    )
                )
                _ = linker.sync(stable_plugins)
                receipts.append("stable-skill-projection-restored")

            # 4. Normalize the exact durable target before acquiring new resources.
            if (
                action.base_artifact_pointer is not None
                or action.candidate_artifact_pointer is not None
            ):
                self._normalize_runtime_recovery_pointer(action)
            elif "@" in plugin_id:
                raise RuntimeError(
                    "installed runtime recovery 缺少 exact artifact pointer"
                )
            cancelled = False
            candidate_snapshot = self._snapshot_store.unpromoted_candidate
            if action.recovery_target == "base" and candidate_snapshot is not None:
                _, cancelled = await _complete_critical(
                    self._snapshot_store.discard_latest(candidate_snapshot)
                )
            if action.recovery_target == "base" and ready is not None:
                self._ready_candidate = None
            if action.recovery_target == "base" and prepared is not None:
                _ = self._prepared_generations.pop(plugin_id, None)
                _, prepared_cancelled = await _complete_critical(
                    self._dispose_generation(prepared, state="discarded")
                )
                cancelled = cancelled or prepared_cancelled
            if action.recovery_target == "candidate" and ready is not None:
                if self.current_snapshot is not ready.snapshot:
                    raise RuntimeError(
                        "runtime recovery candidate target 尚未成为 stable"
                    )
                self._ready_candidate = None

            # 5. Rebuild the exact stable Channel owner after identity normalization.
            restored_channel_runtime: ChannelGeneration | None = None
            current_channel_identity = self._channel_catalog_identity(current)
            channel_publication_failed = (
                bool(channel_tokens) or "channel-publication" in resource
            )
            if channel_publication_failed and current is None:
                self._active_channel_generation = None
                self._active_channel_catalog_identity = None
            elif channel_publication_failed and current is not None:
                active_runtime = self._active_channel_generation
                if current_channel_identity is None:
                    self._active_channel_generation = None
                    self._active_channel_catalog_identity = None
                elif (
                    active_runtime is None
                    or self._channel_generation_host.get(
                        active_runtime.snapshot_id
                    )
                    is None
                    or self._active_channel_catalog_identity
                    != current_channel_identity
                ):
                    restored_channel_runtime = (
                        await self._channel_generation_host.start_formal(
                            current,
                            self._channel_provider_factories(current),
                            boot_owner="plugin-manager-recovery",
                        )
                    )

            # 6. Open the exact Channel owner before any public admission resumes.
            if restored_channel_runtime is not None:
                self._active_channel_generation = restored_channel_runtime
                self._active_channel_catalog_identity = current_channel_identity
                restored_channel_runtime.open_admission()
                receipts.append("stable-channel-runtime-restored")
            receipt = ";".join(receipts) or "runtime-owner-already-clean"
            _, resume_cancelled = await _complete_critical(
                self._snapshot_store.resume(self.current_snapshot)
            )
            endpoint_resume_cancelled = False
            participant_only_recovery = all(
                item.startswith("channel-binding:")
                or item.startswith("channel-publication:")
                or item.startswith("activity-publication")
                for item in resource.split(",")
                if item
            )
            if self._endpoint_resumer is not None and not participant_only_recovery:
                _, endpoint_resume_cancelled = await _complete_critical(
                    self._endpoint_resumer()
                )
            self._reload_journal.finish_recovery(
                action,
                retry_receipt=receipt,
            )
            if cancelled or resume_cancelled or endpoint_resume_cancelled:
                raise asyncio.CancelledError
            active = self._active_generations.get(plugin_id)
            return {
                "plugin_id": plugin_id,
                "publication_state": "recovered",
                "recovery_target": action.recovery_target,
                "generation_id": (
                    None if active is None else active.generation_id
                ),
                "snapshot_id": (
                    None
                    if self.current_snapshot is None
                    else self.current_snapshot.snapshot_id
                ),
                "retry_receipt": receipt,
            }

    async def _restore_ready_runtime(
        self,
        ready: _ReadyPluginCandidate,
    ) -> None:
        """Replace isolated validation resources before formal endpoint commit."""

        generation = ready.candidate
        production = generation.production_contributions
        if production is None:
            return
        production_data_dir = generation.production_data_dir
        if production_data_dir is None:
            raise RuntimeError("候选缺少 production plugin-data identity")
        candidate_runtime = self._composition_generation_host.get(
            generation.generation_id
        )
        expected_mcp_catalog_digests = (
            None
            if candidate_runtime is None
            else candidate_runtime.mcp_catalog_digests
        )

        # 1. 隔离 Root 已封存，先停止其任务，再进入任何 formal await。
        if self._dashboard_validation_releaser is not None:
            await self._dashboard_validation_releaser(ready.snapshot)
        await self._stop_composition_generation_runtime(generation)
        validation_root = ready.snapshot.composition_root
        if validation_root is not None:
            await validation_root.dispose()

        # 2. Stop isolated services after every validation lease has ended.
        if generation.validation_managed_services:
            if self._candidate_service_stopper is None:
                raise RuntimeError("候选 managed service 隔离宿主未绑定")
            if self._candidate_service_health_check is None:
                raise RuntimeError("候选 managed service 健康检查未绑定")
            await self._candidate_service_health_check(generation.generation_id)
        if generation.mcp_catalog is not None:
            self._mcp_host.assert_healthy(generation.generation_id)
        if generation.validation_managed_services:
            assert self._candidate_service_stopper is not None
            await self._candidate_service_stopper(generation.generation_id)

        # 3. Reconnect MCP with formal endpoint env, then refresh snapshot payload.
        if generation.mcp_catalog is not None:
            await self._mcp_host.close(generation.generation_id)
            generation.mcp_catalog = None
        generation.contributions = production
        generation.data_dir = production_data_dir
        from agent.plugins.context import PreparedPluginKVStore

        if not isinstance(generation.instance, ComposablePlugin):
            context = cast(Any, generation.instance).context
            context.data_dir = production_data_dir
            context.workspace = self._workspace
            context.kv_store = PreparedPluginKVStore(
                production_data_dir / ".kv.json",
                can_write=lambda: _generation_can_write(generation),
                writer_id=generation.generation_id,
            )
        if production.mcp_servers or production.proactive_sources:
            generation.mcp_catalog = await self._mcp_host.prepare(
                generation.generation_id,
                server_specs=production.mcp_servers,
                required_tools=_required_mcp_tools(production.proactive_sources),
                scope=generation.scope,
            )
            generation.scope.defer(
                "production_mcp_catalog",
                lambda: self._mcp_host.close(generation.generation_id),
            )
        replacement = await self._compile_generation_snapshot(
            generation,
            allow_unready_stable_composition=True,
        )
        if replacement.snapshot_id != ready.snapshot.snapshot_id:
            await self._dispose_unreferenced_composition_root(replacement)
            raise RuntimeError(
                "候选隔离资源恢复后 snapshot identity 发生变化: "
                f"{ready.snapshot.snapshot_id} -> {replacement.snapshot_id}"
            )
        try:
            await self._stop_replaced_composition_runtime(generation)
            await self._start_composition_generation_runtime(
                generation,
                replacement,
                mode="formal",
                expected_mcp_catalog_digests=expected_mcp_catalog_digests,
            )
        except BaseException:
            try:
                await self._stop_composition_generation_runtime(generation)
                await self._restore_replaced_composition_runtime(generation)
            except BaseException as restore_error:
                raise RuntimeError(
                    "candidate formal runtime 失败后旧 stable runtime 恢复失败"
                ) from restore_error
            await self._dispose_unreferenced_composition_root(replacement)
            raise
        _replace_snapshot_payload(ready.snapshot, replacement)
        validation_workspace = generation.validation_workspace
        generation.validation_workspace = None
        if validation_workspace is not None:
            _remove_validation_data_dir(validation_workspace.parent)
        generation.validation_data_inventory = ()
        self._compile_snapshot_event_handlers(ready.snapshot)
        if self._dashboard_preparer is not None:
            self._dashboard_preparer(ready.snapshot)
        if not isinstance(generation.instance, ComposablePlugin):
            cast(Any, generation.instance).context.tool_registry = (
                ready.snapshot.tool_registry
            )
        generation.production_contributions = None
        generation.validation_managed_services = {}
        generation.production_data_dir = None

    async def drop_candidate(self, plugin_id: str) -> dict[str, object]:
        """Discard the one ready installed candidate and preserve stable."""

        async with self._candidate_prepare_lock:
            return await self._drop_ready(plugin_id)

    async def _drop_ready(self, plugin_id: str) -> dict[str, object]:
        ready = self._require_ready_candidate(plugin_id)
        tx_id = ready.candidate.reload_tx_id
        if tx_id is None:
            raise RuntimeError("latest candidate 缺少 reload transaction")
        phase = self._reload_journal.get(tx_id).phase
        if phase in {"latest_ready", "promoting"}:
            self._advance_reload(
                ready.candidate,
                "discarding",
                error="candidate behavior rejected",
            )
        elif phase != "discarding":
            raise RuntimeError(f"latest candidate 不能从 {phase} discard")
        artifact_base = _installed_artifact_base(ready.candidate)
        if artifact_base is not None:
            _restore_ready_pointer(ready, artifact_base)
        _, cancelled = await _complete_critical(
            self._snapshot_store.discard_latest(ready.snapshot)
        )
        retained = self._reload_journal.get(tx_id)
        if retained.phase in {"cleanup_failed", "degraded"}:
            raise RuntimeError(
                "candidate runtime cleanup 未完成，必须先执行 recovery"
            )
        self._advance_reload(
            ready.candidate,
            "aborted",
            error="candidate behavior rejected",
        )
        self._ready_candidate = None
        result = self._publication_status(
            plugin_id,
            active=ready.previous,
            candidate=ready.candidate,
            publication_state="discarded",
        )
        logger.info(
            "plugin_snapshot_status %s",
            json.dumps(result, ensure_ascii=False, sort_keys=True),
        )
        if cancelled:
            raise asyncio.CancelledError
        return result

    def candidate_status(self, plugin_id: str | None = None) -> dict[str, object]:
        ready = self._ready_candidate
        transaction = None
        if ready is not None and (plugin_id is None or ready.plugin_id == plugin_id):
            tx_id = ready.candidate.reload_tx_id
            if tx_id is None:
                raise RuntimeError("latest candidate 缺少 reload transaction")
            transaction = self._reload_journal.get(tx_id)
        else:
            latest = self._reload_journal.latest(plugin_id=plugin_id)
            if latest is not None and latest.phase not in {"complete", "recovered"}:
                transaction = latest
        return {
            "stable_snapshot_id": (
                self.current_snapshot.snapshot_id
                if self.current_snapshot is not None
                else None
            ),
            "latest_snapshot_id": (
                self.latest_snapshot.snapshot_id
                if self.latest_snapshot is not None
                else None
            ),
            "candidate_plugin_id": (
                transaction.plugin_id if transaction is not None else None
            ),
            "candidate_generation_id": (
                transaction.generation_id if transaction is not None else None
            ),
            "candidate_state": None if transaction is None else transaction.phase,
            "candidate_source_revision": (
                None if transaction is None else transaction.source_revision
            ),
            "candidate_reload_tx_id": (
                None if transaction is None else transaction.tx_id
            ),
            "candidate_error": None if transaction is None else transaction.error,
        }

    def candidate_child_evidence(
        self,
        plugin_id: str,
        generation_id: str,
        items: tuple[object, ...],
    ) -> tuple[str, ...]:
        """返回 child 真实成功使用的候选 Tool 或 Skill 证据。"""

        # 1. 从冻结的 latest snapshot 判定 owner，不信任 child 自报。
        ready = self._require_ready_candidate(plugin_id)
        generation = ready.candidate
        if generation.generation_id != generation_id:
            raise RuntimeError(
                "candidate child generation 身份不一致: "
                f"expected={generation.generation_id} actual={generation_id}"
            )
        registry = ready.snapshot.tool_registry
        if registry is None:
            raise RuntimeError("candidate RuntimeSnapshot 缺少 ToolRegistry")
        plugin_name = str(getattr(generation.instance, "name", plugin_id))
        owned_tools = registry.get_source_tool_names(
            "plugin", plugin_name, risk="read-only"
        )
        for server_name in generation.contributions.mcp_servers:
            owned_tools.update(
                registry.get_source_tool_names(
                    "mcp", server_name, risk="read-only"
                )
            )
        owned_skills = {
            skill_dir.name
            for root in generation.contributions.skill_roots
            for skill_dir in root.iterdir()
            if skill_dir.is_dir() and (skill_dir / "SKILL.md").is_file()
        }
        skill_catalog_generation_id = (
            generation.skill_catalog.generation_id
            if generation.skill_catalog is not None
            else None
        )

        # 2. 只接受成功工具 item 或本轮真实注入的候选 Skill。
        evidence: set[str] = set()
        for item in items:
            kind = getattr(getattr(item, "kind", None), "value", None)
            data = getattr(item, "data", None)
            if not isinstance(data, dict):
                continue
            if kind == "toolCall":
                name = data.get("name")
                if data.get("status") == "success" and name in owned_tools:
                    evidence.add(f"tool:{name}")
                provenance = data.get("runtimeProvenance")
                if (
                    data.get("status") == "success"
                    and name == "load_skill"
                    and isinstance(provenance, dict)
                    and provenance.get("kind") == "plugin-skill"
                    and provenance.get("pluginId") == plugin_id
                    and provenance.get("skillName") in owned_skills
                    and provenance.get("skillCatalogGenerationId")
                    == skill_catalog_generation_id
                    and provenance.get("runtimeSnapshotId") == ready.snapshot.snapshot_id
                ):
                    evidence.add(f"skill:{provenance['skillName']}")
        return tuple(sorted(evidence))

    def _ready_candidate_status(self) -> dict[str, object]:
        ready = self._ready_candidate
        if ready is None:
            raise RuntimeError("没有等待 promote/discard 的插件候选")
        return self._publication_status(
            ready.plugin_id,
            active=ready.previous,
            candidate=ready.candidate,
            publication_state="latest_ready",
        )

    def _require_ready_candidate(self, plugin_id: str) -> _ReadyPluginCandidate:
        ready = self._ready_candidate
        if ready is None:
            raise RuntimeError("没有等待 promote/discard 的插件候选")
        if ready.plugin_id != plugin_id:
            raise RuntimeError(f"latest 属于其他插件: {ready.plugin_id}")
        return ready

    async def _publish_prepared(self, plugin_id: str) -> dict[str, object]:
        generation = self._prepared_generations.get(plugin_id)
        if generation is None:
            raise KeyError(f"插件没有待发布候选: {plugin_id}")
        if generation.reload_tx_id is not None and (
            self._reload_journal.get(generation.reload_tx_id).phase != "prepared"
        ):
            raise RuntimeError("插件候选已被 runtime recovery 撤销准入")
        active = self._active_generations.get(plugin_id)
        stage_latest = _installed_generation_is_candidate(generation)
        production = generation.production_contributions or generation.contributions
        production_endpoint_changed = (
            active.contributions.managed_services if active is not None else {}
        ) != production.managed_services or (
            active.contributions.channels if active is not None else ()
        ) != production.channels
        try:
            if stage_latest:
                if (
                    generation.production_contributions is None
                    or generation.production_data_dir is None
                ):
                    raise RuntimeError("installed candidate 缺少隔离 plugin-data 身份")
                if (
                    generation.mcp_catalog is not None
                    and not generation.contributions.mcp_servers
                    and not generation.contributions.proactive_sources
                ):
                    await self._mcp_host.close(generation.generation_id)
                    generation.mcp_catalog = None
                if generation.validation_managed_services:
                    if self._candidate_service_starter is None:
                        raise RuntimeError("候选 managed service 隔离宿主未绑定")
                    await self._candidate_service_starter(
                        generation.generation_id,
                        generation.validation_managed_services,
                    )
                    candidate_service_stopper = self._candidate_service_stopper
                    assert candidate_service_stopper is not None
                    generation.scope.defer(
                        "validation_managed_services",
                        lambda: candidate_service_stopper(
                            generation.generation_id
                        ),
                    )
            workspace_generations = tuple(
                item
                for item in (
                    self._active_workspace_mcp,
                    self._prepared_workspace_mcp,
                )
                if item is not None
            )
            conflicts = sorted(
                set(generation.contributions.mcp_servers).intersection(
                    server_name
                    for item in workspace_generations
                    for server_name in item.catalog.servers
                )
            )
            if conflicts:
                raise RuntimeError(
                    f"插件 MCP 与 workspace server 名称冲突: {', '.join(conflicts)}"
                )
            prepared_snapshot = generation.runtime_snapshot
            generation.runtime_snapshot = await self._compile_generation_snapshot(
                generation,
                candidate_owner=generation,
            )
            await self._start_composition_generation_runtime(
                generation,
                generation.runtime_snapshot,
                mode="candidate",
            )
            if (
                prepared_snapshot is not None
                and prepared_snapshot is not generation.runtime_snapshot
            ):
                await self._dispose_unreferenced_composition_root(
                    prepared_snapshot
                )
            snapshot = generation.runtime_snapshot
            if not isinstance(generation.instance, ComposablePlugin):
                cast(Any, generation.instance).context.tool_registry = (
                    snapshot.tool_registry
                )
        except (asyncio.CancelledError, Exception) as error:
            error_text = str(error) or type(error).__name__
            self._record_failed_gate(
                plugin_id=plugin_id,
                revision=generation.source_revision,
                check_id="publish_rebase",
                reason=error_text,
            )
            await self.discard_prepared(
                plugin_id,
                error=f"publish_rebase: {error_text}",
            )
            raise
        try:
            await self._prepare_generation(generation)
        except (asyncio.CancelledError, Exception) as error:
            error_text = str(error) or type(error).__name__
            self._record_failed_gate(
                plugin_id=plugin_id,
                revision=generation.source_revision,
                check_id="prepare",
                reason=error_text,
            )
            await self.discard_prepared(
                plugin_id,
                error=f"prepare: {error_text}",
            )
            if isinstance(error, asyncio.CancelledError):
                raise
            result = self._publication_status(
                plugin_id,
                active=active,
                candidate=generation,
                publication_state="failed",
            )
            logger.info(
                "plugin_snapshot_status %s",
                json.dumps(result, ensure_ascii=False, sort_keys=True),
            )
            return result

        old_services = (
            active.contributions.managed_services if active is not None else {}
        )
        new_services = generation.contributions.managed_services
        old_channels = active.contributions.channels if active is not None else ()
        new_channels = generation.contributions.channels
        old_commands = self.stable_telegram_command_catalog()
        new_commands = self._snapshot_bot_commands(snapshot)
        current = self.current_snapshot
        v3_runtime_handoff = self._composition_runtime_declared(
            snapshot,
            plugin_id,
        ) or (
            current is not None
            and self._composition_runtime_declared(current, plugin_id)
        )
        exclusive_endpoint_changed = (
            old_services != new_services
            or old_channels != new_channels
            or v3_runtime_handoff
        )
        command_catalog_changed = old_commands != new_commands
        v3_channel_catalog_changed = (
            self._channel_catalog_identity(current)
            != self._channel_catalog_identity(snapshot)
        )
        publication_gated = not stage_latest and (
            exclusive_endpoint_changed
            or command_catalog_changed
            or v3_channel_catalog_changed
        )
        if stage_latest and production_endpoint_changed:
            self._reload_journal.annotate(
                cast(str, generation.reload_tx_id),
                {
                    "event": "exclusive_endpoints_deferred",
                    "managed_services_changed": (
                        active is None
                        or active.contributions.managed_services
                        != production.managed_services
                    ),
                    "channels_changed": (
                        active is None
                        and bool(production.channels)
                        or active is not None
                        and active.contributions.channels != production.channels
                    ),
                },
            )
        self._compile_snapshot_event_handlers(snapshot)
        if self._dashboard_preparer is not None:
            try:
                self._dashboard_preparer(snapshot)
            except Exception as error:
                error_text = str(error) or type(error).__name__
                self._record_failed_gate(
                    plugin_id=plugin_id,
                    revision=generation.source_revision,
                    check_id="dashboard",
                    reason=error_text,
                )
                await self.discard_prepared(
                    plugin_id,
                    error=f"dashboard: {error_text}",
                )
                return self._publication_status(
                    plugin_id,
                    active=active,
                    candidate=generation,
                    publication_state="failed",
                )

        quiesced_snapshot: RuntimeSnapshot | None = None
        if publication_gated:
            from agent.plugins.snapshot import get_current_runtime_lease

            if (
                exclusive_endpoint_changed or v3_channel_catalog_changed
            ) and get_current_runtime_lease() is not None:
                error_text = "持有 RuntimeSnapshot lease 时不能切换独占端点"
                await self.discard_prepared(
                    plugin_id,
                    error=f"endpoint_lease: {error_text}",
                )
                raise RuntimeError(error_text)
            quiesced_snapshot = self._snapshot_store.pause_admission()
            try:
                if exclusive_endpoint_changed and self._endpoint_quiescer is not None:
                    await self._endpoint_quiescer()
                if quiesced_snapshot is not None and (
                    exclusive_endpoint_changed or v3_channel_catalog_changed
                ):
                    await self._snapshot_store.wait_for_no_leases(quiesced_snapshot)
            except BaseException as error:
                error_text = str(error) or type(error).__name__
                await self._snapshot_store.resume(quiesced_snapshot)
                if exclusive_endpoint_changed and self._endpoint_resumer is not None:
                    await self._endpoint_resumer()
                await self.discard_prepared(
                    plugin_id,
                    error=f"endpoint_quiesce: {error_text}",
                )
                raise
        transaction = self._snapshot_store.begin_publish(
            snapshot,
            admission_gated=quiesced_snapshot is not None,
        )
        self._advance_reload(
            generation,
            "validating",
            candidate_snapshot_id=snapshot.snapshot_id,
        )
        try:
            await asyncio.wait_for(
                self._post_publish_invariants(generation, snapshot),
                timeout=self.POST_PUBLISH_TIMEOUT_SECONDS,
            )
        except (asyncio.CancelledError, Exception):
            _ = self._prepared_generations.pop(plugin_id, None)
            generation.state = "aborted"
            await self._abort_failed_publication(
                generation,
                transaction,
                error="post-publish invariant failed",
            )
            if self._endpoint_resumer is not None and exclusive_endpoint_changed:
                await self._endpoint_resumer()
            raise

        provisional_started = False
        provisional_cancelled = False
        if not stage_latest:
            try:
                self._snapshot_store.seal_pending_validation(snapshot)
                if publication_gated:
                    _, provisional_cancelled = await _complete_critical(
                        self._snapshot_store.commit_provisional(transaction)
                    )
                    provisional_started = True
                _ = await self._restore_direct_candidate_runtime(
                    generation,
                    validation_snapshot=snapshot,
                )
            except (asyncio.CancelledError, Exception) as error:
                error_text = str(error) or type(error).__name__
                self._record_failed_gate(
                    plugin_id=plugin_id,
                    revision=generation.source_revision,
                    check_id="production_rebuild",
                    reason=error_text,
                )
                _ = self._prepared_generations.pop(plugin_id, None)
                generation.state = "aborted"
                previous_runtime = (
                    generation.replaced_composition_runtime_generation
                )
                if (
                    previous_runtime is not None
                    and self._composition_generation_host.get(
                        previous_runtime.generation_id
                    )
                    is None
                ):
                    self._record_composition_runtime_failure(
                        generation,
                        error,
                        formal_effects=(
                            "candidate_pointer_restored",
                            "old_runtime_restore_uncertain",
                        ),
                    )
                runtime_restore_uncertain = (
                    previous_runtime is not None
                    and self._composition_generation_host.get(
                        previous_runtime.generation_id
                    )
                    is None
                )
                if provisional_started:
                    _, rollback_cancelled = await _complete_critical(
                        self._snapshot_store.rollback_provisional(
                            transaction,
                            keep_candidate_latest=False,
                            reopen_previous=not runtime_restore_uncertain,
                        )
                    )
                    provisional_cancelled = (
                        provisional_cancelled or rollback_cancelled
                    )
                await self._abort_failed_publication(
                    generation,
                    transaction,
                    error=f"production_rebuild: {error_text}",
                    reopen_previous=not runtime_restore_uncertain,
                )
                if (
                    self._endpoint_resumer is not None
                    and exclusive_endpoint_changed
                    and not runtime_restore_uncertain
                    and self.current_snapshot is not None
                    and self.current_snapshot.accepting_leases
                ):
                    await self._endpoint_resumer()
                raise

        commit_error: BaseException | None = None
        commit_cancelled = provisional_cancelled
        from agent.plugins.context import PreparedPluginKVStore

        def open_candidate() -> None:
            self._advance_reload(generation, "commit_started")
            generation.state = "activating"
            if not isinstance(generation.instance, ComposablePlugin):
                context = cast(Any, generation.instance).context
                context.data_dir = generation.data_dir
                context.session_manager = self._session_manager
                context.memory_engine = self._memory_engine
                context.llm = self._llm
                try:
                    cast(Any, generation.instance).activate()
                except BaseException:
                    context.data_dir = None
                    raise
                if (
                    isinstance(context.kv_store, PreparedPluginKVStore)
                    and not stage_latest
                ):
                    context.kv_store.commit()
            if generation.staged_event_bus is not None:
                generation.staged_event_bus.publish()
            if not stage_latest:
                self._activate_published_generation(generation, active)
            generation.state = "candidate" if stage_latest else "active"

        previous_snapshot = transaction.previous
        if (
            not stage_latest
            and generation.reload_tx_id is not None
            and previous_snapshot is not None
        ):
            self._drain_transactions[previous_snapshot.snapshot_id] = (
                generation.reload_tx_id
            )
        try:
            if stage_latest:
                _, commit_cancelled = await _complete_critical(
                    self._snapshot_store.commit_latest(
                        transaction,
                        before_open=open_candidate,
                    )
                )
            else:
                _, final_commit_cancelled = await _complete_critical(
                    self._commit_snapshot_with_publication_participants(
                        transaction,
                        plugin_id=plugin_id,
                        old_services=old_services,
                        new_services=new_services,
                        old_channels=old_channels,
                        new_channels=new_channels,
                        old_commands=old_commands,
                        new_commands=new_commands,
                        promote_latest=False,
                        force_provisional=exclusive_endpoint_changed,
                        provisional_started=provisional_started,
                        before_open=open_candidate,
                        after_open=(
                            None
                            if active is None
                            else lambda: self._retire_generation(active)
                        ),
                    )
                )
                commit_cancelled = commit_cancelled or final_commit_cancelled
        except BaseException as error:
            commit_error = error

        if (
            commit_error is not None
            and self._snapshot_store.pending_candidate is snapshot
        ):
            if previous_snapshot is not None:
                _ = self._drain_transactions.pop(
                    previous_snapshot.snapshot_id,
                    None,
                )
            _ = self._prepared_generations.pop(plugin_id, None)
            generation.state = "aborted"
            await self._abort_failed_publication(
                generation,
                transaction,
                error=str(commit_error) or type(commit_error).__name__,
                finish_journal=False,
                reopen_previous=not isinstance(
                    commit_error,
                    _PublicationParticipantRestoreError,
                ),
            )
            runtime_restore_error: BaseException | None = None
            try:
                await self._restore_replaced_composition_runtime(generation)
            except BaseException as error:
                runtime_restore_error = error
            participant_restore_error = isinstance(
                commit_error,
                _PublicationParticipantRestoreError,
            )
            if runtime_restore_error is not None:
                self._record_composition_runtime_failure(
                    generation,
                    runtime_restore_error,
                    formal_effects=(
                        "candidate_pointer_restored",
                        "old_runtime_restore_uncertain",
                    ),
                )
            elif participant_restore_error:
                participant_resource = (
                    "plugin-endpoint,channel-publication"
                    if exclusive_endpoint_changed
                    else "channel-publication"
                )
                participant_effects = (
                    (
                        "endpoint_restore_uncertain",
                        "stable_channel_restore_uncertain",
                    )
                    if exclusive_endpoint_changed
                    else ("stable_channel_restore_uncertain",)
                )
                self._record_composition_runtime_failure(
                    generation,
                    cast(BaseException, commit_error),
                    resource=participant_resource,
                    formal_effects=participant_effects,
                )
            else:
                self._abort_reload(
                    generation,
                    error=str(commit_error) or type(commit_error).__name__,
                )
            if (
                self._endpoint_resumer is not None
                and exclusive_endpoint_changed
                and self.current_snapshot is not None
                and self.current_snapshot.accepting_leases
            ):
                await self._endpoint_resumer()
            if runtime_restore_error is not None:
                raise RuntimeError(
                    "Snapshot commit 失败后旧 v3 runtime 恢复失败"
                ) from runtime_restore_error
            if (
                exclusive_endpoint_changed
                and isinstance(commit_error, _PublicationParticipantSwitchError)
            ):
                return self._publication_status(
                    plugin_id,
                    active=active,
                    candidate=generation,
                    publication_state="failed",
                )
            raise commit_error
        if commit_error is None:
            generation.publication_created_data_dir = False

        _ = self._prepared_generations.pop(plugin_id)
        if stage_latest:
            self._ready_candidate = _ReadyPluginCandidate(
                plugin_id=plugin_id,
                previous=active,
                candidate=generation,
                snapshot=snapshot,
            )
            self._advance_reload(generation, "latest_ready")
            if commit_error is not None:
                raise commit_error
            if commit_cancelled:
                raise asyncio.CancelledError
            result = self._publication_status(
                plugin_id,
                active=active,
                candidate=generation,
                publication_state="latest_ready",
            )
            logger.info(
                "plugin_snapshot_status %s",
                json.dumps(result, ensure_ascii=False, sort_keys=True),
            )
            return result

        self._track_reload_drain(generation, previous_snapshot)
        self._scopes[generation.module_path] = generation.scope
        self._loaded.add(generation.module_path)
        generation.state = "active"
        self._active_generations[plugin_id] = generation
        generation.replaced_composition_runtime_generation = None
        if active is not None:
            active.state = "retired"
        self._channels = [
            channel
            for item in self._active_generations.values()
            for channel in item.contributions.channels
        ]
        resume_cancelled = False
        if self._endpoint_resumer is not None and exclusive_endpoint_changed:
            _, resume_cancelled = await _complete_critical(self._endpoint_resumer())
        if commit_error is not None:
            raise commit_error
        if commit_cancelled or resume_cancelled:
            raise asyncio.CancelledError
        result = self._publication_status(
            plugin_id,
            active=active,
            candidate=generation,
            publication_state="committed",
        )
        logger.info(
            "plugin_snapshot_status %s",
            json.dumps(result, ensure_ascii=False, sort_keys=True),
        )
        return result

    async def _restore_direct_candidate_runtime(
        self,
        generation: PluginGeneration,
        *,
        validation_snapshot: RuntimeSnapshot,
    ) -> RuntimeSnapshot:
        """直接发布前用正式 runtime 重建并关闭 candidate Root。"""

        candidate_runtime = self._composition_generation_host.get(
            generation.generation_id
        )
        expected_mcp_catalog_digests = (
            None
            if candidate_runtime is None
            else candidate_runtime.mcp_catalog_digests
        )
        if self._dashboard_validation_releaser is not None:
            await self._dashboard_validation_releaser(validation_snapshot)
        await self._stop_composition_generation_runtime(generation)
        previous_root = validation_snapshot.composition_root
        if previous_root is not None:
            await previous_root.dispose()
        created_data_dir = not generation.data_dir.exists()
        ensure_workspace_plugin_data_dir(generation.data_dir, self._workspace)
        production_snapshot: RuntimeSnapshot | None = None
        try:
            production_snapshot = await self._compile_generation_snapshot(
                generation,
                allow_unready_stable_composition=True,
            )
            if production_snapshot.snapshot_id != validation_snapshot.snapshot_id:
                await self._dispose_unreferenced_composition_root(production_snapshot)
                raise RuntimeError(
                    "候选隔离资源恢复后 snapshot identity 发生变化: "
                    f"{validation_snapshot.snapshot_id} -> "
                    f"{production_snapshot.snapshot_id}"
                )
            await self._stop_replaced_composition_runtime(generation)
            await self._start_composition_generation_runtime(
                generation,
                production_snapshot,
                mode="formal",
                expected_mcp_catalog_digests=expected_mcp_catalog_digests,
            )
        except BaseException:
            try:
                await self._stop_composition_generation_runtime(generation)
                await self._restore_replaced_composition_runtime(generation)
            except BaseException as restore_error:
                raise RuntimeError(
                    "production runtime 失败后旧 stable runtime 恢复失败"
                ) from restore_error
            if production_snapshot is not None:
                await self._dispose_unreferenced_composition_root(
                    production_snapshot
                )
            if created_data_dir:
                _remove_validation_data_dir(generation.data_dir)
            raise
        generation.publication_created_data_dir = created_data_dir
        validation_event_handlers = validation_snapshot.event_handlers
        _replace_snapshot_payload(validation_snapshot, production_snapshot)
        validation_snapshot.event_handlers = validation_event_handlers
        validation_workspace = generation.validation_workspace
        generation.validation_workspace = None
        if self._dashboard_preparer is not None:
            self._dashboard_preparer(validation_snapshot)
        generation.runtime_snapshot = validation_snapshot
        if validation_workspace is not None:
            _remove_validation_data_dir(validation_workspace.parent)
        generation.validation_data_inventory = ()
        if not isinstance(generation.instance, ComposablePlugin):
            cast(Any, generation.instance).context.tool_registry = (
                validation_snapshot.tool_registry
            )
        return validation_snapshot

    def _activate_published_generation(
        self,
        generation: PluginGeneration,
        previous: PluginGeneration | None,
    ) -> None:
        plugin_dir = generation.plugin_dir.resolve(strict=False)
        published_module = sys.modules[generation.module_path]
        stable_alias = None
        if previous is not None:
            stable_alias = self._stable_aliases.get(previous.module_path)
        retired_module = None
        if stable_alias is None:
            retired_module = next(
                (
                    module_path
                    for module_path, info in self._active_plugins.items()
                    if module_path != generation.module_path
                    and info.plugin_id == generation.plugin_id
                ),
                None,
            )
            if retired_module is not None:
                stable_alias = self._stable_aliases.get(retired_module)
        if stable_alias is None:
            stable_alias = generation.module_path.rsplit("__g", 1)[0]

        # 先完成可能失败的查找，再替换 stable import alias。
        self._remove_module_tree(stable_alias)
        self._fresh_importer.register(stable_alias, plugin_dir)
        plugin_registry.register_instance(stable_alias, generation.instance)
        sys.modules[stable_alias] = published_module
        if previous is not None:
            _ = self._stable_aliases.pop(previous.module_path, None)
        if retired_module is not None:
            _ = self._stable_aliases.pop(retired_module, None)
        self._stable_aliases[generation.module_path] = stable_alias
        self._active_plugins[generation.module_path] = ActivePluginInfo(
            plugin_id=generation.plugin_id,
            plugin_dir=plugin_dir,
            manifest=generation.contributions.manifest,
            module_path=generation.module_path,
            skill_roots=generation.contributions.skill_roots,
            drift_skill_roots=generation.contributions.drift_skill_roots,
            mcp_servers=generation.contributions.mcp_servers,
        )

    async def _prepare_generation(
        self,
        generation: PluginGeneration,
    ) -> None:
        if generation.prepare_started:
            return
        if isinstance(generation.instance, ComposablePlugin):
            assert generation.runtime_snapshot is not None
            generation.prepare_started = True
            generation.minimum_resource_count = generation.scope.resource_count
            return
        from agent.plugins.context import PreparedPluginKVStore

        instance = cast(Any, generation.instance)
        context = instance.context
        staged_event_bus = ScopedEventBus(
            self._event_bus,
            generation.scope,
            staged=True,
        )
        generation.staged_event_bus = staged_event_bus
        context.event_bus = staged_event_bus
        context.kv_store = PreparedPluginKVStore(
            generation.data_dir / ".kv.json",
            can_write=lambda: _generation_can_write(generation),
            writer_id=generation.generation_id,
        )
        context._can_start_tasks = lambda: generation.state in {
            "activating",
            "active",
            "candidate",
        }
        context.scope = generation.scope
        assert generation.runtime_snapshot is not None
        context.tool_registry = generation.runtime_snapshot.tool_registry
        generation.prepare_started = True
        await instance.prepare()
        generation.minimum_resource_count = generation.scope.resource_count

    def _compile_snapshot_event_handlers(self, snapshot: RuntimeSnapshot) -> None:
        handlers: dict[type[object], list[Any]] = {}
        for generation in snapshot.generations.values():
            for metadata in plugin_registry.get_handlers_by_module_path(
                generation.module_path
            ):
                if metadata.kind != MetadataKind.LIFECYCLE:
                    continue
                event_type = _EVENT_TYPE_MAP.get(metadata.event_type)  # type: ignore[arg-type]
                if event_type is None:
                    continue
                handlers.setdefault(event_type, []).append(
                    functools.partial(metadata.handler, generation.instance)
                )
            staged = generation.staged_event_bus
            if staged is None:
                continue
            for event_type, handler in staged.staged_handlers():
                handlers.setdefault(event_type, []).append(handler)
        snapshot.event_handlers = MappingProxyType(
            {
                event_type: tuple(event_handlers)
                for event_type, event_handlers in handlers.items()
            }
        )

    async def _post_publish_invariants(
        self,
        generation: PluginGeneration,
        snapshot: RuntimeSnapshot,
    ) -> None:
        await self._post_snapshot_invariants(snapshot)
        if snapshot.generations.get(generation.plugin_id) is not generation:
            raise RuntimeError("RuntimeSnapshot generation 不一致")

    async def _post_snapshot_invariants(
        self,
        snapshot: RuntimeSnapshot,
    ) -> None:
        await asyncio.sleep(0)
        if snapshot.state == "committed":
            if self.current_snapshot is not snapshot:
                raise RuntimeError("RuntimeSnapshot 已提交指针不一致")
        elif (
            snapshot.state != "validating"
            or self._snapshot_store.pending_candidate is not snapshot
        ):
            raise RuntimeError("RuntimeSnapshot 候选事务不一致")
        catalog_id = snapshot.skill_catalog_generation_id
        if catalog_id is not None and self._skill_host.get(catalog_id) is None:
            raise RuntimeError("RuntimeSnapshot skill catalog 不可用")
        for generation_id in snapshot.mcp_catalog_generation_ids.values():
            catalog = self._mcp_host.get(generation_id)
            if catalog is None:
                raise RuntimeError("RuntimeSnapshot MCP catalog 不可用")
            if any(not server.client.connected for server in catalog.servers.values()):
                raise RuntimeError("RuntimeSnapshot MCP client 已断开")
        workspace_mcp = snapshot.workspace_mcp_generation
        if workspace_mcp is not None:
            if (
                self._mcp_host.get(workspace_mcp.generation_id)
                is not workspace_mcp.catalog
            ):
                raise RuntimeError("RuntimeSnapshot workspace MCP catalog 不可用")
            self._validate_workspace_mcp_generation(workspace_mcp)
        for item in snapshot.generations.values():
            if item.scope.closed:
                raise RuntimeError("RuntimeSnapshot 插件作用域已关闭")
            if item.scope.resource_count < item.minimum_resource_count:
                raise RuntimeError("RuntimeSnapshot 插件资源数量不足")
            if (
                item.job_catalog is not None
                and self._job_host.get(item.generation_id) is not item.job_catalog
            ):
                raise RuntimeError("RuntimeSnapshot Job catalog 不可用")
            if (
                item.proactive_catalog is not None
                and self._proactive_host.get(item.generation_id)
                is not item.proactive_catalog
            ):
                raise RuntimeError("RuntimeSnapshot proactive catalog 不可用")

    def _advance_reload(
        self,
        generation: PluginGeneration,
        phase: ReloadPhase,
        *,
        candidate_snapshot_id: str | None = None,
        error: str = "",
        resource: str | None = None,
        formal_effects: tuple[str, ...] | None = None,
        recovery_action: RecoveryActionName | None = None,
        attempt_count: int | None = None,
        details: dict[str, object] | None = None,
        recovery_target: RecoveryTarget | None = None,
    ) -> None:
        tx_id = generation.reload_tx_id
        if tx_id is None:
            return
        self._reload_journal.advance(
            tx_id,
            phase,
            candidate_snapshot_id=candidate_snapshot_id,
            error=error,
            resource=resource,
            formal_effects=formal_effects,
            recovery_action=recovery_action,
            attempt_count=attempt_count,
            details=details,
            recovery_target=recovery_target,
        )

    def _abort_reload(
        self,
        generation: PluginGeneration,
        *,
        error: str,
    ) -> None:
        tx_id = generation.reload_tx_id
        if tx_id is None:
            return
        phase = self._reload_journal.get(tx_id).phase
        if phase in {
            "complete",
            "aborted",
            "recovered",
            "cleanup_failed",
            "degraded",
        }:
            return
        self._advance_reload(generation, "aborted", error=error)

    async def _abort_failed_publication(
        self,
        generation: PluginGeneration,
        transaction: SnapshotTransaction,
        *,
        error: str,
        finish_journal: bool = True,
        reopen_previous: bool = True,
    ) -> None:
        """撤销失败发布，并留下可被启动恢复判定的持久状态。"""

        # 1. pointer 失败时保留未完成 journal，让重启按磁盘事实恢复。
        pointer_error: BaseException | None = None
        try:
            _discard_generation_candidate_pointer(generation)
        except BaseException as caught:
            pointer_error = caught

        # 2. snapshot drain 失败不能阻止已恢复 pointer 的 journal 终态。
        snapshot_error: BaseException | None = None
        try:
            _, _ = await _complete_critical(
                self._snapshot_store.abort(
                    transaction,
                    reopen_previous=reopen_previous,
                )
            )
        except BaseException as caught:
            snapshot_error = caught
        tx_id = generation.reload_tx_id
        if snapshot_error is not None and tx_id is not None:
            phase = self._reload_journal.get(tx_id).phase
            if phase not in {"cleanup_failed", "degraded"}:
                self._record_composition_runtime_failure(
                    generation,
                    snapshot_error,
                    resource="runtime-snapshot-drain",
                    formal_effects=(
                        "candidate_pointer_restored",
                        "candidate_runtime_cleanup_pending",
                    ),
                )
        if finish_journal and pointer_error is None and (
            tx_id is None
            or self._reload_journal.get(tx_id).phase
            not in {"cleanup_failed", "degraded"}
        ):
            self._abort_reload(generation, error=error)

        # 3. Root 已排空后恢复本次 publication 才创建的正式数据身份。
        if snapshot_error is not None:
            raise RuntimeError(
                "候选发布失败后 RuntimeSnapshot 回收失败"
            ) from snapshot_error
        if generation.publication_created_data_dir:
            _remove_validation_data_dir(generation.data_dir)
            generation.publication_created_data_dir = False

        # 4. 清理异常优先暴露，避免把半完成恢复伪装成原始发布失败。
        if pointer_error is not None:
            raise RuntimeError(
                "候选发布失败后 artifact pointer 恢复失败"
            ) from pointer_error

    def _track_reload_drain(
        self,
        generation: PluginGeneration,
        previous_snapshot: RuntimeSnapshot | None,
    ) -> None:
        tx_id = generation.reload_tx_id
        if tx_id is None:
            return
        phase = self._reload_journal.get(tx_id).phase
        if phase == "latest_ready":
            self._advance_reload(generation, "promoting")
            phase = "promoting"
        if phase in {"commit_started", "promoting"}:
            self._advance_reload(generation, "committed")
        if previous_snapshot is None:
            self._advance_reload(generation, "complete")
            return
        snapshot_id = previous_snapshot.snapshot_id
        if snapshot_id in self._drained_before_commit:
            self._drained_before_commit.remove(snapshot_id)
            self._advance_reload(generation, "complete")
            return
        self._advance_reload(generation, "draining")
        self._drain_transactions[snapshot_id] = tx_id

    def _finish_drained_reload(self, snapshot_id: str) -> None:
        tx_id = self._drain_transactions.pop(snapshot_id, None)
        if tx_id is None:
            return
        record = self._reload_journal.get(tx_id)
        if record.phase in {"commit_started", "promoting"}:
            self._drained_before_commit.add(snapshot_id)
            return
        if record.phase == "committed":
            self._reload_journal.advance(tx_id, "draining")
            record = self._reload_journal.get(tx_id)
        if record.phase == "draining":
            self._reload_journal.advance(tx_id, "complete")

    def _publication_status(
        self,
        plugin_id: str,
        *,
        active: PluginGeneration | None,
        candidate: PluginGeneration,
        publication_state: str,
    ) -> dict[str, object]:
        return {
            "plugin_id": plugin_id,
            "old_generation": active.generation_id if active is not None else None,
            "new_generation": candidate.generation_id,
            "snapshot_id": (
                self.latest_snapshot.snapshot_id
                if publication_state == "latest_ready"
                and self.latest_snapshot is not None
                else (
                    self.current_snapshot.snapshot_id
                    if self.current_snapshot is not None
                    else None
                )
            ),
            "stable_snapshot_id": (
                self.current_snapshot.snapshot_id
                if self.current_snapshot is not None
                else None
            ),
            "publication_state": publication_state,
        }

    async def _prepare_changed(
        self,
        *,
        discovered: dict[str, dict[str, str]],
        plugin_ids: set[str] | None = None,
        force_reprepare: bool = False,
    ) -> list[dict[str, object]]:
        results: list[dict[str, object]] = []
        for plugin_id, active in tuple(self._active_generations.items()):
            if plugin_ids is not None and plugin_id not in plugin_ids:
                continue
            mod = discovered.get(plugin_id)
            if mod is None:
                continue
            plugin_dir = Path(mod["plugin_root"])
            try:
                source_revision = _source_revision(plugin_dir)
                config_revision = _file_revision(
                    _resolve_plugin_data_dir(
                        mod["name"],
                        mod,
                        self._workspace,
                    )
                    / "config.local.toml"
                )
            except Exception:
                source_revision = ""
                config_revision = ""
            current_prepared = self._prepared_generations.get(plugin_id)
            if force_reprepare and current_prepared is not None:
                await self.discard_prepared(plugin_id, preserve_latest=True)
                current_prepared = None
            matches_active = (
                source_revision == active.source_revision
                and config_revision == active.config_revision
            )
            if matches_active:
                if current_prepared is None:
                    continue
                await self.discard_prepared(plugin_id)
                result = {
                    "plugin_id": plugin_id,
                    "active_generation": active.generation_id,
                    "prepared_generation": None,
                    "gate_status": "active",
                    "candidate_revision": source_revision,
                    "skills": (
                        list(active.skill_catalog.names)
                        if active.skill_catalog is not None
                        else []
                    ),
                    "skill_descriptions": _skill_descriptions(active),
                    "drift_skill_descriptions": _drift_skill_descriptions(active),
                    "skill_body_hashes": _skill_body_hashes(active, drift=False),
                    "drift_skill_body_hashes": _skill_body_hashes(
                        active,
                        drift=True,
                    ),
                    "mcp_tools": _mcp_tool_names(active),
                    "readiness_checks": _gate_check_evidence(
                        active,
                        "readiness_semantic_checks",
                    ),
                    "jobs": _job_keys(active),
                    "proactive_sources": _proactive_source_keys(active),
                    "job_specs": _job_spec_evidence(active),
                    "proactive_source_specs": _proactive_source_spec_evidence(active),
                    "snapshot_id": (
                        self.current_snapshot.snapshot_id
                        if self.current_snapshot is not None
                        else None
                    ),
                }
                results.append(result)
                _log_candidate_status(result)
                continue
            if (
                current_prepared is not None
                and source_revision == current_prepared.source_revision
                and config_revision == current_prepared.config_revision
            ):
                continue
            await self.discard_prepared(plugin_id, preserve_latest=True)
            prepared = await self._load_one(mod, activate=False)
            if prepared is None:
                _discard_installed_candidate_mod(mod)
            gate = self.latest_gate(plugin_id)
            result: dict[str, object] = {
                "plugin_id": plugin_id,
                "active_generation": active.generation_id,
                "prepared_generation": (
                    prepared.generation_id if prepared is not None else None
                ),
                "gate_status": gate.status if gate is not None else "failed",
                "candidate_revision": (
                    gate.candidate_revision if gate is not None else ""
                ),
                "skills": (
                    list(prepared.skill_catalog.names)
                    if prepared is not None and prepared.skill_catalog is not None
                    else []
                ),
                "skill_descriptions": (
                    _skill_descriptions(prepared) if prepared is not None else {}
                ),
                "drift_skill_descriptions": (
                    _drift_skill_descriptions(prepared) if prepared is not None else {}
                ),
                "skill_body_hashes": (
                    _skill_body_hashes(prepared, drift=False)
                    if prepared is not None
                    else {}
                ),
                "drift_skill_body_hashes": (
                    _skill_body_hashes(prepared, drift=True)
                    if prepared is not None
                    else {}
                ),
                "mcp_tools": _mcp_tool_names(prepared) if prepared is not None else [],
                "readiness_checks": (
                    _gate_check_evidence(prepared, "readiness_semantic_checks")
                    if prepared is not None
                    else []
                ),
                "jobs": _job_keys(prepared) if prepared is not None else [],
                "proactive_sources": (
                    _proactive_source_keys(prepared) if prepared is not None else []
                ),
                "job_specs": (
                    _job_spec_evidence(prepared) if prepared is not None else {}
                ),
                "proactive_source_specs": (
                    _proactive_source_spec_evidence(prepared)
                    if prepared is not None
                    else {}
                ),
                "snapshot_id": (
                    self.current_snapshot.snapshot_id
                    if self.current_snapshot is not None
                    else None
                ),
            }
            results.append(result)
            _log_candidate_status(result)
        return results

    async def _load_one(
        self,
        mod: dict[str, str],
        *,
        activate: bool = True,
        stage_stable: bool = False,
    ) -> PluginGeneration | None:
        stable_module_path = mod["import_path"]
        plugin_dir = Path(mod["plugin_root"])
        initial_plugin_id = _resolve_plugin_id(mod)
        if activate and initial_plugin_id in self._active_generations:
            return self._active_generations[initial_plugin_id]
        plugin_manifest = load_plugin_manifest(
            _plugins_home(self._installed_cache_root)
        )
        if (
            not mod.get("package_id")
            and plugin_manifest.get(initial_plugin_id, True) is False
        ):
            logger.info("插件已禁用（manifest.toml）: %s", initial_plugin_id)
            return None
        tool_names: list[str] = []
        hook_count_before = len(self._tool_hooks)
        before_turn_count_before = len(self._before_turn_modules)
        before_reasoning_count_before = len(self._before_reasoning_modules)
        prompt_render_count_before = len(self._prompt_render_modules)
        before_step_count_before = len(self._before_step_modules)
        after_step_count_before = len(self._after_step_modules)
        after_reasoning_count_before = len(self._after_reasoning_modules)
        after_turn_count_before = len(self._after_turn_modules)
        proactive_module_count_before = len(self._proactive_modules)
        proactive_lifecycle_count_before = len(self._proactive_lifecycles)
        proactive_factory_count_before = len(self._proactive_module_factories)
        proactive_runtime_factory_count_before = len(self._proactive_runtime_factories)
        proactive_source_count_before = len(self._proactive_sources)
        job_count_before = len(self._jobs)
        channel_count_before = len(self._channels)
        self._generation_sequence += 1
        generation_sequence = self._generation_sequence
        module_path = mod["module_path"].strip()
        static_manifest: StaticPluginManifest | None = None
        manifest_path = plugin_dir / "akashic.plugin.toml"
        if manifest_path.exists() or manifest_path.is_symlink():
            try:
                # Static identity is the admission source.  No plugin module is
                # imported until this parse and the discovered entrypoint agree.
                static_manifest = load_static_plugin_manifest(plugin_dir)
                expected_module_path = plugin_dir / static_manifest.entrypoint
                discovered_entrypoint = mod.get("entrypoint", "plugin.py")
                if discovered_entrypoint != static_manifest.entrypoint:
                    raise RuntimeError(
                        "source discovery entrypoint 与静态 manifest 不一致: "
                        f"discovered={discovered_entrypoint} "
                        f"manifest={static_manifest.entrypoint}"
                    )
                if mod.get("manifest_digest", "") != static_manifest.identity_digest:
                    raise RuntimeError("source discovery manifest identity 已漂移")
                if Path(module_path).resolve(strict=False) != expected_module_path.resolve(
                    strict=False
                ):
                    raise RuntimeError(
                        "source discovery module path 与静态 manifest 不一致: "
                        f"discovered={module_path} expected={expected_module_path}"
                    )
            except Exception as error:
                raise RuntimeError(
                    f"插件 {initial_plugin_id} 静态 manifest admission 失败: {error}"
                ) from error
        try:
            source_revision = _source_revision(plugin_dir)
        except Exception as error:
            revision = hashlib.sha256(f"{plugin_dir}:{error}".encode()).hexdigest()
            generation_id = f"{initial_plugin_id}:{revision[:12]}:{generation_sequence}"
            reload_tx_id = (
                self._begin_reload_attempt(
                    plugin_id=initial_plugin_id,
                    generation_id=generation_id,
                    source_revision=revision,
                    config_revision="",
                    plugin_dir=plugin_dir,
                    source_type=mod.get("source_type", "builtin"),
                )
                if not activate
                else None
            )
            error_text = str(error) or type(error).__name__
            self._record_failed_gate(
                plugin_id=initial_plugin_id,
                revision=revision,
                check_id="source_boundary",
                reason=error_text,
            )
            self._abort_reload_attempt(
                reload_tx_id,
                error=f"source_boundary: {error_text}",
            )
            return None
        data_dir = _resolve_plugin_data_dir(
            mod["name"],
            mod,
            self._workspace,
        )
        validate_workspace_plugin_data_path(data_dir, self._workspace)
        config_revision = _file_revision(data_dir / "config.local.toml")
        generation_id = (
            f"{initial_plugin_id}:{source_revision[:12]}:{generation_sequence}"
        )
        reload_tx_id = (
            self._begin_reload_attempt(
                plugin_id=initial_plugin_id,
                generation_id=generation_id,
                source_revision=source_revision,
                config_revision=config_revision,
                plugin_dir=plugin_dir,
                source_type=mod.get("source_type", "builtin"),
            )
            if not activate
            else None
        )
        mp = (
            f"{stable_module_path}__g{generation_sequence}_"
            f"{source_revision[:8]}_{self._manager_namespace}"
        )
        if not module_path:
            error_text = f"插件缺少 plugin.py: {plugin_dir}"
            self._record_failed_gate(
                plugin_id=initial_plugin_id,
                revision=source_revision,
                check_id="plugin_module",
                reason=error_text,
            )
            self._abort_reload_attempt(
                reload_tx_id,
                error=f"plugin_module: {error_text}",
            )
            raise RuntimeError(error_text)
        # V2_REMOVAL(static-manifest-admission)：无 manifest 的 plugin.py import
        # 仅服务迁移期 v2/旧 v3；pure-v3 fleet 必须先完成 import-free admission。
        try:
            self._import_plugin(mp, Path(module_path))
        except Exception as error:
            logger.warning("插件 %s 导入失败: %s", mod["name"], error)
            error_text = str(error) or type(error).__name__
            self._record_failed_gate(
                plugin_id=initial_plugin_id,
                revision=source_revision,
                check_id="import",
                reason=error_text,
            )
            self._abort_reload_attempt(
                reload_tx_id,
                error=f"import: {error_text}",
            )
            return None
        loaded_module = sys.modules.get(mp)
        if static_manifest is not None:
            try:
                if not isinstance(loaded_module, ModuleType):
                    raise RuntimeError("v3 插件模块未保留在 import registry")
                validate_module_exports(
                    static_manifest,
                    loaded_module,
                    plugin_root=plugin_dir,
                )
            except Exception as error:
                self._remove_module_tree(mp)
                error_text = str(error) or type(error).__name__
                self._record_failed_gate(
                    plugin_id=initial_plugin_id,
                    revision=source_revision,
                    check_id="static_manifest_exports",
                    reason=error_text,
                )
                self._abort_reload_attempt(
                    reload_tx_id,
                    error=f"static_manifest_exports: {error_text}",
                )
                return None
        is_v3 = (
            loaded_module is not None
            and getattr(loaded_module, "api_version", None) == 3
        )
        private_members = {item.member for item in PRIVATE_PROACTIVE_DEFINITIONS}
        if (
            isinstance(loaded_module, ModuleType)
            and getattr(loaded_module, "name", None) in private_members
        ):
            try:
                admit_private_proactive_module(loaded_module)
            except Exception as error:
                self._remove_module_tree(mp)
                error_text = str(error) or type(error).__name__
                self._record_failed_gate(
                    plugin_id=initial_plugin_id,
                    revision=source_revision,
                    check_id="private_proactive_admission",
                    reason=error_text,
                )
                self._abort_reload_attempt(
                    reload_tx_id,
                    error=f"private_proactive_admission: {error_text}",
                )
                raise RuntimeError(
                    f"private proactive admission 失败: {initial_plugin_id}: {error_text}"
                ) from error
        cls = plugin_registry.get_class(mp)
        if not is_v3 and cls is None:
            logger.warning("插件 %s 未注册类", mod["name"])
            self._remove_module_tree(mp)
            self._record_failed_gate(
                plugin_id=initial_plugin_id,
                revision=source_revision,
                check_id="plugin_class",
                reason="plugin.py 未注册 Plugin 子类",
            )
            self._abort_reload_attempt(
                reload_tx_id,
                error="plugin_class: plugin.py 未注册 Plugin 子类",
            )
            return None
        try:
            if is_v3:
                if not isinstance(loaded_module, ModuleType):
                    raise RuntimeError("v3 插件模块未保留在 import registry")
                instance: Any = ComposablePlugin.from_module(loaded_module)
                config_model = instance.ConfigModel
            else:
                assert cls is not None
                instance = cls()
                config_model = getattr(cls, "ConfigModel", None)
            name = str(instance.name or mod["name"]).strip()
            if not name:
                raise RuntimeError("插件缺少 name")
            plugin_id = f"{name}@{mod['marketplace']}" if mod["marketplace"] else name
            if plugin_id != initial_plugin_id:
                raise RuntimeError(
                    f"插件目录身份与声明不一致: directory={initial_plugin_id} declared={plugin_id}"
                )
            credential_paths = (
                _static_channel_credential_paths(static_manifest)
                if is_v3 and static_manifest is not None
                else ()
            )
            credential_alias_groups = (
                _validate_channel_credential_schema(
                    config_model,
                    credential_paths=credential_paths,
                )
                if is_v3
                else ()
            )
            config_projection = _read_plugin_config_projection(
                data_dir,
                credential_paths=credential_paths,
                credential_alias_groups=credential_alias_groups,
            )
            plugin_config = _validate_plugin_config_projection(
                config_projection,
                config_model,
            )
        except Exception as error:
            self._remove_module_tree(mp)
            error_text = str(error) or type(error).__name__
            check_id = "config" if isinstance(error, _PluginConfigError) else "identity"
            self._record_failed_gate(
                plugin_id=initial_plugin_id,
                revision=source_revision,
                check_id=check_id,
                reason=error_text,
            )
            self._abort_reload_attempt(
                reload_tx_id,
                error=f"{check_id}: {error_text}",
            )
            return None
        if not stage_stable and activate:
            ensure_workspace_plugin_data_dir(data_dir, self._workspace)
        scope = PluginScope(plugin_id)
        if not isinstance(instance, ComposablePlugin):
            from agent.plugins.context import PluginContext, PluginKVStore

            instance.context = PluginContext(
                event_bus=None,  # type: ignore[arg-type]
                tool_registry=None,
                plugin_id=plugin_id,
                plugin_dir=plugin_dir,
                data_dir=None,
                kv_store=PluginKVStore(data_dir / ".kv.json", writable=False),
                config=plugin_config,
                workspace=self._workspace,
                session_manager=None,
                memory_engine=None,
                llm=None,
                scope=None,
                generation_id=generation_id,
            )
        plugin_registry.register_instance(mp, instance)
        prepare_started = False
        generation: PluginGeneration | None = None

        async def rollback_load(error: str) -> None:
            if reload_tx_id is not None:
                phase = self._reload_journal.get(reload_tx_id).phase
                if phase not in {"complete", "aborted", "recovered"}:
                    self._reload_journal.advance(
                        reload_tx_id,
                        "aborted",
                        error=error,
                    )
            if generation is not None and generation.runtime_snapshot is not None:
                await self._dispose_unreferenced_composition_root(
                    generation.runtime_snapshot
                )
            terminator = getattr(instance, "terminate", None)
            if prepare_started and callable(terminator):
                try:
                    typed_terminator = cast(
                        Callable[[], Awaitable[None]],
                        terminator,
                    )
                    await typed_terminator()
                except (asyncio.CancelledError, Exception) as terminate_error:
                    self._cleanup_failures.append(
                        CleanupFailure(
                            resource=f"plugin:{plugin_id}:terminate",
                            error=str(terminate_error)
                            or type(terminate_error).__name__,
                        )
                    )
            self._cleanup_failures.extend(await scope.aclose())
            if generation is not None and generation.boot_created_data_dir:
                _remove_validation_data_dir(generation.data_dir)
                generation.boot_created_data_dir = False
            self._remove_module_tree(mp)
            for tool_name in tool_names:
                if self._tool_registry is not None:
                    self._tool_registry.unregister(tool_name)
            del self._tool_hooks[hook_count_before:]
            del self._before_turn_modules[before_turn_count_before:]
            del self._before_reasoning_modules[before_reasoning_count_before:]
            del self._prompt_render_modules[prompt_render_count_before:]
            del self._before_step_modules[before_step_count_before:]
            del self._after_step_modules[after_step_count_before:]
            del self._after_reasoning_modules[after_reasoning_count_before:]
            del self._after_turn_modules[after_turn_count_before:]
            del self._proactive_modules[proactive_module_count_before:]
            del self._proactive_lifecycles[proactive_lifecycle_count_before:]
            del self._proactive_module_factories[proactive_factory_count_before:]
            del self._proactive_runtime_factories[
                proactive_runtime_factory_count_before:
            ]
            del self._proactive_sources[proactive_source_count_before:]
            del self._jobs[job_count_before:]
            del self._channels[channel_count_before:]

        try:
            load_phase = "declarations"
            if isinstance(instance, ComposablePlugin):
                instance.bind_static_services(self._composition_service_view())
            contributions = self._collect_candidate_contributions(
                instance=instance,
                plugin_id=plugin_id,
                plugin_dir=plugin_dir,
                data_dir=data_dir,
                module_path=mp,
                source_revision=source_revision,
            )
            gate_result = self._validate_candidate(
                instance=instance,
                plugin_id=plugin_id,
                revision=source_revision,
                contributions=contributions,
            )
            self._gate_results[plugin_id] = gate_result
            if gate_result.status == "failed":
                raise _CandidateRejected(gate_result)
            generation = PluginGeneration(
                plugin_id=plugin_id,
                generation_id=generation_id,
                module_path=mp,
                source_revision=source_revision,
                config_revision=config_revision,
                plugin_dir=plugin_dir,
                data_dir=data_dir,
                config=(
                    plugin_config
                    if isinstance(instance, ComposablePlugin)
                    else None
                ),
                config_projection=(
                    config_projection
                    if isinstance(instance, ComposablePlugin)
                    else {}
                ),
                instance=instance,
                scope=scope,
                contributions=contributions,
                gate_result=gate_result,
                source_type=cast(
                    Literal["builtin", "installed"],
                    mod["source_type"],
                ),
                static_manifest=static_manifest,
                entrypoint=(
                    static_manifest.entrypoint
                    if static_manifest is not None
                    else "plugin.py"
                ),
                state="prepared",
                reload_tx_id=reload_tx_id,
            )
            if stage_stable:
                generation.boot_created_data_dir = not data_dir.exists()
                ensure_workspace_plugin_data_dir(data_dir, self._workspace)
            catalog_generations = [
                active_generation
                for active_generation in self._active_generations.values()
                if active_generation.plugin_id != plugin_id
            ]
            catalog_generations.append(generation)
            catalog_generations = self._static_active_generations(
                catalog_generations
            )
            ignored_generations = self._static_active_generations(
                [*self._active_generations.values(), generation]
            )
            try:
                skill_catalog = self._skill_host.prepare(
                    generation_id,
                    normal_roots=PluginSkillHost.roots_for(
                        catalog_generations,
                        drift=False,
                    ),
                    drift_roots=PluginSkillHost.roots_for(
                        catalog_generations,
                        drift=True,
                    ),
                    ignored_normal_roots=tuple(
                        root
                        for item in ignored_generations
                        for root in item.contributions.skill_roots
                    ),
                    ignored_drift_roots=tuple(
                        root
                        for item in ignored_generations
                        for root in item.contributions.drift_skill_roots
                    ),
                )
            except Exception as error:
                gate_result = _with_gate_check(
                    gate_result,
                    check_id="skill_catalog",
                    passed=False,
                    evidence=str(error),
                )
                self._gate_results[plugin_id] = gate_result
                raise _CandidateRejected(gate_result) from error
            gate_result = _with_gate_check(
                gate_result,
                check_id="skill_catalog",
                passed=True,
                evidence=list(skill_catalog.names),
            )
            self._gate_results[plugin_id] = gate_result
            generation.gate_result = gate_result
            generation.skill_catalog = skill_catalog
            scope.defer(
                "skill_catalog",
                lambda: self._skill_host.close(generation_id),
            )
            try:
                job_catalog = self._job_host.prepare(
                    generation_id,
                    contributions.jobs,
                )
                scope.defer(
                    "job_catalog",
                    lambda: self._job_host.close(generation_id),
                )
                proactive_catalog = self._proactive_host.prepare(
                    generation_id,
                    contributions.proactive_sources,
                )
                scope.defer(
                    "proactive_catalog",
                    lambda: self._proactive_host.close(generation_id),
                )
            except Exception as error:
                gate_result = _with_gate_check(
                    gate_result,
                    check_id="activity_catalogs",
                    passed=False,
                    evidence=str(error),
                )
                self._gate_results[plugin_id] = gate_result
                raise _CandidateRejected(gate_result) from error
            generation.job_catalog = job_catalog
            generation.proactive_catalog = proactive_catalog
            contributions = replace(
                contributions,
                jobs=tuple(job_catalog.jobs.values()),
                proactive_sources=tuple(proactive_catalog.sources.values()),
            )
            generation.contributions = contributions
            gate_result = _with_gate_check(
                gate_result,
                check_id="activity_catalogs",
                passed=True,
                evidence={
                    "jobs": sorted(job_catalog.jobs),
                    "proactive_sources": sorted(proactive_catalog.sources),
                },
            )
            self._gate_results[plugin_id] = gate_result
            generation.gate_result = gate_result
            if not activate:
                validation_root = (
                    self._workspace
                    / "runtime"
                    / "plugin-validation"
                    / generation.generation_id
                )
                if validation_root.exists():
                    raise RuntimeError(
                        f"候选验证目录已存在: {validation_root}"
                    )
                validation_workspace = validation_root / "workspace"
                generation.validation_workspace = validation_workspace
                generation.scope.defer(
                    "validation_plugin_data",
                    lambda: asyncio.to_thread(
                        _remove_validation_data_dir, validation_root
                    ),
                )
            if not activate and _installed_generation_is_candidate(generation):
                generation.production_contributions = contributions
                generation.production_data_dir = generation.data_dir
                assert generation.validation_workspace is not None
                validation_data_dir = (
                    generation.validation_workspace
                    / "plugin-data"
                    / generation.data_dir.name
                )
                validation_data_dir.parent.mkdir(parents=True, exist_ok=True)
                generation.validation_data_inventory = _copy_validation_data(
                    generation.data_dir,
                    validation_data_dir,
                    _candidate_data_exclude_paths(generation),
                )
                generation.data_dir = validation_data_dir
                generation.contributions = _validation_contributions(
                    generation,
                    self._active_generations.get(plugin_id),
                    validation_workspace=generation.validation_workspace,
                )
                contributions = generation.contributions
            if (
                not activate
                or contributions.mcp_servers
                or contributions.proactive_sources
            ):
                try:
                    mcp_catalog = await self._mcp_host.prepare(
                        generation_id,
                        server_specs=contributions.mcp_servers,
                        required_tools=_required_mcp_tools(
                            contributions.proactive_sources
                        ),
                        scope=scope,
                    )
                except Exception as error:
                    gate_result = _with_gate_check(
                        gate_result,
                        check_id="mcp_readiness",
                        passed=False,
                        evidence=str(error),
                        gate_id="G1/G2/G3-readiness",
                    )
                    self._gate_results[plugin_id] = gate_result
                    raise _CandidateRejected(gate_result) from error
                generation.mcp_catalog = mcp_catalog
                scope.defer(
                    "mcp_catalog",
                    lambda: self._mcp_host.close(generation_id),
                )
                try:
                    raw_readiness_checks: object = (
                        await instance.readiness_semantic_checks(
                            PluginReadinessContext(
                                generation_id=generation_id,
                                mcp_catalog=mcp_catalog,
                                job_catalog=job_catalog,
                                proactive_catalog=proactive_catalog,
                            )
                        )
                    )
                    if not isinstance(raw_readiness_checks, list):
                        raise RuntimeError("readiness_semantic_checks 返回值不是 list")
                    readiness_checks = cast(list[object], raw_readiness_checks)
                except Exception as error:
                    readiness_passed = False
                    readiness_evidence: object = str(error)
                else:
                    invalid_readiness = [
                        check
                        for check in readiness_checks
                        if not isinstance(check, PluginSemanticCheck)
                        or not check.passed
                    ]
                    readiness_passed = not invalid_readiness
                    normalized_readiness: list[dict[str, object]] = []
                    for check in readiness_checks:
                        if isinstance(check, PluginSemanticCheck):
                            normalized_readiness.append(
                                {
                                    "check_id": check.check_id,
                                    "passed": check.passed,
                                    "evidence": check.evidence,
                                }
                            )
                        else:
                            normalized_readiness.append(
                                {
                                    "check_id": "invalid",
                                    "passed": False,
                                    "evidence": repr(check),
                                }
                            )
                    readiness_evidence = normalized_readiness
                gate_result = _with_gate_check(
                    gate_result,
                    check_id="mcp_readiness",
                    passed=True,
                    evidence=list(mcp_catalog.tool_names),
                    gate_id="G1/G2/G3-readiness",
                )
                gate_result = _with_gate_check(
                    gate_result,
                    check_id="readiness_semantic_checks",
                    passed=readiness_passed,
                    evidence=readiness_evidence,
                )
                self._gate_results[plugin_id] = gate_result
                generation.gate_result = gate_result
                if gate_result.status == "failed":
                    raise _CandidateRejected(gate_result)
                if not activate:
                    generation.runtime_snapshot = await self._compile_generation_snapshot(
                        generation,
                        candidate_owner=generation,
                    )
                    self._advance_reload(
                        generation,
                        "prepared",
                        candidate_snapshot_id=generation.runtime_snapshot.snapshot_id,
                    )
                    generation.minimum_resource_count = scope.resource_count
                    self._prepared_generations[plugin_id] = generation
                    return generation
            if stage_stable:
                return generation
            generation.runtime_snapshot = await self._compile_generation_snapshot(
                generation,
                allow_pending_composition=True,
            )
            from agent.plugins.context import PreparedPluginKVStore

            load_phase = "prepare"
            prepare_started = not isinstance(instance, ComposablePlugin)
            await self._prepare_generation(generation)
            generation.state = "activating"
            if not isinstance(instance, ComposablePlugin):
                instance.context.data_dir = data_dir
                instance.context.session_manager = self._session_manager
                instance.context.memory_engine = self._memory_engine
                instance.context.llm = self._llm
                instance.activate()
                if isinstance(instance.context.kv_store, PreparedPluginKVStore):
                    instance.context.kv_store.commit()
            load_phase = "publish"
            self._register_tools(instance, mp, tool_names)
            self._bind_tool_hooks(instance, mp)
            self._publish_contributions(contributions)
            self._channels.extend(contributions.channels)
            if generation.staged_event_bus is not None:
                generation.staged_event_bus.publish()
            generation.minimum_resource_count = scope.resource_count
        except asyncio.CancelledError:
            rollback_task = asyncio.create_task(
                rollback_load(f"candidate {load_phase} cancelled"),
                name=f"plugin_rollback:{plugin_id}",
            )
            while not rollback_task.done():
                try:
                    await asyncio.shield(rollback_task)
                except asyncio.CancelledError:
                    continue
            await rollback_task
            raise
        except _CandidateRejected as error:
            logger.warning(
                "插件 %s 候选验证失败: %s",
                mod["name"],
                error.gate.failure_reason,
            )
            await rollback_load(_gate_failure_details(error.gate))
            return None
        except Exception as error:
            logger.warning("插件 %s 加载失败，回滚: %s", mod["name"], error)
            self._record_failed_gate(
                plugin_id=plugin_id,
                revision=source_revision,
                check_id=load_phase,
                reason=str(error),
            )
            await rollback_load(str(error) or type(error).__name__)
            return None
        self._scopes[mp] = scope
        self._loaded.add(mp)
        self._active_plugins[mp] = ActivePluginInfo(
            plugin_id=plugin_id,
            plugin_dir=plugin_dir,
            manifest=contributions.manifest,
            module_path=mp,
            skill_roots=contributions.skill_roots,
            drift_skill_roots=contributions.drift_skill_roots,
            mcp_servers=contributions.mcp_servers,
        )
        generation.state = "active"
        self._active_generations[plugin_id] = generation
        self._stable_aliases[mp] = stable_module_path
        self._remove_module_tree(stable_module_path)
        self._fresh_importer.register(stable_module_path, plugin_dir)
        plugin_registry.register_instance(stable_module_path, instance)
        sys.modules[stable_module_path] = sys.modules[mp]
        assert generation.runtime_snapshot is not None
        self._compile_snapshot_event_handlers(generation.runtime_snapshot)
        await self._publish_committed_snapshot(generation.runtime_snapshot)
        if generation.mcp_catalog is not None:
            self._mcp_host.mark_active(generation.generation_id)
        logger.info("插件已加载: %s", mod["name"])
        return generation

    async def _compile_generation_snapshot(
        self,
        generation: PluginGeneration,
        *,
        allow_pending_composition: bool = False,
        candidate_owner: PluginGeneration | None = None,
        allow_unready_stable_composition: bool = False,
    ) -> RuntimeSnapshot:
        generations = dict(self._active_generations)
        generations[generation.plugin_id] = generation
        composition_root, created_root = await self._resolve_composition_root(
            generations,
            allow_pending=allow_pending_composition,
            candidate_owner=candidate_owner,
        )
        try:
            private_proactive_catalog = build_private_proactive_catalog(
                generations.values(),
                root_instance_token=(
                    None
                    if composition_root is None
                    else composition_root.instance_token
                ),
            )
            current = self.current_snapshot
            # V2_REMOVAL: legacy-only candidate 消失后删除 stable Root Health 豁免。
            reuses_stable_root = (
                allow_unready_stable_composition
                and candidate_owner is None
                and current is not None
                and composition_root is current.composition_root
            )
            snapshot = self._snapshot_compiler.compile(
                generations,
                catalog_generation=generation,
                workspace_mcp_generation=self._active_workspace_mcp,
                composition_root=composition_root,
                private_proactive_catalog=private_proactive_catalog,
                require_composition_ready=not reuses_stable_root,
            )
            _validate_static_manifest_runtime(snapshot, generations)
            if reuses_stable_root and composition_root is not None:
                snapshot.composition_health_exempt_root_token = (
                    composition_root.instance_token
                )
            snapshot.tool_registry = self._compile_snapshot_tools(
                generations,
                self._active_workspace_mcp,
            )
            snapshot.tool_hooks = self._compile_snapshot_tool_hooks(generations)
            self._validate_snapshot_command_claims(snapshot)
            return snapshot
        except Exception as error:
            if created_root and composition_root is not None:
                await composition_root.dispose()
            gate = _with_gate_check(
                generation.gate_result,
                check_id="runtime_snapshot",
                passed=False,
                evidence=str(error),
            )
            generation.gate_result = gate
            self._gate_results[generation.plugin_id] = gate
            raise _CandidateRejected(gate) from error

    def _read_existing_session_compaction(self, session_key: str):
        """读取同一 Session 的消息与 active compaction 语义。"""

        session_manager = self._session_manager
        if session_manager is None:
            raise RuntimeError("Session Read Service 缺少 SessionManager")
        session = session_manager.get_existing(session_key)
        compaction = session_manager.control_store.get_active_compaction(session_key)
        return session, compaction

    async def _resolve_composition_root(
        self,
        generations: dict[str, PluginGeneration],
        *,
        allow_pending: bool = False,
        candidate_owner: PluginGeneration | None = None,
    ) -> tuple[CompositionRoot | None, bool]:
        """复用 stable Root，或挂载一个完整且隔离的 v3 generation 拓扑。"""

        # 1. 只有 stable-to-stable 的纯 payload 变化可以复用 Root。
        ordered = tuple(
            generation
            for generation in sorted(
                generations.values(), key=lambda item: item.plugin_id
            )
            if isinstance(generation.instance, ComposablePlugin)
        )
        current = self.current_snapshot
        current_ordered = (
            tuple(
                generation
                for generation in sorted(
                    current.generations.values(), key=lambda item: item.plugin_id
                )
                if isinstance(generation.instance, ComposablePlugin)
            )
            if current is not None
            else ()
        )
        if (
            candidate_owner is None
            and current is not None
            and len(ordered) == len(current_ordered)
            and all(
                left is right
                for left, right in zip(ordered, current_ordered, strict=True)
            )
        ):
            return current.composition_root, False
        if not ordered:
            return None, False

        # 2. candidate 总是创建独立 Root；stable 拓扑变化也创建完整 Root。
        identity = "|".join(
            f"{item.plugin_id}:{item.generation_id}" for item in ordered
        )
        root = CompositionRoot(
            "plugins:" + hashlib.sha256(identity.encode()).hexdigest()[:16],
            candidate_incident_limit=(1024 if candidate_owner is not None else None),
        )
        try:
            _ = await root.context.provide(COMMANDS, PluginCommands())
            if any(
                CHANNELS in cast(ComposablePlugin, item.instance).inject
                for item in ordered
            ):
                _ = await root.context.provide(
                    CHANNELS,
                    PluginChannels(root.instance_token),
                )
            if any(
                MCP_SERVERS in cast(ComposablePlugin, item.instance).inject
                for item in ordered
            ):
                _ = await root.context.provide(
                    MCP_SERVERS,
                    PluginMcpServers(root.instance_token),
                )
            if any(
                MANAGED_PROCESSES in cast(ComposablePlugin, item.instance).inject
                for item in ordered
            ):
                _ = await root.context.provide(
                    MANAGED_PROCESSES,
                    PluginManagedProcesses(root.instance_token),
                )
            if any(
                PROACTIVE_COMPONENTS in cast(ComposablePlugin, item.instance).inject
                for item in ordered
            ):
                _ = await root.context.provide(
                    PROACTIVE_COMPONENTS,
                    PluginProactiveComponents(root.instance_token),
                )
            if any(
                BACKGROUND_JOBS in cast(ComposablePlugin, item.instance).inject
                for item in ordered
            ):
                _ = await root.context.provide(
                    BACKGROUND_JOBS,
                    PluginBackgroundJobs(root.instance_token),
                )
            if any(
                UI_SLOTS in cast(ComposablePlugin, item.instance).inject
                for item in ordered
            ):
                _ = await root.context.provide(UI_SLOTS, PluginUiSlots())
            if self._session_manager is not None and any(
                SESSION_READ in cast(ComposablePlugin, item.instance).inject
                for item in ordered
            ):
                session_read = (
                    SessionReadService(self._read_existing_session_compaction)
                    if candidate_owner is None
                    else SessionReadService.candidate_validation()
                )
                _ = await root.context.provide(SESSION_READ, session_read)
            if self._interaction_undo is not None and any(
                INTERACTION_UNDO in cast(ComposablePlugin, item.instance).inject
                for item in ordered
            ):
                interaction_undo = (
                    InteractionUndoService(self._interaction_undo.undo_latest)
                    if candidate_owner is None
                    else InteractionUndoService.candidate_validation()
                )
                _ = await root.context.provide(INTERACTION_UNDO, interaction_undo)
            memory_runtime = self._get_composition_memory_runtime()
            if memory_runtime is not None:
                _ = await root.context.provide(
                    MEMORY_RUNTIME,
                    memory_runtime,
                )
            if (
                self._memory_engine is not None
                and isinstance(self._memory_engine, MemoryTurnRuntimeApi)
                and any(
                    MEMORY_TURN_RUNTIME
                    in cast(ComposablePlugin, item.instance).inject
                    for item in ordered
                )
            ):
                memory_turn_runtime = (
                    MemoryTurnRuntime(self._memory_engine)
                    if candidate_owner is None
                    else MemoryTurnRuntime.candidate_validation()
                )
                _ = await root.context.provide(
                    MEMORY_TURN_RUNTIME,
                    memory_turn_runtime,
                )
            if candidate_owner is None:
                for item in ordered:
                    await self._mount_generation_composition(root, item)
            else:
                await self._mount_candidate_composition(
                    root,
                    ordered,
                    candidate_owner=candidate_owner,
                )
            receipt = root.receipt()
            if not receipt.ready:
                missing_services = tuple(
                    sorted(
                        {
                            service
                            for fiber in receipt.fibers
                            if fiber.name in receipt.required_pending
                            for service in fiber.missing_services
                        }
                    )
                )
                if (
                    allow_pending
                    and receipt.required_pending
                    and all(
                        fiber.state == FiberState.PENDING
                        for fiber in receipt.fibers
                        if fiber.name in receipt.required_pending
                    )
                    and not receipt.required_degraded
                    and not receipt.incident_overflowed
                    and not receipt.external_effects
                ):
                    self._composition_pending = missing_services
                    await root.dispose()
                    return None, False
                raise RuntimeError(
                    "v3 插件组合拓扑未就绪: "
                    f"required_pending={receipt.required_pending}, "
                    f"missing_services={missing_services}, "
                    f"required_degraded={receipt.required_degraded}, "
                    f"incidents={receipt.incidents}, "
                    f"incident_overflowed={receipt.incident_overflowed}, "
                    f"external_effects={receipt.external_effects}"
                )
            self._composition_pending = ()
        except BaseException:
            await root.dispose()
            raise
        return root, True

    def _validate_snapshot_command_claims(
        self,
        snapshot: RuntimeSnapshot,
    ) -> None:
        """Validate v2 claims against the frozen v3 command namespace."""

        claims: set[tuple[str, str]] = set()
        for generation in snapshot.generations.values():
            if isinstance(generation.instance, ComposablePlugin):
                continue
            for getter_name in (
                "telegram_bot_commands",
                "mobile_bot_commands",
            ):
                getter = getattr(generation.instance, getter_name, None)
                if getter is None:
                    continue
                typed_getter = cast(Callable[[], list[tuple[str, str]]], getter)
                for command, _description in typed_getter():
                    claims.add((str(command), generation.plugin_id))
        if claims:
            owners = ", ".join(f"{name}:{owner}" for name, owner in sorted(claims))
            raise RuntimeError(
                "v2 channel command ABI 已删除，请迁移到 COMMANDS: " + owners
            )

    def _get_composition_memory_runtime(self) -> MemoryRuntimeInfo | None:
        """为本 Manager 构建的全部 Root 冻结同一份 Memory 描述能力。"""

        if self._composition_memory_runtime is _UNRESOLVED_MEMORY_RUNTIME:
            if self._memory_engine is None:
                raise RuntimeError("Memory runtime 冻结状态与 engine 不一致")
            self._composition_memory_runtime = _memory_runtime_info(
                self._memory_engine
            )
        return cast(
            MemoryRuntimeInfo | None,
            self._composition_memory_runtime,
        )

    def _composition_service_view(self) -> ServiceView:
        """冻结静态 v3 声明可读取的 Core service 输入。"""

        values: dict[Any, object] = {}
        memory_runtime = self._get_composition_memory_runtime()
        if memory_runtime is not None:
            values[MEMORY_RUNTIME] = memory_runtime
        return ServiceView.freeze(values)

    @staticmethod
    def _static_active_generations(
        generations: list[PluginGeneration],
    ) -> list[PluginGeneration]:
        """用 snapshot 相同的 active 合同过滤静态 catalog。"""

        # V2_REMOVAL(static-active)：v2 删除后不再保留未冻结的 legacy generation。
        return [
            generation
            for generation in generations
            if not isinstance(generation.instance, ComposablePlugin)
            or plugin_is_active(
                generation.instance, plugin_id=generation.plugin_id
            )
        ]

    async def _mount_generation_composition(
        self,
        root: CompositionRoot,
        generation: PluginGeneration,
    ) -> None:
        """用 generation 自己的正式 runtime 挂载一个 v3 插件。"""

        plugin = cast(ComposablePlugin, generation.instance)
        for name in plugin.workspace_roots:
            _ = resolve_declared_workspace_root(self._workspace, name)
        _ = await root.mount(
            plugin,
            name=generation.plugin_id,
            runtime=PluginRuntime(
                plugin_id=generation.plugin_id,
                plugin_dir=generation.plugin_dir,
                data_dir=generation.data_dir,
                workspace=self._workspace,
                config=generation.config,
                workspace_roots=plugin.workspace_roots,
            ),
        )

    async def _mount_candidate_composition(
        self,
        root: CompositionRoot,
        ordered: tuple[PluginGeneration, ...],
        *,
        candidate_owner: PluginGeneration,
    ) -> None:
        """在 candidate-owned runtime 中重建全部未变化的 v3 参与者。"""

        validation_workspace = candidate_owner.validation_workspace
        if validation_workspace is None:
            raise RuntimeError(
                f"候选缺少隔离 workspace: {candidate_owner.plugin_id}"
            )
        attempt_root = (
            validation_workspace.parent / "composition" / secrets.token_hex(8)
        )
        attempt_workspace = attempt_root / "workspace"
        root._defer_internal_cleanup(  # pyright: ignore[reportPrivateUsage]
            "candidate_attempt_data",
            lambda: _remove_validation_data_dir(attempt_root),
        )
        clones: list[tuple[PluginGeneration, ComposablePlugin, str, Path, object]] = []
        for generation in ordered:
            clone, module_path, data_dir, config = self._clone_candidate_composable(
                generation,
                candidate_owner=candidate_owner,
                attempt_workspace=attempt_workspace,
            )
            root._defer_internal_cleanup(  # pyright: ignore[reportPrivateUsage]
                f"candidate_module:{module_path}",
                lambda module_path=module_path: self._remove_module_tree(module_path),
            )
            original = cast(ComposablePlugin, generation.instance)
            if clone.workspace_roots != original.workspace_roots:
                raise RuntimeError(
                    "candidate workspace_roots 与 generation 冻结声明不一致: "
                    f"{generation.plugin_id}"
                )
            clones.append((generation, clone, module_path, data_dir, config))
        self._project_candidate_workspace_roots(
            tuple(item[1] for item in clones),
            attempt_workspace,
        )
        for generation, clone, _module_path, data_dir, config in clones:
            _ = await root.mount(
                clone,
                name=generation.plugin_id,
                runtime=PluginRuntime(
                    plugin_id=generation.plugin_id,
                    plugin_dir=generation.plugin_dir,
                    data_dir=data_dir,
                    workspace=attempt_workspace,
                    config=config,
                    workspace_roots=clone.workspace_roots,
                ),
            )

    def _project_candidate_workspace_roots(
        self,
        plugins: tuple[ComposablePlugin, ...],
        attempt_workspace: Path,
    ) -> None:
        """把声明式共享目录复制到一次 candidate attempt。"""

        # 1. 全部 generation 由同一个 Manager workspace 发布。
        names: set[str] = set()
        for plugin in plugins:
            names.update(plugin.workspace_roots)

        # 2. 缺失目录保持缺失；已有目录获得独立副本。
        for name in sorted(names):
            source = resolve_declared_workspace_root(self._workspace, name)
            if not source.exists():
                continue
            _ = shutil.copytree(source, attempt_workspace / name)

    def _clone_candidate_composable(
        self,
        generation: PluginGeneration,
        *,
        candidate_owner: PluginGeneration,
        attempt_workspace: Path,
    ) -> tuple[ComposablePlugin, str, Path, object]:
        """重新导入一个 stable v3 插件并绑定 candidate 临时数据。"""

        plugin_dir = generation.plugin_dir
        data_dir = attempt_workspace / "plugin-data" / generation.data_dir.name
        _ = data_dir.parent.mkdir(parents=True, exist_ok=True)
        inventory = _copy_validation_data(
            generation.data_dir,
            data_dir,
            _candidate_data_exclude_paths(generation),
        )
        if generation is candidate_owner:
            generation.validation_data_inventory = inventory
        module_path = (
            f"{generation.module_path}__candidate_"
            f"{candidate_owner.generation_id.replace(':', '_')}_"
            f"{secrets.token_hex(4)}"
        )
        entrypoint = generation.entrypoint
        self._import_plugin(module_path, plugin_dir / entrypoint)
        try:
            module = sys.modules[module_path]
            if generation.static_manifest is not None:
                validate_module_exports(
                    generation.static_manifest,
                    module,
                    plugin_root=plugin_dir,
                )
            clone = ComposablePlugin.from_module(module)
            credential_paths = (
                _static_channel_credential_paths(generation.static_manifest)
                if generation.static_manifest is not None
                else ()
            )
            _validate_channel_credential_schema(
                cast(type[BaseModel] | None, clone.ConfigModel),
                credential_paths=credential_paths,
            )
            config = _validate_plugin_config_projection(
                generation.config_projection,
                cast(type[BaseModel] | None, clone.ConfigModel),
            )
            clone.bind_static_services(self._composition_service_view())
            plugin_registry.register_instance(module_path, clone)
            return clone, module_path, data_dir, config
        except BaseException:
            self._remove_module_tree(module_path)
            raise

    async def _start_composition_generation_runtime(
        self,
        generation: PluginGeneration,
        snapshot: RuntimeSnapshot,
        *,
        mode: Literal["candidate", "formal"],
        expected_mcp_catalog_digests: Mapping[str, str] | None = None,
    ) -> CompositionRuntimeGeneration | None:
        """Start one exact Root runtime and refresh snapshot Tool routes."""

        if (
            generation.reload_tx_id is not None
            and self._composition_runtime_declared(snapshot, generation.plugin_id)
        ):
            boot_id = os.environ.get("AKASHIC_BOOT_ID", "").strip()
            if boot_id:
                self._reload_journal.mark_runtime_owner(
                    generation.reload_tx_id,
                    boot_id,
                )
        self._composition_runtime_generations[generation.generation_id] = generation
        try:
            runtime = await self._composition_generation_host.start(
                generation,
                snapshot,
                mode=mode,
                expected_mcp_catalog_digests=expected_mcp_catalog_digests,
            )
        except BaseException:
            if (
                self._composition_generation_host.failure(generation.generation_id)
                is None
            ):
                _ = self._composition_runtime_generations.pop(
                    generation.generation_id,
                    None,
                )
            raise
        if runtime is None:
            _ = self._composition_runtime_generations.pop(
                generation.generation_id,
                None,
            )
        self._refresh_composition_runtime_tools(snapshot)
        return runtime

    async def _stop_composition_generation_runtime(
        self,
        generation: PluginGeneration,
    ) -> None:
        """Stop one generation before its exact Root is disposed."""

        await self._composition_generation_host.stop(generation.generation_id)
        _ = self._composition_runtime_generations.pop(
            generation.generation_id,
            None,
        )

    async def _stop_replaced_composition_runtime(
        self,
        generation: PluginGeneration,
    ) -> None:
        """Stop the old stable runtime after admission and leases are drained."""

        previous = self._active_generations.get(generation.plugin_id)
        if (
            previous is None
            or self._composition_generation_host.get(previous.generation_id) is None
        ):
            return
        await self._stop_composition_generation_runtime(previous)
        generation.replaced_composition_runtime_generation = previous

    async def _restore_replaced_composition_runtime(
        self,
        generation: PluginGeneration,
    ) -> None:
        """Restore an old stable runtime before reopening its snapshot."""

        previous = generation.replaced_composition_runtime_generation
        if previous is None:
            return
        snapshot = self.current_snapshot
        if snapshot is None or snapshot.generations.get(previous.plugin_id) is not previous:
            raise RuntimeError("旧 stable runtime snapshot 身份已失效")
        await self._start_composition_generation_runtime(
            previous,
            snapshot,
            mode="formal",
        )
        generation.replaced_composition_runtime_generation = None

    async def _rollback_composition_runtime_replacement(
        self,
        generation: PluginGeneration,
    ) -> None:
        """Stop an unpublished formal runtime and restore the prior stable owner."""

        await self._stop_composition_generation_runtime(generation)
        await self._restore_replaced_composition_runtime(generation)

    def _record_composition_runtime_failure(
        self,
        generation: PluginGeneration,
        error: BaseException,
        *,
        resource: str = "composition-runtime",
        formal_effects: tuple[str, ...],
    ) -> None:
        """Persist one executable runtime failure without releasing its owner."""

        tx_id = self._ensure_runtime_recovery_transaction(generation)
        failure = self._composition_generation_host.failure(
            generation.generation_id
        )
        if failure is None:
            action: RecoveryActionName = (
                "retry_generation_cleanup"
                if resource == "runtime-snapshot-drain"
                else "retry_runtime_recovery"
            )
        else:
            action = failure.action
        phase: ReloadPhase = (
            "degraded" if action == "retry_runtime_recovery" else "cleanup_failed"
        )
        failure_resource = (
            f"{resource}:{generation.generation_id}"
            if failure is None
            else ",".join((*failure.resource_names, resource))
        )
        failure_error = (
            str(error) or type(error).__name__
            if failure is None
            else failure.error
        )
        self._reload_journal.advance(
            tx_id,
            phase,
            error=failure_error,
            resource=failure_resource,
            formal_effects=formal_effects,
            recovery_action=action,
            recovery_target=self._composition_recovery_target(
                generation,
                tx_id=tx_id,
            ),
        )
        ready = self._ready_candidate
        if (
            ready is not None
            and ready.candidate.reload_tx_id == tx_id
            and self._snapshot_store.unpromoted_candidate is ready.snapshot
        ):
            _ = self._snapshot_store.pause_candidate_admission(ready.snapshot)

    def _on_composition_runtime_failure(
        self,
        failure: CompositionRuntimeFailure,
    ) -> None:
        """Persist a watchdog failure for the exact generation owner."""

        generation = self._composition_runtime_generations.get(
            failure.generation_id
        )
        if generation is None:
            raise RuntimeError(
                "v3 runtime failure 缺少 Manager generation owner: "
                f"{failure.generation_id}"
            )
        self._record_composition_runtime_failure(
            generation,
            RuntimeError(failure.error),
            formal_effects=("runtime_watchdog_failure",),
        )

    def _ensure_runtime_recovery_transaction(
        self,
        generation: PluginGeneration,
    ) -> str:
        """Create a durable owner when cleanup fails outside an active reload."""

        tx_id = generation.reload_tx_id
        if tx_id is not None:
            phase = self._reload_journal.get(tx_id).phase
            if phase not in {"complete", "aborted", "recovered"}:
                return tx_id

        # 1. A stable failure joins the one in-flight candidate transaction.
        candidate = self._prepared_generations.get(generation.plugin_id)
        ready = self._ready_candidate
        if ready is not None and ready.plugin_id == generation.plugin_id:
            candidate = ready.candidate
        current = self.current_snapshot
        if (
            candidate is not None
            and candidate is not generation
            and current is not None
            and current.generations.get(generation.plugin_id) is generation
            and candidate.reload_tx_id is not None
        ):
            candidate_record = self._reload_journal.get(candidate.reload_tx_id)
            if candidate_record.phase not in {"complete", "aborted", "recovered"}:
                if candidate_record.base_generation_id != generation.generation_id:
                    raise RuntimeError(
                        "runtime recovery candidate base generation 身份不一致"
                    )
                return candidate.reload_tx_id

        # 2. Freeze the exact stable artifact identity before exposing recovery.
        base_snapshot = self.current_snapshot
        base_generation = (
            None
            if base_snapshot is None
            else base_snapshot.generations.get(generation.plugin_id)
        )
        base_pointer: str | None = None
        candidate_pointer: str | None = None
        plugin_base = _installed_artifact_base(generation)
        if plugin_base is not None:
            pointers = read_pointers(plugin_base)
            if pointers is None:
                raise RuntimeError(
                    f"runtime cleanup recovery 缺少 artifact pointer: {plugin_base}"
                )
            base_pointer = pointers.stable.path
            if (
                base_snapshot is not None
                and base_generation is generation
            ):
                candidate_pointer = generation.plugin_dir.relative_to(
                    plugin_base
                ).as_posix()

        # 3. Persist the process boot owner before returning the cleanup failure.
        tx_id = self._reload_journal.begin(
            plugin_id=generation.plugin_id,
            base_snapshot_id=(
                None if base_snapshot is None else base_snapshot.snapshot_id
            ),
            base_generation_id=(
                None if base_generation is None else base_generation.generation_id
            ),
            generation_id=generation.generation_id,
            source_revision=generation.source_revision,
            config_revision=generation.config_revision,
            base_artifact_pointer=base_pointer,
            candidate_artifact_pointer=candidate_pointer,
        )
        generation.reload_tx_id = tx_id
        boot_id = os.environ.get("AKASHIC_BOOT_ID", "").strip()
        if boot_id:
            self._reload_journal.mark_runtime_owner(tx_id, boot_id)
        return tx_id

    def _record_drained_composition_runtime_failure(
        self,
        snapshot: RuntimeSnapshot,
        generation: PluginGeneration,
        error: BaseException,
    ) -> None:
        """Persist one retained Host owner while allowing Root and module drain."""

        drain_tx_id = self._drain_transactions.get(snapshot.snapshot_id)
        tx_id = drain_tx_id or generation.reload_tx_id
        if tx_id is None:
            return
        record = self._reload_journal.get(tx_id)
        if record.phase in {"complete", "aborted", "recovered"}:
            return
        failure = self._composition_generation_host.failure(
            generation.generation_id
        )
        action: RecoveryActionName = (
            "retry_generation_cleanup" if failure is None else failure.action
        )
        phase: ReloadPhase = (
            "degraded" if action == "retry_runtime_recovery" else "cleanup_failed"
        )
        self._reload_journal.advance(
            tx_id,
            phase,
            error=(
                str(error) or type(error).__name__
                if failure is None
                else failure.error
            ),
            resource=(
                f"composition-runtime:{generation.generation_id}"
                if failure is None
                else ",".join(failure.resource_names)
            ),
            formal_effects=(
                "committed_generation_retained",
                "old_runtime_cleanup_pending",
            )
            if drain_tx_id is not None
            else (
                "candidate_pointer_restored",
                "candidate_runtime_cleanup_pending",
            ),
            recovery_action=action,
            recovery_target=(
                record.recovery_target
                or (
                    "candidate"
                    if drain_tx_id is not None
                    else self._composition_recovery_target(
                        generation,
                        tx_id=tx_id,
                    )
                )
            ),
        )

    def _composition_recovery_target(
        self,
        generation: PluginGeneration,
        *,
        tx_id: str | None = None,
    ) -> RecoveryTarget:
        """Resolve the exact durable artifact selected at failure time."""

        if tx_id is None:
            tx_id = generation.reload_tx_id
        if tx_id is None:
            return "base"
        record = self._reload_journal.get(tx_id)
        if record.generation_id != generation.generation_id:
            if record.base_generation_id == generation.generation_id:
                return "base"
            raise RuntimeError(
                "runtime failure generation 不属于 recovery transaction"
            )
        if (
            record.phase in {"cleanup_failed", "degraded"}
            and record.recovery_target is not None
        ):
            return record.recovery_target
        base_pointer = record.base_artifact_pointer
        candidate_pointer = record.candidate_artifact_pointer
        plugin_base = _installed_artifact_base(generation)
        if plugin_base is None or candidate_pointer is None:
            current = self.current_snapshot
            if (
                current is not None
                and current.generations.get(generation.plugin_id) is generation
            ):
                return "candidate"
            return "base"
        pointers = read_pointers(plugin_base)
        if pointers is None:
            raise RuntimeError(
                f"runtime failure 缺少 durable artifact pointer: {plugin_base}"
            )
        if pointers.stable.path == candidate_pointer:
            return "candidate"
        if pointers.stable.path == base_pointer:
            return "base"
        raise RuntimeError(
            "runtime failure artifact pointer 超出 reload transaction: "
            f"stable={pointers.stable.path} base={base_pointer} "
            f"candidate={candidate_pointer}"
        )

    def _refresh_composition_runtime_tools(
        self,
        snapshot: RuntimeSnapshot,
    ) -> None:
        """Rebuild ToolRegistry and attach every exact live v3 MCP facade."""

        snapshot.tool_registry = self._compile_snapshot_tools(
            dict(snapshot.generations),
            snapshot.workspace_mcp_generation,
        )
        for generation in sorted(
            snapshot.generations.values(),
            key=lambda item: item.plugin_id,
        ):
            runtime = self._composition_generation_host.get(
                generation.generation_id
            )
            snapshot.tool_registry = self._composition_generation_host.attach_tools(
                snapshot.tool_registry,
                runtime,
            )

    @staticmethod
    def _composition_runtime_declared(
        snapshot: RuntimeSnapshot,
        plugin_id: str,
    ) -> bool:
        """Return whether one plugin owns MCP/process declarations in a snapshot."""

        return any(
            binding.descriptor.owner == plugin_id
            for registry in (
                snapshot.managed_process_registry,
                snapshot.mcp_server_registry,
            )
            if registry is not None
            for binding in registry.values()
        )

    def _compile_snapshot_tools(
        self,
        generations: dict[str, PluginGeneration],
        workspace_mcp: WorkspaceMcpGeneration | None = None,
    ) -> Any:
        if self._tool_registry is None:
            return None
        plugin_mcp_sources = {
            ("mcp", server_name)
            for generation in generations.values()
            for server_name in generation.contributions.mcp_servers
        }
        workspace_mcp_sources: set[tuple[str, str]] = (
            {("mcp", server_name) for server_name in workspace_mcp.catalog.servers}
            if workspace_mcp is not None
            else set()
        )
        registry = self._tool_registry.fork(
            excluded_source_types={"plugin"},
            excluded_sources=plugin_mcp_sources | workspace_mcp_sources,
        )
        for generation in sorted(generations.values(), key=lambda item: item.plugin_id):
            plugin_name = getattr(
                generation.instance,
                "name",
                generation.plugin_id,
            )
            for md in plugin_registry.get_handlers_by_module_path(
                generation.module_path
            ):
                if md.kind != MetadataKind.TOOL:
                    continue
                tool = _build_plugin_tool(generation.instance, md)
                if registry.has_tool(tool.name):
                    raise RuntimeError(f"插件工具名称重复: {tool.name}")
                registry.register(
                    tool,
                    risk=md.tool_risk or "read-write",
                    always_on=bool(md.tool_always_on),
                    search_hint=md.tool_search_hint,
                    source_type="plugin",
                    source_name=plugin_name,
                )
            if generation.mcp_catalog is None:
                continue
            for server in generation.mcp_catalog.servers.values():
                server_spec = generation.contributions.mcp_servers[server.name]
                candidate_read_only_tools = _candidate_mcp_read_only_tools(
                    generation,
                    server.name,
                    server_spec,
                    {tool.name for tool in server.tools},
                )
                for tool in server.tools:
                    if registry.has_tool(tool.name):
                        raise RuntimeError(f"MCP 工具名称重复: {tool.name}")
                    registry.register(
                        tool,
                        risk=(
                            "read-only"
                            if tool.name in candidate_read_only_tools
                            else "external-side-effect"
                        ),
                        source_type="mcp",
                        source_name=server.name,
                    )
        if workspace_mcp is not None:
            for server in workspace_mcp.catalog.servers.values():
                for tool in server.tools:
                    if registry.has_tool(tool.name):
                        raise RuntimeError(f"workspace MCP 工具名称重复: {tool.name}")
                    registry.register(
                        tool,
                        risk="external-side-effect",
                        source_type="mcp",
                        source_name=server.name,
                    )
        return registry

    def _compile_workspace_mcp_snapshot(
        self,
        generation: WorkspaceMcpGeneration,
    ) -> RuntimeSnapshot:
        snapshot = self._snapshot_compiler.compile(
            self._active_generations,
            snapshot_revision=generation.revision,
            workspace_mcp_generation=generation,
            composition_root=(
                self.current_snapshot.composition_root
                if self.current_snapshot is not None
                else None
            ),
            private_proactive_catalog=build_private_proactive_catalog(
                self._active_generations.values(),
                root_instance_token=(
                    None
                    if self.current_snapshot is None
                    or self.current_snapshot.composition_root is None
                    else self.current_snapshot.composition_root.instance_token
                ),
            ),
        )
        snapshot.tool_registry = self._compile_snapshot_tools(
            self._active_generations,
            generation,
        )
        snapshot.tool_hooks = self._compile_snapshot_tool_hooks(
            self._active_generations
        )
        self._compile_snapshot_event_handlers(snapshot)
        return snapshot

    @staticmethod
    def _validate_workspace_mcp_generation(
        generation: WorkspaceMcpGeneration,
    ) -> None:
        if generation.scope.closed:
            raise RuntimeError("workspace MCP 候选作用域已关闭")
        if any(
            not server.client.connected
            for server in generation.catalog.servers.values()
        ):
            raise RuntimeError("workspace MCP 候选 client 已断开")

    def _compile_snapshot_tool_hooks(
        self,
        generations: dict[str, PluginGeneration],
    ) -> tuple[ToolHook, ...]:
        # V2_REMOVAL(tool-hooks)：typed Tool events 接管后删除 metadata 编译。
        hooks: list[ToolHook] = []
        for generation in sorted(generations.values(), key=lambda item: item.plugin_id):
            for metadata in plugin_registry.get_handlers_by_module_path(
                generation.module_path
            ):
                if metadata.kind != MetadataKind.TOOL_HOOK:
                    continue
                hooks.append(
                    _PluginToolHook(
                        name=(
                            f"plugin:{getattr(generation.instance, 'name', generation.module_path)}:"
                            f"{metadata.handler_name}"
                        ),
                        handler=functools.partial(
                            metadata.handler, generation.instance
                        ),
                        tool_name_filter=metadata.hook_tool_name,
                    )
                )
        return tuple(hooks)

    async def _publish_committed_snapshot(
        self,
        snapshot: RuntimeSnapshot,
    ) -> None:
        if self._snapshot_store.current is None:
            registry = snapshot.channel_registry
            activity_declared = self._activity_catalog_identity(snapshot) is not None
            if (registry is not None and registry.descriptors) or activity_declared:
                transaction = self._snapshot_store.begin_publish(snapshot)
                await self._commit_snapshot_with_publication_participants(
                    transaction,
                    plugin_id="stable-boot",
                    old_services={},
                    new_services={},
                    old_channels=(),
                    new_channels=(),
                    old_commands=(),
                    new_commands=(),
                    promote_latest=False,
                )
                return
            self._snapshot_store.install(snapshot)
            return
        transaction = self._snapshot_store.begin_publish(snapshot)
        if (
            self._channel_catalog_identity(transaction.previous)
            != self._channel_catalog_identity(snapshot)
            or self._activity_catalog_identity(transaction.previous)
            != self._activity_catalog_identity(snapshot)
        ):
            await self._commit_snapshot_with_publication_participants(
                transaction,
                plugin_id="stable-batch",
                old_services={},
                new_services={},
                old_channels=(),
                new_channels=(),
                old_commands=(),
                new_commands=(),
                promote_latest=False,
            )
            return
        await self._snapshot_store.commit(transaction)

    def _collect_candidate_contributions(
        self,
        *,
        instance: Any,
        plugin_id: str,
        plugin_dir: Path,
        data_dir: Path,
        module_path: str,
        source_revision: str,
    ) -> PluginContributions:
        if isinstance(instance, ComposablePlugin):
            return PluginContributions(
                manifest={
                    "name": instance.name,
                    "version": instance.version,
                    "desc": instance.desc,
                    "author": instance.author,
                },
                skill_roots=_resolve_declared_roots(
                    plugin_dir,
                    instance.skill_roots,
                ),
                drift_skill_roots=_resolve_declared_roots(
                    plugin_dir,
                    instance.drift_skill_roots,
                ),
                dashboard_module=_resolve_dashboard_module(
                    plugin_dir,
                    instance.dashboard_module,
                ),
            )
        # V2_REMOVAL(plugin-contribution-collector-v2)：以下分支逐项调用 v2 class 领域方法。
        # command/MCP/process/channel/proactive/mobile/phase 首个 v3 consumer 建立 capability 后
        # 按迁移地图逐族删除；最后删除整个 legacy branch。
        cls = cast(type[Any], type(instance))
        _reject_legacy_mobile_ui_api(cls, plugin_id)
        sources: list[RegisteredProactiveSource] = []
        for source in _load_module_list(instance, "proactive_sources"):
            if not isinstance(source, ProactiveSourceSpec):
                raise RuntimeError(
                    f"插件 {plugin_id}.proactive_sources 返回值不是 ProactiveSourceSpec"
                )
            sources.append(RegisteredProactiveSource(plugin_id=plugin_id, spec=source))
        jobs: list[RegisteredPluginJob] = []
        for spec in _load_module_list(instance, "jobs"):
            if not isinstance(spec, PluginJobSpec):
                raise RuntimeError(f"插件 {plugin_id}.jobs 返回值不是 PluginJobSpec")
            jobs.append(
                RegisteredPluginJob(
                    plugin_id=plugin_id,
                    plugin_context=instance.context,
                    spec=spec,
                )
            )
        mobile_ui_asset = _resolve_mobile_ui_asset(
            plugin_dir,
            cls.mobile_ui(),
        )
        mobile_ui_query = (
            None
            if mobile_ui_asset is None
            else _require_sync_mobile_ui_handler(
                getattr(instance, "mobile_ui_query", None),
                "query",
                plugin_id,
            )
        )
        mobile_ui_available = (
            None
            if mobile_ui_asset is None
            else _require_sync_mobile_ui_handler(
                getattr(instance, "mobile_ui_available", None),
                "available",
                plugin_id,
            )
        )
        return PluginContributions(
            manifest={
                "name": str(instance.name or ""),
                "version": str(instance.version or ""),
                "desc": str(instance.desc or ""),
                "author": str(instance.author or ""),
            },
            skill_roots=_resolve_declared_roots(plugin_dir, cls.skill_roots()),
            drift_skill_roots=_resolve_declared_roots(
                plugin_dir,
                cls.drift_skill_roots(),
            ),
            mcp_servers=_resolve_mcp_servers(
                plugin_dir,
                data_dir,
                self._workspace,
                cls.mcp_servers(),
            ),
            managed_services=_resolve_managed_services(
                plugin_dir,
                data_dir,
                self._workspace,
                cls.managed_services(),
                source_revision=source_revision,
            ),
            before_turn_modules=tuple(
                _load_module_list(instance, "before_turn_modules")
            ),
            before_reasoning_modules=tuple(
                _load_module_list(instance, "before_reasoning_modules")
            ),
            prompt_render_modules=tuple(
                _load_module_list(instance, "prompt_render_modules")
            ),
            before_step_modules=tuple(
                _load_module_list(instance, "before_step_modules")
            ),
            after_step_modules=tuple(_load_module_list(instance, "after_step_modules")),
            after_reasoning_modules=tuple(
                _load_module_list(instance, "after_reasoning_modules")
            ),
            after_turn_modules=tuple(_load_module_list(instance, "after_turn_modules")),
            proactive_modules=tuple(_load_module_list(instance, "proactive_modules")),
            proactive_lifecycles=tuple(
                _load_module_list(instance, "proactive_lifecycles")
            ),
            proactive_module_factories=tuple(
                _load_module_list(instance, "proactive_module_factories")
            ),
            proactive_runtime_factories=tuple(
                _load_module_list(instance, "proactive_runtime_factories")
            ),
            proactive_sources=tuple(sources),
            jobs=tuple(jobs),
            channels=cast(
                tuple[Channel, ...],
                tuple(_load_module_list(instance, "channels")),
            ),
            dashboard_module=_resolve_dashboard_module(
                plugin_dir,
                cls.dashboard_module(),
            ),
            mobile_ui_asset=mobile_ui_asset,
            mobile_ui_query=mobile_ui_query,
            mobile_ui_available=mobile_ui_available,
        )

    def _validate_candidate(
        self,
        *,
        instance: Any,
        plugin_id: str,
        revision: str,
        contributions: PluginContributions,
    ) -> GateResult:
        checks: list[GateCheckResult] = []
        current = self._active_generations.get(plugin_id)
        other_generations = [
            generation
            for generation in self._active_generations.values()
            if generation.plugin_id != plugin_id
        ]
        other_generations.extend(
            generation
            for prepared_id, generation in self._prepared_generations.items()
            if prepared_id != plugin_id
        )

        def check(check_id: str, passed: bool, evidence: object = "") -> None:
            checks.append(
                GateCheckResult(
                    check_id=check_id,
                    status="passed" if passed else "failed",
                    evidence=evidence,
                )
            )

        composable = isinstance(instance, ComposablePlugin)
        check(
            "api_version",
            getattr(instance, "api_version", None) == (3 if composable else 2),
            getattr(instance, "api_version", None),
        )
        if composable:
            check("lifecycle_api", True, {"contract": "apply(ctx, config)"})
        else:
            lifecycle_type = type(instance)
            legacy_lifecycle = [
                name for name in ("initialize",) if name in lifecycle_type.__dict__
            ]
            check(
                "lifecycle_api",
                not legacy_lifecycle
                and inspect.iscoroutinefunction(instance.prepare)
                and not inspect.iscoroutinefunction(instance.activate)
                and not inspect.iscoroutinefunction(instance.retire)
                and inspect.iscoroutinefunction(instance.terminate),
                {"legacy": legacy_lifecycle},
            )
        metadata = plugin_registry.get_handlers_by_module_path(
            type(instance).__module__
        )
        tool_names = [
            md.tool_name or md.handler_name
            for md in metadata
            if md.kind == MetadataKind.TOOL
        ]
        duplicate_tools = _duplicates(tool_names)
        current_tool_names = (
            {
                metadata.tool_name or metadata.handler_name
                for metadata in plugin_registry.get_handlers_by_module_path(
                    current.module_path
                )
                if metadata.kind == MetadataKind.TOOL
            }
            if current is not None
            else set()
        )
        occupied_tools = (
            sorted(
                name
                for name in tool_names
                if self._tool_registry.has_tool(name) and name not in current_tool_names
            )
            if self._tool_registry is not None
            else []
        )
        check(
            "tool_names",
            not duplicate_tools and not occupied_tools,
            {"duplicates": duplicate_tools, "occupied": occupied_tools},
        )
        source_ids = [source.spec.id for source in contributions.proactive_sources]
        source_errors = [
            source.spec.id
            for source in contributions.proactive_sources
            if not source.spec.id
            or not source.spec.channels
            or not set(source.spec.channels).issubset({"alert", "content", "context"})
            or not source.spec.server
            or not source.spec.fetch_tool
            or source.spec.fetch_page_size < 0
            or source.spec.server not in contributions.mcp_servers
        ]
        check(
            "proactive_sources",
            not _duplicates(source_ids) and not source_errors,
            {"duplicates": _duplicates(source_ids), "invalid": source_errors},
        )
        occupied_servers = {
            server_name
            for generation in other_generations
            for server_name in generation.contributions.mcp_servers
        }
        if self._active_workspace_mcp is not None:
            occupied_servers.update(self._active_workspace_mcp.catalog.servers)
        check(
            "mcp_servers",
            not occupied_servers.intersection(contributions.mcp_servers),
            sorted(occupied_servers.intersection(contributions.mcp_servers)),
        )
        job_ids = [job.spec.id for job in contributions.jobs]
        check(
            "job_ids",
            all(job_ids) and not _duplicates(job_ids) if job_ids else True,
            _duplicates(job_ids),
        )
        channel_names = [
            str(getattr(channel, "name", "")).strip()
            for channel in contributions.channels
        ]
        occupied_channels = {
            str(getattr(channel, "name", "")).strip()
            for generation in other_generations
            for channel in generation.contributions.channels
        }
        check(
            "channel_names",
            (
                (
                    all(channel_names)
                    and not _duplicates(channel_names)
                    and not occupied_channels.intersection(channel_names)
                )
                if channel_names
                else True
            ),
            {
                "duplicates": _duplicates(channel_names),
                "occupied": sorted(occupied_channels.intersection(channel_names)),
            },
        )
        phase_groups = (
            ("before_turn_modules", contributions.before_turn_modules),
            ("before_reasoning_modules", contributions.before_reasoning_modules),
            ("prompt_render_modules", contributions.prompt_render_modules),
            ("before_step_modules", contributions.before_step_modules),
            ("after_step_modules", contributions.after_step_modules),
            ("after_reasoning_modules", contributions.after_reasoning_modules),
            ("after_turn_modules", contributions.after_turn_modules),
        )
        try:
            for field_name, candidate_modules in phase_groups:
                active_modules = [
                    module
                    for generation in other_generations
                    for module in getattr(generation.contributions, field_name)
                ]
                _ = RuntimeSnapshotCompiler.order_plugin_modules(
                    tuple([*active_modules, *candidate_modules])
                )
        except RuntimeError as error:
            check("phase_graph", False, str(error))
        else:
            check("phase_graph", True)
        lifecycle_ids = [
            lifecycle.id
            for lifecycle in contributions.proactive_lifecycles
            if isinstance(lifecycle, ProactiveLifecycleSpec)
        ]
        check(
            "proactive_lifecycles",
            len(lifecycle_ids) == len(contributions.proactive_lifecycles)
            and not _duplicates(lifecycle_ids)
            and not {
                lifecycle.id
                for generation in other_generations
                for lifecycle in generation.contributions.proactive_lifecycles
                if isinstance(lifecycle, ProactiveLifecycleSpec)
            }.intersection(lifecycle_ids),
            {
                "duplicates": _duplicates(lifecycle_ids),
                "occupied": sorted(
                    {
                        lifecycle.id
                        for generation in other_generations
                        for lifecycle in generation.contributions.proactive_lifecycles
                        if isinstance(lifecycle, ProactiveLifecycleSpec)
                    }.intersection(lifecycle_ids)
                ),
            },
        )
        lifecycle_structure_errors: list[str] = []
        for lifecycle in contributions.proactive_lifecycles:
            if not isinstance(lifecycle, ProactiveLifecycleSpec):
                continue
            if (
                not lifecycle.id
                or any(not value for value in lifecycle.initial_slots)
                or any(not value for value in lifecycle.terminal_slots)
                or len(set(lifecycle.initial_slots)) != len(lifecycle.initial_slots)
                or len(set(lifecycle.terminal_slots)) != len(lifecycle.terminal_slots)
            ):
                lifecycle_structure_errors.append(f"{lifecycle.id}: slots")
                continue
            try:
                _ = ProactiveLifecycleBuilder().build(
                    ProactiveLifecycleSpec(
                        id=lifecycle.id,
                        modules=lifecycle.modules,
                        initial_slots=lifecycle.initial_slots,
                    )
                )
            except RuntimeError as error:
                lifecycle_structure_errors.append(f"{lifecycle.id}: {error}")
        check(
            "proactive_lifecycle_structure",
            not lifecycle_structure_errors,
            lifecycle_structure_errors,
        )
        try:
            semantic_checks = instance.static_semantic_checks()
        except Exception as error:
            check("semantic_checks", False, str(error))
        else:
            invalid_semantic = [
                semantic
                for semantic in semantic_checks
                if not isinstance(semantic, PluginSemanticCheck) or not semantic.passed
            ]
            check(
                "semantic_checks",
                not invalid_semantic,
                [
                    getattr(semantic, "evidence", repr(semantic))
                    for semantic in invalid_semantic
                ],
            )
        failed = [item for item in checks if item.status == "failed"]
        return GateResult(
            gate_id="G1/G3-static",
            plugin_id=plugin_id,
            candidate_revision=revision,
            status="failed" if failed else "passed",
            checks=tuple(checks),
            failure_reason="; ".join(item.check_id for item in failed),
        )

    def _publish_contributions(self, contributions: PluginContributions) -> None:
        self._before_turn_modules.extend(contributions.before_turn_modules)
        self._before_reasoning_modules.extend(contributions.before_reasoning_modules)
        self._prompt_render_modules.extend(contributions.prompt_render_modules)
        self._before_step_modules.extend(contributions.before_step_modules)
        self._after_step_modules.extend(contributions.after_step_modules)
        self._after_reasoning_modules.extend(contributions.after_reasoning_modules)
        self._after_turn_modules.extend(contributions.after_turn_modules)
        self._proactive_modules.extend(contributions.proactive_modules)
        self._proactive_lifecycles.extend(contributions.proactive_lifecycles)
        self._proactive_module_factories.extend(
            contributions.proactive_module_factories
        )
        self._proactive_runtime_factories.extend(
            contributions.proactive_runtime_factories
        )
        self._proactive_sources.extend(contributions.proactive_sources)
        self._jobs.extend(contributions.jobs)

    def _record_failed_gate(
        self,
        *,
        plugin_id: str,
        revision: str,
        check_id: str,
        reason: str,
    ) -> None:
        self._gate_results[plugin_id] = GateResult(
            gate_id="G1/G3-static",
            plugin_id=plugin_id,
            candidate_revision=revision,
            status="failed",
            checks=(
                GateCheckResult(
                    check_id=check_id,
                    status="failed",
                    evidence=reason,
                ),
            ),
            failure_reason=reason,
        )

    def _import_plugin(self, module_name: str, path: Path) -> None:
        self._fresh_importer.register(module_name, path.parent)
        spec = self._fresh_importer.root_spec(module_name, path)
        if spec is None or spec.loader is None:
            self._fresh_importer.unregister(module_name)
            raise ImportError(f"无法加载插件文件: {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)  # type: ignore[union-attr]
        except BaseException:
            self._remove_module_tree(module_name)
            raise

    def _remove_module_tree(self, module_name: str) -> None:
        self._fresh_importer.unregister(module_name)
        plugin_registry.remove_module_tree(module_name)
        for imported_name in tuple(sys.modules):
            if imported_name == module_name or imported_name.startswith(
                f"{module_name}."
            ):
                _ = sys.modules.pop(imported_name, None)

    def _register_tools(
        self,
        instance: Any,
        module_path: str,
        tool_names: list[str],
    ) -> None:
        if self._tool_registry is None:
            return
        for md in plugin_registry.get_handlers_by_module_path(module_path):
            # 1. 只处理 TOOL 类型元数据
            if md.kind != MetadataKind.TOOL:
                continue
            tool = _build_plugin_tool(instance, md)
            tool_name = tool.name
            # 3. 注册到 ToolRegistry，标记来源为 plugin
            plugin_name = getattr(instance, "name", None) or module_path
            if self._tool_registry.has_tool(tool_name):
                raise RuntimeError(f"插件工具名称重复: {tool_name}")
            tool_names.append(tool_name)
            self._tool_registry.register(
                tool,
                risk=md.tool_risk or "read-write",
                always_on=bool(md.tool_always_on),
                search_hint=md.tool_search_hint,
                source_type="plugin",
                source_name=plugin_name,
            )
            logger.info("插件工具已注册: %s (来自 %s)", tool_name, plugin_name)

    def _bind_tool_hooks(self, instance: Any, module_path: str) -> None:
        # V2_REMOVAL(tool-hooks)：legacy 非 generation load path，随 v2 Manager 删除。
        for md in plugin_registry.get_handlers_by_module_path(module_path):
            if md.kind != MetadataKind.TOOL_HOOK:
                continue
            bound = functools.partial(md.handler, instance)
            hook = _PluginToolHook(
                name=f"plugin:{getattr(instance, 'name', module_path)}:{md.handler_name}",
                handler=bound,
                tool_name_filter=md.hook_tool_name,
            )
            self._tool_hooks.append(hook)
            logger.info("插件 tool hook 已注册: %s", hook.name)

    async def terminate_all(self) -> None:
        """完成快照、插件生命周期和作用域资源的全量关闭。"""

        from agent.plugins.context import allow_plugin_cleanup_writes

        # 1. 先收束正式 Channel owner，再允许对应插件 Root 进入 drain。
        externally_cancelled = False
        channel_runtime = self._active_channel_generation
        if channel_runtime is not None:
            _ = self._snapshot_store.pause_admission()
            channel_runtime.close_admission()
            try:
                _, cancelled = await _complete_critical(channel_runtime.stop())
                externally_cancelled = externally_cancelled or cancelled
            except BaseException as error:
                current = asyncio.current_task()
                externally_cancelled = externally_cancelled or (
                    current is not None and current.cancelling() > 0
                )
                self._cleanup_failures.append(
                    CleanupFailure(
                        resource=f"channel-generation:{channel_runtime.snapshot_id}",
                        error=str(error) or type(error).__name__,
                    )
                )
                raise RuntimeError(
                    "Channel runtime cleanup 未完成，generation owner 已保留"
                ) from error
            else:
                self._active_channel_generation = None
                self._active_channel_catalog_identity = None
        activity_host = self._activity_host
        if activity_host is not None and activity_host.active is not None:
            _ = self._snapshot_store.pause_admission()
            try:
                _, cancelled = await _complete_critical(activity_host.close())
                externally_cancelled = externally_cancelled or cancelled
            except BaseException as error:
                self._cleanup_failures.append(
                    CleanupFailure(
                        resource="activity-host",
                        error=str(error) or type(error).__name__,
                    )
                )
                raise RuntimeError(
                    "Activity runtime cleanup 未完成，generation owner 已保留"
                ) from error

        # 2. 关闭当前 generation admission，再完成快照回收。
        for generation in self._active_generations.values():
            self._retire_generation(generation)
        _, snapshot_cancelled = await _complete_critical(self._snapshot_store.close())
        externally_cancelled = externally_cancelled or snapshot_cancelled
        self._ready_candidate = None
        for plugin_id in tuple(self._prepared_generations):
            _, cancelled = await _complete_critical(self.discard_prepared(plugin_id))
            externally_cancelled = externally_cancelled or cancelled
        if self._prepared_workspace_mcp is not None:
            _, cancelled = await _complete_critical(
                self._discard_workspace_mcp_candidate()
            )
            externally_cancelled = externally_cancelled or cancelled

        # 3. 逐插件终止并消费全部 cleanup failures。
        for mp in list(self._loaded):
            active_info = self._active_plugins.get(mp)
            instance = plugin_registry.get_instance(mp)
            terminator = getattr(instance, "terminate", None)
            if callable(terminator):
                try:
                    typed_terminator = cast(
                        Callable[[], Awaitable[None]],
                        terminator,
                    )
                    generation = (
                        None
                        if active_info is None
                        else self._active_generations.get(active_info.plugin_id)
                    )
                    writer_id = "" if generation is None else generation.generation_id
                    with allow_plugin_cleanup_writes(writer_id):
                        _, cancelled = await _complete_critical(typed_terminator())
                    externally_cancelled = externally_cancelled or cancelled
                except (asyncio.CancelledError, Exception) as error:
                    current = asyncio.current_task()
                    externally_cancelled = externally_cancelled or (
                        current is not None and current.cancelling() > 0
                    )
                    error_text = str(error) or type(error).__name__
                    logger.warning("插件 terminate 失败 (%s): %s", mp, error_text)
                    self._cleanup_failures.append(
                        CleanupFailure(
                            resource=f"plugin:{mp}:terminate",
                            error=error_text,
                        )
                    )
            scope = self._scopes.pop(mp, None)
            if scope is not None:
                generation = (
                    None
                    if active_info is None
                    else self._active_generations.get(active_info.plugin_id)
                )
                writer_id = "" if generation is None else generation.generation_id
                with allow_plugin_cleanup_writes(writer_id):
                    cleanup_failures, cancelled = await _complete_critical(
                        scope.aclose()
                    )
                self._cleanup_failures.extend(cleanup_failures)
                externally_cancelled = externally_cancelled or cancelled

            # 4. 注销工具、模块和运行时注册。
            for md in plugin_registry.get_handlers_by_module_path(mp):
                if md.kind == MetadataKind.TOOL and self._tool_registry is not None:
                    self._tool_registry.unregister(md.tool_name or md.handler_name)
            self._remove_module_tree(mp)
            stable_alias = self._stable_aliases.pop(mp, None)
            if stable_alias is not None:
                active_alias = plugin_registry.get_instance(stable_alias)
                if active_alias is instance:
                    self._remove_module_tree(stable_alias)
                else:
                    self._fresh_importer.unregister(stable_alias)
            if active_info is not None:
                generation = self._active_generations.get(active_info.plugin_id)
                if generation is not None and generation.module_path == mp:
                    _ = self._active_generations.pop(active_info.plugin_id)
                    generation.state = "retired"
            _ = self._active_plugins.pop(mp, None)
        self._loaded.clear()
        self._active_plugins.clear()
        self._tool_hooks.clear()
        self._before_turn_modules.clear()
        self._before_reasoning_modules.clear()
        self._prompt_render_modules.clear()
        self._before_step_modules.clear()
        self._after_step_modules.clear()
        self._after_reasoning_modules.clear()
        self._after_turn_modules.clear()
        self._proactive_modules.clear()
        self._proactive_lifecycles.clear()
        self._proactive_module_factories.clear()
        self._proactive_runtime_factories.clear()
        self._proactive_sources.clear()
        self._jobs.clear()
        self._channels.clear()
        self._scopes.clear()
        self._active_generations.clear()
        self._draining_generations.clear()
        self._prepared_generations.clear()
        self._active_workspace_mcp = None
        self._prepared_workspace_mcp = None
        self._stable_aliases.clear()
        if externally_cancelled:
            raise asyncio.CancelledError


class _PluginConfigError(Exception):
    pass


class _CandidateRejected(Exception):
    def __init__(self, gate: GateResult) -> None:
        super().__init__(gate.failure_reason)
        self.gate = gate


class _StablePluginFailed(Exception):
    """标识一个可以排除后重试的 legacy stable 参与者。"""

    def __init__(
        self,
        generation: PluginGeneration,
        phase: str,
        cause: Exception,
    ) -> None:
        super().__init__(str(cause))
        self.generation = generation
        self.phase = phase
        self.cause = cause


def _gate_failure_details(gate: GateResult) -> str:
    """把失败 Gate 的 check 与证据压成可持久诊断文本。"""
    return (
        "; ".join(
            f"{check.check_id}: {check.evidence}"
            for check in gate.checks
            if check.status == "failed"
        )
        or gate.failure_reason
    )


def _with_gate_check(
    gate: GateResult,
    *,
    check_id: str,
    passed: bool,
    evidence: object,
    gate_id: str | None = None,
) -> GateResult:
    check = GateCheckResult(
        check_id=check_id,
        status="passed" if passed else "failed",
        evidence=evidence,
    )
    checks = (*gate.checks, check)
    failed = [item.check_id for item in checks if item.status == "failed"]
    return GateResult(
        gate_id=gate_id or gate.gate_id,
        plugin_id=gate.plugin_id,
        candidate_revision=gate.candidate_revision,
        status="failed" if failed else "passed",
        checks=checks,
        failure_reason="; ".join(failed),
    )


def _load_plugin_config(
    data_dir: Path,
    config_model: type[BaseModel] | None = None,
) -> Any:
    projection = _read_plugin_config_projection(data_dir)
    return _validate_plugin_config_projection(projection, config_model)


def _read_plugin_config_projection(
    data_dir: Path,
    *,
    credential_paths: tuple[str, ...] = (),
    credential_alias_groups: tuple[tuple[str, ...], ...] = (),
) -> dict[str, object]:
    """Read plugin config and replace declared secret values with opaque refs."""

    # 1. Core alone reads the formal file before plugin config validation.
    config_path = data_dir / "config.local.toml"
    raw_config: dict[str, Any] = {}
    if config_path.exists():
        try:
            raw_config = tomllib.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as e:
            raise _PluginConfigError(str(e)) from e
    for aliases in credential_alias_groups:
        present = tuple(path for path in aliases if _config_path_exists(raw_config, path))
        if len(present) > 1:
            raise _PluginConfigError(
                "同一 channel credential 不得同时声明多个 physical alias: "
                + ", ".join(present)
            )
    projected = cast(dict[str, object], copy.deepcopy(raw_config))
    for path in credential_paths:
        _redact_plugin_config_path(projected, path)
    return projected


def _validate_channel_credential_schema(
    config_model: type[BaseModel] | None,
    *,
    credential_paths: tuple[str, ...],
) -> tuple[tuple[str, ...], ...]:
    """Bind every opaque credential field to its complete physical alias set."""

    # 1. Discover opaque credential fields from the validated Pydantic schema.
    groups = _collect_channel_credential_aliases(config_model)
    schema_paths = tuple(sorted(path for group in groups for path in group))

    # 2. Static admission owns the complete raw-path declaration.
    expected = tuple(sorted(credential_paths))
    if schema_paths != expected:
        raise _PluginConfigError(
            "ConfigModel credential aliases 与静态 manifest 不一致: "
            f"schema={schema_paths} manifest={expected}"
        )
    return groups


def _collect_channel_credential_aliases(
    config_model: type[BaseModel] | None,
    *,
    prefix: tuple[str, ...] = (),
    seen: frozenset[type[BaseModel]] = frozenset(),
) -> tuple[tuple[str, ...], ...]:
    """Collect physical input paths for direct CredentialRef fields."""

    if config_model is None:
        return ()
    if not isinstance(config_model, type) or not issubclass(config_model, BaseModel):
        raise _PluginConfigError("ConfigModel 必须继承 pydantic.BaseModel")
    if config_model in seen:
        return ()

    groups: list[tuple[str, ...]] = []
    next_seen = seen | {config_model}
    validate_by_name = bool(
        config_model.model_config.get("validate_by_name")
        or config_model.model_config.get("populate_by_name")
    )
    validate_by_alias = config_model.model_config.get("validate_by_alias") is not False
    for name, field_info in config_model.model_fields.items():
        aliases = _pydantic_input_aliases(
            name,
            field_info.validation_alias,
            field_info.alias,
            validate_by_name=validate_by_name,
            validate_by_alias=validate_by_alias,
        )
        annotation = field_info.annotation
        if _annotation_contains_credential_ref(annotation):
            if not _is_opaque_credential_annotation(annotation):
                raise _PluginConfigError(
                    f"channel credential 字段只能是 CredentialRef 或 None: {name}"
                )
            groups.append(
                tuple(sorted(".".join((*prefix, *alias)) for alias in aliases))
            )
            continue
        nested_model = _optional_basemodel_type(annotation)
        if nested_model is None:
            continue
        for alias in aliases:
            groups.extend(
                _collect_channel_credential_aliases(
                    nested_model,
                    prefix=(*prefix, *alias),
                    seen=next_seen,
                )
            )

    paths = [path for group in groups for path in group]
    if len(paths) != len(set(paths)):
        raise _PluginConfigError("ConfigModel credential physical alias 重复")
    return tuple(sorted(groups))


def _pydantic_input_aliases(
    field_name: str,
    validation_alias: str | AliasPath | AliasChoices | None,
    alias: str | None,
    *,
    validate_by_name: bool,
    validate_by_alias: bool,
) -> tuple[tuple[str, ...], ...]:
    """Normalize one Pydantic field's accepted mapping paths."""

    configured_alias = validation_alias or alias
    if configured_alias is None:
        choices: tuple[str | AliasPath, ...] = (field_name,)
    elif validate_by_alias:
        choices = (
            tuple(configured_alias.choices)
            if isinstance(configured_alias, AliasChoices)
            else (configured_alias,)
        )
        if validate_by_name:
            choices = (*choices, field_name)
    else:
        choices = (field_name,)
    paths: list[tuple[str, ...]] = []
    for choice in choices:
        raw_path = choice.path if isinstance(choice, AliasPath) else (choice,)
        if not raw_path or any(not isinstance(part, str) or not part for part in raw_path):
            raise _PluginConfigError(
                f"channel credential alias 只支持对象字符串路径: {field_name}"
            )
        paths.append(tuple(cast(tuple[str, ...], raw_path)))
    return tuple(sorted(set(paths)))


def _annotation_contains_credential_ref(annotation: object) -> bool:
    if annotation is CredentialRef:
        return True
    return any(_annotation_contains_credential_ref(item) for item in get_args(annotation))


def _is_opaque_credential_annotation(annotation: object) -> bool:
    if annotation is CredentialRef:
        return True
    origin = get_origin(annotation)
    return origin in {Union, UnionType} and all(
        item is CredentialRef or item is type(None) for item in get_args(annotation)
    )


def _optional_basemodel_type(annotation: object) -> type[BaseModel] | None:
    candidates = tuple(item for item in get_args(annotation) if item is not type(None))
    value = candidates[0] if len(candidates) == 1 else annotation
    if isinstance(value, type) and issubclass(value, BaseModel):
        return value
    return None


def _config_path_exists(config: Mapping[str, object], path: str) -> bool:
    current: object = config
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return False
        current = current[part]
    return True


def _validate_plugin_config_projection(
    projection: Mapping[str, object],
    config_model: type[BaseModel] | None,
) -> Any:
    """Validate an already redacted config projection through plugin schema."""

    # 1. Each candidate clone receives a fresh value owned by its module class.
    raw_config = cast(dict[str, Any], copy.deepcopy(dict(projection)))
    if config_model is not None:
        if not isinstance(config_model, type) or not issubclass(
            config_model, BaseModel
        ):
            raise _PluginConfigError("ConfigModel 必须继承 pydantic.BaseModel")
        try:
            return config_model.model_validate(raw_config)
        except ValidationError as e:
            raise _PluginConfigError(_format_validation_error(e)) from e
    from agent.plugins.config import PluginConfig

    return PluginConfig(raw_config) if raw_config else None


def _redact_plugin_config_path(config: dict[str, object], path: str) -> None:
    """Replace one present non-empty config leaf with an opaque credential ref."""

    parts = tuple(path.split("."))
    current: dict[str, object] = config
    for part in parts[:-1]:
        value = current.get(part)
        if value is None:
            return
        if not isinstance(value, dict):
            raise _PluginConfigError(
                f"channel credential path 不是对象路径: {path}"
            )
        current = cast(dict[str, object], value)
    leaf = parts[-1]
    if leaf not in current:
        return
    value = current[leaf]
    if value is None or value == "":
        return
    current[leaf] = CredentialRef(parts)


def _static_channel_credential_paths(
    manifest: StaticPluginManifest,
) -> tuple[str, ...]:
    """Flatten manifest channel credential declarations without ambiguity."""

    return tuple(
        sorted(
            {
                path
                for _channel, paths in manifest.channel_credentials
                for path in paths
            }
        )
    )


def _format_validation_error(error: ValidationError) -> str:
    parts: list[str] = []
    for item in error.errors():
        path = ".".join(str(part) for part in item.get("loc", ())) or "<root>"
        parts.append(f"{path}: {item.get('msg', 'invalid')}")
    return "; ".join(parts)


def _load_module_list(instance: Any, method_name: str) -> list[object]:
    provider = getattr(instance, method_name, None)
    if provider is None:
        return []
    if not callable(provider):
        raise RuntimeError(
            f"插件 {type(instance).__name__}.{method_name} 不是可调用对象"
        )
    try:
        loaded = provider()
    except Exception as e:
        raise RuntimeError(
            f"插件 {type(instance).__name__}.{method_name} 声明失败: {e}"
        ) from e
    if loaded is None:
        raise RuntimeError(
            f"插件 {type(instance).__name__}.{method_name} 返回值不能为 None"
        )
    if not isinstance(loaded, list):
        raise RuntimeError(
            f"插件 {type(instance).__name__}.{method_name} 返回值不是 list"
        )
    return loaded


def _resolve_plugin_id(mod: dict[str, str]) -> str:
    name = mod["name"]
    marketplace = mod.get("marketplace", "").strip()
    if not marketplace:
        return name
    return f"{name}@{marketplace}"


def _resolve_plugin_data_dir(
    name: str,
    mod: dict[str, str],
    workspace: Path,
) -> Path:
    """把插件可写数据固定到当前 workspace 的独立目录。"""

    # 1. 交给统一路径边界校验插件身份
    marketplace = mod.get("marketplace", "").strip()
    suffix = marketplace or "builtin"
    return workspace_plugin_data_dir(workspace, name, suffix)


def _plugins_home(installed_cache_root: Path | None) -> Path:
    if installed_cache_root is not None:
        return installed_cache_root.parent
    return plugins_root()


def _installed_artifact_base(generation: PluginGeneration) -> Path | None:
    if generation.source_type != "installed":
        return None
    plugin_dir = generation.plugin_dir
    plugin_base = (
        plugin_dir.parent.parent
        if plugin_dir.parent.name == ".artifacts"
        else plugin_dir.parent
    )
    state_path = pointer_state_path(plugin_base)
    if not state_path.exists() and not state_path.is_symlink():
        return None
    return plugin_base


def _installed_generation_is_candidate(generation: PluginGeneration) -> bool:
    """Return whether this installed generation is the explicit latest pointer."""

    if generation.source_type != "installed":
        return False
    plugin_dir = generation.plugin_dir
    return _installed_candidate_base_from_root(plugin_dir) is not None


def _installed_candidate_base(generation: PluginGeneration) -> Path | None:
    if generation.source_type != "installed":
        return None
    plugin_dir = generation.plugin_dir
    return _installed_candidate_base_from_root(plugin_dir)


def _discard_generation_candidate_pointer(generation: PluginGeneration) -> None:
    plugin_base = _installed_candidate_base(generation)
    if plugin_base is not None:
        _ = discard_latest_pointer(plugin_base)


def _installed_candidate_base_from_root(plugin_dir: Path) -> Path | None:
    """Resolve the candidate pointer owner for an exact installed latest root."""

    # 1. Legacy installs and stable==latest keep immediate publish compatibility.
    plugin_base = _installed_artifact_base_from_root(plugin_dir)
    stable = read_pointer(plugin_base, "stable")
    latest = read_pointer(plugin_base, "latest")
    if stable is None and latest is None:
        return None
    if stable is None or latest is None:
        raise RuntimeError(f"插件 artifact pointer 必须成对存在: {plugin_base}")
    if stable == latest:
        return None

    # 2. Candidate operations must own the exact durable latest root.
    latest_root = resolve_pointer(plugin_base, latest)
    if latest_root is None or plugin_dir.resolve() != latest_root.resolve():
        raise RuntimeError(f"插件 generation 与 latest pointer 不一致: {plugin_dir}")
    return plugin_base


def _switch_ready_pointer(
    ready: _ReadyPluginCandidate,
    plugin_base: Path,
) -> None:
    """在候选仍拥有磁盘 pointer 时原子提升它。"""

    previous, candidate = _ready_artifact_pointers(ready, plugin_base)
    pointers = read_pointers(plugin_base)
    if pointers is None or (pointers.stable, pointers.latest) not in {
        (previous, candidate),
        (candidate, candidate),
    }:
        raise RuntimeError(f"插件 artifact pointer 已被其他发布改变: {plugin_base}")
    _ = write_pointers(plugin_base, stable=candidate, latest=candidate)


def _restore_ready_pointer(
    ready: _ReadyPluginCandidate,
    plugin_base: Path,
) -> None:
    """把 ready candidate 的完整指针对恢复到先前 stable。"""

    previous, candidate = _ready_artifact_pointers(ready, plugin_base)
    pointers = read_pointers(plugin_base)
    if pointers is None or (pointers.stable, pointers.latest) not in {
        (previous, candidate),
        (candidate, candidate),
        (previous, previous),
    }:
        raise RuntimeError(f"插件 artifact pointer 已被其他发布改变: {plugin_base}")
    _ = write_pointers(plugin_base, stable=previous, latest=previous)


def _ready_artifact_pointers(
    ready: _ReadyPluginCandidate,
    plugin_base: Path,
) -> tuple[ArtifactPointer, ArtifactPointer]:
    """解析 ready candidate 事务拥有的前后 artifact pointer。"""

    candidate_root = ready.candidate.plugin_dir
    candidate = relative_artifact_pointer(plugin_base, candidate_root)
    if ready.previous is None:
        return ArtifactPointer(None), candidate
    previous_root = ready.previous.plugin_dir
    previous_base = _installed_artifact_base_from_root(previous_root)
    if previous_base.resolve() != plugin_base.resolve():
        raise RuntimeError("latest candidate 与 stable 不属于同一插件 artifact")
    return relative_artifact_pointer(plugin_base, previous_root), candidate


def _discard_installed_candidate_mod(mod: dict[str, str]) -> None:
    if mod.get("source_type") != "installed":
        return
    plugin_base = _installed_candidate_base_from_root(Path(mod["plugin_root"]))
    if plugin_base is not None:
        _ = discard_latest_pointer(plugin_base)


def _installed_artifact_base_from_root(plugin_dir: Path) -> Path:
    return (
        plugin_dir.parent.parent
        if plugin_dir.parent.name == ".artifacts"
        else plugin_dir.parent
    )


def _mod_source_revision(mod: dict[str, str] | None) -> str | None:
    if mod is None:
        return None
    return _source_revision(Path(mod["plugin_root"]))


def _resolve_declared_roots(
    plugin_dir: Path,
    declared: tuple[str, ...],
) -> tuple[Path, ...]:
    plugin_root = plugin_dir.resolve(strict=False)
    roots: list[Path] = []
    seen: set[Path] = set()
    for raw_path in declared:
        path = (plugin_dir / raw_path).resolve(strict=False)
        _require_plugin_path(plugin_root, path, "能力目录")
        if not path.is_dir():
            raise RuntimeError(f"插件能力目录不存在: {path}")
        if path in seen:
            raise RuntimeError(f"插件能力目录重复: {path}")
        seen.add(path)
        roots.append(path)
    return tuple(roots)


def _memory_runtime_info(memory_engine: object) -> MemoryRuntimeInfo:
    describe = getattr(memory_engine, "describe", None)
    if not callable(describe):
        raise RuntimeError("Core Memory engine 缺少 describe()")
    descriptor = describe()
    name = getattr(descriptor, "name", None)
    if not isinstance(name, str) or not name or name.strip() != name:
        raise RuntimeError("Core Memory engine descriptor.name 无效")
    return MemoryRuntimeInfo(name=name)


def _resolve_dashboard_module(plugin_dir: Path, declared: str | None) -> Path | None:
    if declared is None:
        return None
    path = (plugin_dir / declared).resolve(strict=False)
    root = plugin_dir.resolve(strict=False)
    if not path.is_relative_to(root) or path.suffix != ".py" or not path.is_file():
        raise RuntimeError(f"插件 dashboard module 无效: {declared}")
    return path


def _reject_legacy_mobile_ui_api(cls: type[Any], plugin_id: str) -> None:
    """拒绝已移除的移动 UI v1 声明，避免插件被静默降级。"""

    legacy_methods = tuple(
        name
        for name in (
            "mobile_ui_module",
            "mobile_ui_stylesheet",
            "mobile_ui_call",
        )
        if inspect.getattr_static(cls, name, None) is not None
    )
    if legacy_methods:
        raise RuntimeError(
            f"插件 {plugin_id} 使用已移除的 Mobile UI v1 API: "
            f"{', '.join(legacy_methods)}；请迁移到 mobile_ui 和 mobile_ui_query"
        )


def _resolve_mobile_ui_asset(
    plugin_dir: Path,
    declared: MobileUiContribution | None,
) -> MobileUiAsset | None:
    """在插件激活边界固化并校验移动 UI 资产。"""

    if declared is None:
        return None
    if not isinstance(declared, MobileUiContribution):
        raise RuntimeError("插件 mobile UI 声明必须是 MobileUiContribution")
    navigation = declared.navigation
    return _resolve_composable_mobile_ui_asset(
        plugin_dir,
        module=declared.module,
        stylesheet=declared.stylesheet,
        navigation_label=None if navigation is None else navigation.label,
        navigation_description=(
            None if navigation is None else navigation.description
        ),
        slots=tuple(declared.slots),
    )


def _resolve_composable_mobile_ui_asset(
    plugin_dir: Path,
    *,
    module: str,
    stylesheet: str | None,
    navigation_label: str | None,
    navigation_description: str | None,
    slots: tuple[str, ...],
) -> MobileUiAsset:
    """Use the same strict asset boundary for v2 and v3 declarations."""

    return resolve_mobile_ui_asset(
        plugin_dir,
        module=module,
        stylesheet=stylesheet,
        navigation_label=navigation_label,
        navigation_description=navigation_description,
        slots=slots,
    )


def _require_sync_mobile_ui_handler(
    handler: object,
    field_name: str,
    plugin_id: str,
) -> Any:
    if not callable(handler):
        raise RuntimeError(
            f"插件 {plugin_id} mobile UI {field_name} handler 必须可调用"
        )
    if inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(
        getattr(handler, "__call__", None)
    ):
        raise RuntimeError(
            f"插件 {plugin_id} mobile UI {field_name} handler 必须是同步函数"
        )
    return handler


def _resolve_managed_services(
    plugin_dir: Path,
    data_dir: Path,
    workspace: Path,
    declared: list[ManagedServiceSpec],
    *,
    source_revision: str,
) -> dict[str, dict[str, Any]]:
    services: dict[str, dict[str, Any]] = {}
    plugin_root = plugin_dir.resolve(strict=False)
    for spec in declared:
        if (
            not isinstance(spec, ManagedServiceSpec)
            or not spec.id
            or not spec.command
            or spec.startup_timeout_seconds <= 0
            or not all(isinstance(item, str) and item for item in spec.command)
            or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in spec.env.items()
            )
            or not isinstance(spec.readiness_url, str)
            or not isinstance(spec.validation_port_env, str)
            or (
                spec.validation_port_env
                and not spec.validation_port_env.replace("_", "A").isalnum()
            )
        ):
            raise RuntimeError(f"插件 managed service 声明无效: {spec!r}")
        if spec.id in services:
            raise RuntimeError(f"插件 managed service 名称重复: {spec.id}")
        command = [
            _resolve_command_item(plugin_root, item, executable=index == 0)
            for index, item in enumerate(spec.command)
        ]
        cwd_path = Path(spec.cwd)
        resolved_cwd = (
            cwd_path.resolve(strict=False)
            if cwd_path.is_absolute()
            else (plugin_root / cwd_path).resolve(strict=False)
        )
        _require_plugin_path(plugin_root, resolved_cwd, "managed service cwd")
        cwd = str(resolved_cwd)
        if _is_python_command(command[0]):
            runtime_root = _resolve_mcp_runtime_root(plugin_dir, cwd, command)
            if runtime_root is not None:
                venv_python = _venv_python(runtime_root / ".venv")
                if venv_python.exists():
                    command[0] = str(venv_python)
        services[spec.id] = {
            "command": command,
            "cwd": cwd,
            "env": {
                **spec.env,
                "AKA_PLUGIN_DATA_DIR": str(data_dir),
                "AKASHIC_WORKSPACE": str(workspace),
            },
            "readiness_url": spec.readiness_url,
            "startup_timeout_seconds": spec.startup_timeout_seconds,
            "revision": source_revision,
            "validation_port_env": spec.validation_port_env,
        }
    return services


def _resolve_mcp_servers(
    plugin_dir: Path,
    data_dir: Path,
    workspace: Path,
    declared: list[McpServerSpec],
) -> dict[str, dict[str, Any]]:
    servers: dict[str, dict[str, Any]] = {}
    plugin_root = plugin_dir.resolve(strict=False)
    for spec in declared:
        if not isinstance(spec, McpServerSpec) or not spec.name or not spec.command:
            raise RuntimeError(f"插件 MCP server 声明无效: {spec!r}")
        if not all(isinstance(item, str) and item for item in spec.command):
            raise RuntimeError(f"插件 MCP command 声明无效: {spec.name}")
        if not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in spec.env.items()
        ):
            raise RuntimeError(f"插件 MCP env 声明无效: {spec.name}")
        if (
            not isinstance(spec.candidate_read_only_tools, tuple)
            or not all(
                isinstance(value, str) and value
                for value in spec.candidate_read_only_tools
            )
            or len(set(spec.candidate_read_only_tools))
            != len(spec.candidate_read_only_tools)
        ):
            raise RuntimeError(f"插件 MCP candidate 只读工具声明无效: {spec.name}")
        if spec.name in servers:
            raise RuntimeError(f"插件 MCP server 名称重复: {spec.name}")
        command = [
            _resolve_command_item(plugin_root, item, executable=index == 0)
            for index, item in enumerate(spec.command)
        ]
        cwd_path = Path(spec.cwd)
        resolved_cwd = (
            cwd_path.resolve(strict=False)
            if cwd_path.is_absolute()
            else (plugin_root / cwd_path).resolve(strict=False)
        )
        _require_plugin_path(plugin_root, resolved_cwd, "MCP cwd")
        cwd = str(resolved_cwd)
        env = {
            **spec.env,
            "AKA_PLUGIN_DATA_DIR": str(data_dir),
            "AKASHIC_WORKSPACE": str(workspace),
        }
        if _is_python_command(command[0]):
            runtime_root = _resolve_mcp_runtime_root(plugin_dir, cwd, command)
            if runtime_root is not None:
                venv_python = _venv_python(runtime_root / ".venv")
                if venv_python.exists():
                    command[0] = str(venv_python)
        servers[spec.name] = {
            "command": command,
            "env": env,
            "cwd": cwd,
            "candidate_read_only_tools": spec.candidate_read_only_tools,
        }
    return servers


def _validation_contributions(
    candidate: PluginGeneration,
    previous: PluginGeneration | None,
    *,
    validation_workspace: Path,
) -> PluginContributions:
    """构造候选路径隔离视图；同 UID 进程仍可绕过它，它不是安全沙箱。"""

    # 1. Allocate one loopback port for every changed managed service.
    production = candidate.contributions
    old_services = (
        previous.contributions.managed_services if previous is not None else {}
    )
    validation_services: dict[str, dict[str, Any]] = {}
    validation_env: dict[str, str] = {}
    for service_id, spec in production.managed_services.items():
        if old_services.get(service_id) == spec:
            continue
        port_env = str(spec.get("validation_port_env") or "")
        readiness_url = str(spec.get("readiness_url") or "")
        if not port_env or not readiness_url:
            raise RuntimeError(
                "候选插件改变独占 managed service，但未声明通用隔离端口: "
                f"plugin={candidate.plugin_id} service={service_id} "
                "需要 ManagedServiceSpec.validation_port_env 和 readiness_url"
            )
        port = _allocate_validation_port()
        validation_env[port_env] = str(port)
        isolated = dict(spec)
        isolated["env"] = {
            **dict(spec.get("env") or {}),
            port_env: str(port),
            "AKA_PLUGIN_DATA_DIR": str(candidate.data_dir),
            "AKASHIC_WORKSPACE": str(validation_workspace),
        }
        isolated["readiness_url"] = _replace_url_port(readiness_url, port)
        validation_services[service_id] = isolated

    # 2. Candidate MCP processes inherit only declared endpoint variables.
    mcp_servers = {
        name: {
            **spec,
            "env": {
                **dict(spec.get("env") or {}),
                **validation_env,
                "AKA_PLUGIN_DATA_DIR": str(candidate.data_dir),
                "AKASHIC_WORKSPACE": str(validation_workspace),
            },
        }
        for name, spec in production.mcp_servers.items()
    }
    candidate.validation_managed_services = validation_services
    return replace(
        production,
        mcp_servers=mcp_servers,
        managed_services=validation_services,
        channels=(previous.contributions.channels if previous is not None else ()),
    )


def _candidate_mcp_read_only_tools(
    generation: PluginGeneration,
    server_name: str,
    server_spec: dict[str, Any],
    available_tools: set[str],
) -> frozenset[str]:
    """严格校验并返回只对候选开放的 MCP 只读工具集合。"""

    # 1. 正式 snapshot 不继承候选验证专用的只读声明。
    if (
        generation.production_data_dir is None
        or generation.data_dir == generation.production_data_dir
    ):
        return frozenset()

    # 2. 候选声明精确匹配且默认拒绝；未知工具直接失败。
    raw_names = server_spec.get("candidate_read_only_tools", ())
    if not isinstance(raw_names, tuple) or not all(
        isinstance(name, str) and name for name in raw_names
    ):
        raise RuntimeError(
            f"MCP candidate 只读工具声明无效: server={server_name}"
        )
    declared = frozenset(raw_names)
    public_names = frozenset(
        f"mcp_{server_name}__{remote_name}" for remote_name in declared
    )
    unknown = public_names.difference(available_tools)
    if unknown:
        raise RuntimeError(
            "MCP candidate 只读工具不存在: "
            f"server={server_name} tools={', '.join(sorted(declared))}"
        )
    return public_names


def _allocate_validation_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return cast(int, listener.getsockname()[1])


def _remove_validation_data_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _candidate_data_exclude_paths(
    generation: PluginGeneration,
) -> tuple[str, ...]:
    """Return candidate-copy exclusions owned by static validation policy."""

    manifest = generation.static_manifest
    if manifest is None:
        return ()
    excluded = set(manifest.exclude_data_paths)
    if manifest.channel_credentials:
        excluded.add("config.local.toml")
    return tuple(sorted(excluded))


def _copy_validation_data(
    source: Path,
    target: Path,
    exclude_paths: tuple[str, ...],
) -> tuple[str, ...]:
    """Copy plugin data to a candidate tree while returning copied file paths."""

    validate_workspace_plugin_data_path(source, source.parents[1])
    excluded = tuple(PurePosixPath(item).as_posix() for item in exclude_paths)

    # 1. A new plugin has no formal bytes; candidate starts from an empty tree.
    if not source.exists():
        target.mkdir(parents=True)
        return ()
    source_root = source.resolve(strict=True)

    # 2. Candidate data must never retain an edge back into formal storage.
    for directory, dirnames, filenames in os.walk(source_root, followlinks=False):
        root = Path(directory)
        for name in (*dirnames, *filenames):
            path = root / name
            if path.is_symlink():
                raise RuntimeError(
                    f"candidate plugin-data 不允许复制符号链接: {path}"
                )

    # 3. Excluded paths are omitted before copytree opens their contents.
    def ignore(directory: str, names: list[str]) -> list[str]:
        current = Path(directory).resolve(strict=True)
        relative_dir = current.relative_to(source_root)
        ignored: list[str] = []
        for name in names:
            relative = (relative_dir / name).as_posix()
            if any(
                relative == item or relative.startswith(item + "/")
                for item in excluded
            ):
                ignored.append(name)
        return ignored

    shutil.copytree(source_root, target, ignore=ignore)

    # 4. Freeze a relative file inventory for review and Gate evidence.
    inventory: list[str] = []
    for directory, _dirnames, filenames in os.walk(target):
        root = Path(directory)
        for filename in filenames:
            inventory.append(root.joinpath(filename).relative_to(target).as_posix())
    return tuple(sorted(inventory))


def _replace_url_port(url: str, port: int) -> str:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or parsed.hostname is None:
        raise RuntimeError(f"managed service readiness_url 无效: {url}")
    host = parsed.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return urlunsplit(
        (parsed.scheme, f"{host}:{port}", parsed.path, parsed.query, parsed.fragment)
    )


def _replace_snapshot_payload(
    target: RuntimeSnapshot,
    source: RuntimeSnapshot,
) -> None:
    """刷新无 lease 候选载荷，并保留 store 拥有的生命周期字段。"""

    if target.lease_count or target.state not in {"validating", "committed"}:
        raise RuntimeError("只能刷新无 lease 的 candidate snapshot")
    for name in (
        "generations",
        "before_turn_modules",
        "before_reasoning_modules",
        "prompt_render_modules",
        "before_step_modules",
        "after_step_modules",
        "after_reasoning_modules",
        "after_turn_modules",
        "jobs",
        "proactive_sources",
        "proactive_modules",
        "proactive_lifecycles",
        "proactive_module_factories",
        "proactive_runtime_factories",
        "tool_hooks",
        "channels",
        "skill_catalog_generation_id",
        "mcp_catalog_generation_ids",
        "workspace_mcp_generation",
        "managed_services",
        "dashboard_bindings",
        "mobile_ui_registry",
        "mobile_ui_registry_identity",
        "channel_registry",
        "channel_registry_identity",
        "mcp_server_registry",
        "mcp_server_registry_identity",
        "managed_process_registry",
        "managed_process_registry_identity",
        "tool_registry",
        "plugin_skill_index",
        "command_registry",
        "event_handlers",
        "proactive_component_catalog",
        "proactive_component_catalog_identity",
        "private_proactive_catalog",
        "private_proactive_catalog_identity",
        "background_job_catalog",
        "background_job_catalog_identity",
        "composition_root",
        "composition_topology",
        "composition_active_plugin_ids",
        "composition_health_exempt_root_token",
    ):
        setattr(target, name, getattr(source, name))


def _validate_static_manifest_runtime(
    snapshot: RuntimeSnapshot,
    generations: Mapping[str, PluginGeneration],
) -> None:
    """Reconcile static MCP/process policy with the frozen Root projection."""

    all_manifests = {
        plugin_id: generation.static_manifest
        for plugin_id, generation in generations.items()
        if generation.static_manifest is not None
    }
    if not all_manifests:
        return

    # 1. Install staging owns the interpreter used by every declared Python runtime.
    for _plugin_id, generation in generations.items():
        manifest = generation.static_manifest
        if manifest is None:
            continue
        runtime_commands: list[tuple[str, tuple[str, ...]]] = []
        for runtime in manifest.python:
            _ = staged_python_interpreter(generation.plugin_dir, runtime)
        for kind, declarations in (
            ("mcp", manifest.mcp_servers),
            ("process", manifest.managed_processes),
        ):
            for declaration in declarations:
                runtime_commands.append(
                    (
                        f"{kind}:{declaration.name}",
                        materialize_static_command(
                            generation.plugin_dir,
                            manifest,
                            declaration,
                        ),
                    )
                )
        generation.static_runtime_commands = tuple(
            sorted(runtime_commands, key=lambda item: item[0])
        )

    # 2. Compare every static owner's import-free declarations with the exact
    # Root-frozen descriptors.  Missing, extra, and field drift all fail closed.
    if snapshot.composition_active_plugin_ids is None:
        raise RuntimeError("静态 v3 manifest snapshot 缺少 active plugin projection")
    active_plugin_ids = set(snapshot.composition_active_plugin_ids)
    manifests = {
        plugin_id: manifest
        for plugin_id, manifest in all_manifests.items()
        if plugin_id in active_plugin_ids
    }
    expected: set[tuple[object, ...]] = set()
    for plugin_id, manifest in manifests.items():
        assert manifest is not None
        expected.update(
            (
                plugin_id,
                declaration.name,
                declaration.command,
                declaration.cwd,
                declaration.env,
                declaration.required_tools,
                declaration.candidate_read_only_tools,
                declaration.endpoint_env,
                declaration.candidate_env,
            )
            for declaration in manifest.mcp_servers
        )
    registry = snapshot.mcp_server_registry
    actual: set[tuple[object, ...]] = set()
    if registry is not None:
        static_owners = set(all_manifests)
        actual.update(
            (
                descriptor.owner,
                descriptor.name,
                descriptor.command,
                descriptor.cwd,
                descriptor.env,
                descriptor.required_tools,
                descriptor.candidate_read_only_tools,
                tuple(
                    (endpoint.env, endpoint.process)
                    for endpoint in descriptor.endpoint_env
                ),
                descriptor.candidate_env,
            )
            for descriptor in registry.descriptors
            if descriptor.owner in static_owners
        )
    if actual != expected:
        missing = sorted(expected - actual, key=repr)
        extra = sorted(actual - expected, key=repr)
        raise RuntimeError(
            "静态 manifest MCP 声明与 Root frozen registry 不一致: "
            f"missing={missing!r} extra={extra!r}"
        )

    expected_processes: set[tuple[object, ...]] = set()
    for plugin_id, manifest in manifests.items():
        assert manifest is not None
        expected_processes.update(
            (
                plugin_id,
                declaration.name,
                declaration.command,
                declaration.cwd,
                declaration.env,
                declaration.port_env,
                declaration.formal_port,
                declaration.readiness_path,
                declaration.startup_timeout_seconds,
            )
            for declaration in manifest.managed_processes
        )
    process_registry = snapshot.managed_process_registry
    actual_processes: set[tuple[object, ...]] = set()
    if process_registry is not None:
        static_owners = set(all_manifests)
        actual_processes.update(
            (
                descriptor.owner,
                descriptor.name,
                descriptor.command,
                descriptor.cwd,
                descriptor.env,
                descriptor.port_env,
                descriptor.formal_port,
                descriptor.readiness_path,
                descriptor.startup_timeout_seconds,
            )
            for descriptor in process_registry.descriptors
            if descriptor.owner in static_owners
        )
    if actual_processes != expected_processes:
        missing = sorted(expected_processes - actual_processes, key=repr)
        extra = sorted(actual_processes - expected_processes, key=repr)
        raise RuntimeError(
            "静态 manifest managed process 声明与 Root frozen registry 不一致: "
            f"missing={missing!r} extra={extra!r}"
        )


def _resolve_command_item(
    plugin_dir: Path,
    item: str,
    *,
    executable: bool,
) -> str:
    path = Path(item)
    if path.is_absolute() or PureWindowsPath(item).is_absolute():
        if executable and path.is_file() and os.access(path, os.X_OK):
            return item
        raise RuntimeError(f"插件 MCP command 绝对路径不允许越过 artifact: {item}")
    if "/" not in item and "\\" not in item and not item.startswith("."):
        return item
    resolved = (
        path.resolve(strict=False)
        if path.is_absolute()
        else (plugin_dir / path).resolve(strict=False)
    )
    _require_plugin_path(plugin_dir, resolved, "MCP command")
    if not resolved.is_file():
        raise RuntimeError(f"插件 MCP command 文件不存在: {item}")
    return str(resolved)


def _require_plugin_path(plugin_dir: Path, path: Path, label: str) -> None:
    try:
        _ = path.relative_to(plugin_dir)
    except ValueError as error:
        raise RuntimeError(f"插件 {label} 越界: {path}") from error


def _is_python_command(value: str) -> bool:
    return Path(value).name.lower() in {"python", "python3", "python.exe"}


def _resolve_mcp_runtime_root(
    plugin_dir: Path,
    cwd: str,
    command: list[str],
) -> Path | None:
    candidates: list[Path] = []
    if len(command) >= 2:
        script_path = Path(command[1])
        if script_path.is_absolute():
            candidates.append(script_path.parent)
    candidates.extend([Path(cwd), plugin_dir])
    for candidate in candidates:
        if (candidate / "requirements.txt").exists():
            return candidate
    return None


def _venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _build_plugin_tool(instance: Any, metadata: Any) -> Any:
    from agent.tools.base import Tool as AgentTool

    bound = functools.partial(metadata.handler, instance, None)
    tool_name = metadata.tool_name or metadata.handler_name
    tool_class = type(
        f"PluginTool_{tool_name}",
        (AgentTool,),
        {
            "name": tool_name,
            "description": (metadata.handler.__doc__ or "").strip(),
            "parameters": metadata.tool_schema
            or {"type": "object", "properties": {}, "required": []},
            "execute": _make_execute(bound),
        },
    )
    return tool_class()


def _make_execute(bound: Any) -> Any:
    # 预先提取插件函数接受的参数名（排除 self/event），用于过滤 Registry 注入的 context 字段
    sig = inspect.signature(bound)
    accepted = frozenset(
        name for name in sig.parameters if name not in ("self", "event")
    )

    # 工厂函数把 bound 和 accepted 锁进闭包，避免动态 type() 时 self 顶掉 bound
    async def execute(self: Any, **kwargs: Any) -> str:
        filtered = {k: v for k, v in kwargs.items() if k in accepted}
        result = bound(**filtered)
        if inspect.isawaitable(result):
            result = await result
        return str(result)

    return execute


class _PluginToolHook(ToolHook):
    """将插件的 @on_tool_pre handler 适配为 ToolExecutor 的 ToolHook 接口。"""

    # V2_REMOVAL(tool-hooks)：四矩阵 Gate 全绿且 consumer 迁完后删除 adapter。

    event = "pre_tool_use"
    snapshot_managed = True

    def __init__(
        self,
        name: str,
        handler: Any,
        tool_name_filter: str | None = None,
    ) -> None:
        self.name = name
        self._handler = handler
        self._tool_name_filter = tool_name_filter

    def matches(self, ctx: HookContext) -> bool:
        if self._tool_name_filter is None:
            return True
        return ctx.request.tool_name == self._tool_name_filter

    async def run(self, ctx: HookContext) -> HookOutcome:
        # 1. 构造 PreToolCtx（复制 arguments，避免插件直接改原对象）
        event = PreToolCtx(
            session_key=ctx.request.session_key,
            channel=ctx.request.channel,
            chat_id=ctx.request.chat_id,
            tool_name=ctx.request.tool_name,
            arguments=dict(ctx.current_arguments),
            call_id=ctx.request.call_id,
            source=ctx.request.source,
            request_text=ctx.request.request_text,
            tool_batch=ctx.request.tool_batch,
            tool_batch_index=ctx.request.tool_batch_index,
        )
        # 2. 调插件 handler，返回值决定行为
        result = self._handler(event)
        if inspect.isawaitable(result):
            result = await result
        # 3. None → 不改参；dict → 新 arguments；HookOutcome → 允许插件直接 deny
        if result is None:
            return HookOutcome()
        if isinstance(result, HookOutcome):
            return result
        if isinstance(result, dict):
            return HookOutcome(updated_input=cast("dict[str, Any]", result))
        return HookOutcome()


def _file_revision(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(str(path.resolve(strict=False)).encode())
    if path.is_file():
        digest.update(path.read_bytes())
    else:
        digest.update(b"<missing>")
    return digest.hexdigest()


def _source_revision(plugin_dir: Path) -> str:
    digest = hashlib.sha256()
    root = plugin_dir.resolve(strict=False)
    excluded = {
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        "__pycache__",
        "node_modules",
    }
    for current, directories, filenames in os.walk(plugin_dir, followlinks=False):
        directories[:] = sorted(name for name in directories if name not in excluded)
        current_path = Path(current)
        for name in [*directories, *sorted(filenames)]:
            path = current_path / name
            relative = path.relative_to(plugin_dir)
            if path.is_symlink():
                resolved = path.resolve(strict=False)
                _require_plugin_path(root, resolved, "源码符号链接")
                digest.update(str(relative).encode())
                digest.update(os.readlink(path).encode())
                if resolved.is_file():
                    digest.update(resolved.read_bytes())
                continue
            if not path.is_file():
                continue
            resolved = path.resolve(strict=False)
            _require_plugin_path(root, resolved, "源码文件")
            digest.update(str(relative).encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()


def _source_metadata_revision(plugin_dir: Path) -> bytes:
    digest = hashlib.sha256()
    excluded = {
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        "__pycache__",
        "node_modules",
    }
    for current, directories, filenames in os.walk(plugin_dir, followlinks=False):
        directories[:] = sorted(name for name in directories if name not in excluded)
        current_path = Path(current)
        for name in [*directories, *sorted(filenames)]:
            path = current_path / name
            relative = path.relative_to(plugin_dir)
            try:
                stat = path.lstat()
            except FileNotFoundError:
                continue
            digest.update(str(relative).encode())
            digest.update(str(stat.st_mtime_ns).encode())
            digest.update(str(stat.st_size).encode())
            if path.is_symlink():
                digest.update(os.readlink(path).encode())
    return digest.digest()


def _path_metadata(path: Path) -> bytes:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return f"{path}:missing".encode()
    return f"{path}:{stat.st_mtime_ns}:{stat.st_size}".encode()


def _duplicates(values: list[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return sorted(duplicates)


def _skill_descriptions(generation: PluginGeneration) -> dict[str, str]:
    catalog = generation.skill_catalog
    if catalog is None:
        return {}
    return {
        name: record.description
        for name, record in sorted(catalog.normal.records.items())
    }


def _drift_skill_descriptions(generation: PluginGeneration) -> dict[str, str]:
    catalog = generation.skill_catalog
    if catalog is None:
        return {}
    return {
        name: record.description
        for name, record in sorted(catalog.drift.records.items())
    }


def _skill_body_hashes(
    generation: PluginGeneration,
    *,
    drift: bool,
) -> dict[str, str]:
    catalog = generation.skill_catalog
    if catalog is None:
        return {}
    records = catalog.drift.records if drift else catalog.normal.records
    return {
        name: hashlib.sha256(record.content.encode()).hexdigest()
        for name, record in sorted(records.items())
    }


def _mcp_tool_names(generation: PluginGeneration) -> list[str]:
    catalog = generation.mcp_catalog
    return list(catalog.tool_names) if catalog is not None else []


def _required_mcp_tools(
    sources: tuple[RegisteredProactiveSource, ...],
) -> dict[str, tuple[str, ...]]:
    required: dict[str, list[str]] = {}
    for source in sources:
        names = required.setdefault(source.spec.server, [])
        names.append(source.spec.fetch_tool)
        if source.spec.ack_tool:
            names.append(source.spec.ack_tool)
    return {
        server_name: tuple(tool_names) for server_name, tool_names in required.items()
    }


def _log_candidate_status(result: dict[str, object]) -> None:
    logger.info(
        "plugin_candidate_status plugin=%s gate=%s active=%s prepared=%s "
        "revision=%s counts=skills:%d,drift_skills:%d,mcp:%d,jobs:%d,sources:%d",
        result["plugin_id"],
        result["gate_status"],
        result["active_generation"],
        result["prepared_generation"] or "-",
        str(result["candidate_revision"])[:12],
        len(cast(list[object], result["skills"])),
        len(cast(dict[object, object], result["drift_skill_descriptions"])),
        len(cast(list[object], result["mcp_tools"])),
        len(cast(list[object], result["jobs"])),
        len(cast(list[object], result["proactive_sources"])),
    )
    logger.debug(
        "plugin_candidate_status_detail %s",
        json.dumps(result, ensure_ascii=False, sort_keys=True),
    )


def _job_keys(generation: PluginGeneration) -> list[str]:
    catalog = generation.job_catalog
    return sorted(catalog.jobs) if catalog is not None else []


def _proactive_source_keys(generation: PluginGeneration) -> list[str]:
    catalog = generation.proactive_catalog
    return sorted(catalog.sources) if catalog is not None else []


def _job_spec_evidence(generation: PluginGeneration) -> dict[str, object]:
    catalog = generation.job_catalog
    if catalog is None:
        return {}
    return {
        key: [
            (
                {"type": "interval", "seconds": trigger.seconds}
                if isinstance(trigger, IntervalTrigger)
                else {
                    "type": "event",
                    "event": trigger.event_type.__name__,
                }
            )
            for trigger in job.spec.triggers
        ]
        for key, job in sorted(catalog.jobs.items())
    }


def _proactive_source_spec_evidence(
    generation: PluginGeneration,
) -> dict[str, object]:
    catalog = generation.proactive_catalog
    if catalog is None:
        return {}
    return {
        key: {
            "server": source.spec.server,
            "fetch_tool": source.spec.fetch_tool,
            "ack_tool": source.spec.ack_tool,
            "fetch_page_size": source.spec.fetch_page_size,
        }
        for key, source in sorted(catalog.sources.items())
    }


def _gate_check_evidence(
    generation: PluginGeneration,
    check_id: str,
) -> object:
    for check in reversed(generation.gate_result.checks):
        if check.check_id == check_id:
            return check.evidence
    return []
