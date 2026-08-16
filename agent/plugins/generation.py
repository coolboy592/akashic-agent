from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Callable

    from agent.plugin_composition.ui_slots import MobileUiQueryHandler
    from agent.plugins.jobs import RegisteredPluginJob
    from agent.plugins.scope import PluginScope, ScopedEventBus
    from agent.plugins.specs import RegisteredProactiveSource
    from infra.channels.contract import Channel
    from agent.plugins.skill_host import PreparedSkillCatalog
    from agent.mcp.host import PreparedMcpCatalog
    from agent.plugins.activity_host import PreparedJobCatalog, PreparedProactiveCatalog
    from agent.plugins.static_manifest import StaticPluginManifest
    from agent.plugins.snapshot import RuntimeSnapshot


GateStatus = Literal["passed", "failed"]


@dataclass(frozen=True)
class MobileUiAsset:
    module: str
    module_sha256: str
    module_bytes: int
    stylesheet: str
    stylesheet_sha256: str | None
    stylesheet_bytes: int
    navigation_label: str | None
    navigation_description: str | None
    slots: tuple[str, ...]


@dataclass(frozen=True)
class PluginSemanticCheck:
    check_id: str
    passed: bool
    evidence: object = ""


@dataclass(frozen=True)
class PluginReadinessContext:
    generation_id: str
    mcp_catalog: PreparedMcpCatalog
    job_catalog: PreparedJobCatalog
    proactive_catalog: PreparedProactiveCatalog


@dataclass(frozen=True)
class GateCheckResult:
    check_id: str
    status: GateStatus
    evidence: object = ""


@dataclass(frozen=True)
class GateResult:
    gate_id: str
    plugin_id: str
    candidate_revision: str
    status: GateStatus
    checks: tuple[GateCheckResult, ...]
    failure_reason: str = ""


@dataclass(frozen=True)
class PluginContributions:
    # V2_REMOVAL(plugin-contributions-v2)：phase/MCP/process/proactive/job/channel/mobile 字段是
    # Manager 对 v2 固定方法的冻结结果。对应首个真实 v3 capability 与 full-fleet Gate 建立后
    # 逐族删除；manifest/Skill/Dashboard 等 v3 暂用字段须先迁入明确的 generation projection。
    manifest: dict[str, object]
    skill_roots: tuple[Path, ...] = ()
    drift_skill_roots: tuple[Path, ...] = ()
    mcp_servers: dict[str, dict[str, Any]] = field(default_factory=dict)
    managed_services: dict[str, dict[str, Any]] = field(default_factory=dict)
    before_turn_modules: tuple[object, ...] = ()
    before_reasoning_modules: tuple[object, ...] = ()
    prompt_render_modules: tuple[object, ...] = ()
    before_step_modules: tuple[object, ...] = ()
    after_step_modules: tuple[object, ...] = ()
    after_reasoning_modules: tuple[object, ...] = ()
    after_turn_modules: tuple[object, ...] = ()
    proactive_modules: tuple[object, ...] = ()
    proactive_lifecycles: tuple[object, ...] = ()
    proactive_module_factories: tuple[object, ...] = ()
    proactive_runtime_factories: tuple[object, ...] = ()
    proactive_sources: tuple[RegisteredProactiveSource, ...] = ()
    jobs: tuple[RegisteredPluginJob, ...] = ()
    channels: tuple[Channel, ...] = ()
    dashboard_module: Path | None = None
    mobile_ui_asset: MobileUiAsset | None = None
    # V2_REMOVAL(mobile-ui-contribution-v2)：仅在旧 generation 迁移完成前保留这组三元组；
    # v3 handler 只存在 RuntimeSnapshot.mobile_ui_registry 的 exact Root binding。
    mobile_ui_query: MobileUiQueryHandler | None = None
    mobile_ui_available: Callable[[], bool] | None = None


@dataclass
class PluginGeneration:
    plugin_id: str
    generation_id: str
    module_path: str
    source_revision: str
    config_revision: str
    plugin_dir: Path
    data_dir: Path
    config: object | None
    instance: object
    scope: PluginScope
    contributions: PluginContributions
    gate_result: GateResult
    config_projection: dict[str, object] = field(default_factory=dict)
    source_type: Literal["builtin", "installed"] = "builtin"
    static_manifest: StaticPluginManifest | None = None
    static_runtime_commands: tuple[tuple[str, tuple[str, ...]], ...] = ()
    composition_runtime_cleanup_registered: bool = False
    replaced_composition_runtime_generation: PluginGeneration | None = None
    entrypoint: str = "plugin.py"
    skill_catalog: PreparedSkillCatalog | None = None
    mcp_catalog: PreparedMcpCatalog | None = None
    job_catalog: PreparedJobCatalog | None = None
    proactive_catalog: PreparedProactiveCatalog | None = None
    runtime_snapshot: RuntimeSnapshot | None = None
    staged_event_bus: ScopedEventBus | None = None
    prepare_started: bool = False
    retire_started: bool = False
    minimum_resource_count: int = 0
    state: str = "active"
    lease_count: int = 0
    reload_tx_id: str | None = None
    production_contributions: PluginContributions | None = None
    validation_managed_services: dict[str, dict[str, Any]] = field(default_factory=dict)
    production_data_dir: Path | None = None
    boot_created_data_dir: bool = False
    publication_created_data_dir: bool = False
    validation_workspace: Path | None = None
    validation_data_inventory: tuple[str, ...] = ()
