from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from agent.plugins.generation_activity_host import ActivityHost
from agent.plugins.generation_proactive_bridge import CommittedProactiveBridge
from agent.plugins.generation_proactive_host import (
    DomainEffectContext,
    ProactiveActivityAdapter,
    ProactiveDomainEffects,
)
from agent.plugins.manager import PluginManager
from bus.event_bus import EventBus
from proactive_v2.frame import ProactiveFrame, new_proactive_frame


_PLUGIN_SOURCE = '''
import asyncio

from agent.plugin_composition import PROACTIVE_COMPONENTS, ProactiveModuleDefinition

api_version = 3
name = "effect_oracle"
version = "1.0.0"
inject = (PROACTIVE_COMPONENTS,)
MODE = "success"
RECORDS = {}
TRANSACTION_CALLS = 0
LAST_EFFECT = None
STARTED = asyncio.Event()


async def apply(ctx, config):
    await ctx.require(PROACTIVE_COMPONENTS).register(
        ctx,
        ProactiveModuleDefinition(
            slot="proactive.effect_oracle",
            lifecycle_id="default.proactive.frame.v1",
            handler_export="run_effect",
            domain_effect="emotion.state",
            domain_effect_lookup_export="lookup_effect",
        ),
    )


def lookup_effect(context):
    return RECORDS.get(context.invocation_id)


async def run_effect(context, frame):
    global LAST_EFFECT, TRANSACTION_CALLS
    effects = context.domain_effects
    if effects is None:
        raise RuntimeError("missing domain effects")
    LAST_EFFECT = effects
    if MODE == "cancel":
        STARTED.set()
        await asyncio.Future()

    def transaction(effect_context):
        global TRANSACTION_CALLS
        TRANSACTION_CALLS += 1
        RECORDS[effect_context.invocation_id] = {
            "state": "committed",
            "invocation_id": effect_context.invocation_id,
            "effect_id": effect_context.effect_id,
            "idempotency_key": effect_context.idempotency_key,
            "attempt": effect_context.attempt,
            "result_digest": "oracle-digest",
        }
        if MODE == "crash":
            raise SystemExit("crash after plugin receipt")
        if MODE == "failure":
            RECORDS.pop(effect_context.invocation_id)
            raise RuntimeError("ordinary transaction failure")

    receipt = await effects.run("emotion.state", transaction)
    frame.slots["effect_receipt"] = receipt.result_digest
    return frame
'''


async def _manager(tmp_path: Path) -> PluginManager:
    plugin_dir = tmp_path / "plugins" / "effect_oracle"
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "plugin.py").write_text(_PLUGIN_SOURCE, encoding="utf-8")
    manager = PluginManager(
        plugin_dirs=[tmp_path / "plugins"],
        event_bus=EventBus(),
        tool_registry=None,
        workspace=tmp_path / "workspace",
        installed_cache_root=tmp_path / "cache",
    )
    manager.bind_activity_host(ActivityHost((ProactiveActivityAdapter(),)))
    await manager.load_all()
    return manager


async def _run_manager_tick(
    manager: PluginManager,
    frame: ProactiveFrame,
) -> ProactiveFrame:
    snapshot = manager.current_snapshot
    activity = manager.activity_host
    if snapshot is None or activity is None:
        raise AssertionError("manager did not publish a stable snapshot/activity")
    lease = await manager.snapshot_store.acquire(snapshot.snapshot_id)
    admission = activity.acquire(lease)
    bridge = CommittedProactiveBridge(activity)
    token = bridge.bind_execution(lease)
    try:
        runtime = bridge.runtime_for(snapshot)
        modules = bridge.lifecycle_modules(
            runtime,
            lifecycle_id="default.proactive.frame.v1",
        )
        if len(modules) != 1:
            raise AssertionError(f"unexpected module count: {len(modules)}")
        return await modules[0].run(frame)
    finally:
        bridge.reset_execution(token)
        await admission.release()
        await lease.release()


def _plugin_module(manager: PluginManager):
    generation = manager.generation("effect_oracle")
    if generation is None:
        raise AssertionError("effect_oracle generation missing")
    return generation.instance.module


@pytest.mark.asyncio
async def test_manager_activityhost_tick_success_and_crash_reentry(
    tmp_path: Path,
) -> None:
    manager = await _manager(tmp_path)
    try:
        module = _plugin_module(manager)
        success_frame = new_proactive_frame("telegram:oracle")
        first_success = await _run_manager_tick(manager, success_frame)
        assert first_success.slots["effect_receipt"] == "oracle-digest"
        assert module.TRANSACTION_CALLS == 1

        module.MODE = "crash"
        frame = new_proactive_frame("telegram:oracle")

        first = await _run_manager_tick(manager, frame)
        second = await _run_manager_tick(manager, frame)

        assert first.slots["effect_receipt"] == "oracle-digest"
        assert second.slots["effect_receipt"] == "oracle-digest"
        assert module.TRANSACTION_CALLS == 2
        assert len(module.RECORDS) == 2
        assert module.LAST_EFFECT.closed
        assert manager.activity_host is not None
        assert manager.activity_host.active is not None
        assert manager.activity_host.active.in_flight == 0
    finally:
        await manager.terminate_all()


@pytest.mark.asyncio
async def test_manager_activityhost_tick_failure_and_cancel_cleanup(
    tmp_path: Path,
) -> None:
    manager = await _manager(tmp_path)
    try:
        module = _plugin_module(manager)
        module.MODE = "failure"
        with pytest.raises(RuntimeError, match="ordinary transaction failure"):
            await _run_manager_tick(manager, new_proactive_frame("telegram:oracle"))
        assert module.RECORDS == {}
        assert module.LAST_EFFECT.closed

        module.MODE = "cancel"
        task = asyncio.create_task(
            _run_manager_tick(manager, new_proactive_frame("telegram:oracle"))
        )
        await module.STARTED.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert module.LAST_EFFECT.closed
        assert manager.activity_host is not None
        assert manager.activity_host.active is not None
        assert manager.activity_host.active.in_flight == 0
    finally:
        await manager.terminate_all()


def test_domain_effect_reentry_after_core_process_crash_is_idempotent(
    tmp_path: Path,
) -> None:
    receipt_path = tmp_path / "plugin-receipt.json"
    child = """
import json
import os
import sys
from pathlib import Path

path = Path(sys.argv[1])
record = {
    "state": "committed",
    "invocation_id": "proactive:effect_oracle:tick-1",
    "effect_id": "emotion.state",
    "idempotency_key": "tick-1:effect_oracle:proactive.effect_oracle",
    "attempt": 1,
    "result_digest": "after-crash",
}
path.write_text(json.dumps(record), encoding="utf-8")
os._exit(137)
"""
    env = os.environ.copy()
    project_root = str(Path(__file__).parents[1])
    env["PYTHONPATH"] = project_root + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run(
        [sys.executable, "-c", child, str(receipt_path)],
        env=env,
        check=False,
    )
    assert result.returncode == 137

    context = DomainEffectContext(
        invocation_id="proactive:effect_oracle:tick-1",
        plugin_id="effect_oracle",
        job_name="proactive.effect_oracle",
        semantic_job_id="effect_oracle:proactive.effect_oracle",
        event_id="tick-1",
        snapshot_id="snapshot-1",
        effect_id="emotion.state",
        idempotency_key="tick-1:effect_oracle:proactive.effect_oracle",
        attempt=1,
        generation_id="effect_oracle:generation-1",
        tick_id="tick-1",
    )

    def lookup(_context):
        return json.loads(receipt_path.read_text(encoding="utf-8"))

    transaction_calls = 0

    async def reentry() -> None:
        nonlocal transaction_calls
        effects = ProactiveDomainEffects(context=context, lookup=lookup)

        async def transaction(_effect_context):
            nonlocal transaction_calls
            transaction_calls += 1

        receipt = await effects.run("emotion.state", transaction)
        assert receipt.result_digest == "after-crash"
        effects.close()

    asyncio.run(reentry())
    assert transaction_calls == 0
