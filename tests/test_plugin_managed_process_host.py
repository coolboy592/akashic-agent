from __future__ import annotations

import asyncio
import socket
import sys
from pathlib import Path
from urllib.request import urlopen

import pytest

from agent.plugin_composition import ManagedProcessDefinition
from agent.plugins.managed_process_host import ManagedProcessGenerationHost
from utils.process_group import OwnedProcessGroup


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _http_definition(script: Path, *, formal_port: int) -> ManagedProcessDefinition:
    return ManagedProcessDefinition(
        name="calendar_api",
        command=(sys.executable, str(script)),
        cwd=str(script.parent),
        env={},
        port_env="PORT",
        formal_port=formal_port,
        readiness_path="/health",
        startup_timeout_seconds=3.0,
    )


def _write_http_server(script: Path, *, exit_first: bool = False) -> None:
    first_exit = (
        "from pathlib import Path\n"
        "counter = Path('attempts')\n"
        "attempt = int(counter.read_text()) + 1 if counter.exists() else 1\n"
        "counter.write_text(str(attempt))\n"
        "if attempt == 1:\n"
        "    import threading\n"
        "    threading.Thread(target=server.serve_forever, daemon=True).start()\n"
        "    time.sleep(0.15)\n"
        "    raise SystemExit(17)\n"
        if exit_first
        else ""
    )
    script.write_text(
        "import os, sys, time\n"
        "from http.server import BaseHTTPRequestHandler, HTTPServer\n"
        "class Handler(BaseHTTPRequestHandler):\n"
        "    def do_GET(self):\n"
        "        self.send_response(200); self.end_headers(); self.wfile.write(b'ready')\n"
        "    def log_message(self, *args): pass\n"
        "server = HTTPServer(('127.0.0.1', int(os.environ['PORT'])), Handler)\n"
        "print('managed stdout', flush=True)\n"
        "print('managed stderr', file=sys.stderr, flush=True)\n"
        + first_exit
        + "server.serve_forever()\n",
        encoding="utf-8",
    )


async def _wait_until(predicate, *, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition did not become true before timeout")
        await asyncio.sleep(0.02)


@pytest.mark.asyncio
async def test_candidate_uses_temporary_port_and_bounded_logs(tmp_path: Path) -> None:
    script = tmp_path / "server.py"
    _write_http_server(script)
    health: list[tuple[str, str, bool, str]] = []
    incidents: list[tuple[str, str, str, str]] = []
    host = ManagedProcessGenerationHost(
        on_health=lambda *value: health.append(value),
        on_incident=lambda *value: incidents.append(value),
        log_max_bytes=64,
        log_max_lines=4,
    )
    formal_port = _free_port()
    generation = await host.start_candidate(
        "candidate-1",
        {"calendar_api": _http_definition(script, formal_port=formal_port)},
    )

    endpoint = generation.endpoint("calendar_api")
    assert endpoint.mode == "candidate"
    assert endpoint.port != formal_port
    with urlopen(endpoint.readiness_url, timeout=1) as response:
        assert response.read() == b"ready"
    await asyncio.sleep(0.05)
    logs = generation.logs("calendar_api")
    assert any("managed stdout" in line for line in logs.stdout)
    assert any("managed stderr" in line for line in logs.stderr)
    assert host.health("candidate-1", "calendar_api")
    assert ("candidate-1", "calendar_api", True, "ready") in health
    assert incidents == []

    await host.stop_generation("candidate-1")
    assert host.get("candidate-1") is None
    with pytest.raises(OSError):
        urlopen(endpoint.readiness_url, timeout=0.2)


@pytest.mark.asyncio
async def test_formal_fixed_port_and_candidate_are_isolated(tmp_path: Path) -> None:
    script = tmp_path / "server.py"
    _write_http_server(script)
    host = ManagedProcessGenerationHost()
    formal_port = _free_port()
    definition = _http_definition(script, formal_port=formal_port)

    formal = await host.start_formal("formal-1", {definition.name: definition})
    candidate = await host.start_candidate("candidate-1", {definition.name: definition})
    assert formal.endpoint("calendar_api").port == formal_port
    assert candidate.endpoint("calendar_api").port != formal_port

    await host.stop_generation("candidate-1")
    with urlopen(formal.endpoint("calendar_api").readiness_url, timeout=1) as response:
        assert response.status == 200
    await host.stop_generation("formal-1")


@pytest.mark.asyncio
async def test_process_exit_recovers_with_new_epoch_without_stale_resurrection(
    tmp_path: Path,
) -> None:
    script = tmp_path / "recover.py"
    _write_http_server(script, exit_first=True)
    incidents: list[tuple[str, str, str, str]] = []
    host = ManagedProcessGenerationHost(
        on_incident=lambda *value: incidents.append(value),
        recovery_backoff_seconds=(0.01, 0.01),
        recovery_stable_seconds=60,
    )
    generation = await host.start_candidate(
        "candidate-recover",
        {"calendar_api": _http_definition(script, formal_port=_free_port())},
    )
    initial = generation.endpoint("calendar_api")

    def recovered() -> bool:
        try:
            return generation.endpoint("calendar_api").epoch > initial.epoch
        except RuntimeError:
            return False

    await _wait_until(
        recovered,
    )
    recovered = generation.endpoint("calendar_api")
    assert recovered.epoch > initial.epoch
    assert recovered.port != initial.port
    assert any(item[2] == "process_exit" for item in incidents)
    assert host.health("candidate-recover", "calendar_api")

    await host.stop_generation("candidate-recover")
    await asyncio.sleep(0.05)
    assert host.get("candidate-recover") is None


@pytest.mark.asyncio
async def test_cleanup_failure_retains_tombstone_until_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    script = tmp_path / "server.py"
    _write_http_server(script)
    host = ManagedProcessGenerationHost()
    generation_id = "candidate-cleanup"
    await host.start_candidate(
        generation_id,
        {"calendar_api": _http_definition(script, formal_port=_free_port())},
    )
    original_terminate = OwnedProcessGroup.terminate
    calls = 0

    async def fail_once(self: OwnedProcessGroup, *, timeout_s: float) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("injected terminate failure")
        await original_terminate(self, timeout_s=timeout_s)

    monkeypatch.setattr(OwnedProcessGroup, "terminate", fail_once)
    with pytest.raises(RuntimeError, match="cleanup failed"):
        await host.stop_generation(generation_id)
    tombstone = host.tombstone(generation_id)
    assert tombstone is not None
    assert tombstone.action == "retry_generation_cleanup"
    assert host.get(generation_id) is not None

    monkeypatch.setattr(OwnedProcessGroup, "terminate", original_terminate)
    await host.retry_generation_cleanup(generation_id)
    assert host.tombstone(generation_id) is None
    assert host.get(generation_id) is None
