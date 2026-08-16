from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from types import ModuleType, SimpleNamespace
from typing import Any, cast

import pytest

from agent.plugin_composition.channels import (
    ChannelCapability,
    ChannelDeliveryReceipt,
    ChannelDefinition,
    ChannelFactoryFreezeInput,
    ChannelReady,
    ChannelInboundMessage,
    InboundIdentity,
    CredentialRef,
    DeliveryStatus,
    InboundEnvelope,
    InboundOwner,
    OutboundEnvelope,
    PluginChannels,
    ProviderClientFactory,
    ProviderDeliveryReceipt,
    ProviderDeliveryRequest,
    RawInbound,
    StopReceipt,
    _freeze_plugin_channels,
    channel_config_revision,
)
from agent.plugin_composition.model import CompositionError, ServiceKey
from agent.plugins.channel_generation_host import (
    ChannelGenerationHost,
    ChannelStartRecord,
)
from agent.plugins.composable import ComposablePlugin
from agent.plugins.generation import GateResult, PluginContributions, PluginGeneration
from bus.queue import MessageBus


@dataclass
class ClientFactory:
    created: int = 0
    closed: int = 0
    fail_close: bool = False

    async def create(self, credentials: Any) -> object:
        self.created += 1
        return object()

    async def aclose(self) -> None:
        self.closed += 1
        if self.fail_close:
            raise RuntimeError("factory close failed")


class Adapter:
    def __init__(
        self,
        context: Any,
        *,
        fail_start: bool = False,
        fail_stop: bool = False,
        block_stop: bool = False,
        wrong_receipt: bool = False,
        cancel_stop: bool = False,
        cancel_start: bool = False,
    ) -> None:
        self.context = context
        self.fail_start = fail_start
        self.fail_stop = fail_stop
        self.block_stop = block_stop
        self.wrong_receipt = wrong_receipt
        self.cancel_stop = cancel_stop
        self.cancel_start = cancel_start
        self.started = 0
        self.stopped = 0
        self.deliveries: list[str] = []
        self.release = asyncio.Event()
        self.stop_started = asyncio.Event()
        self.stop_release = asyncio.Event()

    async def start(self) -> ChannelReady:
        self.started += 1
        if self.cancel_start:
            raise asyncio.CancelledError
        if self.fail_start:
            raise RuntimeError("start failed")
        return ChannelReady(self.context.binding_token)

    async def deliver(self, request: ProviderDeliveryRequest) -> ProviderDeliveryReceipt:
        self.deliveries.append(request.delivery_id)
        if not self.release.is_set():
            await self.release.wait()
        delivery_id = "wrong" if self.wrong_receipt else request.delivery_id
        return ProviderDeliveryReceipt(delivery_id, DeliveryStatus.DELIVERED, ("p1",))

    async def stop(self) -> StopReceipt:
        self.stopped += 1
        self.stop_started.set()
        if self.block_stop:
            await self.stop_release.wait()
        if self.cancel_stop:
            raise asyncio.CancelledError
        if self.fail_stop:
            raise RuntimeError("stop failed")
        return StopReceipt(self.context.binding_token, True)


async def _noop_record(record: ChannelStartRecord) -> None:
    return None


async def _noop_failure(failure: Any) -> None:
    return None


def _host(**kwargs: Any) -> ChannelGenerationHost:
    kwargs.setdefault("on_before_start", _noop_record)
    kwargs.setdefault("config_revision_checker", _noop_record)
    kwargs.setdefault("on_failure", _noop_failure)
    return ChannelGenerationHost(**kwargs)


class _FakeSnapshotLease:
    def __init__(
        self,
        snapshot: Any,
        *,
        release_gate: asyncio.Event | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.active = True
        self.release_gate = release_gate
        self.forks: list[_FakeSnapshotLease] = []

    def fork(self) -> _FakeSnapshotLease:
        if not self.active:
            raise RuntimeError("lease closed")
        child = _FakeSnapshotLease(
            self.snapshot,
            release_gate=self.release_gate,
        )
        self.forks.append(child)
        return child

    async def release(self) -> None:
        if not self.active:
            return
        if self.release_gate is not None:
            await self.release_gate.wait()
        self.active = False


def _module(
    *,
    name: str = "feishu",
    factory_name: str = "make_adapter",
    adapter_cls: type[Adapter] = Adapter,
) -> ModuleType:
    module = ModuleType(f"plugins.{name}")
    module.api_version = 3  # type: ignore[attr-defined]
    module.name = name  # type: ignore[attr-defined]
    module.version = "1"  # type: ignore[attr-defined]
    module.inject = (ServiceKey("core.channels"),)  # type: ignore[attr-defined]
    async def apply(ctx: Any, config: Any) -> None:
        return None

    module.apply = apply  # type: ignore[attr-defined]
    setattr(module, factory_name, lambda context: adapter_cls(context))
    return module


async def _make_snapshot(
    *,
    module: ModuleType | None = None,
    adapter_cls: type[Adapter] = Adapter,
    fail_start: bool = False,
    fail_stop: bool = False,
    block_stop: bool = False,
    wrong_receipt: bool = False,
    cancel_stop: bool = False,
    cancel_start: bool = False,
    cancel_factory: bool = False,
    fail_after: int | None = None,
    factory_events: list[str] | None = None,
    capabilities: frozenset[ChannelCapability] = frozenset(
        {ChannelCapability.OUTBOUND}
    ),
) -> tuple[Any, dict[str, ClientFactory], dict[str, Adapter]]:
    module = module or _module(adapter_cls=adapter_cls)
    plugin = ComposablePlugin.from_module(module)
    root_token = object()
    channels = PluginChannels(root_token)
    from agent.plugin_composition.channels import CredentialRef

    class Fiber:
        activation_token = object()

    class Runtime:
        plugin_id = "plugin.feishu"
        config = {"app_secret": CredentialRef(("app_secret",))}

    class Context:
        fiber = Fiber()
        runtime = Runtime()
        generation_id = "gen-1"

        def report_incident(self, *args: Any) -> Any:
            return None

        def require(self, key: Any) -> Any:
            return channels

        def _root_instance_token(self) -> object:
            return root_token

        async def effect(self, setup: Any, label: str) -> Any:
            setup()
            return SimpleNamespace(aclose=lambda: None)

        async def health(self, *args: Any, **kwargs: Any) -> Any:
            return SimpleNamespace()

    channel_names = tuple(getattr(module, "channel_names", ("feishu",)))
    config_projection = {"app_secret": CredentialRef(("app_secret",))}
    for channel_name in channel_names:
        await channels.register(
            cast(Any, Context()),
            ChannelDefinition(
                name=channel_name,
                capabilities=capabilities,
                factory_export="make_adapter",
                inbound_identity=(
                    InboundIdentity.PROVIDER_MESSAGE_ID
                    if ChannelCapability.INBOUND in capabilities
                    else None
                ),
                credential_paths=("app_secret",),
            ),
        )
    registry = _freeze_plugin_channels(
        channels,
        root_token,
        factory_provenance_by_owner={
            "plugin.feishu": ChannelFactoryFreezeInput(
                "gen-1",
                "source-1",
                channel_config_revision(config_projection),
            )
        },
    )
    generation = PluginGeneration(
        plugin_id="plugin.feishu",
        generation_id="gen-1",
        module_path="plugins/feishu/plugin.py",
        source_revision="source-1",
        config_revision="raw-config-1",
        plugin_dir=__import__("pathlib").Path("/tmp/plugin"),
        data_dir=__import__("pathlib").Path("/tmp/plugin-data"),
        config={"app_secret": "raw-secret"},
        instance=plugin,
        scope=cast(Any, object()),
        contributions=PluginContributions(manifest={}),
        gate_result=GateResult("test", "plugin.feishu", "rev", "passed", ()),
        config_projection=config_projection,
    )
    snapshot = SimpleNamespace(
        snapshot_id="snapshot-1",
        state="committed",
        composition_root=SimpleNamespace(instance_token=root_token),
        channel_registry=registry,
        channel_registry_identity=registry.identity,
        generations={"plugin.feishu": generation},
    )
    adapters: dict[str, Adapter] = {}
    factory_count = 0

    def factory(context: Any) -> Adapter:
        nonlocal factory_count
        factory_count += 1
        if factory_events is not None:
            factory_events.append("factory")
        if cancel_factory:
            raise asyncio.CancelledError
        adapter = Adapter(
            context,
            fail_start=fail_start or (fail_after is not None and factory_count >= fail_after),
            fail_stop=fail_stop,
            block_stop=block_stop,
            wrong_receipt=wrong_receipt,
            cancel_stop=cancel_stop,
            cancel_start=cancel_start,
        )
        adapters[context.binding_token] = adapter
        return adapter

    setattr(module, "make_adapter", factory)
    return snapshot, {channel_name: ClientFactory() for channel_name in channel_names}, adapters


@pytest.mark.asyncio
async def test_formal_binding_starts_closed_and_delivers_after_open() -> None:
    snapshot, factories, adapters = await _make_snapshot()
    host = _host()
    generation = await host.start(snapshot, factories)
    binding = generation.channel("feishu")
    assert not binding.admission_open
    with pytest.raises(RuntimeError, match="关闭"):
        await binding.deliver(ProviderDeliveryRequest(binding.binding_token, "d1", "u", "hi"))
    binding.open_admission()
    request = ProviderDeliveryRequest(binding.binding_token, "d1", "u", "hi")
    task = asyncio.create_task(binding.deliver(request))
    await asyncio.sleep(0)
    assert binding.in_flight == 1
    binding.close_admission()
    assert binding.in_flight == 1
    next_request = ProviderDeliveryRequest(binding.binding_token, "d2", "u", "hi")
    with pytest.raises(RuntimeError, match="关闭"):
        await binding.deliver(next_request)
    for adapter in adapters.values():
        adapter.release.set()
    receipt = await task
    assert receipt.delivery_id == "d1"
    await generation.stop()
    assert factories["feishu"].closed == 1


@pytest.mark.asyncio
async def test_exact_binding_lease_blocks_stop_until_snapshot_fork_closes() -> None:
    snapshot, factories, _ = await _make_snapshot()
    host = _host()
    generation = await host.start(snapshot, factories)
    generation.open_admission()
    source = _FakeSnapshotLease(snapshot)

    owner = host.acquire_binding(cast(Any, source), "feishu")
    assert owner.snapshot_id == snapshot.snapshot_id
    assert owner.generation_id == "gen-1"
    assert owner.channel_name == "feishu"
    assert owner.active
    stop = asyncio.create_task(generation.stop())
    await asyncio.sleep(0)
    assert not stop.done()

    await owner.aclose()
    assert not owner.active
    assert source.active
    assert len(source.forks) == 1 and not source.forks[0].active
    await stop


@pytest.mark.asyncio
async def test_exact_binding_lease_dispatches_one_outbound_envelope() -> None:
    snapshot, factories, adapters = await _make_snapshot()
    host = _host()
    generation = await host.start(snapshot, factories)
    generation.open_admission()
    source = _FakeSnapshotLease(snapshot)
    owner = host.acquire_binding(cast(Any, source), "feishu")
    envelope = OutboundEnvelope(
        logical_delivery_id="d1",
        delivery_id="d1",
        attempt_sequence=1,
        snapshot_id=snapshot.snapshot_id,
        generation_id="gen-1",
        binding_token=owner.binding_token,
        channel="feishu",
        recipient="u",
        body="hi",
        metadata={},
    )
    for adapter in adapters.values():
        adapter.release.set()

    receipt = await host.dispatch_outbound(envelope, owner)

    assert receipt == ChannelDeliveryReceipt(
        "d1",
        DeliveryStatus.DELIVERED,
        ("p1",),
    )
    assert tuple(adapters.values())[0].deliveries == ["d1"]
    await owner.aclose()
    await generation.stop()


@pytest.mark.asyncio
async def test_outbound_dispatch_rejects_foreign_host_binding() -> None:
    snapshot, factories, _ = await _make_snapshot()
    host = _host()
    generation = await host.start(snapshot, factories)
    generation.open_admission()
    owner = host.acquire_binding(cast(Any, _FakeSnapshotLease(snapshot)), "feishu")
    envelope = OutboundEnvelope(
        logical_delivery_id="d1",
        delivery_id="d1",
        attempt_sequence=1,
        snapshot_id=snapshot.snapshot_id,
        generation_id="gen-1",
        binding_token=owner.binding_token,
        channel="feishu",
        recipient="u",
        body="hi",
        metadata={},
    )

    with pytest.raises(RuntimeError, match="不属于当前 Host"):
        await _host().dispatch_outbound(envelope, owner)

    await owner.aclose()
    await generation.stop()


@pytest.mark.asyncio
async def test_formal_ingress_acquires_exact_binding_and_deduplicates() -> None:
    snapshot, factories, adapters = await _make_snapshot(
        capabilities=frozenset(
            {ChannelCapability.INBOUND, ChannelCapability.OUTBOUND}
        )
    )
    sources: list[_FakeSnapshotLease] = []

    async def acquire() -> _FakeSnapshotLease:
        source = _FakeSnapshotLease(snapshot)
        sources.append(source)
        return source

    bus = MessageBus()
    host = _host(snapshot_lease_acquirer=acquire)
    host.bind_inbound_publisher(bus.publish_channel_inbound)
    generation = await host.start(snapshot, factories)
    generation.open_admission()
    adapter = tuple(adapters.values())[0]
    assert adapter.context.ingress is not None
    raw = RawInbound(
        message_id="provider-message-1",
        message=ChannelInboundMessage(
            channel="feishu",
            sender="user",
            chat_id="chat",
            content="hello",
            timestamp=datetime.now(timezone.utc),
            metadata={},
        ),
    )

    assert await adapter.context.ingress.admit(raw) is True
    assert await adapter.context.ingress.admit(raw) is False
    assert len(sources) == 1 and not sources[0].active
    assert len(sources[0].forks) == 1 and sources[0].forks[0].active
    envelope = await bus.consume_inbound()
    assert envelope.message_id == raw.message_id  # type: ignore[union-attr]
    await bus.release_channel_inbound(envelope, InboundOwner.LANE)  # type: ignore[arg-type]
    assert not sources[0].forks[0].active

    await generation.stop()


@pytest.mark.asyncio
async def test_outbound_only_binding_rejects_ingress_before_runtime_ports() -> None:
    snapshot, factories, adapters = await _make_snapshot()
    acquired = 0

    async def acquire() -> _FakeSnapshotLease:
        nonlocal acquired
        acquired += 1
        return _FakeSnapshotLease(snapshot)

    published: list[InboundEnvelope] = []

    async def publish(envelope: InboundEnvelope) -> None:
        published.append(envelope)

    host = _host(snapshot_lease_acquirer=acquire)
    host.bind_inbound_publisher(publish)
    generation = await host.start(snapshot, factories)
    generation.open_admission()
    raw = RawInbound(
        message_id="provider-message-outbound-only",
        message=ChannelInboundMessage(
            channel="feishu",
            sender="user",
            chat_id="chat",
            content="hello",
            timestamp=datetime.now(timezone.utc),
            metadata={},
        ),
    )

    assert tuple(adapters.values())[0].context.ingress is None

    assert acquired == 0
    assert published == []
    await generation.stop()


@pytest.mark.asyncio
async def test_formal_ingress_rejects_different_stable_snapshot_and_releases_claim() -> None:
    snapshot, factories, adapters = await _make_snapshot(
        capabilities=frozenset(
            {ChannelCapability.INBOUND, ChannelCapability.OUTBOUND}
        )
    )
    other_snapshot = SimpleNamespace(snapshot_id="other-snapshot", generations={})
    wrong = _FakeSnapshotLease(other_snapshot)
    right = _FakeSnapshotLease(snapshot)
    acquired = [wrong, right]

    async def acquire() -> _FakeSnapshotLease:
        return acquired.pop(0)

    bus = MessageBus()
    host = _host(snapshot_lease_acquirer=acquire)
    host.bind_inbound_publisher(bus.publish_channel_inbound)
    generation = await host.start(snapshot, factories)
    generation.open_admission()
    raw = RawInbound(
        message_id="provider-message-race",
        message=ChannelInboundMessage(
            channel="feishu",
            sender="user",
            chat_id="chat",
            content="hello",
            timestamp=datetime.now(timezone.utc),
            metadata={},
        ),
    )
    adapter = tuple(adapters.values())[0]

    with pytest.raises(RuntimeError, match="stable snapshot 不一致"):
        await adapter.context.ingress.admit(raw)

    assert not wrong.active
    assert await adapter.context.ingress.admit(raw) is True
    envelope = await bus.consume_inbound()
    assert isinstance(envelope, InboundEnvelope)
    await bus.release_channel_inbound(envelope, InboundOwner.LANE)
    await generation.stop()


@pytest.mark.asyncio
async def test_formal_ingress_scopes_dedupe_by_provider_identity_and_persists_mapping() -> None:
    snapshot, factories, adapters = await _make_snapshot(
        capabilities=frozenset(
            {ChannelCapability.INBOUND, ChannelCapability.OUTBOUND}
        )
    )
    mapping: dict[tuple[str, str], str] = {}

    async def acquire() -> _FakeSnapshotLease:
        return _FakeSnapshotLease(snapshot)

    async def remember(channel: str, identity: str, recipient: str) -> None:
        mapping[(channel, identity)] = recipient

    host = _host(
        snapshot_lease_acquirer=acquire,
        identity_resolver=lambda channel, identity: mapping.get((channel, identity)),
        identity_rememberer=remember,
    )
    bus = MessageBus()
    host.bind_inbound_publisher(bus.publish_channel_inbound)
    generation = await host.start(snapshot, factories)
    generation.open_admission()
    ingress = tuple(adapters.values())[0].context.ingress
    identity = tuple(adapters.values())[0].context.identity
    assert ingress is not None and identity is not None

    def raw(provider: str, recipient: str) -> RawInbound:
        return RawInbound(
            message_id="same-provider-message-id",
            provider_identity=provider,
            recipient=recipient,
            message=ChannelInboundMessage(
                channel="feishu",
                sender=provider,
                chat_id=recipient,
                content="hello",
                timestamp=datetime.now(timezone.utc),
                metadata={},
            ),
        )

    assert await ingress.admit(raw("open-a", "chat-a")) is True
    assert await ingress.admit(raw("open-b", "chat-b")) is True
    assert identity.resolve("open-a") == "chat-a"
    assert identity.resolve("open-b") == "chat-b"
    first = await bus.consume_inbound()
    second = await bus.consume_inbound()
    assert isinstance(first, InboundEnvelope)
    assert isinstance(second, InboundEnvelope)
    await bus.release_channel_inbound(first, InboundOwner.LANE)
    await bus.release_channel_inbound(second, InboundOwner.LANE)
    await generation.stop()


@pytest.mark.asyncio
async def test_identity_write_failure_releases_dedupe_claim_before_snapshot_acquire() -> None:
    snapshot, factories, adapters = await _make_snapshot(
        capabilities=frozenset(
            {ChannelCapability.INBOUND, ChannelCapability.OUTBOUND}
        )
    )
    acquire_calls = 0
    fail = True

    async def acquire() -> _FakeSnapshotLease:
        nonlocal acquire_calls
        acquire_calls += 1
        return _FakeSnapshotLease(snapshot)

    async def remember(_channel: str, _identity: str, _recipient: str) -> None:
        nonlocal fail
        if fail:
            fail = False
            raise OSError("identity store unavailable")

    host = _host(
        snapshot_lease_acquirer=acquire,
        identity_resolver=lambda _channel, _identity: None,
        identity_rememberer=remember,
    )
    bus = MessageBus()
    host.bind_inbound_publisher(bus.publish_channel_inbound)
    generation = await host.start(snapshot, factories)
    generation.open_admission()
    ingress = tuple(adapters.values())[0].context.ingress
    assert ingress is not None
    raw = RawInbound(
        message_id="identity-retry",
        provider_identity="open-id",
        recipient="chat-id",
        message=ChannelInboundMessage(
            channel="feishu",
            sender="open-id",
            chat_id="chat-id",
            content="hello",
            timestamp=datetime.now(timezone.utc),
            metadata={},
        ),
    )

    with pytest.raises(OSError, match="identity store unavailable"):
        await ingress.admit(raw)

    assert acquire_calls == 1
    assert await ingress.admit(raw) is True
    assert acquire_calls == 2
    envelope = await bus.consume_inbound()
    assert isinstance(envelope, InboundEnvelope)
    await bus.release_channel_inbound(envelope, InboundOwner.LANE)
    await generation.stop()


@pytest.mark.asyncio
async def test_identity_write_is_owned_by_binding_drain_during_publication() -> None:
    snapshot, factories, adapters = await _make_snapshot(
        capabilities=frozenset(
            {ChannelCapability.INBOUND, ChannelCapability.OUTBOUND}
        )
    )
    remember_started = asyncio.Event()
    remember_release = asyncio.Event()
    mapping: dict[str, str] = {}

    async def acquire() -> _FakeSnapshotLease:
        return _FakeSnapshotLease(snapshot)

    async def remember(_channel: str, identity: str, recipient: str) -> None:
        remember_started.set()
        await remember_release.wait()
        mapping[identity] = recipient

    host = _host(
        snapshot_lease_acquirer=acquire,
        identity_resolver=lambda _channel, identity: mapping.get(identity),
        identity_rememberer=remember,
    )
    bus = MessageBus()
    host.bind_inbound_publisher(bus.publish_channel_inbound)
    generation = await host.start(snapshot, factories)
    generation.open_admission()
    ingress = tuple(adapters.values())[0].context.ingress
    assert ingress is not None
    raw = RawInbound(
        message_id="publication-race",
        provider_identity="open-id",
        recipient="chat-id",
        message=ChannelInboundMessage(
            channel="feishu",
            sender="open-id",
            chat_id="chat-id",
            content="hello",
            timestamp=datetime.now(timezone.utc),
            metadata={},
        ),
    )

    admission = asyncio.create_task(ingress.admit(raw))
    await remember_started.wait()
    stop = asyncio.create_task(generation.stop())
    await asyncio.sleep(0)
    assert not stop.done()

    remember_release.set()
    assert await admission is True
    assert mapping == {"open-id": "chat-id"}
    envelope = await bus.consume_inbound()
    assert isinstance(envelope, InboundEnvelope)
    await bus.release_channel_inbound(envelope, InboundOwner.LANE)
    await stop


@pytest.mark.asyncio
async def test_binding_lease_cancel_waits_for_exact_snapshot_release() -> None:
    snapshot, factories, _ = await _make_snapshot()
    host = _host()
    generation = await host.start(snapshot, factories)
    generation.open_admission()
    release_gate = asyncio.Event()
    source = _FakeSnapshotLease(snapshot, release_gate=release_gate)
    owner = host.acquire_binding(cast(Any, source), "feishu")

    closing = asyncio.create_task(owner.aclose())
    await asyncio.sleep(0)
    closing.cancel()
    await asyncio.sleep(0)
    assert not closing.done()
    release_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert not owner.active
    assert len(source.forks) == 1 and not source.forks[0].active
    await generation.stop()


@pytest.mark.asyncio
async def test_wrong_binding_and_receipt_identity_fail_loud() -> None:
    snapshot, factories, _ = await _make_snapshot()
    host = _host()
    generation = await host.start(snapshot, factories)
    binding = generation.channel("feishu")
    binding.open_admission()
    with pytest.raises(RuntimeError, match="binding token"):
        await binding.deliver(ProviderDeliveryRequest("wrong", "d1", "u", "hi"))
    await generation.stop()

    snapshot, factories, adapters = await _make_snapshot(wrong_receipt=True)
    host = _host()
    generation = await host.start(snapshot, factories)
    binding = generation.channel("feishu")
    binding.open_admission()
    for adapter in adapters.values():
        adapter.release.set()
    with pytest.raises(RuntimeError, match="receipt identity"):
        await binding.deliver(ProviderDeliveryRequest(binding.binding_token, "d1", "u", "hi"))
    await generation.stop()


@pytest.mark.asyncio
async def test_journal_callback_happens_before_start_and_failure_keeps_count_zero() -> None:
    events: list[str] = []
    records: list[ChannelStartRecord] = []
    snapshot, factories, _ = await _make_snapshot(factory_events=events)

    async def before(record: ChannelStartRecord) -> None:
        records.append(record)
        events.append("journal")

    async def check(record: ChannelStartRecord) -> None:
        events.append("config-check")

    host = _host(on_before_start=before, config_revision_checker=check)
    generation = await host.start(snapshot, factories)
    assert events == ["journal", "config-check", "factory"]
    assert records[0].source_revision == "source-1"
    assert records[0].config_revision == channel_config_revision(
        {"app_secret": CredentialRef(("app_secret",))}
    )
    assert records[0].raw_config_revision == "raw-config-1"
    assert len(records[0].descriptor_digest) == 64
    assert records[0].factory_export == "make_adapter"
    assert records[0].artifact_pointer == "/tmp/plugin"
    assert records[0].target == "formal"
    assert records[0].boot_owner == "plugin-manager"
    assert host.start_count(snapshot.snapshot_id, "feishu") == 1
    await generation.stop()

    async def fail_before(record: ChannelStartRecord) -> None:
        raise RuntimeError("journal failed")

    events = []
    snapshot, _, _ = await _make_snapshot(factory_events=events)
    host = _host(on_before_start=fail_before)
    with pytest.raises(RuntimeError, match="journal failed"):
        await host.start(snapshot, {"feishu": ClientFactory()})
    assert host.start_count(snapshot.snapshot_id, "feishu") == 0
    assert events == []


def test_durable_callbacks_are_mandatory() -> None:
    with pytest.raises(TypeError):
        ChannelGenerationHost(
            on_before_start=None,  # type: ignore[arg-type]
            config_revision_checker=_noop_record,
        )
    with pytest.raises(TypeError):
        ChannelGenerationHost(
            on_before_start=_noop_record,
            config_revision_checker=None,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_config_revision_checker_failure_is_before_factory_and_start() -> None:
    events: list[str] = []
    snapshot, factories, _ = await _make_snapshot(factory_events=events)

    async def check(record: ChannelStartRecord) -> None:
        raise RuntimeError("config revision drift")

    host = _host(config_revision_checker=check)
    with pytest.raises(RuntimeError, match="config revision drift"):
        await host.start(snapshot, factories)
    assert events == []
    assert factories["feishu"].closed == 1
    assert host.start_count(snapshot.snapshot_id, "feishu") == 0


@pytest.mark.asyncio
async def test_empty_registry_is_repeatable_noop_without_lock_or_fiber_owner() -> None:
    snapshot, _, _ = await _make_snapshot()
    root_token = snapshot.composition_root.instance_token
    empty_channels = PluginChannels(root_token)
    empty_registry = _freeze_plugin_channels(empty_channels, root_token)
    snapshot.channel_registry = empty_registry
    snapshot.channel_registry_identity = empty_registry.identity
    snapshot.generations = {}
    host = _host()
    first = await host.start(snapshot, {})
    second = await host.start(snapshot, {})
    assert first.snapshot_id == second.snapshot_id == snapshot.snapshot_id
    assert await first.stop() == ()
    assert await second.stop() == ()
    assert host._locks == {}
    assert not hasattr(host, "fiber")
    assert not hasattr(host, "context")


@pytest.mark.asyncio
async def test_partial_start_rolls_back_started_adapter_and_provider_factory() -> None:
    module = _module()
    module.channel_names = ("feishu", "qqbot")  # type: ignore[attr-defined]
    snapshot, failing_factories, adapters = await _make_snapshot(
        module=module,
        fail_after=2,
    )
    host = _host()
    with pytest.raises(RuntimeError, match="start failed"):
        await host.start(snapshot, failing_factories)
    assert all(factory.closed == 1 for factory in failing_factories.values())
    assert len(adapters) == 2
    assert sum(adapter.stopped for adapter in adapters.values()) == 2
    assert host.failure(snapshot.snapshot_id) is None


@pytest.mark.asyncio
async def test_stop_failure_retains_tombstone_and_retry_cleans_exact_owner() -> None:
    snapshot, factories, adapters = await _make_snapshot(fail_stop=True)
    host = _host()
    generation = await host.start(snapshot, factories)
    with pytest.raises(RuntimeError, match="cleanup"):
        await generation.stop()
    tombstone = host.failure(snapshot.snapshot_id, "feishu")
    assert tombstone is not None
    assert tombstone.binding_token == generation.channel("feishu").binding_token
    assert tombstone.artifact_pointer == "/tmp/plugin"
    assert tombstone.factory_export == "make_adapter"
    assert tombstone.source_revision == "source-1"
    assert tombstone.config_revision == channel_config_revision(
        {"app_secret": CredentialRef(("app_secret",))}
    )
    assert tombstone.raw_config_revision == "raw-config-1"
    assert len(tombstone.descriptor_digest) == 64
    assert tombstone.target == "formal"
    assert tombstone.boot_owner == "plugin-manager"
    assert tombstone.adapter_stop_settled is True
    assert tombstone.adapter_stop_succeeded is False
    assert tombstone.factory_close_settled is True
    assert tombstone.factory_close_succeeded is True
    with pytest.raises(RuntimeError, match="未知"):
        await host.retry_generation_cleanup("wrong-binding-token")
    adapter = next(iter(adapters.values()))
    adapter.fail_stop = False
    await host.retry_generation_cleanup(tombstone.binding_token)
    assert adapter.stopped == 2
    assert factories["feishu"].closed == 1
    assert host.failure(snapshot.snapshot_id) is None


@pytest.mark.asyncio
async def test_retry_skips_successful_adapter_stop_when_factory_close_failed() -> None:
    snapshot, factories, adapters = await _make_snapshot()
    factories["feishu"].fail_close = True
    host = _host()
    generation = await host.start(snapshot, factories)
    with pytest.raises(RuntimeError, match="cleanup"):
        await generation.stop()
    adapter = next(iter(adapters.values()))
    assert adapter.stopped == 1
    assert factories["feishu"].closed == 1
    tombstone = host.failure(snapshot.snapshot_id, "feishu")
    assert tombstone is not None
    assert tombstone.adapter_stop_succeeded is True
    assert tombstone.factory_close_settled is True
    assert tombstone.factory_close_succeeded is False
    factories["feishu"].fail_close = False
    await host.retry_generation_cleanup(tombstone.binding_token)
    assert adapter.stopped == 1
    assert factories["feishu"].closed == 2


@pytest.mark.asyncio
async def test_provider_cancel_and_failure_callback_cancel_retain_tombstone() -> None:
    snapshot, factories, _ = await _make_snapshot(cancel_stop=True)

    async def on_failure(record: Any) -> None:
        raise asyncio.CancelledError

    host = _host(on_failure=on_failure)
    generation = await host.start(snapshot, factories)
    with pytest.raises(asyncio.CancelledError):
        await generation.stop()
    assert host.failure(snapshot.snapshot_id, "feishu") is not None


@pytest.mark.asyncio
async def test_failure_callback_error_is_not_logged_as_success() -> None:
    snapshot, factories, _ = await _make_snapshot(fail_stop=True)

    async def on_failure(record: Any) -> None:
        raise RuntimeError("journal unavailable")

    host = _host(on_failure=on_failure)
    generation = await host.start(snapshot, factories)
    with pytest.raises(RuntimeError, match="journal unavailable"):
        await generation.stop()
    assert host.failure(snapshot.snapshot_id, "feishu") is not None


@pytest.mark.asyncio
async def test_factory_and_adapter_start_cancellation_keep_exact_tombstones() -> None:
    snapshot, factories, _ = await _make_snapshot(cancel_factory=True)
    host = _host()
    with pytest.raises(asyncio.CancelledError):
        await host.start(snapshot, factories)
    factory_failure = host.failure(snapshot.snapshot_id, "feishu")
    assert factory_failure is not None
    assert factory_failure.binding_token
    assert factories["feishu"].closed == 1
    await host.retry_generation_cleanup(factory_failure.binding_token)
    assert host.failure(snapshot.snapshot_id) is None

    snapshot, factories, adapters = await _make_snapshot(cancel_start=True)
    host = _host()
    with pytest.raises(asyncio.CancelledError):
        await host.start(snapshot, factories)
    adapter_failure = host.failure(snapshot.snapshot_id, "feishu")
    assert adapter_failure is not None
    assert adapter_failure.adapter is next(iter(adapters.values()))
    assert factories["feishu"].closed == 1
    await host.retry_generation_cleanup(adapter_failure.binding_token)
    assert host.failure(snapshot.snapshot_id) is None


@pytest.mark.asyncio
async def test_async_factory_and_noncallable_factory_are_rejected_before_start() -> None:
    snapshot, factories, _ = await _make_snapshot()

    async def async_factory(context: Any) -> Adapter:
        return Adapter(context)

    setattr(snapshot.generations["plugin.feishu"].instance.module, "make_adapter", async_factory)
    with pytest.raises(TypeError, match="async"):
        await _host().start(snapshot, factories)
    assert factories["feishu"].closed == 1

    snapshot, factories, _ = await _make_snapshot()
    setattr(snapshot.generations["plugin.feishu"].instance.module, "make_adapter", None)
    with pytest.raises(TypeError, match="不可调用"):
        await _host().start(snapshot, factories)
    assert factories["feishu"].closed == 1


@pytest.mark.asyncio
async def test_exact_root_and_factory_provenance_are_required() -> None:
    snapshot, factories, _ = await _make_snapshot()
    snapshot.composition_root = SimpleNamespace(instance_token=object())
    with pytest.raises(RuntimeError, match="exact composition Root"):
        await _host().start(snapshot, factories)

    snapshot, factories, _ = await _make_snapshot()
    object.__setattr__(snapshot.channel_registry.factories[0], "config_revision", "drift")
    with pytest.raises(RuntimeError):
        await _host().start(snapshot, factories)


@pytest.mark.asyncio
async def test_caller_cancellation_waits_for_cleanup() -> None:
    snapshot, factories, adapters = await _make_snapshot(block_stop=True)
    host = _host()
    generation = await host.start(snapshot, factories)
    stop_task = asyncio.create_task(generation.stop())
    adapter = next(iter(adapters.values()))
    await adapter.stop_started.wait()
    stop_task.cancel()
    stop_task.cancel()
    adapter.stop_release.set()
    with pytest.raises(asyncio.CancelledError):
        await stop_task
    assert factories["feishu"].closed == 1
    assert host.failure(snapshot.snapshot_id) is None
