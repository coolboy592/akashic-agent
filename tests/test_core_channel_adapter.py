from __future__ import annotations

import pytest
from types import SimpleNamespace

from agent.plugin_composition.channels import (
    ChannelReady,
    ChannelCapability,
    CommittedChannelCatalog,
    CoreChannelDefinition,
    DeliveryStatus as ChannelDeliveryState,
    OutboundEnvelope,
    ProviderDeliveryRequest,
)
from bootstrap.core_channel_adapter import (
    CoreLegacyChannelAdapter,
    LEGACY_ATTACHMENT_METADATA_KEY,
    map_legacy_delivery_receipt,
)
from agent.plugins.channel_generation_host import ChannelGenerationHost
from bus.events import (
    AttachmentKind,
    ChannelAttachment,
    DeliveryReceipt,
    DeliveryStatus,
)


class _ExistingChannel:
    name = "web"

    def __init__(self) -> None:
        self.received = []

    async def _deliver_message(self, message):
        self.received.append(message)
        return DeliveryReceipt(
            DeliveryStatus.SUCCESS,
            canonical_media=tuple(item.source for item in message.attachments),
        )


class _ProviderFactory:
    async def create(self, credentials):
        raise AssertionError("Core adapter must not resolve credentials")

    async def aclose(self):
        return None


class _SnapshotLease:
    def __init__(self, snapshot) -> None:
        self.snapshot = snapshot
        self.active = True

    def fork(self):
        return _SnapshotLease(self.snapshot)

    async def release(self):
        self.active = False


@pytest.mark.asyncio
async def test_legacy_adapter_preserves_existing_attachment_delivery() -> None:
    channel = _ExistingChannel()
    adapter = CoreLegacyChannelAdapter(channel, "binding-1")
    request = ProviderDeliveryRequest(
        binding_token="binding-1",
        delivery_id="delivery-1",
        recipient="chat-1",
        body="report",
        metadata={
            LEGACY_ATTACHMENT_METADATA_KEY: (
                {"kind": "file", "source": "/tmp/report.pdf", "filename": "report.pdf"},
            )
        },
    )

    assert await adapter.start() == ChannelReady("binding-1")
    receipt = await adapter.deliver(request)

    assert receipt.status is ChannelDeliveryState.DELIVERED
    assert channel.received[0].attachments == (
        ChannelAttachment(AttachmentKind.FILE, "/tmp/report.pdf", "report.pdf"),
    )
    assert (await adapter.stop()).resources_closed is True


@pytest.mark.parametrize(
    ("legacy_status", "detail", "expected"),
    (
        (DeliveryStatus.SUCCESS, None, ChannelDeliveryState.DELIVERED),
        (DeliveryStatus.PARTIAL, "one part committed", ChannelDeliveryState.UNKNOWN),
        (DeliveryStatus.FAILED, "provider timeout", ChannelDeliveryState.UNKNOWN),
        (DeliveryStatus.FAILED, "pre-effect:no provider call", ChannelDeliveryState.REJECTED),
    ),
)
def test_telegram_qq_legacy_receipt_mapping_is_non_retryable(
    legacy_status: DeliveryStatus,
    detail: str | None,
    expected: ChannelDeliveryState,
) -> None:
    receipt = map_legacy_delivery_receipt(
        "delivery-1",
        DeliveryReceipt(legacy_status, detail=detail),
    )

    assert receipt.delivery_id == "delivery-1"
    assert receipt.status is expected


@pytest.mark.asyncio
async def test_core_catalog_host_start_exact_binding_attachment_and_stop() -> None:
    channel = _ExistingChannel()

    def factory(context):
        return CoreLegacyChannelAdapter(channel, context.binding_token)

    definition = CoreChannelDefinition(
        name="web",
        capabilities=frozenset({ChannelCapability.OUTBOUND}),
        factory=factory,
        inbound_identity=None,
        source_revision="core-test-source",
        config_revision="core-test-config",
        generation_id="core-test-generation",
    )
    root = object()
    catalog = CommittedChannelCatalog(
        core_definitions=(definition,),
        root_instance_token=root,
    )
    snapshot = SimpleNamespace(
        snapshot_id="core-snapshot",
        state="committed",
        composition_root=SimpleNamespace(instance_token=root),
        channel_registry=None,
        channel_registry_identity=None,
        channel_catalog=catalog,
        generations={},
    )
    async def noop(*_args):
        return None

    host = ChannelGenerationHost(
        on_before_start=noop,
        config_revision_checker=noop,
        on_failure=noop,
    )
    generation = await host.start_formal(snapshot, {"web": _ProviderFactory()})
    generation.open_admission()
    lease = host.acquire_binding(_SnapshotLease(snapshot), "web")
    envelope = OutboundEnvelope(
        logical_delivery_id="delivery-1",
        delivery_id="delivery-1",
        attempt_sequence=1,
        snapshot_id=lease.snapshot_id,
        generation_id=lease.generation_id,
        binding_token=lease.binding_token,
        channel="web",
        recipient="chat-1",
        body="report",
        metadata={
            LEGACY_ATTACHMENT_METADATA_KEY: (
                {"kind": "file", "source": "/tmp/report.pdf", "filename": "report.pdf"},
            )
        },
    )

    receipt = await host.dispatch_outbound(envelope, lease)

    assert receipt.status is ChannelDeliveryState.DELIVERED
    assert channel.received[0].attachments[0].source == "/tmp/report.pdf"
    await lease.aclose()
    stop_receipts = await generation.stop()
    assert stop_receipts[0].resources_closed is True
