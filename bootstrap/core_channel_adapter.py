from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

from agent.plugin_composition.channels import (
    ChannelAdapter,
    ChannelCapability,
    ChannelFactoryContext,
    ChannelReady,
    CoreChannelDefinition,
    DeliveryStatus as ChannelDeliveryStatus,
    ProviderDeliveryReceipt,
    ProviderDeliveryRequest,
    StopReceipt,
)
from bus.events import (
    AttachmentKind,
    ChannelAttachment,
    ChannelMessage,
    DeliveryReceipt,
    DeliveryStatus,
)


LEGACY_ATTACHMENT_METADATA_KEY = "_core_legacy_attachments"


class CoreLegacyChannelAdapter:
    """Adapt one already-started Core channel to the committed v3 receipt ABI."""

    def __init__(self, channel: object, binding_token: str) -> None:
        self._channel = channel
        self._binding_token = binding_token
        deliver = getattr(channel, "_deliver_message", None)
        if not callable(deliver):
            raise TypeError("Core channel 缺少 _deliver_message(ChannelMessage)")
        self._deliver_legacy = cast(Any, deliver)

    async def start(self) -> ChannelReady:
        return ChannelReady(self._binding_token)

    async def deliver(self, request: ProviderDeliveryRequest) -> ProviderDeliveryReceipt:
        """Invoke the existing channel delivery path and map its settled receipt."""

        # 1. Reconstruct the legacy message only inside this migration adapter.
        metadata = dict(request_metadata(request))
        attachments = _decode_legacy_attachments(metadata.pop(LEGACY_ATTACHMENT_METADATA_KEY, ()))
        message = ChannelMessage(
            channel=str(getattr(self._channel, "name")),
            chat_id=request.recipient,
            content=request.body,
            attachments=attachments,
            metadata=metadata,
        )

        # 2. The old channel owns provider effects; only its settled receipt crosses v3.
        receipt = await self._deliver_legacy(message)
        if not isinstance(receipt, DeliveryReceipt):
            raise TypeError("Core channel _deliver_message 必须返回 DeliveryReceipt")
        return map_legacy_delivery_receipt(request.delivery_id, receipt)

    async def stop(self) -> StopReceipt:
        # The old ChannelHost remains the lifecycle owner during migration.
        return StopReceipt(self._binding_token, resources_closed=True)


def build_core_channel_definition(channel: object) -> CoreChannelDefinition:
    """Project one existing Core channel into a stable committed definition."""

    name = getattr(channel, "name", None)
    if not isinstance(name, str) or not name:
        raise ValueError("Core channel name 无效")

    def factory(context: ChannelFactoryContext) -> ChannelAdapter:
        return CoreLegacyChannelAdapter(channel, context.binding_token)

    return CoreChannelDefinition(
        name=name,
        capabilities=frozenset({ChannelCapability.OUTBOUND}),
        factory=factory,
        inbound_identity=None,
        source_revision="core-builtins-v1",
        config_revision="core-builtins-v1",
        generation_id="core-builtins-v1",
        config={"channel": name},
    )


def map_legacy_delivery_receipt(
    delivery_id: str,
    receipt: DeliveryReceipt,
) -> ProviderDeliveryReceipt:
    """Map legacy success/partial/failure into the non-retryable v3 states."""

    if receipt.status is DeliveryStatus.SUCCESS:
        return ProviderDeliveryReceipt(delivery_id, ChannelDeliveryStatus.DELIVERED)
    if receipt.status is DeliveryStatus.FAILED and _legacy_pre_effect(receipt):
        return ProviderDeliveryReceipt(
            delivery_id,
            ChannelDeliveryStatus.REJECTED,
            error=receipt.detail or "legacy provider rejected before effect",
        )
    return ProviderDeliveryReceipt(
        delivery_id,
        ChannelDeliveryStatus.UNKNOWN,
        error=receipt.detail or "legacy provider effect uncertain",
    )


def _legacy_pre_effect(receipt: DeliveryReceipt) -> bool:
    """Return whether a legacy FAILED receipt explicitly proves no provider effect."""

    return bool(receipt.detail and receipt.detail.startswith("pre-effect:"))


def request_metadata(request: ProviderDeliveryRequest) -> Mapping[str, object]:
    """Read migration metadata carried by the formal provider request DTO."""

    # Only Core migration adapters interpret this private attachment projection;
    # plugin providers continue to receive the formal text-only C14 request.
    metadata = getattr(request, "metadata", {})
    if not isinstance(metadata, Mapping):
        raise TypeError("Core migration request metadata 必须是 mapping")
    return cast(Mapping[str, object], metadata)


def encode_legacy_attachments(
    attachments: Sequence[ChannelAttachment],
) -> list[dict[str, str | None]]:
    """Encode existing path-based attachments for the migration-only adapter."""

    result: list[dict[str, str | None]] = []
    for attachment in attachments:
        result.append(
            {
                "kind": attachment.kind.value,
                "source": attachment.source,
                "filename": attachment.filename,
            }
        )
    return result


def _decode_legacy_attachments(value: object) -> tuple[ChannelAttachment, ...]:
    if value in (None, ()):
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError("Core migration attachments 必须是数组")
    result: list[ChannelAttachment] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise TypeError("Core migration attachment 必须是对象")
        kind = item.get("kind")
        source = item.get("source")
        filename = item.get("filename")
        if not isinstance(kind, str) or not isinstance(source, str) or not source:
            raise ValueError("Core migration attachment kind/source 无效")
        if filename is not None and not isinstance(filename, str):
            raise TypeError("Core migration attachment filename 无效")
        try:
            attachment_kind = AttachmentKind(kind)
        except ValueError as error:
            raise ValueError(f"Core migration attachment kind 无效: {kind}") from error
        result.append(ChannelAttachment(attachment_kind, source, filename))
    return tuple(result)


__all__ = [
    "CoreLegacyChannelAdapter",
    "LEGACY_ATTACHMENT_METADATA_KEY",
    "build_core_channel_definition",
    "encode_legacy_attachments",
    "map_legacy_delivery_receipt",
]
