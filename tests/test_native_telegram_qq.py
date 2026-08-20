from __future__ import annotations

import base64
import hashlib
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from agent.plugin_composition.channels import (
    AttachmentKind,
    AttachmentRef,
    ChannelFactoryContext,
    ChannelReady,
    DeliveryStatus,
    ProviderDeliveryRequest,
)

from tests.test_channel_clients import (
    _Bus,
    _SessionManager,
    _import_qq_channel,
    _import_telegram_channel,
)


class _ProviderFactory:
    async def create(self, _credentials: object) -> object:
        raise AssertionError("native channel must reuse the existing provider owner")

    async def aclose(self) -> None:
        raise AssertionError(
            "native channel must not close the existing provider owner"
        )


class _Lease:
    def __init__(self, ref: AttachmentRef, data: bytes, events: list[str]) -> None:
        self.ref = ref
        self._data = data
        self._events = events

    async def read_bytes(self, *, max_bytes: int) -> bytes:
        assert max_bytes >= len(self._data)
        self._events.append(f"read:{self.ref.artifact_id}")
        return self._data

    async def aclose(self) -> None:
        self._events.append(f"close:{self.ref.artifact_id}")


class _ReadPort:
    def __init__(self, blobs: dict[str, bytes], events: list[str]) -> None:
        self._blobs = blobs
        self._events = events

    async def acquire(self, ref: AttachmentRef) -> _Lease:
        self._events.append(f"acquire:{ref.artifact_id}")
        return _Lease(ref, self._blobs[ref.artifact_id], self._events)


def _context(
    binding_token: str,
    read_port: _ReadPort,
) -> ChannelFactoryContext:
    return ChannelFactoryContext(
        snapshot_id="snapshot-1",
        generation_id="generation-1",
        binding_token=binding_token,
        config={},
        credentials={},
        provider_client_factory=_ProviderFactory(),
        ingress=None,
        identity=None,
        attachment_read=read_port,
    )


def _ref(
    artifact_id: str,
    kind: AttachmentKind,
    filename: str,
    media_type: str,
    data: bytes,
) -> AttachmentRef:
    return AttachmentRef(
        artifact_id=artifact_id,
        kind=kind,
        filename=filename,
        media_type=media_type,
        size_bytes=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
    )


@pytest.mark.asyncio
async def test_telegram_v3_adapter_delivers_in_order_from_exact_leases(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    mod = _import_telegram_channel(monkeypatch)
    channel = mod.TelegramChannel("token", _Bus(), _SessionManager(tmp_path))
    channel._telegram_outbound_limiter = mod.TelegramOutboundLimiter(
        send_interval_s=0.0,
        edit_interval_s=0.0,
        typing_interval_s=0.0,
        global_interval_s=0.0,
        retry_padding_s=0.0,
    )
    channel._app.initialize = AsyncMock()
    channel._app.start = AsyncMock()
    events: list[str] = []
    photo = b"photo"
    document = b"document"
    image_ref = _ref("image-1", AttachmentKind.IMAGE, "image.png", "image/png", photo)
    file_ref = _ref(
        "file-1", AttachmentKind.FILE, "report.pdf", "application/pdf", document
    )
    read_port = _ReadPort(
        {image_ref.artifact_id: photo, file_ref.artifact_id: document},
        events,
    )
    context = _context("telegram-binding", read_port)

    async def send_message(**kwargs: Any) -> None:
        events.append(f"text:{kwargs['text']}")

    async def send_photo(**kwargs: Any) -> None:
        events.append(f"photo:{kwargs['photo'].getvalue().decode()}")

    async def send_document(**kwargs: Any) -> None:
        events.append(
            f"file:{kwargs['filename']}:{kwargs['document'].getvalue().decode()}"
        )

    channel._app.bot.send_message = send_message
    channel._app.bot.send_photo = send_photo
    channel._app.bot.send_document = send_document
    adapter = channel.build_v3_adapter(context)

    assert await adapter.start() == ChannelReady("telegram-binding")
    channel._app.initialize.assert_not_awaited()
    channel._app.start.assert_not_awaited()
    receipt = await adapter.deliver(
        ProviderDeliveryRequest(
            binding_token="telegram-binding",
            delivery_id="delivery-1",
            recipient="123",
            body="hello",
            attachments=(image_ref, file_ref),
        )
    )

    assert receipt.status is DeliveryStatus.DELIVERED
    assert events == [
        "text:hello",
        "acquire:image-1",
        "read:image-1",
        "close:image-1",
        "photo:photo",
        "acquire:file-1",
        "read:file-1",
        "close:file-1",
        "file:report.pdf:document",
    ]
    assert (await adapter.stop()).resources_closed is True

    with pytest.raises(RuntimeError, match="binding token"):
        await adapter.deliver(
            ProviderDeliveryRequest(
                binding_token="wrong-binding",
                delivery_id="wrong-binding",
                recipient="123",
                body="must not send",
            )
        )


@pytest.mark.asyncio
async def test_telegram_v3_adapter_maps_pre_and_post_provider_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    mod = _import_telegram_channel(monkeypatch)
    channel = mod.TelegramChannel("token", _Bus(), _SessionManager(tmp_path))
    events: list[str] = []
    data = b"payload"
    ref = _ref("file-1", AttachmentKind.FILE, "a.txt", "text/plain", data)
    adapter = channel.build_v3_adapter(
        _context("telegram-binding", _ReadPort({ref.artifact_id: data}, events))
    )

    rejected = await adapter.deliver(
        ProviderDeliveryRequest(
            binding_token="telegram-binding",
            delivery_id="invalid-recipient",
            recipient="not-a-chat",
            body="hello",
        )
    )
    assert rejected.status is DeliveryStatus.REJECTED

    async def fail_send_message(**_kwargs: Any) -> None:
        raise RuntimeError("provider failed")

    channel._app.bot.send_message = fail_send_message
    unknown = await adapter.deliver(
        ProviderDeliveryRequest(
            binding_token="telegram-binding",
            delivery_id="provider-failure",
            recipient="123",
            body="hello",
        )
    )
    assert unknown.status is DeliveryStatus.UNKNOWN

    async def fail_read(_ref: AttachmentRef) -> _Lease:
        raise RuntimeError("read rejected")

    read_port = SimpleNamespace(acquire=fail_read)
    no_provider = channel.build_v3_adapter(_context("telegram-binding-2", read_port))
    rejected_attachment = await no_provider.deliver(
        ProviderDeliveryRequest(
            binding_token="telegram-binding-2",
            delivery_id="read-failure",
            recipient="123",
            body="",
            attachments=(ref,),
        )
    )
    assert rejected_attachment.status is DeliveryStatus.REJECTED


@pytest.mark.asyncio
async def test_qq_v3_adapter_delivers_group_text_and_binary_payloads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    mod = _import_qq_channel(monkeypatch)
    channel = mod.QQChannel(
        "42",
        _Bus(),
        _SessionManager(tmp_path),
        http_requester=SimpleNamespace(),
    )
    events: list[tuple[object, ...]] = []

    class _Api:
        async def send_group_text(self, group_id: int, body: str) -> None:
            events.append(("text", group_id, body))

        async def send_group_image(self, group_id: int, uri: str) -> None:
            events.append(
                ("image", group_id, base64.b64decode(uri.removeprefix("base64://")))
            )

        async def send_group_file(self, group_id: int, uri: str, filename: str) -> None:
            events.append(
                (
                    "file",
                    group_id,
                    filename,
                    base64.b64decode(uri.removeprefix("base64://")),
                )
            )

    channel._api = _Api()

    async def run(coro: Any) -> object:
        return await coro

    channel._run_on_bot_loop = AsyncMock(side_effect=run)
    image = b"qq-image"
    document = b"qq-document"
    image_ref = _ref("image-1", AttachmentKind.IMAGE, "image.jpg", "image/jpeg", image)
    file_ref = _ref("file-1", AttachmentKind.FILE, "report.txt", "text/plain", document)
    events_lease: list[str] = []
    adapter = channel.build_v3_adapter(
        _context(
            "qq-binding",
            _ReadPort(
                {image_ref.artifact_id: image, file_ref.artifact_id: document},
                events_lease,
            ),
        )
    )

    receipt = await adapter.deliver(
        ProviderDeliveryRequest(
            binding_token="qq-binding",
            delivery_id="delivery-1",
            recipient="gqq:100",
            body="hello",
            attachments=(image_ref, file_ref),
        )
    )

    assert receipt.status is DeliveryStatus.DELIVERED
    assert events == [
        ("text", 100, "hello"),
        ("image", 100, image),
        ("file", 100, "report.txt", document),
    ]
    assert events_lease == [
        "acquire:image-1",
        "read:image-1",
        "close:image-1",
        "acquire:file-1",
        "read:file-1",
        "close:file-1",
    ]


@pytest.mark.asyncio
async def test_qq_v3_adapter_rejects_invalid_recipient_before_provider(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    mod = _import_qq_channel(monkeypatch)
    channel = mod.QQChannel(
        "42",
        _Bus(),
        _SessionManager(tmp_path),
        http_requester=SimpleNamespace(),
    )
    channel._api = SimpleNamespace()
    adapter = channel.build_v3_adapter(_context("qq-binding", _ReadPort({}, [])))

    receipt = await adapter.deliver(
        ProviderDeliveryRequest(
            binding_token="qq-binding",
            delivery_id="invalid-recipient",
            recipient="gqq:not-a-number",
            body="hello",
        )
    )
    assert receipt.status is DeliveryStatus.REJECTED
