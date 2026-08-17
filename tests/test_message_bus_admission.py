from __future__ import annotations

import asyncio
import gc
import logging
import weakref
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from agent.control.models import TurnRequest, TurnStatus
from agent.control.runtime import ConversationRuntime
from agent.plugin_composition.channels import (
    AttachmentKind,
    AttachmentRef,
    ChannelInboundMessage,
    ChannelDeliveryReceipt,
    DeliveryStatus,
    InboundEnvelope,
    InboundOwner,
    InboundState,
    OutboundEnvelope,
)
from bus.events import InboundMessage, OutboundMessage
from bus.queue import MessageBus
from session.manager import SessionManager
from session.store import SessionStore


def _v3_outbound() -> tuple[OutboundEnvelope, SimpleNamespace]:
    envelope = OutboundEnvelope(
        logical_delivery_id="delivery-1",
        delivery_id="delivery-1",
        attempt_sequence=1,
        snapshot_id="snapshot-1",
        generation_id="generation-1",
        binding_token="binding-1",
        channel="feishu",
        recipient="chat-1",
        body="hello",
        metadata={"kind": "final"},
    )
    binding = SimpleNamespace(
        snapshot_id="snapshot-1",
        generation_id="generation-1",
        binding_token="binding-1",
        channel_name="feishu",
        active=True,
    )
    return envelope, binding


class _InboundLease:
    def __init__(self, close_gate: asyncio.Event | None = None) -> None:
        self.snapshot_id = "snapshot-1"
        self.generation_id = "generation-1"
        self.binding_token = "binding-1"
        self.channel_name = "feishu"
        self.snapshot_lease = SimpleNamespace(
            active=True,
            snapshot=SimpleNamespace(snapshot_id=self.snapshot_id),
            validation_candidate_plugin_ids=frozenset(),
        )
        self.closed = 0
        self.closed_event = asyncio.Event()
        self.close_gate = close_gate
        self.close_started = asyncio.Event()

    @property
    def active(self) -> bool:
        return self.closed == 0

    async def aclose(self) -> None:
        self.close_started.set()
        if self.close_gate is not None:
            await self.close_gate.wait()
        self.closed += 1
        self.closed_event.set()


def _v3_inbound(
    close_gate: asyncio.Event | None = None,
    *,
    message_id: str = "message-1",
    attachments: tuple[AttachmentRef, ...] = (),
    metadata: dict[str, object] | None = None,
) -> tuple[InboundEnvelope, _InboundLease]:
    lease = _InboundLease(close_gate)
    envelope = InboundEnvelope(
        message_id=message_id,
        snapshot_id=lease.snapshot_id,
        generation_id=lease.generation_id,
        binding_token=lease.binding_token,
        message=ChannelInboundMessage(
            channel="feishu",
            sender="user-1",
            chat_id="chat-1",
            content="hello",
            timestamp=datetime.now(timezone.utc),
            metadata=metadata or {},
            attachments=attachments,
        ),
        lease=lease,
    )
    return envelope, lease


@pytest.mark.asyncio
async def test_worker_error_before_turn_owner_keeps_handoff_and_releases_admission(
    tmp_path: Path,
) -> None:
    store = SessionStore(tmp_path / "sessions.db")
    manager = SessionManager(tmp_path / "workspace")
    session_key = "mobile:cleanup"
    manager.save(manager.get_or_create(session_key))
    _, admission_id = manager.admit_existing(session_key)
    bus = MessageBus()
    bus.bind_durable_inbound_store(store)
    item = InboundMessage(
        "mobile",
        "device:1",
        "cleanup",
        "hello",
        metadata={"session_key_override": session_key, "client_message_id": "client-1"},
        session_admission_id=admission_id,
    )
    await bus.publish_inbound(item)
    consumed = await bus.consume_inbound()
    assert consumed is item
    item_ref = weakref.ref(item)

    class _Runtime:
        async def wait_thread_available(self, _session_key: str) -> None:
            return None

        async def start_turn(self, _request: object) -> object:
            raise RuntimeError("worker stopped before turn submission")

    from bootstrap.passive_worker import PassiveMessageWorker

    worker = PassiveMessageWorker(
        bus,
        _Runtime(),  # type: ignore[arg-type]
        SimpleNamespace(session_manager=manager),  # type: ignore[arg-type]
    )
    lane = asyncio.Queue()
    lane.put_nowait(consumed)
    worker._lane_queues[session_key] = lane
    lane_task = asyncio.create_task(worker._run_lane(session_key, lane))
    await lane_task

    # 1. start_turn 建立 turn owner 前失败：不 complete_inbound，row 与 owner 保留。
    assert manager.control_store.list_turns(session_key) == []
    assert len(store.list_inbound_handoffs()) == 1
    owner_key = id(item)
    assert owner_key in bus._inbound_accepted
    del consumed
    del item
    gc.collect()
    assert item_ref() is not None

    # 2. session admission 恰一次释放，同一会话可再次取得。
    assert (
        store._conn.execute(
            "SELECT 1 FROM session_admissions WHERE admission_id = ?",
            (admission_id,),
        ).fetchone()
        is None
    )
    _, reacquired = manager.admit_existing(session_key)
    manager.release_admission(reacquired)

    # 3. 没有删除授权：不启动 cleanup retry，row 继续由 durable owner 持有。
    assert bus._inbound_cleanup_tasks == {}
    await bus.aclose()
    assert len(store.list_inbound_handoffs()) == 1
    assert owner_key in bus._inbound_accepted
    manager.close()
    store.close()


@pytest.mark.asyncio
async def test_mobile_attachment_acquire_failure_releases_session_admission(
    tmp_path: Path,
) -> None:
    store = SessionStore(tmp_path / "sessions.db")
    manager = SessionManager(tmp_path / "workspace")
    session_key = "mobile:missing-attachment"
    manager.save(manager.get_or_create(session_key))
    _, admission_id = manager.admit_existing(session_key)
    bus = MessageBus()
    bus.bind_durable_inbound_store(store)
    item = InboundMessage(
        "mobile",
        "device:1",
        "missing-attachment",
        "hello",
        metadata={
            "session_key_override": session_key,
            "client_message_id": "client-missing-attachment",
            "attachment_ids": ["artifact-missing"],
        },
        session_admission_id=admission_id,
    )
    await bus.publish_inbound(item)
    consumed = await bus.consume_inbound()

    class _Runtime:
        async def start_turn(self, _request: object) -> object:
            raise AssertionError("attachment acquisition failure must precede turn start")

    from bootstrap.passive_worker import PassiveMessageWorker

    worker = PassiveMessageWorker(
        bus,
        _Runtime(),  # type: ignore[arg-type]
        SimpleNamespace(session_manager=manager),  # type: ignore[arg-type]
    )
    lane = asyncio.Queue()
    lane.put_nowait(consumed)
    worker._lane_queues[session_key] = lane
    await worker._run_lane(session_key, lane)

    assert len(store.list_inbound_handoffs()) == 1
    assert (
        store._conn.execute(
            "SELECT 1 FROM session_admissions WHERE admission_id = ?",
            (admission_id,),
        ).fetchone()
        is None
    )
    assert item.session_admission_id is None
    await bus.aclose()
    manager.close()
    store.close()


@pytest.mark.asyncio
async def test_persistent_cleanup_failure_is_bounded_and_shutdown_cancels_retry(
    tmp_path: Path,
) -> None:
    store = SessionStore(tmp_path / "sessions.db")
    bus = MessageBus()
    bus.bind_durable_inbound_store(store)
    item = InboundMessage(
        "mobile",
        "device:1",
        "persistent",
        "hello",
        metadata={"client_message_id": "client-persistent"},
    )
    await bus.publish_inbound(item)
    consumed = await bus.consume_inbound()
    attempts = 0

    def always_fail(_handoff_id: str) -> None:
        nonlocal attempts
        attempts += 1
        raise OSError("persistent delete failure")

    store.complete_inbound_handoff = always_fail  # type: ignore[method-assign]
    with pytest.raises(OSError, match="persistent delete failure"):
        await bus.complete_inbound(consumed)
    await asyncio.sleep(0.35)
    assert 2 <= attempts <= 4
    assert len(bus._inbound_accepted) == 1
    assert len(bus._inbound_cleanup_tasks) == 1
    await bus.aclose()
    assert bus._inbound_cleanup_tasks == {}
    assert (
        store._conn.execute(
            "SELECT 1 FROM inbound_handoffs WHERE handoff_id = ?",
            (consumed.handoff_id,),
        ).fetchone()
        is not None
    )
    store.close()


@pytest.mark.asyncio
async def test_cleanup_finalize_failure_is_fatal_and_observable(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = SessionStore(tmp_path / "sessions.db")
    bus = MessageBus()
    bus.bind_durable_inbound_store(store)
    item = InboundMessage(
        "mobile",
        "device:1",
        "fatal",
        "hello",
        metadata={"client_message_id": "client-fatal"},
    )
    await bus.publish_inbound(item)
    consumed = await bus.consume_inbound()
    original_complete = store.complete_inbound_handoff
    failed = True

    def fail_once(handoff_id: str) -> None:
        nonlocal failed
        if failed:
            failed = False
            raise OSError("temporary delete failure")
        original_complete(handoff_id)

    store.complete_inbound_handoff = fail_once  # type: ignore[method-assign]

    async def fatal_finalize(_owner_key: int, _owner: object) -> None:
        raise RuntimeError("owner mismatch")

    bus._finalize_inbound_owner = fatal_finalize  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR):
        with pytest.raises(OSError, match="temporary delete failure"):
            await bus.complete_inbound(consumed)
        await asyncio.sleep(0.2)
    assert bus._inbound_cleanup_error is not None
    assert bus._inbound_cleanup_tasks == {}
    assert "event=runtime_fatal" in caplog.text
    assert "owner=message_bus.inbound_cleanup" in caplog.text
    with pytest.raises(RuntimeError, match="cleanup owner failed"):
        await bus.aclose()
    store.close()


@pytest.mark.asyncio
async def test_awaited_outbound_receipt_true_only_after_subscriber_commit() -> None:
    bus = MessageBus()
    committed: list[str] = []

    async def callback(msg: OutboundMessage) -> None:
        committed.append(msg.content)

    bus.subscribe_outbound("mobile", callback)
    dispatch = asyncio.create_task(bus.dispatch_outbound())
    delivered = await asyncio.wait_for(
        bus.publish_outbound_awaited(
            OutboundMessage("mobile", "one", "final", control_turn_id="turn:1")
        ),
        timeout=1,
    )
    assert delivered is True
    assert committed == ["final"]
    bus.stop()
    dispatch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dispatch
    assert bus._pending_outbound_receipts == set()


@pytest.mark.asyncio
async def test_awaited_outbound_receipt_false_without_subscriber() -> None:
    bus = MessageBus()
    dispatch = asyncio.create_task(bus.dispatch_outbound())
    delivered = await asyncio.wait_for(
        bus.publish_outbound_awaited(OutboundMessage("mobile", "one", "final")),
        timeout=1,
    )
    assert delivered is False
    bus.stop()
    dispatch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dispatch
    assert bus._pending_outbound_receipts == set()


@pytest.mark.asyncio
async def test_awaited_outbound_receipt_false_after_two_callback_failures() -> None:
    bus = MessageBus()
    attempts = {"count": 0}

    async def callback(_msg: OutboundMessage) -> None:
        attempts["count"] += 1
        raise RuntimeError("channel unavailable")

    bus.subscribe_outbound("mobile", callback)
    dispatch = asyncio.create_task(bus.dispatch_outbound())
    delivered = await asyncio.wait_for(
        bus.publish_outbound_awaited(OutboundMessage("mobile", "one", "final")),
        timeout=5,
    )
    assert delivered is False
    assert attempts["count"] >= 2
    bus.stop()
    dispatch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dispatch


@pytest.mark.asyncio
async def test_awaited_outbound_two_failures_never_sends_fallback() -> None:
    bus = MessageBus()
    attempts: list[str] = []

    async def callback(msg: OutboundMessage) -> None:
        attempts.append(msg.content)
        raise RuntimeError("channel down")

    bus.subscribe_outbound("mobile", callback)
    dispatch = asyncio.create_task(bus.dispatch_outbound())
    delivered = await asyncio.wait_for(
        bus.publish_outbound_awaited(OutboundMessage("mobile", "one", "final")),
        timeout=5,
    )
    assert delivered is False
    # awaited 封套：恰 2 次原始 terminal 内容，严禁降级文案占用终态。
    assert attempts == ["final", "final"]
    bus.stop()
    dispatch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dispatch
    assert bus._pending_outbound_receipts == set()


@pytest.mark.asyncio
async def test_fire_and_forget_two_failures_still_sends_fallback() -> None:
    bus = MessageBus()
    attempts: list[str] = []

    async def callback(msg: OutboundMessage) -> None:
        attempts.append(msg.content)
        raise RuntimeError("channel down")

    bus.subscribe_outbound("mobile", callback)
    dispatch = asyncio.create_task(bus.dispatch_outbound())
    await bus.publish_outbound(OutboundMessage("mobile", "one", "final"))

    async def reached_three() -> None:
        while len(attempts) < 3:
            await asyncio.sleep(0.01)

    await asyncio.wait_for(reached_three(), timeout=5)
    bus.stop()
    dispatch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dispatch
    # fire-and-forget 保持既有 fallback：两次失败后发送降级文案。
    assert attempts == ["final", "final", "（消息发送失败，请稍后重试）"]


@pytest.mark.asyncio
async def test_aclose_drains_queued_outbound_and_releases_chat_lane_state() -> None:
    bus = MessageBus()
    delivered: list[str] = []

    async def callback(msg: OutboundMessage) -> None:
        delivered.append(msg.content)

    bus.subscribe_outbound("mobile", callback)
    pending_a = asyncio.create_task(
        bus.publish_outbound_awaited(OutboundMessage("mobile", "one", "a"))
    )
    pending_b = asyncio.create_task(
        bus.publish_outbound_awaited(OutboundMessage("mobile", "two", "b"))
    )
    await bus.publish_outbound(OutboundMessage("mobile", "two", "c"))
    await asyncio.sleep(0)
    assert bus.outbound_size == 3
    assert len(bus._pending_outbound_receipts) == 2

    # 1. aclose 排空未 dispatch 项：receipt 收束为 False、lane pending 回滚。
    await bus.aclose()
    assert bus.outbound_size == 0
    assert bus._pending_outbound_receipts == set()
    assert bus.chat_lane._states == {}
    assert (await pending_a) is False
    assert (await pending_b) is False
    assert delivered == []


@pytest.mark.asyncio
async def test_live_reserve_and_recovery_race_never_duplicates_handoff(
    tmp_path: Path,
) -> None:
    store = SessionStore(tmp_path / "sessions.db")
    manager = SessionManager(tmp_path / "workspace")
    session_key = "mobile:race"
    manager.save(manager.get_or_create(session_key))

    async def execute(request: TurnRequest) -> str:
        return f"echo:{request.input}"

    runtime = ConversationRuntime(manager.control_store, execute)
    bus = MessageBus()
    bus.bind_durable_inbound_store(store)
    delivered: list[OutboundMessage] = []

    async def on_outbound(msg: OutboundMessage) -> None:
        delivered.append(msg)

    bus.subscribe_outbound("mobile", on_outbound)
    dispatch = asyncio.create_task(bus.dispatch_outbound())
    item = InboundMessage(
        "mobile",
        "device:1",
        "race",
        "hello",
        metadata={
            "session_key_override": session_key,
            "client_message_id": "client:race",
        },
    )
    # 门控：live publish 在 durable reserve 落库后、accepted owner 登记前暂停。
    original_pending = bus._chat_lane.mark_passive_pending
    reserve_committed = asyncio.Event()
    resume_live = asyncio.Event()

    async def gated_pending(channel: str, chat_id: str) -> None:
        reserve_committed.set()
        await resume_live.wait()
        await original_pending(channel, chat_id)

    bus._chat_lane.mark_passive_pending = gated_pending  # type: ignore[method-assign]
    live = asyncio.create_task(bus.publish_inbound(item))
    await asyncio.wait_for(reserve_committed.wait(), timeout=2)

    # 1. 同窗口启动恢复：必须等待 durable lock，不能看到半登记的 live row。
    recovery = asyncio.create_task(bus.recover_durable_inbounds())
    await asyncio.sleep(0.05)
    assert not recovery.done()
    assert len(store.list_inbound_handoffs()) == 1

    resume_live.set()
    await asyncio.wait_for(live, timeout=2)
    await asyncio.wait_for(recovery, timeout=2)

    # 2. 同一 handoff 只有一 queue item / 一 accepted owner。
    assert bus.inbound_size == 1
    assert len(bus._inbound_accepted) == 1

    # 3. 处理一次只建一个 turn，无重复 client_message_id 冲突。
    from bootstrap.passive_worker import PassiveMessageWorker

    worker = PassiveMessageWorker(
        bus,
        runtime,
        cast(Any, SimpleNamespace(session_manager=manager)),
    )
    consumed = await bus.consume_inbound()
    assert isinstance(consumed, InboundMessage)
    await worker._run_message(consumed)
    turns = manager.control_store.list_turns(session_key)
    assert len(turns) == 1
    assert turns[0].status is TurnStatus.COMPLETED
    assert manager.control_store.list_inbound_handoffs() == []
    assert [msg.content for msg in delivered] == ["echo:hello"]
    dispatch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dispatch
    await runtime.shutdown()
    manager.close()
    store.close()


@pytest.mark.asyncio
async def test_awaited_outbound_receipt_settled_false_on_aclose_without_leak() -> None:
    bus = MessageBus()
    pending = asyncio.create_task(
        bus.publish_outbound_awaited(OutboundMessage("mobile", "one", "final"))
    )
    await asyncio.sleep(0)
    assert len(bus._pending_outbound_receipts) == 1
    await bus.aclose()
    assert (await pending) is False
    assert bus._pending_outbound_receipts == set()


@pytest.mark.asyncio
async def test_awaited_outbound_receipt_settled_false_on_dispatch_cancel() -> None:
    bus = MessageBus()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def callback(_msg: OutboundMessage) -> None:
        entered.set()
        await release.wait()

    bus.subscribe_outbound("mobile", callback)
    dispatch = asyncio.create_task(bus.dispatch_outbound())
    pending = asyncio.create_task(
        bus.publish_outbound_awaited(OutboundMessage("mobile", "one", "final"))
    )
    await asyncio.wait_for(entered.wait(), timeout=1)
    dispatch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await dispatch
    assert (await pending) is False
    assert bus._pending_outbound_receipts == set()
    release.set()


@pytest.mark.asyncio
async def test_v3_channel_outbound_returns_exact_provider_receipt_once() -> None:
    bus = MessageBus()
    envelope, binding = _v3_outbound()
    calls = 0

    async def deliver(
        received: OutboundEnvelope,
        owner: Any,
    ) -> ChannelDeliveryReceipt:
        nonlocal calls
        calls += 1
        assert received is envelope
        assert owner is binding
        return ChannelDeliveryReceipt(
            delivery_id=received.delivery_id,
            status=DeliveryStatus.DELIVERED,
            provider_ids=("provider-1",),
        )

    bus.bind_channel_outbound_dispatcher(deliver)
    dispatch = asyncio.create_task(bus.dispatch_outbound())
    receipt = await bus.publish_channel_outbound_awaited(envelope, binding)
    assert receipt.status is DeliveryStatus.DELIVERED
    assert receipt.provider_ids == ("provider-1",)
    assert calls == 1
    bus.stop()
    await dispatch


@pytest.mark.asyncio
async def test_v3_direct_channel_outbound_waits_for_passive_turn() -> None:
    bus = MessageBus()
    envelope, binding = _v3_outbound()
    entered = asyncio.Event()

    async def deliver(
        received: OutboundEnvelope,
        _owner: Any,
    ) -> ChannelDeliveryReceipt:
        entered.set()
        return ChannelDeliveryReceipt(
            received.delivery_id,
            DeliveryStatus.DELIVERED,
        )

    bus.bind_channel_outbound_dispatcher(deliver)
    await bus.chat_lane.mark_passive_pending(
        envelope.channel,
        envelope.recipient,
    )
    dispatch = asyncio.create_task(bus.dispatch_outbound())
    pending = asyncio.create_task(
        bus.publish_channel_outbound_awaited(
            envelope,
            binding,
            passive=False,
        )
    )
    await asyncio.sleep(0)
    assert not entered.is_set()
    assert not pending.done()

    await bus.chat_lane.mark_passive_done(
        envelope.channel,
        envelope.recipient,
    )
    receipt = await asyncio.wait_for(pending, timeout=1)
    assert receipt.status is DeliveryStatus.DELIVERED
    assert entered.is_set()
    bus.stop()
    await dispatch


@pytest.mark.asyncio
async def test_v3_direct_channel_wait_is_rejected_by_terminal_bus_close() -> None:
    bus = MessageBus()
    envelope, binding = _v3_outbound()
    calls = 0

    async def deliver(
        _received: OutboundEnvelope,
        _owner: Any,
    ) -> ChannelDeliveryReceipt:
        nonlocal calls
        calls += 1
        raise AssertionError("关闭前仍在 lane 等待的 direct push 不得调用 provider")

    bus.bind_channel_outbound_dispatcher(deliver)
    await bus.chat_lane.mark_passive_pending(
        envelope.channel,
        envelope.recipient,
    )
    dispatch = asyncio.create_task(bus.dispatch_outbound())
    pending = asyncio.create_task(
        bus.publish_channel_outbound_awaited(
            envelope,
            binding,
            passive=False,
        )
    )
    while bus.outbound_size:
        await asyncio.sleep(0)

    await asyncio.wait_for(bus.aclose(), timeout=1)
    receipt = await asyncio.wait_for(pending, timeout=1)

    assert receipt.status is DeliveryStatus.REJECTED
    assert calls == 0
    assert dispatch.done()
    await bus.chat_lane.mark_passive_done(
        envelope.channel,
        envelope.recipient,
    )


@pytest.mark.asyncio
async def test_v3_passive_channel_outbound_does_not_wait_for_its_own_turn() -> None:
    bus = MessageBus()
    envelope, binding = _v3_outbound()
    calls = 0

    async def deliver(
        received: OutboundEnvelope,
        _owner: Any,
    ) -> ChannelDeliveryReceipt:
        nonlocal calls
        calls += 1
        return ChannelDeliveryReceipt(
            received.delivery_id,
            DeliveryStatus.DELIVERED,
        )

    bus.bind_channel_outbound_dispatcher(deliver)
    await bus.chat_lane.mark_passive_pending(
        envelope.channel,
        envelope.recipient,
    )
    dispatch = asyncio.create_task(bus.dispatch_outbound())

    receipt = await asyncio.wait_for(
        bus.publish_channel_outbound_awaited(
            envelope,
            binding,
            passive=True,
        ),
        timeout=1,
    )

    assert receipt.status is DeliveryStatus.DELIVERED
    assert calls == 1
    await bus.chat_lane.mark_passive_done(
        envelope.channel,
        envelope.recipient,
    )
    bus.stop()
    await dispatch


@pytest.mark.asyncio
async def test_v3_channel_outbound_exception_is_unknown_without_retry() -> None:
    bus = MessageBus()
    envelope, binding = _v3_outbound()
    calls = 0

    async def fail_after_effect(
        _received: OutboundEnvelope,
        _owner: Any,
    ) -> ChannelDeliveryReceipt:
        nonlocal calls
        calls += 1
        raise RuntimeError("provider receipt lost")

    bus.bind_channel_outbound_dispatcher(fail_after_effect)
    dispatch = asyncio.create_task(bus.dispatch_outbound())
    receipt = await bus.publish_channel_outbound_awaited(envelope, binding)
    assert receipt.status is DeliveryStatus.UNKNOWN
    assert receipt.error == "provider receipt lost"
    assert calls == 1
    bus.stop()
    await dispatch


@pytest.mark.asyncio
async def test_v3_channel_outbound_cancel_waits_for_provider_settlement() -> None:
    bus = MessageBus()
    envelope, binding = _v3_outbound()
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def blocked_delivery(
        received: OutboundEnvelope,
        _owner: Any,
    ) -> ChannelDeliveryReceipt:
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return ChannelDeliveryReceipt(
            received.delivery_id,
            DeliveryStatus.DELIVERED,
        )

    bus.bind_channel_outbound_dispatcher(blocked_delivery)
    dispatch = asyncio.create_task(bus.dispatch_outbound())
    pending = asyncio.create_task(
        bus.publish_channel_outbound_awaited(envelope, binding)
    )
    await asyncio.wait_for(entered.wait(), timeout=1)
    pending.cancel()
    await asyncio.sleep(0)
    assert not pending.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert calls == 1
    assert bus._pending_channel_receipts == set()
    bus.stop()
    await dispatch


@pytest.mark.asyncio
async def test_v3_channel_queued_receipt_rejected_on_bus_close() -> None:
    bus = MessageBus()
    envelope, binding = _v3_outbound()
    pending = asyncio.create_task(
        bus.publish_channel_outbound_awaited(envelope, binding)
    )
    await asyncio.sleep(0)
    assert len(bus._pending_channel_receipts) == 1
    await bus.aclose()
    receipt = await pending
    assert receipt.status is DeliveryStatus.REJECTED
    assert bus._pending_channel_receipts == set()


@pytest.mark.asyncio
async def test_v3_channel_publish_after_bus_close_is_immediately_rejected() -> None:
    bus = MessageBus()
    envelope, binding = _v3_outbound()
    calls = 0

    async def deliver(
        _received: OutboundEnvelope,
        _owner: Any,
    ) -> ChannelDeliveryReceipt:
        nonlocal calls
        calls += 1
        raise AssertionError("closed bus must not dispatch")

    bus.bind_channel_outbound_dispatcher(deliver)
    await bus.aclose()

    receipt = await bus.publish_channel_outbound_awaited(envelope, binding)

    assert receipt.status is DeliveryStatus.REJECTED
    assert calls == 0
    assert bus.outbound_size == 0
    await bus.dispatch_outbound()
    assert calls == 0


@pytest.mark.asyncio
async def test_v3_channel_publish_blocked_at_lane_is_rejected_by_concurrent_close() -> None:
    bus = MessageBus()
    envelope, binding = _v3_outbound()
    key, state = bus._chat_lane._acquire_state(
        envelope.channel,
        envelope.recipient,
    )
    try:
        await state.condition.acquire()
        pending = asyncio.create_task(
            bus.publish_channel_outbound_awaited(envelope, binding)
        )
        await asyncio.sleep(0)
        closing = asyncio.create_task(bus.aclose())
        await asyncio.sleep(0)
        assert not pending.done()
        state.condition.release()
        await closing
        receipt = await pending
    finally:
        if state.condition.locked():
            state.condition.release()
        bus._chat_lane._release_state(key, state)

    assert receipt.status is DeliveryStatus.REJECTED
    assert bus.outbound_size == 0


@pytest.mark.asyncio
async def test_v3_channel_inbound_transfers_bus_lane_loop_and_closes_once() -> None:
    bus = MessageBus()
    envelope, lease = _v3_inbound()

    await bus.publish_channel_inbound(envelope)
    assert (envelope.owner, envelope.state) == (
        InboundOwner.BUS,
        InboundState.BUS_QUEUED,
    )
    consumed = await bus.consume_inbound()
    assert consumed is envelope
    assert (envelope.owner, envelope.state) == (
        InboundOwner.LANE,
        InboundState.LANE_QUEUED,
    )
    envelope.handoff(InboundOwner.LANE, InboundOwner.LOOP)
    await bus.complete_inbound(envelope)
    assert (envelope.owner, envelope.state) == (
        InboundOwner.CLOSED,
        InboundState.TERMINAL,
    )
    assert lease.closed == 1


@pytest.mark.asyncio
async def test_v3_channel_inbound_bus_close_releases_queued_exact_lease() -> None:
    bus = MessageBus()
    envelope, lease = _v3_inbound()
    await bus.publish_channel_inbound(envelope)

    await bus.aclose()

    assert envelope.state is InboundState.TERMINAL
    assert lease.closed == 1
    assert bus.inbound_size == 0


@pytest.mark.asyncio
async def test_v3_channel_inbound_blocked_at_lane_is_closed_by_concurrent_bus_close() -> None:
    bus = MessageBus()
    envelope, lease = _v3_inbound()
    key, state = bus._chat_lane._acquire_state(
        envelope.channel,
        envelope.chat_id,
    )
    try:
        await state.condition.acquire()
        pending = asyncio.create_task(bus.publish_channel_inbound(envelope))
        await asyncio.sleep(0)
        await bus.aclose()
        state.condition.release()
        with pytest.raises(RuntimeError, match="已关闭"):
            await pending
    finally:
        if state.condition.locked():
            state.condition.release()
        bus._chat_lane._release_state(key, state)

    assert envelope.state is InboundState.TERMINAL
    assert lease.closed == 1
    assert bus.inbound_size == 0


@pytest.mark.asyncio
async def test_v3_channel_bus_close_cancellation_drains_every_queued_lease() -> None:
    bus = MessageBus()
    close_gate = asyncio.Event()
    first, first_lease = _v3_inbound(close_gate, message_id="message-1")
    second, second_lease = _v3_inbound(message_id="message-2")
    await bus.publish_channel_inbound(first)
    await bus.publish_channel_inbound(second)

    closing = asyncio.create_task(bus.aclose())
    await first_lease.close_started.wait()
    closing.cancel()
    await asyncio.sleep(0)
    assert not closing.done()
    close_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await closing

    assert first.state is InboundState.TERMINAL
    assert second.state is InboundState.TERMINAL
    assert first_lease.closed == second_lease.closed == 1
    assert bus.inbound_size == 0


@pytest.mark.asyncio
async def test_v3_channel_inbound_release_cancellation_clears_lane_before_return() -> None:
    bus = MessageBus()
    close_gate = asyncio.Event()
    envelope, lease = _v3_inbound(close_gate)
    await bus.publish_channel_inbound(envelope)
    assert await bus.consume_inbound() is envelope

    releasing = asyncio.create_task(
        bus.release_channel_inbound(envelope, InboundOwner.LANE)
    )
    await lease.close_started.wait()
    releasing.cancel()
    await asyncio.sleep(0)
    assert not releasing.done()
    close_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await releasing

    assert envelope.state is InboundState.TERMINAL
    assert lease.closed == 1
    assert bus._chat_lane._states == {}

    completed = False

    async def mark_completed() -> None:
        nonlocal completed
        completed = True

    await asyncio.wait_for(
        bus._chat_lane.run_non_passive(
            envelope.channel,
            envelope.chat_id,
            mark_completed,
        ),
        timeout=1,
    )
    assert completed


@pytest.mark.asyncio
async def test_v3_channel_worker_preserves_exact_binding_through_terminal_delivery(
    tmp_path: Path,
) -> None:
    store = SessionStore(tmp_path / "sessions.db")
    seen_request: list[TurnRequest] = []

    async def execute(request: TurnRequest) -> str:
        from agent.plugins.channel_generation_host import (
            get_current_channel_turn_binding,
        )
        from agent.plugins.snapshot import get_current_runtime_snapshot

        assert get_current_runtime_snapshot() is not None
        assert get_current_runtime_snapshot().snapshot_id == "snapshot-1"
        channel_binding = get_current_channel_turn_binding()
        assert channel_binding is lease
        seen_request.append(request)
        return f"echo:{request.input}"

    runtime = ConversationRuntime(store, execute)
    bus = MessageBus()
    delivered: list[tuple[OutboundEnvelope, object]] = []

    async def dispatch(
        envelope: OutboundEnvelope,
        binding: object,
    ) -> ChannelDeliveryReceipt:
        delivered.append((envelope, binding))
        return ChannelDeliveryReceipt(
            envelope.delivery_id,
            DeliveryStatus.DELIVERED,
            ("provider-1",),
        )

    bus.bind_channel_outbound_dispatcher(dispatch)
    from bootstrap.passive_worker import PassiveMessageWorker

    worker = PassiveMessageWorker(bus, runtime, cast(Any, object()))
    worker_task = asyncio.create_task(worker.run())
    dispatch_task = asyncio.create_task(bus.dispatch_outbound())
    envelope, lease = _v3_inbound()

    await bus.publish_channel_inbound(envelope)
    await asyncio.wait_for(lease.closed_event.wait(), timeout=2)
    while envelope.state is not InboundState.TERMINAL:
        await asyncio.sleep(0)

    assert len(seen_request) == 1
    assert seen_request[0].metadata["channelBindingToken"] == lease.binding_token
    assert (
        seen_request[0].metadata["inboundMetadata"]["client_message_id"]
        == envelope.message_id
    )
    assert len(delivered) == 1
    outbound, binding = delivered[0]
    assert outbound.body == "echo:hello"
    assert outbound.snapshot_id == lease.snapshot_id
    assert outbound.generation_id == lease.generation_id
    assert outbound.binding_token == lease.binding_token
    assert binding is lease
    assert envelope.state is InboundState.TERMINAL
    assert lease.closed == 1

    worker.stop()
    bus.stop()
    await worker_task
    await dispatch_task
    await runtime.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_v3_channel_worker_projects_and_closes_attachment_lease(
    tmp_path: Path,
) -> None:
    from bootstrap.passive_worker import PassiveMessageWorker
    from infra.channels.artifacts import ChannelAttachmentArtifactStore

    store = SessionStore(tmp_path / "sessions.db")
    attachment_store = ChannelAttachmentArtifactStore(
        workspace=tmp_path,
        session_store=store,
    )
    ref = await attachment_store.import_bytes(
        b"attachment-body",
        kind=AttachmentKind.FILE,
        filename="note.txt",
        media_type="text/plain",
    )
    seen_paths: list[str] = []

    async def execute(request: TurnRequest) -> str:
        media = cast(list[str], request.metadata["media"])
        assert len(media) == 1
        assert Path(media[0]).read_bytes() == b"attachment-body"
        seen_paths.extend(media)
        return "ok"

    runtime = ConversationRuntime(store, execute)
    bus = MessageBus()

    async def dispatch(
        envelope: OutboundEnvelope,
        _binding: object,
    ) -> ChannelDeliveryReceipt:
        return ChannelDeliveryReceipt(
            envelope.delivery_id,
            DeliveryStatus.DELIVERED,
        )

    bus.bind_channel_outbound_dispatcher(dispatch)
    worker = PassiveMessageWorker(
        bus,
        runtime,
        cast(Any, object()),
        attachment_store=attachment_store,
    )
    worker_task = asyncio.create_task(worker.run())
    dispatch_task = asyncio.create_task(bus.dispatch_outbound())
    envelope, lease = _v3_inbound(
        attachments=(ref,),
        metadata={"client_message_id": "client-attachment-1"},
    )

    await bus.publish_channel_inbound(envelope)
    await asyncio.wait_for(lease.closed_event.wait(), timeout=2)

    assert seen_paths and not Path(seen_paths[0]).exists()

    worker.stop()
    bus.stop()
    await worker_task
    await dispatch_task
    await runtime.shutdown()
    store.close()


@pytest.mark.asyncio
async def test_v3_channel_worker_cancel_closes_running_and_lane_queued_leases(
    tmp_path: Path,
) -> None:
    store = SessionStore(tmp_path / "sessions.db")
    started = asyncio.Event()
    never = asyncio.Event()

    async def execute(_request: TurnRequest) -> str:
        started.set()
        await never.wait()
        return "unreachable"

    runtime = ConversationRuntime(store, execute)
    bus = MessageBus()

    async def dispatch(
        envelope: OutboundEnvelope,
        _binding: object,
    ) -> ChannelDeliveryReceipt:
        return ChannelDeliveryReceipt(
            envelope.delivery_id,
            DeliveryStatus.DELIVERED,
        )

    bus.bind_channel_outbound_dispatcher(dispatch)
    from bootstrap.passive_worker import PassiveMessageWorker

    worker = PassiveMessageWorker(bus, runtime, cast(Any, object()))
    worker_task = asyncio.create_task(worker.run())
    dispatch_task = asyncio.create_task(bus.dispatch_outbound())
    first, first_lease = _v3_inbound(message_id="message-1")
    second, second_lease = _v3_inbound(message_id="message-2")
    await bus.publish_channel_inbound(first)
    await bus.publish_channel_inbound(second)
    await asyncio.wait_for(started.wait(), timeout=1)

    worker_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(worker_task, timeout=2)

    assert first.state is InboundState.TERMINAL
    assert second.state is InboundState.TERMINAL
    assert first_lease.closed == second_lease.closed == 1
    assert worker._lane_tasks == {}
    assert worker._lane_queues == {}
    assert worker._channel_result_tasks == {}

    bus.stop()
    await dispatch_task
    await runtime.shutdown()
    store.close()
