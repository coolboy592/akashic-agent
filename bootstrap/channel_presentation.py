from __future__ import annotations

from agent.plugin_composition.channels import (
    StreamDeltaPresentation,
    ToolPresentation,
    TurnOutputCompletedPresentation,
    TurnStartedPresentation,
    TurnStreamEvent,
    TurnStreamEventKind,
)
from agent.plugins.channel_generation_host import (
    get_current_channel_turn_binding,
)
from bus.event_bus import EventBus, EventSubscription
from bus.events_lifecycle import (
    StreamDeltaReady,
    ToolCallCompleted,
    ToolCallStarted,
    TurnOutputCompleted,
    TurnStarted,
)


class ChannelTurnPresentationBridge:
    """Project lifecycle events through the exact inbound Channel binding."""

    def __init__(self, event_bus: EventBus) -> None:
        self._sequences: dict[tuple[str, str], int] = {}
        self._subscriptions: tuple[EventSubscription, ...] = (
            event_bus.on(TurnStarted, self._turn_started),
            event_bus.on(StreamDeltaReady, self._stream_delta),
            event_bus.on(ToolCallStarted, self._tool_started),
            event_bus.on(ToolCallCompleted, self._tool_completed),
            event_bus.on(TurnOutputCompleted, self._output_completed),
        )

    async def _turn_started(self, event: TurnStarted) -> None:
        binding = self._binding(event.channel)
        if binding is None:
            return
        key = (binding.binding_token, event.turn_id)
        if key in self._sequences:
            raise RuntimeError(f"turn presentation 重复 started: {event.turn_id}")
        self._sequences[key] = 0
        await binding.publish_turn_event(
            TurnStreamEvent(
                presentation_id=f"preview:{event.turn_id}",
                kind=TurnStreamEventKind.TURN_STARTED,
                payload=TurnStartedPresentation(
                    turn_id=event.turn_id,
                    client_message_id=event.client_message_id,
                ),
            )
        )

    async def _stream_delta(self, event: StreamDeltaReady) -> None:
        binding = self._binding(event.channel)
        if binding is None:
            return
        await binding.publish_turn_event(
            TurnStreamEvent(
                presentation_id=f"preview:{event.turn_id}",
                kind=TurnStreamEventKind.STREAM_DELTA,
                payload=StreamDeltaPresentation(
                    turn_id=event.turn_id,
                    sequence=self._next(binding.binding_token, event.turn_id),
                    text_delta=event.content_delta,
                    reasoning_delta=event.thinking_delta,
                ),
            )
        )

    async def _tool_started(self, event: ToolCallStarted) -> None:
        await self._tool_event(event, TurnStreamEventKind.TOOL_STARTED)

    async def _tool_completed(self, event: ToolCallCompleted) -> None:
        await self._tool_event(event, TurnStreamEventKind.TOOL_COMPLETED)

    async def _tool_event(
        self,
        event: ToolCallStarted | ToolCallCompleted,
        kind: TurnStreamEventKind,
    ) -> None:
        binding = self._binding(event.channel)
        if binding is None:
            return
        await binding.publish_turn_event(
            TurnStreamEvent(
                presentation_id=f"preview:{event.turn_id}",
                kind=kind,
                payload=ToolPresentation(
                    turn_id=event.turn_id,
                    sequence=self._next(binding.binding_token, event.turn_id),
                    tool_call_id=event.call_id,
                    tool_name=event.tool_name,
                ),
            )
        )

    async def _output_completed(self, event: TurnOutputCompleted) -> None:
        binding = self._binding(event.channel)
        if binding is None:
            return
        key = (binding.binding_token, event.turn_id)
        try:
            await binding.publish_turn_event(
                TurnStreamEvent(
                    presentation_id=f"preview:{event.turn_id}",
                    kind=TurnStreamEventKind.TURN_OUTPUT_COMPLETED,
                    payload=TurnOutputCompletedPresentation(
                        turn_id=event.turn_id,
                        sequence=self._next(*key),
                    ),
                )
            )
        finally:
            self._sequences.pop(key, None)

    def _binding(self, channel: str):
        binding = get_current_channel_turn_binding()
        if binding is None:
            return None
        if binding.channel_name != channel:
            raise RuntimeError("turn event channel 与 exact Channel binding 不一致")
        if binding.turn_stream is None:
            return None
        return binding

    def _next(self, binding_token: str, turn_id: str) -> int:
        key = (binding_token, turn_id)
        current = self._sequences.get(key)
        if current is None:
            raise RuntimeError(f"turn presentation 未 started: {turn_id}")
        current += 1
        self._sequences[key] = current
        return current

    async def aclose(self) -> None:
        for subscription in self._subscriptions:
            subscription.close()
        self._sequences.clear()
