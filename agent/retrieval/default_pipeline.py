from __future__ import annotations

import asyncio
from typing import Protocol

from agent.lifecycle.composition import observe_composition_domain_event
from agent.core.types import RetrievalTrace
from agent.looping.ports import MemoryServices
from agent.retrieval.protocol import (
    MemoryRetrievalPipeline,
    RetrievalRequest,
    RetrievalResult,
)
from core.memory.engine import (
    MemoryQuery,
    MemoryQueryFilters,
    MemoryQueryResult,
    MemoryRecord,
    MemoryScope,
)
from core.memory.events import RetrievalCompleted, RetrievalHitSummary


class EventPublisher(Protocol):
    async def fanout(self, event: object) -> None: ...


class DefaultMemoryRetrievalPipeline(MemoryRetrievalPipeline):
    def __init__(
        self,
        memory: MemoryServices,
        event_publisher: EventPublisher | None = None,
    ) -> None:
        self._memory = memory
        self._event_publisher = event_publisher

    # 被动预检索入口：只转换请求形状，检索语义统一交给 MemoryEngine。
    async def retrieve(self, request: RetrievalRequest) -> RetrievalResult:
        # 1. 没有启用记忆引擎时，主链继续无记忆回复。
        if self._memory.engine is None:
            return RetrievalResult(block="", trace=None)

        # 2. 把 agent loop 的上下文转成 engine 的稳定请求协议。
        try:
            result = await self._memory.engine.query(
                MemoryQuery(
                    text=request.message,
                    intent="context",
                    scope=MemoryScope(
                        session_key=request.session_key,
                        channel=request.channel,
                        chat_id=request.chat_id,
                    ),
                    context={
                        "history": request.history,
                        "session_metadata": request.session_metadata,
                        "turn_id": request.turn_id,
                    },
                    filters=MemoryQueryFilters(hints=dict(request.extra or {})),
                    timestamp=request.timestamp,
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            # Query failure is itself a settled observation; the original failure
            # remains visible after the error event is published.
            await self._publish_retrieval_event(
                _build_retrieval_completed(request, None, error=error)
            )
            raise

        await self._publish_retrieval_event(_build_retrieval_completed(request, result))

        # 3. 只返回主链需要注入的文本块和可观测 trace。
        return RetrievalResult(
            block=result.text_block,
            trace=_build_retrieval_trace(result),
        )

    async def _publish_retrieval_event(self, event: RetrievalCompleted) -> None:
        """Publish one request-bound retrieval fact through legacy and v3 seams."""

        # 1. A configured EventBus preserves v2 handler order and bridges to v3.
        if self._event_publisher is not None:
            await self._event_publisher.fanout(event)
            return

        # 2. Direct pipeline users still reach the exact bound composition Root.
        await observe_composition_domain_event(event)


# 把 engine trace 收窄成 agent loop 认识的检索 trace。
def _build_retrieval_trace(
    result: MemoryQueryResult,
) -> RetrievalTrace | None:
    if not result.trace and not result.records and not result.text_block:
        return None
    return RetrievalTrace(
        gate_type=str(result.trace.get("gate_type") or "") or None,
        route_decision=str(result.trace.get("route_decision") or "") or None,
        rewritten_query=str(result.raw.get("rewritten_query") or "") or None,
        injected_count=sum(1 for record in result.records if record.injected),
        raw=result.raw.get("retrieval_event"),
    )


def _build_retrieval_completed(
    request: RetrievalRequest,
    result: MemoryQueryResult | None,
    *,
    error: BaseException | None = None,
) -> RetrievalCompleted:
    """Freeze the engine result into the existing retrieval event contract."""

    records = [] if result is None else list(result.records)
    raw = {} if result is None else result.raw
    trace = {} if result is None else result.trace
    query = _first_nonempty_string(raw.get("rewritten_query"), request.message)
    aux_queries = _string_list(raw.get("aux_queries"))
    if not aux_queries:
        aux_queries = _string_list(trace.get("hyde_hypotheses"))
    hits = [_build_hit_summary(record) for record in records]
    return RetrievalCompleted(
        session_key=request.session_key,
        channel=request.channel,
        chat_id=request.chat_id,
        query=query,
        orig_query=request.message if query != request.message else None,
        hits=hits,
        injected_count=sum(1 for hit in hits if hit.injected),
        route_decision=_optional_string(trace.get("route_decision")),
        aux_queries=aux_queries,
        error=None if error is None else str(error) or type(error).__name__,
    )


def _build_hit_summary(record: MemoryRecord) -> RetrievalHitSummary:
    signals = dict(record.signals)
    confidence_label = signals.get("confidence_label")
    return RetrievalHitSummary(
        item_id=record.id,
        memory_type=record.kind,
        score=float(record.score),
        summary=record.summary[:120],
        injected=bool(record.injected),
        confidence_label=(
            confidence_label if isinstance(confidence_label, str) else ""
        ),
        forced=bool(signals.get("forced", False)),
        metadata=signals,
    )


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def _first_nonempty_string(*values: object) -> str:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _optional_string(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None
