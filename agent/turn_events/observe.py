from __future__ import annotations

from typing import TYPE_CHECKING

from agent.plugin_composition import ObserveEventKey

if TYPE_CHECKING:
    from bus.events_lifecycle import ProactiveFinished
    from core.memory.events import MemoryWritten, RetrievalCompleted


# 这些 key 只描述 Core 已经结算的领域事实，不复制 payload，也不拥有领域状态。
PROACTIVE_FINISHED_EVENT: ObserveEventKey["ProactiveFinished"] = ObserveEventKey(
    "proactive.finished"
)
RETRIEVAL_COMPLETED_EVENT: ObserveEventKey["RetrievalCompleted"] = ObserveEventKey(
    "memory.retrieval.completed"
)
MEMORY_WRITTEN_EVENT: ObserveEventKey["MemoryWritten"] = ObserveEventKey(
    "memory.written"
)

# 事件名已经是稳定合同；短名只避免消费方为同一 key 重新声明一份对象。
PROACTIVE_FINISHED = PROACTIVE_FINISHED_EVENT
RETRIEVAL_COMPLETED = RETRIEVAL_COMPLETED_EVENT
MEMORY_WRITTEN = MEMORY_WRITTEN_EVENT
