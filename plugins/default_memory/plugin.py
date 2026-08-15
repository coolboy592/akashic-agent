from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, cast

from agent.lifecycle.composition import CONTEXT_PREPARED_EVENT
from agent.lifecycle.types import BeforeTurnCtx
from agent.plugin_composition import Context, MEMORY_RUNTIME, ServiceView
from agent.tools.events import TOOL_RESULT, ToolResult

_ITEM_LINE_RE = re.compile(r"^-\s+\[([^\]]+)\]\s*(.*)$")
_META_RE = re.compile(r"（(?P<meta>[^（）]*(?:证据|src|有印象|不确定)[^（）]*)）$")
_RECALL_FILENAME = "recall_inspector.jsonl"

api_version = 3
name = "default_memory"
version = "3.0.0"
desc = "记录默认记忆的上下文注入与显式召回结果"
drift_skill_roots = ("drift/skills",)
dashboard_module = "dashboard.py"

class DefaultMemoryInspector:
    def __init__(self, data_path: Path) -> None:
        self._lock = threading.RLock()
        self._active_turns: dict[str, str] = {}
        self._data_path = data_path

    def record_context_prepare(self, event: BeforeTurnCtx) -> None:
        turn_id = _turn_id(
            event.session_key, event.timestamp.isoformat(), event.content
        )
        self._active_turns[event.session_key] = turn_id
        block = event.retrieved_memory_block or ""
        injected_items = _items_from_block(block)
        all_hits = _hits_from_trace(event.retrieval_trace_raw)
        self._append(
            {
                "kind": "context_prepare",
                "engine": "default",
                "turn_id": turn_id,
                "session_key": event.session_key,
                "channel": event.channel,
                "chat_id": event.chat_id,
                "user_text": event.content,
                "timestamp": event.timestamp.isoformat(),
                "created_at": _now_iso(),
                "context_prepare": {
                    "count": len(all_hits) if all_hits else len(injected_items),
                    "items": all_hits or injected_items,
                    "injected_items": injected_items,
                    "raw_block": block,
                    "retrieval_trace_raw": _jsonable(event.retrieval_trace_raw),
                },
            }
        )

    async def record_recall_memory(self, event: ToolResult) -> None:
        if event.source != "passive" or event.tool_name != "recall_memory":
            return
        turn_id = self._active_turns.get(event.session_key)
        if not turn_id:
            turn_id = _turn_id(
                event.session_key,
                _now_iso(),
                json.dumps(event.arguments, ensure_ascii=False),
            )
        payload = _safe_json(event.result)
        raw_items: object = payload.get("items")
        items: list[dict[str, Any]] = []
        if isinstance(raw_items, list):
            items = [
                cast(dict[str, Any], raw_item)
                for raw_item in cast(list[object], raw_items)
                if isinstance(raw_item, dict)
            ]
        self._append(
            {
                "kind": "recall_memory",
                "engine": "default",
                "turn_id": turn_id,
                "session_key": event.session_key,
                "channel": event.channel,
                "chat_id": event.chat_id,
                "timestamp": _now_iso(),
                "created_at": _now_iso(),
                "recall_memory": {
                    "arguments": dict(cast(Mapping[str, Any], event.arguments)),
                    "status": event.status,
                    "count": len(items),
                    "items": [_compact_item(item) for item in items],
                    "raw_result": payload,
                },
            }
        )

    def _append(self, record: dict[str, Any]) -> None:
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            self._data_path.parent.mkdir(parents=True, exist_ok=True)
            with self._data_path.open("a", encoding="utf-8") as fh:
                _ = fh.write(line + "\n")


async def apply(ctx: Context, config: object) -> None:
    """挂载 default memory inspector listener。"""

    # 1. Core 已用 static service view 决定当前 generation 是否挂载。
    _ = config
    # 2. 正式 runtime 非破坏地接续旧诊断路径，candidate 只看到隔离 workspace。
    data_path = _resolve_recall_data_path(
        data_root=ctx.data_root,
        workspace=ctx.runtime.workspace,
    )
    inspector = DefaultMemoryInspector(data_path)

    # 3. 两个 listener 都由当前 Fiber Effect 持有并逆序清理。
    _ = await ctx.on(CONTEXT_PREPARED_EVENT, inspector.record_context_prepare)
    _ = await ctx.on(TOOL_RESULT, inspector.record_recall_memory)


def is_active(services: ServiceView) -> bool:
    runtime = services.get(MEMORY_RUNTIME)
    return runtime is not None and runtime.name == "default"


def _resolve_recall_data_path(*, data_root: Path, workspace: Path) -> Path:
    """接续旧 JSONL 名字，并拒绝形成两份可写历史。"""

    target = data_root / _RECALL_FILENAME
    # V2_REMOVAL(default-memory-recall-path)：确认全部 runtime 只读写 data_root
    # 且旧 v2 不再可能回滚后，删除 legacy 名字接续分支。
    legacy = workspace / "observe" / _RECALL_FILENAME
    if target.exists():
        if legacy.exists() and not os.path.samefile(target, legacy):
            raise RuntimeError(
                "default_memory recall inspector 新旧路径指向不同文件: "
                f"legacy={legacy} target={target}"
            )
        return target
    if not legacy.exists():
        return target
    try:
        os.link(legacy, target)
    except OSError as error:
        raise RuntimeError(
            "default_memory recall inspector 无法以 hard link 接续旧数据: "
            f"legacy={legacy} target={target}"
        ) from error
    return target


def _turn_id(session_key: str, timestamp: str, content: str) -> str:
    raw = f"{session_key}\n{timestamp}\n{content}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_json(text: str) -> dict[str, Any]:
    try:
        value: object = json.loads(text)
    except json.JSONDecodeError:
        return {"raw": text}
    return cast(dict[str, Any], value) if isinstance(value, dict) else {"raw": value}


def _items_from_block(block: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    section = ""
    for raw_line in block.splitlines():
        line = raw_line.strip()
        if line.startswith("##"):
            section = line.lstrip("#").strip()
            continue
        match = _ITEM_LINE_RE.match(line)
        if not match:
            continue
        item_id, summary = match.groups()
        clean_summary, tags = _split_summary_meta(summary)
        items.append(
            {
                "id": item_id.strip(),
                "summary": clean_summary,
                "tags": tags,
                "section": section,
                "injected": True,
            }
        )
    return items


def _hits_from_trace(trace: Any) -> list[dict[str, Any]]:
    hits = getattr(trace, "hits", None)
    if not isinstance(hits, list):
        return []
    items: list[dict[str, Any]] = []
    for hit in hits:
        item_id = str(getattr(hit, "item_id", "") or "")
        if not item_id:
            continue
        confidence_label = str(getattr(hit, "confidence_label", "") or "")
        tags = [confidence_label] if confidence_label else []
        items.append(
            {
                "id": item_id,
                "summary": _split_summary_meta(str(getattr(hit, "summary", "") or ""))[
                    0
                ],
                "memory_type": str(getattr(hit, "memory_type", "") or ""),
                "score": getattr(hit, "score", None),
                "injected": bool(getattr(hit, "injected", False)),
                "forced": bool(getattr(hit, "forced", False)),
                "tags": tags,
            }
        )
    return items


def _compact_item(item: dict[str, Any]) -> dict[str, Any]:
    summary, tags = _split_summary_meta(str(item.get("summary", "") or ""))
    return {
        "id": str(item.get("id", "") or ""),
        "memory_type": str(item.get("memory_type", "") or ""),
        "summary": summary,
        "tags": tags,
        "happened_at": str(item.get("happened_at", "") or ""),
        "score": item.get("score"),
        "source_ref": str(item.get("source_ref", "") or ""),
    }


def _split_summary_meta(summary: str) -> tuple[str, list[str]]:
    text = summary.strip()
    tags: list[str] = []
    while True:
        match = _META_RE.search(text)
        if match is None:
            return text, tags
        for part in match.group("meta").split("；"):
            label = part.strip()
            if label.startswith("(src:") or label.startswith("src:"):
                continue
            if label == "证据: 可回源原文":
                label = "可回源原文"
            elif label == "证据: 记忆摘要":
                label = "记忆摘要"
            if label and label not in tags:
                tags.append(label)
        text = text[: match.start()].strip()


def _jsonable(value: Any) -> Any:
    try:
        _ = json.dumps(value, ensure_ascii=False)
        return value
    except TypeError:
        return repr(value)
