"""Typed Core Observe contract for one committed proactive-feedback projection."""

from __future__ import annotations

import math
from dataclasses import dataclass

from agent.plugin_composition import ObserveEventKey


PROACTIVE_FEEDBACK_PREVIEW_MAX_CHARS = 2400


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{field} 必须是非空字符串")
    if value != value.strip():
        raise ValueError(f"{field} 不能有首尾空白")
    return value


def _optional_id(value: object, field: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, field)


def _score(value: object, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} 必须是有限数字或 None")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field} 必须是有限数字")
    return normalized


def _nonnegative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} 必须是非负整数")
    return value


def _preview(value: object, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field} 必须是字符串或 None")
    if len(value) > PROACTIVE_FEEDBACK_PREVIEW_MAX_CHARS:
        raise ValueError(
            f"{field} 超过 {PROACTIVE_FEEDBACK_PREVIEW_MAX_CHARS} 字符上限"
        )
    return value


@dataclass(frozen=True, slots=True)
class ProactiveFeedbackCommitted:
    """Describe one plugin-committed feedback row without owning its database."""

    event_id: str
    session_key: str
    user_message_id: str
    assistant_message_id: str
    proactive_message_id: str | None
    feedback_type: str
    confidence: str
    pa_score: float | None
    pua_score: float | None
    lag_seconds: int | None
    candidate_count: int
    matched_by: str
    reason: str
    user_content_preview: str | None = None
    assistant_content_preview: str | None = None
    proactive_content_preview: str | None = None

    def __post_init__(self) -> None:
        for field, value in (
            ("event_id", self.event_id),
            ("session_key", self.session_key),
            ("user_message_id", self.user_message_id),
            ("assistant_message_id", self.assistant_message_id),
            ("feedback_type", self.feedback_type),
            ("confidence", self.confidence),
            ("matched_by", self.matched_by),
            ("reason", self.reason),
        ):
            _required_text(value, field)
        _optional_id(self.proactive_message_id, "proactive_message_id")
        object.__setattr__(self, "pa_score", _score(self.pa_score, "pa_score"))
        object.__setattr__(self, "pua_score", _score(self.pua_score, "pua_score"))
        if self.lag_seconds is not None:
            object.__setattr__(
                self,
                "lag_seconds",
                _nonnegative_int(self.lag_seconds, "lag_seconds"),
            )
        object.__setattr__(
            self,
            "candidate_count",
            _nonnegative_int(self.candidate_count, "candidate_count"),
        )
        for field in (
            "user_content_preview",
            "assistant_content_preview",
            "proactive_content_preview",
        ):
            object.__setattr__(self, field, _preview(getattr(self, field), field))


PROACTIVE_FEEDBACK_COMMITTED_EVENT: ObserveEventKey[
    ProactiveFeedbackCommitted
] = ObserveEventKey("proactive.feedback.committed")
PROACTIVE_FEEDBACK_COMMITTED = PROACTIVE_FEEDBACK_COMMITTED_EVENT


__all__ = [
    "PROACTIVE_FEEDBACK_COMMITTED",
    "PROACTIVE_FEEDBACK_COMMITTED_EVENT",
    "PROACTIVE_FEEDBACK_PREVIEW_MAX_CHARS",
    "ProactiveFeedbackCommitted",
]
