"""Durable outcome state for generation-scoped plugin background jobs."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing, contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Iterator, Mapping, Self


OUTCOMES_DB_FILENAME = "outcomes.sqlite"
_UNSET = object()


class JobOutcomeState(StrEnum):
    """Durable states owned by the background-job outcome ledger."""

    QUEUED = "queued"
    RUNNING = "running"
    CANCELLED = "cancelled"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRY_PENDING = "retry_pending"


class JobOutcomePhase(StrEnum):
    """Recovery phase recorded alongside a job outcome."""

    HANDLER = "handler"
    PROVIDER = "provider"
    DOCUMENTS = "documents"


# Short names keep the state contract easy to use from the host code.
JobState = JobOutcomeState
JobPhase = JobOutcomePhase


class JobOutcomeIdentityError(ValueError):
    """Raised when an invocation reuses an incompatible durable identity."""


class JobOutcomeTransitionError(ValueError):
    """Raised when a requested state or phase transition is not legal."""


class UnknownJobInvocation(KeyError):
    """Raised when a transition or lookup references no durable invocation."""


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} 必须是字符串")
    if not value:
        raise ValueError(f"{field} 不能为空")
    if value.strip() != value:
        raise ValueError(f"{field} 不能有首尾空白")
    return value


def _optional_text(value: object, field: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, field)


def _coerce_state(value: JobOutcomeState | str) -> JobOutcomeState:
    if isinstance(value, JobOutcomeState):
        return value
    try:
        return JobOutcomeState(value)
    except ValueError as exc:
        raise ValueError(f"job outcome state 无效: {value!r}") from exc


def _coerce_phase(value: JobOutcomePhase | str) -> JobOutcomePhase:
    if isinstance(value, JobOutcomePhase):
        return value
    try:
        return JobOutcomePhase(value)
    except ValueError as exc:
        raise ValueError(f"job outcome phase 无效: {value!r}") from exc


def _timestamp(value: datetime | None) -> str:
    current = datetime.now(timezone.utc) if value is None else value
    if not isinstance(current, datetime):
        raise TypeError("时间必须是 datetime")
    if current.tzinfo is None:
        raise ValueError("时间必须带时区")
    return current.astimezone(timezone.utc).isoformat()


def _normalize_event_payload(
    value: Mapping[str, object] | None,
    *,
    event_id: str | None = None,
) -> dict[str, object] | None:
    """Validate and copy the small JSON payload retained for event recovery."""

    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError("event_payload 必须是 JSON object")
    payload = dict(value)
    if any(not isinstance(key, str) or not key for key in payload):
        raise TypeError("event_payload 的 key 必须是非空字符串")
    if event_id is not None and "event_id" in payload:
        if payload["event_id"] != event_id:
            raise JobOutcomeIdentityError(
                "event_payload.event_id 必须与 identity.event_id 一致"
            )
    try:
        json.dumps(payload, ensure_ascii=False, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("event_payload 必须可 JSON 序列化") from exc
    return payload


def _encode_event_payload(value: Mapping[str, object] | None) -> str | None:
    payload = _normalize_event_payload(value)
    if payload is None:
        return None
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _decode_event_payload(value: object) -> dict[str, object] | None:
    if value is None:
        return None
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("JobOutcomeLedger 存在损坏的 event_payload_json") from exc
    if not isinstance(decoded, dict):
        raise RuntimeError("JobOutcomeLedger event_payload_json 必须是 JSON object")
    return _normalize_event_payload(decoded)


@dataclass(frozen=True, slots=True)
class JobOutcomeIdentity:
    """Immutable binding and trigger identity captured at first admission."""

    plugin_id: str
    job_name: str
    invocation_id: str
    snapshot_id: str
    plugin_generation_id: str
    model_generation_id: str
    artifact_identity: str
    source_revision: str
    handler_export: str
    lifecycle_revision: str
    api_revision: str
    event_id: str | None = None
    interval_bucket: str | None = None
    semantic_job_id: str | None = None
    event_payload: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        # 1. Validate the immutable binding and semantic key.
        plugin_id = _required_text(self.plugin_id, "plugin_id")
        job_name = _required_text(self.job_name, "job_name")
        _required_text(self.invocation_id, "invocation_id")
        for field, value in (
            ("snapshot_id", self.snapshot_id),
            ("plugin_generation_id", self.plugin_generation_id),
            ("model_generation_id", self.model_generation_id),
            ("artifact_identity", self.artifact_identity),
            ("source_revision", self.source_revision),
            ("handler_export", self.handler_export),
            ("lifecycle_revision", self.lifecycle_revision),
            ("api_revision", self.api_revision),
        ):
            _required_text(value, field)

        # 2. One admission is keyed by exactly one typed event or interval bucket.
        event_id = _optional_text(self.event_id, "event_id")
        interval_bucket = _optional_text(self.interval_bucket, "interval_bucket")
        if (event_id is None) == (interval_bucket is None):
            raise ValueError("必须恰好提供 event_id 或 interval_bucket")

        # 3. A caller may provide the semantic key, but it cannot diverge from its owner.
        semantic_job_id = f"{plugin_id}:{job_name}"
        if self.semantic_job_id is not None:
            supplied = _required_text(self.semantic_job_id, "semantic_job_id")
            if supplied != semantic_job_id:
                raise JobOutcomeIdentityError(
                    "semantic_job_id 必须等于 plugin_id:job_name"
                )
        object.__setattr__(self, "semantic_job_id", semantic_job_id)
        object.__setattr__(self, "event_id", event_id)
        object.__setattr__(self, "interval_bucket", interval_bucket)
        object.__setattr__(
            self,
            "event_payload",
            _normalize_event_payload(self.event_payload, event_id=event_id),
        )

    @property
    def trigger_identity(self) -> str:
        """Return the canonical event or interval identity used for dedupe."""

        if self.event_id is not None:
            return f"event:{self.event_id}"
        assert self.interval_bucket is not None
        return f"interval:{self.interval_bucket}"

    @property
    def generation_id(self) -> str:
        """Expose the plugin generation under the short host-facing name."""

        return self.plugin_generation_id


@dataclass(frozen=True, slots=True)
class JobOutcomeRecord:
    """One durable invocation outcome and its immutable execution identity."""

    semantic_job_id: str
    plugin_id: str
    job_name: str
    invocation_id: str
    event_id: str | None
    interval_bucket: str | None
    snapshot_id: str
    plugin_generation_id: str
    model_generation_id: str
    artifact_identity: str
    source_revision: str
    handler_export: str
    lifecycle_revision: str
    api_revision: str
    attempt: int
    state: JobOutcomeState
    phase: JobOutcomePhase
    error: str | None
    created_at: str
    updated_at: str
    terminal_result_digest: str | None
    event_payload: Mapping[str, object] | None = None

    @property
    def trigger_identity(self) -> str:
        """Return the event or interval key represented by this record."""

        if self.event_id is not None:
            return f"event:{self.event_id}"
        if self.interval_bucket is not None:
            return f"interval:{self.interval_bucket}"
        raise RuntimeError("持久化 outcome 缺少 event/interval identity")

    @property
    def generation_id(self) -> str:
        """Expose the plugin generation under the short host-facing name."""

        return self.plugin_generation_id

    @property
    def artifact_id(self) -> str:
        """Expose the immutable artifact identity under its short name."""

        return self.artifact_identity

    @property
    def result_digest(self) -> str | None:
        """Expose the terminal digest using the concise result name."""

        return self.terminal_result_digest

    @property
    def terminal(self) -> bool:
        """Return whether this outcome can no longer be executed."""

        return self.state in {
            JobOutcomeState.CANCELLED,
            JobOutcomeState.SUCCEEDED,
            JobOutcomeState.FAILED,
        }

    def identity(self) -> JobOutcomeIdentity:
        """Reconstruct the exact immutable identity captured by this record."""

        return JobOutcomeIdentity(
            plugin_id=self.plugin_id,
            job_name=self.job_name,
            invocation_id=self.invocation_id,
            snapshot_id=self.snapshot_id,
            plugin_generation_id=self.plugin_generation_id,
            model_generation_id=self.model_generation_id,
            artifact_identity=self.artifact_identity,
            source_revision=self.source_revision,
            handler_export=self.handler_export,
            lifecycle_revision=self.lifecycle_revision,
            api_revision=self.api_revision,
            event_id=self.event_id,
            interval_bucket=self.interval_bucket,
            semantic_job_id=self.semantic_job_id,
            event_payload=self.event_payload,
        )


JobOutcome = JobOutcomeRecord
JobOutcomeKey = JobOutcomeIdentity


_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS job_outcomes (
        semantic_job_id TEXT NOT NULL,
        plugin_id TEXT NOT NULL,
        job_name TEXT NOT NULL,
        invocation_id TEXT NOT NULL UNIQUE,
        event_id TEXT,
        interval_bucket TEXT,
        snapshot_id TEXT NOT NULL,
        plugin_generation_id TEXT NOT NULL,
        model_generation_id TEXT NOT NULL,
        artifact_identity TEXT NOT NULL,
        source_revision TEXT NOT NULL,
        handler_export TEXT NOT NULL,
        lifecycle_revision TEXT NOT NULL,
        api_revision TEXT NOT NULL,
        attempt INTEGER NOT NULL CHECK (attempt >= 1),
        state TEXT NOT NULL CHECK (
            state IN ('queued', 'running', 'cancelled', 'succeeded', 'failed', 'retry_pending')
        ),
        phase TEXT NOT NULL CHECK (phase IN ('handler', 'provider', 'documents')),
        error TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        terminal_result_digest TEXT,
        event_payload_json TEXT,
        CHECK (semantic_job_id = plugin_id || ':' || job_name),
        CHECK ((event_id IS NOT NULL) != (interval_bucket IS NOT NULL)),
        CHECK (
            (state IN ('queued', 'running')
                AND error IS NULL
                AND terminal_result_digest IS NULL)
            OR (state = 'cancelled' AND terminal_result_digest IS NULL)
            OR (state = 'succeeded'
                AND error IS NULL
                AND terminal_result_digest IS NOT NULL)
            OR (state IN ('failed', 'retry_pending')
                AND error IS NOT NULL
                AND terminal_result_digest IS NULL)
        )
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS job_outcomes_event_identity
    ON job_outcomes(semantic_job_id, event_id)
    WHERE event_id IS NOT NULL
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS job_outcomes_interval_identity
    ON job_outcomes(semantic_job_id, interval_bucket)
    WHERE interval_bucket IS NOT NULL
    """,
)


_IDENTITY_COLUMNS = (
    "semantic_job_id",
    "plugin_id",
    "job_name",
    "invocation_id",
    "event_id",
    "interval_bucket",
    "snapshot_id",
    "plugin_generation_id",
    "model_generation_id",
    "artifact_identity",
    "source_revision",
    "handler_export",
    "lifecycle_revision",
    "api_revision",
)


class JobOutcomeLedger:
    """Own durable background-job admission, dedupe, and state transitions."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._initialize()

    @classmethod
    def for_workspace(cls, workspace: str | Path) -> Self:
        """Open the Core-owned outcome database under a workspace runtime root."""

        return cls(Path(workspace) / "runtime" / "plugin-jobs" / OUTCOMES_DB_FILENAME)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self) -> None:
        # 1. Create only the narrow runtime owner path; SQLite errors remain visible.
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            journal_mode = str(connection.execute("PRAGMA journal_mode = WAL").fetchone()[0])
            if journal_mode.lower() != "wal":
                raise RuntimeError(f"JobOutcomeLedger 必须使用 WAL: {journal_mode}")
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version not in (0, 1, 2):
                raise RuntimeError(f"JobOutcomeLedger schema 版本不支持: {version}")
            connection.execute("BEGIN IMMEDIATE")
            try:
                for statement in _SCHEMA:
                    connection.execute(statement)
                columns = {
                    str(row["name"])
                    for row in connection.execute(
                        "PRAGMA table_info(job_outcomes)"
                    ).fetchall()
                }
                if "event_payload_json" not in columns:
                    connection.execute(
                        "ALTER TABLE job_outcomes ADD COLUMN event_payload_json TEXT"
                    )
                connection.execute("PRAGMA user_version = 2")
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()

    @contextmanager
    def _write_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()
        finally:
            connection.close()

    def admit(
        self,
        identity: JobOutcomeIdentity | None = None,
        *,
        plugin_id: str | None = None,
        job_name: str | None = None,
        invocation_id: str | None = None,
        event_id: str | None = None,
        interval_bucket: str | None = None,
        snapshot_id: str | None = None,
        plugin_generation_id: str | None = None,
        model_generation_id: str | None = None,
        artifact_identity: str | None = None,
        source_revision: str | None = None,
        handler_export: str | None = None,
        lifecycle_revision: str | None = None,
        api_revision: str | None = None,
        semantic_job_id: str | None = None,
        event_payload: Mapping[str, object] | None = None,
        now: datetime | None = None,
    ) -> JobOutcomeRecord:
        """Insert one queued invocation or return the existing event admission."""

        # 1. Build the immutable identity before opening a write transaction.
        if identity is not None:
            if any(
                value is not None
                for value in (
                    plugin_id,
                    job_name,
                    invocation_id,
                    event_id,
                    interval_bucket,
                    snapshot_id,
                    plugin_generation_id,
                    model_generation_id,
                    artifact_identity,
                    source_revision,
                    handler_export,
                    lifecycle_revision,
                    api_revision,
                    semantic_job_id,
                )
            ):
                raise TypeError("identity 与显式 admission 字段不能同时提供")
            if event_payload is not None:
                if identity.event_payload is not None:
                    raise TypeError("identity 已包含 event_payload")
                identity = replace(identity, event_payload=event_payload)
        else:
            missing = {
                field: value
                for field, value in (
                    ("plugin_id", plugin_id),
                    ("job_name", job_name),
                    ("invocation_id", invocation_id),
                    ("snapshot_id", snapshot_id),
                    ("plugin_generation_id", plugin_generation_id),
                    ("model_generation_id", model_generation_id),
                    ("artifact_identity", artifact_identity),
                    ("source_revision", source_revision),
                    ("handler_export", handler_export),
                    ("lifecycle_revision", lifecycle_revision),
                    ("api_revision", api_revision),
                )
                if value is None
            }
            if missing:
                raise TypeError(f"admit 缺少字段: {', '.join(missing)}")
            identity = JobOutcomeIdentity(
                plugin_id=plugin_id,  # type: ignore[arg-type]
                job_name=job_name,  # type: ignore[arg-type]
                invocation_id=invocation_id,  # type: ignore[arg-type]
                event_id=event_id,
                interval_bucket=interval_bucket,
                snapshot_id=snapshot_id,  # type: ignore[arg-type]
                plugin_generation_id=plugin_generation_id,  # type: ignore[arg-type]
                model_generation_id=model_generation_id,  # type: ignore[arg-type]
                artifact_identity=artifact_identity,  # type: ignore[arg-type]
                source_revision=source_revision,  # type: ignore[arg-type]
                handler_export=handler_export,  # type: ignore[arg-type]
                lifecycle_revision=lifecycle_revision,  # type: ignore[arg-type]
                api_revision=api_revision,  # type: ignore[arg-type]
                semantic_job_id=semantic_job_id,
                event_payload=event_payload,
            )
        assert identity is not None
        created_at = _timestamp(now)

        # 2. Dedupe by invocation first, then by semantic event/bucket identity.
        with self._write_transaction() as connection:
            invocation_row = connection.execute(
                "SELECT * FROM job_outcomes WHERE invocation_id = ?",
                (identity.invocation_id,),
            ).fetchone()
            if invocation_row is not None:
                existing = self._record_from_row(invocation_row)
                if not _same_identity(existing, identity):
                    raise JobOutcomeIdentityError(
                        f"invocation_id 已绑定另一份 identity: {identity.invocation_id}"
                    )
                return existing

            trigger_column = "event_id" if identity.event_id is not None else "interval_bucket"
            trigger_value = identity.event_id or identity.interval_bucket
            existing_row = connection.execute(
                f"""
                SELECT * FROM job_outcomes
                WHERE semantic_job_id = ? AND {trigger_column} = ?
                """,
                (identity.semantic_job_id, trigger_value),
            ).fetchone()
            if existing_row is not None:
                # The first admission owns the exact binding; later generations cannot rerun it.
                return self._record_from_row(existing_row)

            connection.execute(
                """
                INSERT INTO job_outcomes (
                    semantic_job_id, plugin_id, job_name, invocation_id,
                    event_id, interval_bucket, snapshot_id, plugin_generation_id,
                    model_generation_id, artifact_identity, source_revision,
                    handler_export, lifecycle_revision, api_revision, attempt,
                    state, phase, error, created_at, updated_at,
                    terminal_result_digest, event_payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, NULL, ?, ?, NULL, ?)
                """,
                (
                    identity.semantic_job_id,
                    identity.plugin_id,
                    identity.job_name,
                    identity.invocation_id,
                    identity.event_id,
                    identity.interval_bucket,
                    identity.snapshot_id,
                    identity.plugin_generation_id,
                    identity.model_generation_id,
                    identity.artifact_identity,
                    identity.source_revision,
                    identity.handler_export,
                    identity.lifecycle_revision,
                    identity.api_revision,
                    JobOutcomeState.QUEUED.value,
                    JobOutcomePhase.HANDLER.value,
                    created_at,
                    created_at,
                    _encode_event_payload(identity.event_payload),
                ),
            )
            row = connection.execute(
                "SELECT * FROM job_outcomes WHERE invocation_id = ?",
                (identity.invocation_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError("JobOutcomeLedger admission 未返回已写入记录")
            return self._record_from_row(row)

    def get(self, invocation_id: str) -> JobOutcomeRecord | None:
        """Read one invocation without changing its durable state."""

        invocation_id = _required_text(invocation_id, "invocation_id")
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM job_outcomes WHERE invocation_id = ?",
                (invocation_id,),
            ).fetchone()
        return None if row is None else self._record_from_row(row)

    def require(self, invocation_id: str) -> JobOutcomeRecord:
        """Read one invocation and fail loudly when it is absent."""

        record = self.get(invocation_id)
        if record is None:
            raise UnknownJobInvocation(invocation_id)
        return record

    def find_by_event(
        self,
        *,
        plugin_id: str,
        job_name: str,
        event_id: str | None = None,
        interval_bucket: str | None = None,
    ) -> JobOutcomeRecord | None:
        """Read the first admission for one semantic event or interval bucket."""

        owner = _required_text(plugin_id, "plugin_id")
        name = _required_text(job_name, "job_name")
        if (event_id is None) == (interval_bucket is None):
            raise ValueError("必须恰好提供 event_id 或 interval_bucket")
        column = "event_id" if event_id is not None else "interval_bucket"
        value = _required_text(
            event_id if event_id is not None else interval_bucket,
            column,
        )
        with closing(self._connect()) as connection:
            row = connection.execute(
                f"""
                SELECT * FROM job_outcomes
                WHERE semantic_job_id = ? AND {column} = ?
                """,
                (f"{owner}:{name}", value),
            ).fetchone()
        return None if row is None else self._record_from_row(row)

    def transition(
        self,
        invocation_id: str,
        state: JobOutcomeState | str,
        *,
        phase: JobOutcomePhase | str | None = None,
        attempt: int | None = None,
        model_generation_id: str | None = None,
        error: str | None | object = _UNSET,
        terminal_result_digest: str | None | object = _UNSET,
        now: datetime | None = None,
    ) -> JobOutcomeRecord:
        """Apply one legal state transition in one SQLite transaction."""

        invocation_id = _required_text(invocation_id, "invocation_id")
        target_state = _coerce_state(state)
        updated_at = _timestamp(now)

        # 1. Lock and read the current state before checking the transition graph.
        with self._write_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM job_outcomes WHERE invocation_id = ?",
                (invocation_id,),
            ).fetchone()
            if row is None:
                raise UnknownJobInvocation(invocation_id)
            current = self._record_from_row(row)
            target_phase = current.phase if phase is None else _coerce_phase(phase)
            target_attempt = self._next_attempt(current, target_state, target_phase, attempt)
            target_model_generation_id = current.model_generation_id
            if model_generation_id is not None:
                bound_model_generation_id = _required_text(
                    model_generation_id,
                    "model_generation_id",
                )
                if target_state is not JobOutcomeState.RUNNING:
                    raise JobOutcomeTransitionError(
                        "model generation 只能在进入 running 时绑定"
                    )
                if current.model_generation_id not in {
                    "execution-pending",
                    bound_model_generation_id,
                }:
                    raise JobOutcomeIdentityError(
                        "job outcome 已绑定另一份 model generation"
                    )
                target_model_generation_id = bound_model_generation_id
            target_error = (
                current.error
                if error is _UNSET
                else _optional_text(error, "error")
            )
            target_digest = (
                current.terminal_result_digest
                if terminal_result_digest is _UNSET
                else _optional_text(terminal_result_digest, "terminal_result_digest")
            )
            self._validate_transition(
                current,
                target_state,
                target_phase,
                target_attempt,
            )

            if target_state in (JobOutcomeState.RUNNING, JobOutcomeState.SUCCEEDED):
                if error is not _UNSET and target_error is not None:
                    raise JobOutcomeTransitionError(
                        f"{target_state.value} 不得保留 error"
                    )
                target_error = None
            if target_state is JobOutcomeState.RUNNING:
                if (
                    terminal_result_digest is not _UNSET
                    and target_digest is not None
                ):
                    raise JobOutcomeTransitionError(
                        "running 不得保留 terminal_result_digest"
                    )
                target_digest = None
            JobOutcomeLedger._validate_outcome_fields(
                target_state,
                target_error,
                target_digest,
            )

            updated = connection.execute(
                """
                UPDATE job_outcomes
                SET attempt = ?, state = ?, phase = ?, error = ?,
                    updated_at = ?, terminal_result_digest = ?,
                    model_generation_id = ?
                WHERE invocation_id = ? AND state = ?
                """,
                (
                    target_attempt,
                    target_state.value,
                    target_phase.value,
                    target_error,
                    updated_at,
                    target_digest,
                    target_model_generation_id,
                    invocation_id,
                    current.state.value,
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError("JobOutcomeLedger transition 未更新唯一当前记录")
            updated_row = connection.execute(
                "SELECT * FROM job_outcomes WHERE invocation_id = ?",
                (invocation_id,),
            ).fetchone()
            if updated_row is None:
                raise RuntimeError("JobOutcomeLedger transition 未返回已更新记录")
            return self._record_from_row(updated_row)

    def list_pending(self) -> tuple[JobOutcomeRecord, ...]:
        """Read queued/running/retry-pending outcomes for restart recovery."""

        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT * FROM job_outcomes
                WHERE state IN ('queued', 'running', 'retry_pending')
                ORDER BY created_at, invocation_id
                """
            ).fetchall()
        return tuple(self._record_from_row(row) for row in rows)

    def list_all(self) -> tuple[JobOutcomeRecord, ...]:
        """Read every durable outcome in deterministic creation order."""

        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM job_outcomes ORDER BY created_at, invocation_id"
            ).fetchall()
        return tuple(self._record_from_row(row) for row in rows)

    def integrity_check(self) -> None:
        """Fail loudly unless the ledger database passes SQLite integrity checks."""

        with closing(self._connect()) as connection:
            result = connection.execute("PRAGMA integrity_check").fetchone()
            if result is None or str(result[0]) != "ok":
                raise RuntimeError(f"JobOutcomeLedger 完整性检查失败: {result}")
            foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_keys:
            raise RuntimeError(f"JobOutcomeLedger 外键检查失败: {foreign_keys}")

    def close(self) -> None:
        """Keep context-manager compatibility; each operation owns its connection."""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    @staticmethod
    def _record_from_row(row: sqlite3.Row) -> JobOutcomeRecord:
        try:
            state = _coerce_state(str(row["state"]))
            phase = _coerce_phase(str(row["phase"]))
            attempt = int(row["attempt"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("JobOutcomeLedger 存在损坏的 state/phase/attempt") from exc
        record = JobOutcomeRecord(
            semantic_job_id=str(row["semantic_job_id"]),
            plugin_id=str(row["plugin_id"]),
            job_name=str(row["job_name"]),
            invocation_id=str(row["invocation_id"]),
            event_id=None if row["event_id"] is None else str(row["event_id"]),
            interval_bucket=(
                None if row["interval_bucket"] is None else str(row["interval_bucket"])
            ),
            snapshot_id=str(row["snapshot_id"]),
            plugin_generation_id=str(row["plugin_generation_id"]),
            model_generation_id=str(row["model_generation_id"]),
            artifact_identity=str(row["artifact_identity"]),
            source_revision=str(row["source_revision"]),
            handler_export=str(row["handler_export"]),
            lifecycle_revision=str(row["lifecycle_revision"]),
            api_revision=str(row["api_revision"]),
            attempt=attempt,
            state=state,
            phase=phase,
            error=None if row["error"] is None else str(row["error"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            terminal_result_digest=(
                None
                if row["terminal_result_digest"] is None
                else str(row["terminal_result_digest"])
            ),
            event_payload=_decode_event_payload(row["event_payload_json"]),
        )
        try:
            JobOutcomeLedger._validate_outcome_fields(
                record.state,
                record.error,
                record.terminal_result_digest,
            )
        except JobOutcomeTransitionError as exc:
            raise RuntimeError("JobOutcomeLedger 存在损坏的 terminal outcome 字段") from exc
        return record

    @staticmethod
    def _next_attempt(
        current: JobOutcomeRecord,
        target_state: JobOutcomeState,
        target_phase: JobOutcomePhase,
        requested: int | None,
    ) -> int:
        if requested is not None:
            if isinstance(requested, bool) or not isinstance(requested, int):
                raise TypeError("attempt 必须是整数")
            if requested < 1:
                raise ValueError("attempt 必须是正整数")
            return requested
        if (
            current.state is JobOutcomeState.RETRY_PENDING
            and target_state is JobOutcomeState.RUNNING
            and current.phase is not JobOutcomePhase.DOCUMENTS
        ):
            return current.attempt + 1
        return current.attempt

    @staticmethod
    def _validate_transition(
        current: JobOutcomeRecord,
        target_state: JobOutcomeState,
        target_phase: JobOutcomePhase,
        target_attempt: int,
    ) -> None:
        allowed = {
            JobOutcomeState.QUEUED: {
                JobOutcomeState.RUNNING,
                JobOutcomeState.CANCELLED,
            },
            JobOutcomeState.RUNNING: {
                JobOutcomeState.CANCELLED,
                JobOutcomeState.SUCCEEDED,
                JobOutcomeState.FAILED,
                JobOutcomeState.RETRY_PENDING,
            },
            JobOutcomeState.RETRY_PENDING: {
                JobOutcomeState.RUNNING,
                JobOutcomeState.CANCELLED,
                JobOutcomeState.SUCCEEDED,
                JobOutcomeState.FAILED,
            },
            JobOutcomeState.CANCELLED: set(),
            JobOutcomeState.SUCCEEDED: set(),
            JobOutcomeState.FAILED: set(),
        }
        if target_state not in allowed[current.state]:
            raise JobOutcomeTransitionError(
                f"非法 job outcome 状态转移: {current.state.value} -> {target_state.value}"
            )
        if target_attempt < current.attempt:
            raise JobOutcomeTransitionError("attempt 不能回退")
        if current.state is JobOutcomeState.QUEUED and target_phase is not JobOutcomePhase.HANDLER:
            raise JobOutcomeTransitionError("queued 只能以 handler phase 开始 running")
        if target_phase is not current.phase:
            if not (
                current.state is JobOutcomeState.RUNNING
                and target_state is JobOutcomeState.RETRY_PENDING
                and target_phase
                in {
                    JobOutcomePhase.PROVIDER,
                    JobOutcomePhase.DOCUMENTS,
                }
            ):
                raise JobOutcomeTransitionError(
                    "handler phase 只能转入 provider/documents retry phase"
                )
        if current.phase is JobOutcomePhase.DOCUMENTS:
            if target_state in {
                JobOutcomeState.CANCELLED,
                JobOutcomeState.FAILED,
                JobOutcomeState.RUNNING,
            }:
                raise JobOutcomeTransitionError(
                    "documents phase 只能由 Core forward recovery 完成，不能取消/重跑 handler"
                )
            if target_phase is not JobOutcomePhase.DOCUMENTS:
                raise JobOutcomeTransitionError("documents phase 不能退回 handler/provider")
        if target_state is JobOutcomeState.RETRY_PENDING:
            if target_phase not in {
                JobOutcomePhase.HANDLER,
                JobOutcomePhase.PROVIDER,
                JobOutcomePhase.DOCUMENTS,
            }:
                raise JobOutcomeTransitionError("retry_pending phase 无效")
        if (
            current.state is JobOutcomeState.RETRY_PENDING
            and current.phase is not JobOutcomePhase.DOCUMENTS
            and target_state is JobOutcomeState.RUNNING
            and target_phase is not current.phase
        ):
            raise JobOutcomeTransitionError("retry 不能改变 handler/provider phase")

    @staticmethod
    def _validate_outcome_fields(
        state: JobOutcomeState,
        error: str | None,
        terminal_result_digest: str | None,
    ) -> None:
        if state in {JobOutcomeState.QUEUED, JobOutcomeState.RUNNING}:
            if error is not None or terminal_result_digest is not None:
                raise JobOutcomeTransitionError(
                    "queued/running 不得保留 error 或 terminal_result_digest"
                )
            return
        if state is JobOutcomeState.SUCCEEDED:
            if error is not None:
                raise JobOutcomeTransitionError("succeeded 不得保留 error")
            if terminal_result_digest is None:
                raise JobOutcomeTransitionError(
                    "succeeded 必须包含 terminal_result_digest"
                )
            return
        if state in {JobOutcomeState.FAILED, JobOutcomeState.RETRY_PENDING}:
            if error is None:
                raise JobOutcomeTransitionError(
                    f"{state.value} 必须包含 error"
                )
            if terminal_result_digest is not None:
                raise JobOutcomeTransitionError(
                    f"{state.value} 不得包含 terminal_result_digest"
                )
            return
        if terminal_result_digest is not None:
            raise JobOutcomeTransitionError("cancelled 不得包含 terminal_result_digest")


def _same_identity(record: JobOutcomeRecord, identity: JobOutcomeIdentity) -> bool:
    for column in _IDENTITY_COLUMNS:
        if getattr(record, column) == getattr(identity, column):
            continue
        if (
            column == "model_generation_id"
            and identity.model_generation_id == "execution-pending"
            and record.model_generation_id != "execution-pending"
        ):
            continue
        return False
    return True


__all__ = [
    "JobOutcome",
    "JobOutcomeIdentity",
    "JobOutcomeIdentityError",
    "JobOutcomeKey",
    "JobOutcomeLedger",
    "JobOutcomePhase",
    "JobOutcomeRecord",
    "JobOutcomeState",
    "JobOutcomeTransitionError",
    "JobPhase",
    "JobState",
    "OUTCOMES_DB_FILENAME",
    "UnknownJobInvocation",
]
