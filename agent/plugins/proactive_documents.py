"""Durable paired-document recovery primitives for proactive domain effects."""

from __future__ import annotations

import fcntl
import ctypes
import errno
import hashlib
import json
import os
import sqlite3
import secrets
import stat
from collections.abc import Iterator, Mapping, Sequence
from contextlib import closing, contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Protocol, Self


PROACTIVE_CONTEXT = "PROACTIVE_CONTEXT.md"
PROACTIVE_PENDING = "proactive_pending.md"
DOCUMENT_NAMES = (PROACTIVE_CONTEXT, PROACTIVE_PENDING)
_INTENT_VERSION = 1
_INTENT_STATES = frozenset({"prepared", "committing", "aborting"})
_HEX_DIGITS = frozenset("0123456789abcdef")


class ProactiveDocumentsError(RuntimeError):
    """Base error for document intents and receipt fences."""


class DocumentDriftError(ProactiveDocumentsError):
    """Raised when a target document differs from the intent's exact state."""


class DocumentIntentError(ProactiveDocumentsError):
    """Raised when a durable intent is missing, corrupt, or mismatched."""


class ReceiptIdentityError(ProactiveDocumentsError):
    """Raised when a domain receipt does not belong to this invocation."""


class MissingDomainEffectReceipt(ProactiveDocumentsError):
    """Raised when a commit is attempted without a durable domain receipt."""


class ReceiptLookupState(StrEnum):
    """Explicit result states for the domain-effect receipt boundary."""

    FOUND = "found"
    ABSENT = "absent"
    UNAVAILABLE = "unavailable"


class DomainEffectState(StrEnum):
    """Terminal states accepted by the document commit fence."""

    COMMITTED = "committed"


@dataclass(frozen=True, slots=True)
class DomainEffectReceipt:
    """Identify one durable, committed domain effect."""

    effect_id: str
    idempotency_key: str
    state: str
    result_digest: str
    invocation_id: str | None = None
    attempt: int = 1
    _origin: object | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        for field_name, value in (
            ("effect_id", self.effect_id),
            ("idempotency_key", self.idempotency_key),
            ("result_digest", self.result_digest),
        ):
            _required_text(value, field_name)
        state = self.state.value if isinstance(self.state, DomainEffectState) else self.state
        _required_text(state, "state")
        if state != DomainEffectState.COMMITTED.value:
            raise ValueError("DomainEffectReceipt.state 必须是 committed")
        if self.invocation_id is not None:
            _required_text(self.invocation_id, "invocation_id")
        if isinstance(self.attempt, bool) or not isinstance(self.attempt, int):
            raise TypeError("attempt 必须是整数")
        if self.attempt < 1:
            raise ValueError("attempt 必须是正整数")
        object.__setattr__(self, "state", state)

    def validate_for(
        self,
        *,
        invocation_id: str,
        effect_id: str | None,
        idempotency_key: str,
        attempt: int | None = None,
    ) -> None:
        """Reject a receipt that belongs to another invocation or attempt."""

        _required_text(invocation_id, "invocation_id")
        _required_text(idempotency_key, "idempotency_key")
        if self.invocation_id != invocation_id:
            raise ReceiptIdentityError("domain receipt invocation identity 不匹配")
        if effect_id is not None and self.effect_id != effect_id:
            raise ReceiptIdentityError("domain receipt effect identity 不匹配")
        if self.idempotency_key != idempotency_key:
            raise ReceiptIdentityError("domain receipt idempotency key 不匹配")
        if attempt is not None and self.attempt != attempt:
            raise ReceiptIdentityError("domain receipt attempt 不匹配")

    def as_dict(self) -> dict[str, object]:
        """Return the durable JSON representation without the in-memory origin token."""

        if self.invocation_id is None:
            raise ValueError("durable DomainEffectReceipt 必须包含 invocation_id")
        return {
            "effect_id": self.effect_id,
            "idempotency_key": self.idempotency_key,
            "state": self.state,
            "result_digest": self.result_digest,
            "invocation_id": self.invocation_id,
            "attempt": self.attempt,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> Self:
        """Decode and validate one durable receipt object."""

        if not isinstance(value, Mapping):
            raise TypeError("DomainEffectReceipt 必须是 JSON object")
        try:
            return cls(
                effect_id=_required_text(value["effect_id"], "effect_id"),
                idempotency_key=_required_text(value["idempotency_key"], "idempotency_key"),
                state=_required_text(value["state"], "state"),
                result_digest=_required_text(value["result_digest"], "result_digest"),
                invocation_id=_optional_text(value.get("invocation_id"), "invocation_id"),
                attempt=_required_int(value.get("attempt", 1), "attempt"),
            )
        except KeyError as error:
            raise ValueError(f"DomainEffectReceipt 缺少字段: {error.args[0]}") from error


@dataclass(frozen=True, slots=True)
class DomainEffectLookup:
    """Explicit lookup result; ABSENT and UNAVAILABLE are never conflated."""

    state: ReceiptLookupState
    receipt: DomainEffectReceipt | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        state = ReceiptLookupState(self.state)
        if state is ReceiptLookupState.FOUND and self.receipt is None:
            raise ValueError("FOUND lookup 必须包含 receipt")
        if state is not ReceiptLookupState.FOUND and self.receipt is not None:
            raise ValueError("非 FOUND lookup 不能包含 receipt")
        if state is ReceiptLookupState.UNAVAILABLE:
            _required_text(self.error, "lookup error")
        elif self.error is not None:
            raise ValueError("FOUND/ABSENT lookup 不能包含 error")
        object.__setattr__(self, "state", state)


class DomainEffectReceiptLookup(Protocol):
    """Look up durable state and return the Core-issued receipt capability object."""

    def lookup(
        self,
        *,
        invocation_id: str,
        effect_id: str | None,
        idempotency_key: str,
    ) -> DomainEffectLookup: ...


class DomainEffectReceiptStore:
    """SQLite-backed receipt lookup used by tests and a Core-owned adapter."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._origin = object()
        self._issued: dict[tuple[str, str, str], DomainEffectReceipt] = {}
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        return connection

    def _connect_existing(self) -> sqlite3.Connection:
        if not self.path.is_file():
            raise sqlite3.OperationalError(f"domain receipt database missing: {self.path}")
        connection = sqlite3.connect(
            f"{self.path.resolve().as_uri()}?mode=rw",
            timeout=30.0,
            uri=True,
        )
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS domain_effect_receipts (
                    invocation_id TEXT NOT NULL,
                    effect_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    state TEXT NOT NULL,
                    result_digest TEXT NOT NULL,
                    attempt INTEGER NOT NULL CHECK (attempt >= 1),
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (invocation_id, effect_id, idempotency_key)
                )
                """
            )
            connection.commit()

    def record(self, receipt: DomainEffectReceipt) -> DomainEffectReceipt:
        """Insert one receipt idempotently and reject same-key identity drift."""

        if not isinstance(receipt, DomainEffectReceipt):
            raise TypeError("receipt 必须是 DomainEffectReceipt")
        if receipt.invocation_id is None:
            raise ValueError("durable receipt 必须包含 invocation_id")
        payload = receipt.as_dict()
        created_at = datetime.now(timezone.utc).isoformat()
        with closing(self._connect_existing()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """
                    SELECT * FROM domain_effect_receipts
                    WHERE invocation_id = ? AND effect_id = ? AND idempotency_key = ?
                    """,
                    (
                        receipt.invocation_id,
                        receipt.effect_id,
                        receipt.idempotency_key,
                    ),
                ).fetchone()
                if row is not None:
                    existing = self._row_to_receipt(row)
                    if not _same_receipt(existing, receipt):
                        raise ReceiptIdentityError("domain receipt same-key 内容漂移")
                    connection.commit()
                    return self._bind(existing)
                connection.execute(
                    """
                    INSERT INTO domain_effect_receipts (
                        invocation_id, effect_id, idempotency_key, state,
                        result_digest, attempt, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["invocation_id"],
                        payload["effect_id"],
                        payload["idempotency_key"],
                        payload["state"],
                        payload["result_digest"],
                        payload["attempt"],
                        created_at,
                    ),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return self._bind(receipt)

    write = record

    def lookup(
        self,
        *,
        invocation_id: str,
        effect_id: str | None,
        idempotency_key: str,
    ) -> DomainEffectLookup:
        """Return FOUND, ABSENT, or UNAVAILABLE as an explicit durable result."""

        _required_text(invocation_id, "invocation_id")
        _required_text(idempotency_key, "idempotency_key")
        if effect_id is not None:
            _required_text(effect_id, "effect_id")
        try:
            with closing(self._connect_existing()) as connection:
                if effect_id is None:
                    rows = connection.execute(
                        """
                        SELECT * FROM domain_effect_receipts
                        WHERE invocation_id = ? AND idempotency_key = ?
                        """,
                        (invocation_id, idempotency_key),
                    ).fetchall()
                    if len(rows) > 1:
                        raise ReceiptIdentityError(
                            "同一 invocation/idempotency 存在多个 effect receipt"
                        )
                    row = rows[0] if rows else None
                else:
                    row = connection.execute(
                        """
                        SELECT * FROM domain_effect_receipts
                        WHERE invocation_id = ? AND effect_id = ? AND idempotency_key = ?
                        """,
                        (invocation_id, effect_id, idempotency_key),
                    ).fetchone()
        except ReceiptIdentityError:
            raise
        except (OSError, sqlite3.Error) as error:
            return DomainEffectLookup(
                ReceiptLookupState.UNAVAILABLE,
                error=f"domain receipt lookup failed: {type(error).__name__}: {error}",
            )
        if row is None:
            return DomainEffectLookup(ReceiptLookupState.ABSENT)
        return DomainEffectLookup(
            ReceiptLookupState.FOUND,
            receipt=self._bind(self._row_to_receipt(row)),
        )

    def require_authentic(self, receipt: DomainEffectReceipt) -> None:
        """Require a receipt object issued by this store, not a plugin-built copy."""

        if not isinstance(receipt, DomainEffectReceipt) or receipt._origin is not self._origin:
            raise ReceiptIdentityError("domain receipt 不是该 Core adapter 签发的对象")

    def integrity_check(self) -> None:
        """Fail loudly unless the receipt database passes SQLite integrity checks."""

        with closing(self._connect_existing()) as connection:
            result = connection.execute("PRAGMA integrity_check").fetchone()
            if result is None or str(result[0]) != "ok":
                raise RuntimeError(f"domain receipt SQLite integrity_check 失败: {result}")

    def close(self) -> None:
        """Keep context-manager compatibility; each operation owns its connection."""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def _bind(self, receipt: DomainEffectReceipt) -> DomainEffectReceipt:
        if receipt.invocation_id is None:
            raise ValueError("durable receipt 必须包含 invocation_id")
        key = (receipt.invocation_id, receipt.effect_id, receipt.idempotency_key)
        issued = self._issued.get(key)
        if issued is not None:
            if not _same_receipt(issued, receipt):
                raise ReceiptIdentityError("domain receipt same-key 内容漂移")
            return issued
        issued = replace(receipt, _origin=self._origin)
        self._issued[key] = issued
        return issued

    @staticmethod
    def _row_to_receipt(row: sqlite3.Row) -> DomainEffectReceipt:
        return DomainEffectReceipt(
            effect_id=str(row["effect_id"]),
            idempotency_key=str(row["idempotency_key"]),
            state=str(row["state"]),
            result_digest=str(row["result_digest"]),
            invocation_id=str(row["invocation_id"]),
            attempt=int(row["attempt"]),
        )


@dataclass(frozen=True, slots=True)
class ProactiveDocumentDigests:
    """Expected old-state digests; None means the target must be absent."""

    context: str | None = None
    pending: str | None = None

    def __post_init__(self) -> None:
        _optional_digest(self.context, "context digest")
        _optional_digest(self.pending, "pending digest")

    def as_mapping(self) -> dict[str, str | None]:
        return {PROACTIVE_CONTEXT: self.context, PROACTIVE_PENDING: self.pending}

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> Self:
        context_value, pending_value = _document_values(value, "digest")
        return cls(
            context=_optional_digest(context_value, "digest.context"),
            pending=_optional_digest(pending_value, "digest.pending"),
        )


@dataclass(frozen=True, slots=True)
class ProactiveDocumentPair:
    """Complete new bytes for the two Core-owned proactive documents."""

    context: bytes
    pending: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.context, bytes):
            raise TypeError("context new bytes 必须是 bytes")
        if not isinstance(self.pending, bytes):
            raise TypeError("pending new bytes 必须是 bytes")

    def as_mapping(self) -> dict[str, bytes]:
        return {PROACTIVE_CONTEXT: self.context, PROACTIVE_PENDING: self.pending}

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> Self:
        context_value, pending_value = _document_values(value, "content", require_bytes=True)
        return cls(
            context=_required_bytes(context_value, "content.context"),
            pending=_required_bytes(pending_value, "content.pending"),
        )


@dataclass(frozen=True, slots=True)
class ProactiveDocumentIntent:
    """Opaque handle to one fsynced paired-document intent."""

    intent_id: str
    invocation_id: str
    idempotency_key: str
    effect_id: str | None
    path: Path
    expected: ProactiveDocumentDigests
    new_digests: ProactiveDocumentDigests
    state: str
    completed: tuple[str, ...]
    _token: str = field(repr=False)


class DocumentReceiptStatus(StrEnum):
    """Terminal states of a paired-document intent."""

    COMMITTED = "committed"
    ABORTED = "aborted"


@dataclass(frozen=True, slots=True)
class ProactiveDocumentReceipt:
    """Durable terminal result for one paired-document intent."""

    status: DocumentReceiptStatus
    intent_id: str
    invocation_id: str
    idempotency_key: str
    document_digest: str
    effect_id: str | None = None
    effect_result_digest: str | None = None
    created_at: str = ""
    intent_token: str = ""
    intent_effect_id: str | None = None
    intent_expected: ProactiveDocumentDigests | None = None
    intent_new_digests: ProactiveDocumentDigests | None = None
    intent_state: str = ""

    def __post_init__(self) -> None:
        status = DocumentReceiptStatus(self.status)
        for field_name, value in (
            ("intent_id", self.intent_id),
            ("invocation_id", self.invocation_id),
            ("idempotency_key", self.idempotency_key),
            ("document_digest", self.document_digest),
        ):
            _required_text(value, field_name)
        if self.effect_id is not None:
            _required_text(self.effect_id, "effect_id")
        if self.effect_result_digest is not None:
            _required_text(self.effect_result_digest, "effect_result_digest")
        _required_text(self.created_at, "created_at")
        if _optional_digest(self.document_digest, "document_digest") is None:
            raise ValueError("document_digest 必须是小写 SHA-256 hex digest")
        _required_text(self.intent_token, "intent_token")
        _required_text(self.intent_state, "intent_state")
        if self.intent_state not in _INTENT_STATES:
            raise ValueError("intent_state 无效")
        if self.intent_effect_id is not None:
            _required_text(self.intent_effect_id, "intent_effect_id")
        if self.intent_expected is None or self.intent_new_digests is None:
            raise ValueError("terminal receipt 缺少 intent digests")
        if not isinstance(self.intent_expected, ProactiveDocumentDigests):
            raise TypeError("intent_expected 必须是 ProactiveDocumentDigests")
        if not isinstance(self.intent_new_digests, ProactiveDocumentDigests):
            raise TypeError("intent_new_digests 必须是 ProactiveDocumentDigests")
        if status is DocumentReceiptStatus.COMMITTED:
            if self.effect_id is None or self.effect_result_digest is None:
                raise ValueError("committed terminal receipt 必须包含 effect receipt")
            if self.intent_effect_id != self.effect_id:
                raise ValueError("committed terminal receipt effect identity 不匹配")
            if self.intent_state not in {"prepared", "committing"}:
                raise ValueError("committed terminal receipt intent_state 不匹配")
        elif self.effect_id is not None or self.effect_result_digest is not None:
            raise ValueError("aborted terminal receipt 不得包含 effect receipt")
        elif self.intent_state not in {"prepared", "aborting"}:
            raise ValueError("aborted terminal receipt intent_state 不匹配")
        object.__setattr__(self, "status", status)

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "intent_id": self.intent_id,
            "invocation_id": self.invocation_id,
            "idempotency_key": self.idempotency_key,
            "document_digest": self.document_digest,
            "effect_id": self.effect_id,
            "effect_result_digest": self.effect_result_digest,
            "created_at": self.created_at,
            "intent_token": self.intent_token,
            "intent_effect_id": self.intent_effect_id,
            "intent_expected": self.intent_expected.as_mapping(),
            "intent_new_digests": self.intent_new_digests.as_mapping(),
            "intent_state": self.intent_state,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> Self:
        if not isinstance(value, Mapping):
            raise TypeError("document receipt 必须是 JSON object")
        try:
            return cls(
                status=DocumentReceiptStatus(
                    _required_text(value["status"], "status")
                ),
                intent_id=_required_segment(value["intent_id"], "intent_id"),
                invocation_id=_required_segment(value["invocation_id"], "invocation_id"),
                idempotency_key=_required_text(
                    value["idempotency_key"], "idempotency_key"
                ),
                document_digest=_required_text(
                    value["document_digest"], "document_digest"
                ),
                effect_id=_optional_text(value.get("effect_id"), "effect_id"),
                effect_result_digest=_optional_text(
                    value.get("effect_result_digest"), "effect_result_digest"
                ),
                created_at=_required_text(value["created_at"], "created_at"),
                intent_token=_required_text(value["intent_token"], "intent_token"),
                intent_effect_id=_optional_text(
                    value.get("intent_effect_id"), "intent_effect_id"
                ),
                intent_expected=ProactiveDocumentDigests.from_mapping(
                    _required_mapping(value["intent_expected"], "intent_expected")
                ),
                intent_new_digests=ProactiveDocumentDigests.from_mapping(
                    _required_mapping(value["intent_new_digests"], "intent_new_digests")
                ),
                intent_state=_required_text(value["intent_state"], "intent_state"),
            )
        except KeyError as error:
            raise DocumentIntentError(f"document receipt 缺少字段: {error.args[0]}") from error


class ProactiveDocuments:
    """Own durable pair intents while delegating domain-effect truth to Core."""

    def __init__(
        self,
        documents_root: Path | str,
        invocation_id: str,
        *,
        idempotency_key: str | None = None,
        effect_id: str | None = None,
        receipt_lookup: DomainEffectReceiptLookup | None = None,
        receipt_store: DomainEffectReceiptStore | None = None,
        intent_root: Path | str | None = None,
    ) -> None:
        self.documents_root = Path(documents_root).resolve()
        self.invocation_id = _required_segment(invocation_id, "invocation_id")
        selected_key = (
            f"proactive-documents:{self.invocation_id}"
            if idempotency_key is None
            else idempotency_key
        )
        self.idempotency_key = _required_text(
            selected_key,
            "idempotency_key",
        )
        if effect_id is not None:
            _required_text(effect_id, "effect_id")
        self._effect_identity = effect_id
        if receipt_lookup is not None and receipt_store is not None:
            raise TypeError("receipt_lookup 不能与 receipt_store 同时提供")
        self._receipt_lookup = receipt_lookup if receipt_lookup is not None else receipt_store
        state_root = (
            Path(intent_root).resolve()
            if intent_root is not None
            else self.documents_root / "runtime" / "proactive-documents"
        )
        self._state_root = state_root
        self._intents_root = state_root / "intents"
        self._receipts_root = state_root / "receipts"
        self.documents_root.mkdir(parents=True, exist_ok=True)
        self._intents_root.mkdir(parents=True, exist_ok=True)
        self._receipts_root.mkdir(parents=True, exist_ok=True)
        for path in (self._state_root, self._intents_root, self._receipts_root):
            os.chmod(path, 0o700)
        self._lock_path = state_root / ".pair.lock"

    async def prepare_pair(
        self,
        expected: ProactiveDocumentDigests | Mapping[str, object],
        content: ProactiveDocumentPair | Mapping[str, object],
        *,
        idempotency_key: str | None = None,
        effect_id: str | None = None,
    ) -> ProactiveDocumentIntent:
        """Capture exact old state and complete new bytes in one fsynced intent."""

        expected_digests = _coerce_digests(expected)
        new_pair = _coerce_pair(content)
        if idempotency_key is None:
            key = self.idempotency_key
        else:
            key = _required_text(idempotency_key, "idempotency_key")
            if key != self.idempotency_key:
                raise DocumentIntentError("intent idempotency key identity 不匹配")
        selected_effect = self._effect_id(effect_id)
        with self._pair_lock():
            terminal = self._load_terminal_locked()
            if terminal is not None:
                raise DocumentIntentError("invocation 已有 terminal document receipt")
            final_path = self._intent_path()
            if final_path.exists():
                intent = self._load_intent_locked()
                self._check_intent_request(intent, key=key, effect_id=selected_effect)
                if intent.expected != expected_digests or intent.new_digests != _pair_digests(new_pair):
                    raise DocumentIntentError("同一 invocation 的 intent 内容漂移")
                return intent

            # 1. Fence both targets before writing any durable intent bytes.
            current = {
                name: self._read_document(name)
                for name in DOCUMENT_NAMES
            }
            for name, digest in expected_digests.as_mapping().items():
                _assert_expected(name, current[name], digest)

            # 2. Stage old/new bytes, then publish intent.json as the commit marker.
            token = secrets.token_hex(24)
            temporary = self._intents_root / f".{self.invocation_id}.{token}.tmp"
            temporary.mkdir(mode=0o700)
            old_digests = ProactiveDocumentDigests(
                context=current[PROACTIVE_CONTEXT].digest,
                pending=current[PROACTIVE_PENDING].digest,
            )
            new_digests = _pair_digests(new_pair)
            for name, state in current.items():
                if state.exists:
                    _write_bytes_durable(temporary / "old" / name, state.content)
            for name, data in new_pair.as_mapping().items():
                _write_bytes_durable(temporary / "new" / name, data)
            metadata = _intent_metadata(
                invocation_id=self.invocation_id,
                idempotency_key=key,
                effect_id=selected_effect,
                token=token,
                expected=old_digests,
                new_digests=new_digests,
                state="prepared",
                completed=(),
                created_at=_now(),
            )
            _write_json_durable(temporary / "intent.json", metadata)
            _fsync_directory(temporary)
            os.replace(temporary, self._intent_path())
            _fsync_directory(self._intents_root)
            return self._load_intent_locked()

    async def commit_after(
        self,
        intent: ProactiveDocumentIntent,
        effect_receipt: DomainEffectReceipt,
    ) -> ProactiveDocumentReceipt:
        """Forward-commit both documents only after an exact durable effect receipt."""

        with self._pair_lock():
            loaded = self._validate_intent_object_locked(intent)
            receipt = self._require_receipt(loaded, effect_receipt)
            terminal = self._load_terminal_locked()
            if terminal is not None:
                if terminal.status is not DocumentReceiptStatus.COMMITTED:
                    raise DocumentIntentError("已 abort 的 document intent 不能 commit")
                self._validate_terminal(terminal, loaded)
                self._validate_terminal_effect(terminal, loaded, receipt)
                self._assert_targets_new(loaded)
                if loaded.path.exists():
                    self._remove_intent_locked(loaded)
                return terminal

            # 1. Publish a journal state before the first ordered replacement.
            working = replace(loaded, state="committing", completed=())
            self._write_intent_journal(working)

            # 2. Replace old/absent targets in fixed order; any third state is a fence failure.
            completed: list[str] = []
            for name in DOCUMENT_NAMES:
                current = self._read_document(name)
                old_digest = loaded.expected.as_mapping()[name]
                new_digest = loaded.new_digests.as_mapping()[name]
                if current.digest == new_digest:
                    completed.append(name)
                    self._write_intent_journal(
                        replace(working, completed=tuple(completed))
                    )
                    continue
                _assert_expected(name, current, old_digest)
                staged = self._staged_path(loaded, name)
                _atomic_install(
                    self._document_path(name),
                    staged.read_bytes(),
                    expected_state=current,
                )
                installed = self._read_document(name)
                if installed.digest != new_digest:
                    raise RuntimeError(f"document replace digest 不匹配: {name}")
                completed.append(name)
                self._write_intent_journal(
                    replace(working, completed=tuple(completed))
                )

            self._assert_targets_new(loaded)
            terminal = self._make_terminal(
                loaded,
                status=DocumentReceiptStatus.COMMITTED,
                effect_receipt=receipt,
            )
            self._publish_terminal_and_cleanup(loaded, terminal)
            return terminal

    async def abort_prepared(
        self,
        intent: ProactiveDocumentIntent,
    ) -> None:
        """Abort an intent only when Core explicitly proves no domain receipt exists."""

        with self._pair_lock():
            loaded = self._validate_intent_object_locked(intent)
            terminal = self._load_terminal_locked()
            if terminal is not None:
                if terminal.status is DocumentReceiptStatus.COMMITTED:
                    raise ReceiptIdentityError("已 commit 的 document intent 不能 abort")
                self._validate_terminal(terminal, loaded)
                self._validate_terminal_effect(terminal, loaded, None)
                self._assert_targets_old(loaded)
                if loaded.path.exists():
                    self._remove_intent_locked(loaded)
                return
            self._assert_targets_old_or_new(loaded)
            lookup = self._lookup(loaded)
            if lookup.state is ReceiptLookupState.UNAVAILABLE:
                raise MissingDomainEffectReceipt(lookup.error or "domain receipt 不可用")
            if lookup.state is ReceiptLookupState.FOUND:
                raise ReceiptIdentityError("domain receipt 已存在，必须 forward recovery")

            # 1. The absence result is explicit; verify every target is still old or our own new bytes.
            working = replace(loaded, state="aborting", completed=())
            self._write_intent_journal(working)
            for name in DOCUMENT_NAMES:
                current = self._read_document(name)
                old_digest = loaded.expected.as_mapping()[name]
                new_digest = loaded.new_digests.as_mapping()[name]
                if current.digest == old_digest:
                    continue
                if current.digest != new_digest:
                    raise DocumentDriftError(
                        f"abort 前 document 出现第三方 drift: {name}"
                    )
                old_exists = old_digest is not None
                if old_exists:
                    old_bytes = self._old_path(loaded, name).read_bytes()
                    _atomic_install(
                        self._document_path(name),
                        old_bytes,
                        expected_state=current,
                    )
                else:
                    _atomic_remove(
                        self._document_path(name),
                        expected_state=current,
                    )
            self._assert_targets_old(loaded)
            terminal = self._make_terminal(
                loaded,
                status=DocumentReceiptStatus.ABORTED,
                effect_receipt=None,
            )
            self._publish_terminal_and_cleanup(loaded, terminal)
            return

    async def recover_pending(self) -> tuple[ProactiveDocumentReceipt, ...]:
        """Recover every explicit intent by forwarding or aborting from receipt truth."""

        receipts: list[ProactiveDocumentReceipt] = []
        for intent_id in self.pending_intent_ids():
            intent = self.load_intent(intent_id)
            lookup = self._lookup(intent)
            if lookup.state is ReceiptLookupState.UNAVAILABLE:
                raise MissingDomainEffectReceipt(lookup.error or "domain receipt 不可用")
            if lookup.state is ReceiptLookupState.FOUND:
                assert lookup.receipt is not None
                receipts.append(await self.commit_after(intent, lookup.receipt))
            else:
                await self.abort_prepared(intent)
                terminal = self.load_terminal_receipt()
                if terminal is None:
                    raise DocumentIntentError("abort terminal document receipt 缺失")
                receipts.append(terminal)
        return tuple(receipts)

    def pending_intent_ids(self) -> tuple[str, ...]:
        """List this invocation's complete durable intent directory, if present."""

        with self._pair_lock():
            current: tuple[str, ...] = ()
            for path in sorted(self._intents_root.iterdir(), key=lambda item: item.name):
                if path.name.startswith("."):
                    raise DocumentIntentError(f"intent staging 未完成或非法: {path}")
                if not path.is_dir() or path.is_symlink():
                    raise DocumentIntentError(f"intent 目录非法: {path}")
                if path.name == self.invocation_id:
                    current = (path.name,)
            return current

    def load_intent(self, intent_id: str | None = None) -> ProactiveDocumentIntent:
        """Read and validate one complete intent without changing durable state."""

        requested = (
            self.invocation_id
            if intent_id is None
            else _required_segment(intent_id, "intent_id")
        )
        if requested != self.invocation_id:
            raise DocumentIntentError("intent 不属于当前 invocation-bound documents port")
        with self._pair_lock():
            return self._load_intent_locked()

    def load_terminal_receipt(
        self,
        invocation_id: str | None = None,
    ) -> ProactiveDocumentReceipt | None:
        """Read one durable terminal document receipt without deleting evidence."""

        requested = self.invocation_id if invocation_id is None else _required_segment(
            invocation_id, "invocation_id"
        )
        if requested != self.invocation_id:
            raise DocumentIntentError("receipt 不属于当前 invocation-bound documents port")
        with self._pair_lock():
            return self._load_terminal_locked()

    def _effect_id(self, override: str | None) -> str | None:
        if override is None:
            return self._configured_effect_id
        selected = _required_text(override, "effect_id")
        if self._configured_effect_id is not None and selected != self._configured_effect_id:
            raise DocumentIntentError("intent effect identity 不匹配")
        return selected

    @property
    def _configured_effect_id(self) -> str | None:
        return self._effect_identity

    def _lookup(self, intent: ProactiveDocumentIntent) -> DomainEffectLookup:
        lookup = self._receipt_lookup
        if lookup is None:
            return DomainEffectLookup(
                ReceiptLookupState.UNAVAILABLE,
                error="domain receipt lookup 未绑定",
            )
        result = lookup.lookup(
            invocation_id=intent.invocation_id,
            effect_id=intent.effect_id,
            idempotency_key=intent.idempotency_key,
        )
        if result is None:
            raise ReceiptIdentityError("receipt lookup 不能返回 None，必须区分 ABSENT/UNAVAILABLE")
        if not isinstance(result, DomainEffectLookup):
            raise TypeError("receipt lookup 必须返回 DomainEffectLookup")
        return result

    def _require_receipt(
        self,
        intent: ProactiveDocumentIntent,
        supplied: DomainEffectReceipt,
    ) -> DomainEffectReceipt:
        if not isinstance(supplied, DomainEffectReceipt):
            raise TypeError("effect_receipt 必须是 DomainEffectReceipt")
        # Validate the caller-supplied identity before querying durable state.  A
        # forged object must not be reported as an ordinary ABSENT receipt.
        if isinstance(self._receipt_lookup, DomainEffectReceiptStore):
            self._receipt_lookup.require_authentic(supplied)
        supplied.validate_for(
            invocation_id=intent.invocation_id,
            effect_id=intent.effect_id,
            idempotency_key=intent.idempotency_key,
        )
        lookup = self._lookup(intent)
        if lookup.state is ReceiptLookupState.UNAVAILABLE:
            raise MissingDomainEffectReceipt(lookup.error or "domain receipt 不可用")
        if lookup.state is ReceiptLookupState.ABSENT or lookup.receipt is None:
            raise MissingDomainEffectReceipt("domain receipt 尚不存在，不能 commit_after")
        lookup.receipt.validate_for(
            invocation_id=intent.invocation_id,
            effect_id=intent.effect_id,
            idempotency_key=intent.idempotency_key,
        )
        if supplied != lookup.receipt:
            raise ReceiptIdentityError("传入 receipt 与 durable receipt 值不一致")
        if supplied is not lookup.receipt:
            raise ReceiptIdentityError("传入 receipt 不是 Core-issued capability")
        return lookup.receipt

    def _validate_intent_object_locked(
        self,
        intent: ProactiveDocumentIntent,
    ) -> ProactiveDocumentIntent:
        if not isinstance(intent, ProactiveDocumentIntent):
            raise TypeError("intent 必须是 ProactiveDocumentIntent")
        _validate_intent_phase(intent.state, intent.completed)
        if intent.invocation_id != self.invocation_id:
            raise DocumentIntentError("intent invocation identity 不匹配")
        if intent.idempotency_key != self.idempotency_key:
            raise DocumentIntentError("intent idempotency key 不匹配")
        terminal = self._load_terminal_locked()
        if terminal is not None:
            self._validate_terminal(terminal, intent)
            if intent.path != self._intent_path():
                raise DocumentIntentError("terminal intent path 不匹配")
            return intent
        loaded = self._load_intent_locked()
        if intent._token != loaded._token:
            raise DocumentIntentError("intent token 不匹配")
        if (
            intent.intent_id != loaded.intent_id
            or intent.effect_id != loaded.effect_id
            or intent.path != loaded.path
            or intent.expected != loaded.expected
            or intent.new_digests != loaded.new_digests
        ):
            raise DocumentIntentError("intent 内容已漂移")
        return loaded

    def _load_intent_locked(self) -> ProactiveDocumentIntent:
        path = self._intent_path()
        if not path.is_dir() or path.is_symlink():
            raise DocumentIntentError(f"intent 不存在: {path}")
        try:
            metadata = _read_json(path / "intent.json")
        except (OSError, ValueError, TypeError) as error:
            raise DocumentIntentError(f"intent metadata 无法读取: {path}") from error
        intent = _intent_from_metadata(path, metadata)
        if intent.invocation_id != self.invocation_id:
            raise DocumentIntentError("intent invocation_id 不匹配")
        self._check_intent_request(
            intent,
            key=self.idempotency_key,
            effect_id=self._configured_effect_id,
        )
        self._validate_staged_files(intent)
        return intent

    def _check_intent_request(
        self,
        intent: ProactiveDocumentIntent,
        *,
        key: str,
        effect_id: str | None,
    ) -> None:
        if intent.idempotency_key != key:
            raise DocumentIntentError("intent idempotency key 不匹配")
        if effect_id is not None and intent.effect_id != effect_id:
            raise DocumentIntentError("intent effect identity 不匹配")

    def _validate_staged_files(self, intent: ProactiveDocumentIntent) -> None:
        for name in DOCUMENT_NAMES:
            new_path = self._staged_path(intent, name)
            if not new_path.is_file() or new_path.is_symlink():
                raise DocumentIntentError(f"intent new bytes 缺失: {name}")
            if _digest(new_path.read_bytes()) != intent.new_digests.as_mapping()[name]:
                raise DocumentIntentError(f"intent new bytes digest 漂移: {name}")
            old_path = self._old_path(intent, name)
            old_digest = intent.expected.as_mapping()[name]
            if old_digest is None:
                if old_path.exists():
                    raise DocumentIntentError(f"intent absent old state 却存在 bytes: {name}")
            else:
                if not old_path.is_file() or old_path.is_symlink():
                    raise DocumentIntentError(f"intent old bytes 缺失: {name}")
                if _digest(old_path.read_bytes()) != old_digest:
                    raise DocumentIntentError(f"intent old bytes digest 漂移: {name}")

    def _write_intent_journal(self, intent: ProactiveDocumentIntent) -> None:
        metadata = _intent_metadata(
            invocation_id=intent.invocation_id,
            idempotency_key=intent.idempotency_key,
            effect_id=intent.effect_id,
            token=intent._token,
            expected=intent.expected,
            new_digests=intent.new_digests,
            state=intent.state,
            completed=intent.completed,
            created_at=_now(),
        )
        _write_json_durable(intent.path / "intent.json", metadata)

    def _assert_targets_new(self, intent: ProactiveDocumentIntent) -> None:
        for name, digest in intent.new_digests.as_mapping().items():
            current = self._read_document(name)
            if current.digest != digest:
                raise DocumentDriftError(f"document 未达到 new digest: {name}")

    def _assert_targets_old(self, intent: ProactiveDocumentIntent) -> None:
        for name, digest in intent.expected.as_mapping().items():
            current = self._read_document(name)
            if current.digest != digest:
                raise DocumentDriftError(f"document 未恢复 old digest: {name}")

    def _assert_targets_old_or_new(self, intent: ProactiveDocumentIntent) -> None:
        """Reject third-party document states before choosing abort/forward recovery."""

        expected = intent.expected.as_mapping()
        new_digests = intent.new_digests.as_mapping()
        for name in DOCUMENT_NAMES:
            current = self._read_document(name)
            if current.digest not in {expected[name], new_digests[name]}:
                raise DocumentDriftError(f"abort 前 document 出现第三方 drift: {name}")

    def _make_terminal(
        self,
        intent: ProactiveDocumentIntent,
        *,
        status: DocumentReceiptStatus,
        effect_receipt: DomainEffectReceipt | None,
    ) -> ProactiveDocumentReceipt:
        digest = _pair_digest(intent.new_digests)
        return ProactiveDocumentReceipt(
            status=status,
            intent_id=intent.intent_id,
            invocation_id=intent.invocation_id,
            idempotency_key=intent.idempotency_key,
            document_digest=(digest if status is DocumentReceiptStatus.COMMITTED else _pair_digest(intent.expected)),
            effect_id=None if effect_receipt is None else effect_receipt.effect_id,
            effect_result_digest=(
                None if effect_receipt is None else effect_receipt.result_digest
            ),
            created_at=_now(),
            intent_token=intent._token,
            intent_effect_id=intent.effect_id,
            intent_expected=intent.expected,
            intent_new_digests=intent.new_digests,
            intent_state=intent.state,
        )

    def _publish_terminal_and_cleanup(
        self,
        intent: ProactiveDocumentIntent,
        terminal: ProactiveDocumentReceipt,
    ) -> None:
        receipt_path = self._receipt_path()
        existing = self._load_terminal_locked()
        if existing is not None:
            self._validate_terminal(existing, intent)
            if existing != terminal:
                raise DocumentIntentError("terminal document receipt 内容漂移")
        else:
            _write_json_durable(receipt_path, terminal.as_dict())
        self._remove_intent_locked(intent)

    def _validate_terminal(
        self,
        terminal: ProactiveDocumentReceipt,
        intent: ProactiveDocumentIntent,
    ) -> None:
        _validate_intent_phase(intent.state, intent.completed)
        terminal_phase = (intent.state, intent.completed)
        if terminal.status is DocumentReceiptStatus.COMMITTED:
            accepted_phases = {
                ("prepared", ()),
                ("committing", DOCUMENT_NAMES),
            }
        else:
            accepted_phases = {
                ("prepared", ()),
                ("aborting", ()),
            }
        if terminal_phase not in accepted_phases:
            raise DocumentIntentError("terminal document receipt phase 与 durable state 不匹配")
        if (
            terminal.intent_id != intent.intent_id
            or terminal.invocation_id != intent.invocation_id
            or terminal.idempotency_key != intent.idempotency_key
            or terminal.intent_token != intent._token
            or terminal.intent_effect_id != intent.effect_id
            or terminal.intent_expected != intent.expected
            or terminal.intent_new_digests != intent.new_digests
        ):
            raise DocumentIntentError("terminal document receipt 与原 intent 不匹配")

    def _validate_terminal_effect(
        self,
        terminal: ProactiveDocumentReceipt,
        intent: ProactiveDocumentIntent,
        effect_receipt: DomainEffectReceipt | None,
    ) -> None:
        expected_digest = (
            _pair_digest(intent.new_digests)
            if effect_receipt is not None
            else _pair_digest(intent.expected)
        )
        if terminal.document_digest != expected_digest:
            raise DocumentIntentError("terminal document receipt digest 不匹配")
        if effect_receipt is None:
            if terminal.effect_id is not None or terminal.effect_result_digest is not None:
                raise DocumentIntentError("aborted terminal receipt 不得包含 effect receipt")
        elif (
            terminal.effect_id != effect_receipt.effect_id
            or terminal.effect_result_digest != effect_receipt.result_digest
        ):
            raise DocumentIntentError("terminal document receipt effect 不匹配")

    def _remove_intent_locked(self, intent: ProactiveDocumentIntent) -> None:
        path = intent.path
        if not path.is_dir() or path.is_symlink():
            raise DocumentIntentError(f"intent cleanup path 非法: {path}")
        for name in ("intent.json",):
            (path / name).unlink(missing_ok=True)
        for folder in ("old", "new"):
            directory = path / folder
            if directory.exists():
                for name in DOCUMENT_NAMES:
                    (directory / name).unlink(missing_ok=True)
                directory.rmdir()
        path.rmdir()
        _fsync_directory(self._intents_root)

    def _load_terminal_locked(self) -> ProactiveDocumentReceipt | None:
        path = self._receipt_path()
        if not path.exists():
            return None
        if path.is_symlink() or not path.is_file():
            raise DocumentIntentError(f"terminal receipt path 非法: {path}")
        try:
            terminal = ProactiveDocumentReceipt.from_dict(_read_json(path))
        except (OSError, ValueError, TypeError) as error:
            raise DocumentIntentError(f"terminal document receipt 损坏: {path}") from error
        if (
            terminal.intent_id != self.invocation_id
            or terminal.invocation_id != self.invocation_id
            or terminal.idempotency_key != self.idempotency_key
        ):
            raise DocumentIntentError("terminal document receipt invocation identity 不匹配")
        terminal_digests = (
            terminal.intent_new_digests
            if terminal.status is DocumentReceiptStatus.COMMITTED
            else terminal.intent_expected
        )
        if terminal_digests is None:
            raise DocumentIntentError("terminal document receipt 缺少语义 digest")
        expected_digest = _pair_digest(terminal_digests)
        if terminal.document_digest != expected_digest:
            raise DocumentIntentError("terminal document receipt digest 不匹配")
        return terminal

    def _intent_path(self) -> Path:
        return self._intents_root / self.invocation_id

    def _receipt_path(self) -> Path:
        return self._receipts_root / f"{self.invocation_id}.json"

    def _document_path(self, name: str) -> Path:
        _validate_document_name(name)
        return self.documents_root / name

    def _staged_path(self, intent: ProactiveDocumentIntent, name: str) -> Path:
        _validate_document_name(name)
        return intent.path / "new" / name

    def _old_path(self, intent: ProactiveDocumentIntent, name: str) -> Path:
        _validate_document_name(name)
        return intent.path / "old" / name

    def _read_document(self, name: str) -> "_FileState":
        return _read_path_state(self._document_path(name), f"document {name}")

    @contextmanager
    def _pair_lock(self) -> Iterator[None]:
        no_follow = getattr(os, "O_NOFOLLOW", None)
        if no_follow is None:
            raise DocumentIntentError("pair lock 需要 O_NOFOLLOW")
        try:
            descriptor = os.open(
                self._lock_path,
                os.O_RDWR | os.O_CREAT | no_follow,
                0o600,
            )
        except OSError as error:
            raise DocumentIntentError(f"pair lock 无法安全打开: {self._lock_path}") from error
        try:
            lock_stat = os.fstat(descriptor)
            if not stat.S_ISREG(lock_stat.st_mode):
                raise DocumentIntentError(f"pair lock 必须是 regular file: {self._lock_path}")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)


@dataclass(frozen=True, slots=True)
class _FileState:
    exists: bool
    content: bytes
    device: int | None = None
    inode: int | None = None

    @property
    def digest(self) -> str | None:
        return _digest(self.content) if self.exists else None


def _read_path_state(path: Path, label: str) -> _FileState:
    """Read one regular file without blocking on special files or following links."""

    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        return _FileState(False, b"")
    except OSError as error:
        raise DocumentDriftError(f"{label} lstat 失败") from error
    if stat.S_ISLNK(path_stat.st_mode):
        raise DocumentDriftError(f"{label} 不能是 symlink")
    if not stat.S_ISREG(path_stat.st_mode):
        raise DocumentDriftError(f"{label} 必须是 regular file")
    no_follow = getattr(os, "O_NOFOLLOW", None)
    if no_follow is None:
        raise DocumentDriftError(f"{label} 读取需要 O_NOFOLLOW")
    descriptor = -1
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | no_follow)
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise DocumentDriftError(f"{label} 必须是 regular file")
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = -1
            content = stream.read()
            closed = os.fstat(stream.fileno())
        if (opened.st_dev, opened.st_ino) != (closed.st_dev, closed.st_ino):
            raise DocumentDriftError(f"{label} 在读取期间发生 inode drift")
        return _FileState(True, content, opened.st_dev, opened.st_ino)
    except FileNotFoundError:
        return _FileState(False, b"")
    except DocumentDriftError:
        raise
    except OSError as error:
        raise DocumentDriftError(f"{label} 读取失败") from error
    finally:
        if descriptor != -1:
            os.close(descriptor)


def _same_file_snapshot(current: _FileState, expected: _FileState) -> bool:
    if current.exists != expected.exists or current.digest != expected.digest:
        return False
    if not current.exists:
        return True
    return (current.device, current.inode) == (expected.device, expected.inode)


def _assert_file_snapshot(path: Path, expected: _FileState) -> _FileState:
    current = _read_path_state(path, f"document {path.name}")
    if not _same_file_snapshot(current, expected):
        raise DocumentDriftError(f"document CAS fence 不匹配: {path.name}")
    return current


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} 必须是字符串")
    if not value or value.strip() != value:
        raise ValueError(f"{field_name} 必须是非空且无首尾空白字符串")
    return value


def _required_segment(value: object, field_name: str) -> str:
    text = _required_text(value, field_name)
    if (
        text in {".", ".."}
        or Path(text).is_absolute()
        or "/" in text
        or "\\" in text
        or "\x00" in text
    ):
        raise ValueError(f"{field_name} 必须是单一路径 segment")
    return text


def _optional_text(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, field_name)


def _required_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} 必须是整数")
    return value


def _optional_digest(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in _HEX_DIGITS for character in value
    ):
        raise ValueError(f"{field_name} 必须是小写 SHA-256 hex digest 或 None")
    return value


def _required_bytes(value: object, field_name: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    raise TypeError(f"{field_name} 必须是 bytes")


def _digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _pair_digest(digests: ProactiveDocumentDigests) -> str:
    encoded = json.dumps(digests.as_mapping(), sort_keys=True, separators=(",", ":"))
    return _digest(encoded.encode("utf-8"))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _validate_document_name(name: str) -> None:
    if name not in DOCUMENT_NAMES:
        raise ValueError(f"document name 不在 Core allowlist: {name}")


def _document_values(
    value: Mapping[str, object],
    field_name: str,
    *,
    require_bytes: bool = False,
) -> tuple[object, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} 必须是 mapping")
    aliases = {
        PROACTIVE_CONTEXT: "context",
        PROACTIVE_PENDING: "pending",
        "context": "context",
        "pending": "pending",
    }
    normalized: dict[str, object] = {}
    for key, item in value.items():
        if key not in aliases:
            raise ValueError(f"{field_name} 包含未授权 document: {key}")
        canonical = aliases[key]
        if canonical in normalized:
            raise ValueError(f"{field_name} 包含重复 document: {canonical}")
        normalized[canonical] = item
    if set(normalized) != {"context", "pending"}:
        raise ValueError(f"{field_name} 必须同时包含 context 与 pending")
    if require_bytes:
        for key, item in normalized.items():
            if isinstance(item, bytearray):
                normalized[key] = bytes(item)
            elif not isinstance(item, bytes):
                raise TypeError(f"{field_name}.{key} 必须是 bytes")
    return normalized["context"], normalized["pending"]


def _required_mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} 必须是 JSON object")
    normalized: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise TypeError(f"{field_name} 的 key 必须是字符串")
        normalized[key] = item
    return normalized


def _coerce_digests(
    value: ProactiveDocumentDigests | Mapping[str, object],
) -> ProactiveDocumentDigests:
    if isinstance(value, ProactiveDocumentDigests):
        return value
    return ProactiveDocumentDigests.from_mapping(value)


def _coerce_pair(value: ProactiveDocumentPair | Mapping[str, object]) -> ProactiveDocumentPair:
    if isinstance(value, ProactiveDocumentPair):
        return value
    return ProactiveDocumentPair.from_mapping(value)


def _pair_digests(pair: ProactiveDocumentPair) -> ProactiveDocumentDigests:
    return ProactiveDocumentDigests(
        context=_digest(pair.context),
        pending=_digest(pair.pending),
    )


def _assert_expected(name: str, current: _FileState, expected: str | None) -> None:
    if current.digest != expected:
        raise DocumentDriftError(
            f"document old-state fence 不匹配: {name}: expected={expected} actual={current.digest}"
        )


def _validate_intent_phase(state: str, completed: Sequence[str]) -> None:
    """Validate the small journal state machine without treating phase as identity."""

    if state not in _INTENT_STATES:
        raise DocumentIntentError(f"intent state 无效: {state}")
    completed_tuple = tuple(completed)
    if completed_tuple != DOCUMENT_NAMES[: len(completed_tuple)]:
        raise DocumentIntentError("intent completed 必须是 ordered document prefix")
    if state in {"prepared", "aborting"} and completed_tuple:
        raise DocumentIntentError(f"intent state={state} 不得包含 completed documents")


def _same_receipt(left: DomainEffectReceipt, right: DomainEffectReceipt) -> bool:
    return (
        left.effect_id == right.effect_id
        and left.idempotency_key == right.idempotency_key
        and left.state == right.state
        and left.result_digest == right.result_digest
        and left.invocation_id == right.invocation_id
        and left.attempt == right.attempt
    )


def _intent_metadata(
    *,
    invocation_id: str,
    idempotency_key: str,
    effect_id: str | None,
    token: str,
    expected: ProactiveDocumentDigests,
    new_digests: ProactiveDocumentDigests,
    state: str,
    completed: Sequence[str],
    created_at: str,
) -> dict[str, object]:
    return {
        "version": _INTENT_VERSION,
        "intent_id": invocation_id,
        "invocation_id": invocation_id,
        "idempotency_key": idempotency_key,
        "effect_id": effect_id,
        "token": token,
        "expected": expected.as_mapping(),
        "new_digests": new_digests.as_mapping(),
        "state": state,
        "completed": list(completed),
        "created_at": created_at,
    }


def _intent_from_metadata(
    path: Path,
    value: Mapping[str, object],
) -> ProactiveDocumentIntent:
    if value.get("version") != _INTENT_VERSION:
        raise DocumentIntentError("intent version 不支持")
    for field_name in (
        "intent_id",
        "invocation_id",
        "idempotency_key",
        "token",
        "state",
        "completed",
    ):
        if field_name not in value:
            raise DocumentIntentError(f"intent 缺少字段: {field_name}")
    intent_id = _required_segment(value["intent_id"], "intent_id")
    invocation_id = _required_segment(value["invocation_id"], "invocation_id")
    if intent_id != invocation_id:
        raise DocumentIntentError("intent_id 与 invocation_id 不一致")
    completed_raw = value["completed"]
    if not isinstance(completed_raw, list):
        raise DocumentIntentError("intent completed 字段无效")
    completed: list[str] = []
    for item in completed_raw:
        if not isinstance(item, str) or item not in DOCUMENT_NAMES:
            raise DocumentIntentError("intent completed 字段无效")
        completed.append(item)
    expected = ProactiveDocumentDigests.from_mapping(
        _required_mapping(value.get("expected", {}), "intent expected")
    )
    new_digests = ProactiveDocumentDigests.from_mapping(
        _required_mapping(value.get("new_digests", {}), "intent new_digests")
    )
    state = _required_text(value["state"], "intent state")
    _validate_intent_phase(state, completed)
    return ProactiveDocumentIntent(
        intent_id=intent_id,
        invocation_id=invocation_id,
        idempotency_key=_required_text(value["idempotency_key"], "idempotency_key"),
        effect_id=(
            None
            if value.get("effect_id") is None
            else _required_text(value["effect_id"], "effect_id")
        ),
        path=path,
        expected=expected,
        new_digests=new_digests,
        state=state,
        completed=tuple(completed),
        _token=_required_text(value["token"], "intent token"),
    )


def _write_bytes_durable(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        if descriptor != -1:
            os.close(descriptor)
    _fsync_directory(path.parent)


def _write_json_durable(path: Path, value: Mapping[str, object]) -> None:
    encoded = (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    _write_bytes_durable(temporary, encoded)
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _read_json(path: Path) -> dict[str, object]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("JSON root 必须是 object")
    return raw


def _atomic_install(
    target: Path,
    content: bytes,
    *,
    expected_state: _FileState,
) -> None:
    """Install bytes with an inode/content fence and no-overwrite absent CAS."""

    temporary = target.with_name(f".{target.name}.{secrets.token_hex(8)}.tmp")
    retain_temporary = False
    try:
        _assert_file_snapshot(target, expected_state)
        _write_bytes_durable(temporary, content)
        temporary_state = _read_path_state(temporary, f"temporary document {target.name}")
        _assert_file_snapshot(target, expected_state)
        if not expected_state.exists:
            try:
                os.link(temporary, target, follow_symlinks=False)
            except FileExistsError as error:
                raise DocumentDriftError(
                    f"document absent CAS 被第三方插入: {target.name}"
                ) from error
            _fsync_directory(target.parent)
            installed = _read_path_state(target, f"document {target.name}")
            if not _same_file_snapshot(installed, temporary_state):
                raise DocumentDriftError(f"document absent CAS 安装后发生 drift: {target.name}")
            temporary.unlink()
            _fsync_directory(target.parent)
            return

        _rename_exchange(target, temporary)
        try:
            swapped = _read_path_state(temporary, f"swapped document {target.name}")
            installed = _read_path_state(target, f"document {target.name}")
        except ProactiveDocumentsError:
            try:
                rolled_back = _rollback_exchange_if_target_owned(
                    target,
                    temporary,
                    temporary_state,
                )
            except ProactiveDocumentsError:
                retain_temporary = True
                raise
            if not rolled_back:
                retain_temporary = True
            raise
        if (
            _same_file_snapshot(swapped, expected_state)
            and installed.exists
            and installed.device == temporary_state.device
            and installed.inode == temporary_state.inode
            and installed.digest == _digest(content)
        ):
            temporary.unlink()
            _fsync_directory(target.parent)
            return

        # The exchange itself never overwrites a third-party inode. Roll back
        # only while target still points at our temporary inode; otherwise keep
        # the temporary artifact as evidence instead of deleting foreign bytes.
        if (
            installed.exists
            and installed.device == temporary_state.device
            and installed.inode == temporary_state.inode
        ):
            _rename_exchange(target, temporary)
            restored = _read_path_state(target, f"restored document {target.name}")
            if _same_file_snapshot(restored, expected_state):
                leftover = _read_path_state(temporary, f"CAS evidence {target.name}")
                if leftover.digest == _digest(content):
                    temporary.unlink()
                    _fsync_directory(target.parent)
                else:
                    retain_temporary = True
        else:
            retain_temporary = True
        raise DocumentDriftError(f"document replace CAS fence 失败: {target.name}")
    finally:
        if not retain_temporary:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def _atomic_remove(target: Path, *, expected_state: _FileState) -> None:
    """Remove one regular document through a verified no-overwrite cleanup move."""

    if not expected_state.exists:
        raise DocumentIntentError("absent document 不能执行 remove CAS")
    temporary = target.with_name(f".{target.name}.{secrets.token_hex(8)}.remove.tmp")
    retain_temporary = False
    try:
        _assert_file_snapshot(target, expected_state)
        _write_bytes_durable(temporary, b"")
        marker_state = _read_path_state(temporary, f"temporary remove marker {target.name}")
        _assert_file_snapshot(target, expected_state)
        _rename_exchange(target, temporary)
        try:
            swapped = _read_path_state(temporary, f"swapped document {target.name}")
            installed = _read_path_state(target, f"remove marker {target.name}")
        except ProactiveDocumentsError:
            try:
                rolled_back = _rollback_exchange_if_target_owned(
                    target,
                    temporary,
                    marker_state,
                )
            except ProactiveDocumentsError:
                retain_temporary = True
                raise
            if not rolled_back:
                retain_temporary = True
            raise
        marker_is_current = (
            installed.exists
            and installed.device == marker_state.device
            and installed.inode == marker_state.inode
            and installed.digest == marker_state.digest
        )
        if _same_file_snapshot(swapped, expected_state) and marker_is_current:
            try:
                marker_cleanup = _move_for_cleanup(
                    target,
                    marker_state,
                    label=f"remove marker {target.name}",
                )
            except ProactiveDocumentsError:
                retain_temporary = True
                raise
            try:
                current_after_move = _read_path_state(
                    target,
                    f"document {target.name} after remove move",
                )
            except ProactiveDocumentsError:
                retain_temporary = True
                raise
            if current_after_move.exists:
                retain_temporary = True
                raise DocumentDriftError(
                    f"document remove move 后出现第三方插入: {target.name}"
                )
            marker_cleanup.unlink()
            try:
                old_cleanup = _move_for_cleanup(
                    temporary,
                    expected_state,
                    label=f"removed document {target.name}",
                )
            except ProactiveDocumentsError:
                retain_temporary = True
                raise
            old_cleanup.unlink()
            _fsync_directory(target.parent)
            return

        if marker_is_current:
            try:
                _rename_exchange(target, temporary)
                restored = _read_path_state(target, f"restored document {target.name}")
                leftover = _read_path_state(temporary, f"remove CAS evidence {target.name}")
            except ProactiveDocumentsError:
                retain_temporary = True
                raise
            if _same_file_snapshot(restored, expected_state) and _same_file_snapshot(
                leftover, marker_state
            ):
                temporary.unlink()
                _fsync_directory(target.parent)
            else:
                retain_temporary = True
        else:
            retain_temporary = True
        raise DocumentDriftError(f"document remove CAS fence 失败: {target.name}")
    finally:
        if not retain_temporary:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def _move_for_cleanup(
    source: Path,
    expected_state: _FileState,
    *,
    label: str,
) -> Path:
    """Move a path without overwrite, verify its inode/content, and return cleanup path."""

    cleanup = source.with_name(f".{source.name}.{secrets.token_hex(8)}.cleanup")
    try:
        _rename_noreplace(source, cleanup)
    except FileNotFoundError as error:
        raise DocumentDriftError(f"{label} source 在 CAS 前消失") from error
    except FileExistsError as error:
        raise DocumentDriftError(f"{label} cleanup path 已被占用") from error
    try:
        moved = _read_path_state(cleanup, label)
    except ProactiveDocumentsError as error:
        _restore_cleanup(cleanup, source)
        raise DocumentDriftError(f"{label} 被移走后发生 drift") from error
    if _same_file_snapshot(moved, expected_state):
        return cleanup
    _restore_cleanup(cleanup, source)
    raise DocumentDriftError(f"{label} 被移走的 inode/content drift")


def _rollback_exchange_if_target_owned(
    target: Path,
    temporary: Path,
    expected_installed: _FileState,
) -> bool:
    """Restore the pre-exchange path only while target still owns our installed inode."""

    try:
        installed = _read_path_state(target, f"exchanged document {target.name}")
    except ProactiveDocumentsError:
        return False
    if not _same_file_snapshot(installed, expected_installed):
        return False
    _rename_exchange(target, temporary)
    return True


def _restore_cleanup(cleanup: Path, source: Path) -> None:
    """Restore moved evidence only when the source name is still absent."""

    try:
        _rename_noreplace(cleanup, source)
    except (FileExistsError, FileNotFoundError):
        return


def _rename_exchange(left: Path, right: Path) -> None:
    """Atomically exchange two same-directory paths using Linux renameat2."""

    try:
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = libc.renameat2
    except AttributeError as error:
        raise DocumentIntentError("document CAS 需要 renameat2(RENAME_EXCHANGE)") from error
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        -100,
        os.fsencode(left),
        -100,
        os.fsencode(right),
        2,
    )
    if result != 0:
        error_number = ctypes.get_errno()
        raise DocumentIntentError(
            f"document CAS exchange 失败: {left.name}/{right.name}: errno={error_number}"
        )


def _rename_noreplace(source: Path, target: Path) -> None:
    """Atomically move one path only when the destination name is absent."""

    try:
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = libc.renameat2
    except AttributeError as error:
        raise DocumentIntentError("document cleanup 需要 renameat2(RENAME_NOREPLACE)") from error
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        -100,
        os.fsencode(source),
        -100,
        os.fsencode(target),
        1,
    )
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number == errno.EEXIST:
        raise FileExistsError(error_number, os.strerror(error_number), target)
    if error_number == errno.ENOENT:
        raise FileNotFoundError(error_number, os.strerror(error_number), source)
    raise DocumentIntentError(
        f"document cleanup move 失败: {source.name}/{target.name}: errno={error_number}"
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "DOCUMENT_NAMES",
    "PROACTIVE_CONTEXT",
    "PROACTIVE_PENDING",
    "DocumentDriftError",
    "DocumentIntentError",
    "DocumentReceiptStatus",
    "DomainEffectLookup",
    "DomainEffectReceipt",
    "DomainEffectReceiptLookup",
    "DomainEffectReceiptStore",
    "DomainEffectState",
    "MissingDomainEffectReceipt",
    "ProactiveDocumentDigests",
    "ProactiveDocumentIntent",
    "ProactiveDocumentPair",
    "ProactiveDocumentReceipt",
    "ProactiveDocuments",
    "ProactiveDocumentsError",
    "ReceiptIdentityError",
    "ReceiptLookupState",
]
