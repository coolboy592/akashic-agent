from __future__ import annotations

import json
import hashlib
import os
import stat
from dataclasses import replace
from pathlib import Path

import pytest

import agent.plugins.proactive_documents as documents_module
from agent.plugins.proactive_documents import (
    PROACTIVE_CONTEXT,
    PROACTIVE_PENDING,
    DocumentDriftError,
    DocumentIntentError,
    DocumentReceiptStatus,
    DomainEffectLookup,
    DomainEffectReceipt,
    DomainEffectReceiptStore,
    MissingDomainEffectReceipt,
    ProactiveDocumentDigests,
    ProactiveDocumentPair,
    ProactiveDocuments,
    ReceiptIdentityError,
    ReceiptLookupState,
)


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _seed_documents(root: Path) -> tuple[ProactiveDocumentDigests, ProactiveDocumentPair]:
    root.mkdir(parents=True, exist_ok=True)
    (root / PROACTIVE_CONTEXT).write_bytes(b"old-context\n")
    (root / PROACTIVE_PENDING).write_bytes(b"old-pending\n")
    expected = ProactiveDocumentDigests(
        context=_digest(b"old-context\n"),
        pending=_digest(b"old-pending\n"),
    )
    content = ProactiveDocumentPair(
        context=b"new-context\n",
        pending=b"new-pending\n",
    )
    return expected, content


def _receipt(
    invocation_id: str = "invocation-1",
    *,
    attempt: int = 1,
) -> DomainEffectReceipt:
    return DomainEffectReceipt(
        effect_id="emotion.state",
        idempotency_key=f"key-{invocation_id}",
        state="committed",
        result_digest="effect-result-digest",
        invocation_id=invocation_id,
        attempt=attempt,
    )


def _documents(
    root: Path,
    store: DomainEffectReceiptStore | None,
    *,
    invocation_id: str = "invocation-1",
    effect_id: str | None = "emotion.state",
) -> ProactiveDocuments:
    return ProactiveDocuments(
        root,
        invocation_id,
        idempotency_key=f"key-{invocation_id}",
        effect_id=effect_id,
        receipt_store=store,
    )


def test_domain_effect_receipt_store_is_durable_and_three_state(tmp_path: Path) -> None:
    path = tmp_path / "runtime" / "emotion-effects.sqlite"
    first = DomainEffectReceiptStore(path)
    receipt = first.record(_receipt())
    first.integrity_check()

    restarted = DomainEffectReceiptStore(path)
    found = restarted.lookup(
        invocation_id="invocation-1",
        effect_id="emotion.state",
        idempotency_key="key-invocation-1",
    )
    assert found.state is ReceiptLookupState.FOUND
    assert found.receipt == receipt

    absent = restarted.lookup(
        invocation_id="missing",
        effect_id="emotion.state",
        idempotency_key="missing-key",
    )
    assert absent == DomainEffectLookup(ReceiptLookupState.ABSENT)

    with pytest.raises(ReceiptIdentityError, match="漂移"):
        restarted.record(
            DomainEffectReceipt(
                effect_id="emotion.state",
                idempotency_key="key-invocation-1",
                state="committed",
                result_digest="different",
                invocation_id="invocation-1",
                attempt=1,
            )
        )

    path.unlink()
    unavailable = restarted.lookup(
        invocation_id="invocation-1",
        effect_id="emotion.state",
        idempotency_key="key-invocation-1",
    )
    assert unavailable.state is ReceiptLookupState.UNAVAILABLE


@pytest.mark.parametrize("invocation_id", ["../escape", "/absolute", "a/b", r"a\b", ".", ".."])
def test_invocation_id_must_be_one_path_segment_before_any_state_write(
    tmp_path: Path,
    invocation_id: str,
) -> None:
    documents_root = tmp_path / "documents"
    state_root = tmp_path / "outside-state"
    with pytest.raises(ValueError, match="单一路径 segment"):
        ProactiveDocuments(
            documents_root,
            invocation_id,
            intent_root=state_root,
        )
    assert not documents_root.exists()
    assert not state_root.exists()
    assert not (tmp_path / "escape").exists()


@pytest.mark.asyncio
async def test_prepare_persists_complete_old_and_new_bytes_and_reloads(
    tmp_path: Path,
) -> None:
    expected, content = _seed_documents(tmp_path)
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)

    intent = await documents.prepare_pair(expected, content)
    assert intent.expected == expected
    assert intent.new_digests == ProactiveDocumentDigests(
        context=_digest(content.context),
        pending=_digest(content.pending),
    )
    assert (intent.path / "old" / PROACTIVE_CONTEXT).read_bytes() == b"old-context\n"
    assert (intent.path / "old" / PROACTIVE_PENDING).read_bytes() == b"old-pending\n"
    assert (intent.path / "new" / PROACTIVE_CONTEXT).read_bytes() == content.context
    assert (intent.path / "new" / PROACTIVE_PENDING).read_bytes() == content.pending

    restarted = _documents(tmp_path, DomainEffectReceiptStore(tmp_path / "effects.sqlite"))
    assert restarted.load_intent() == intent
    assert restarted.pending_intent_ids() == ("invocation-1",)


@pytest.mark.asyncio
@pytest.mark.parametrize("lock_kind", ["symlink", "fifo"])
async def test_pair_lock_rejects_symlink_and_fifo_without_following_or_blocking(
    tmp_path: Path,
    lock_kind: str,
) -> None:
    expected, content = _seed_documents(tmp_path)
    documents = _documents(tmp_path, DomainEffectReceiptStore(tmp_path / "effects.sqlite"))
    lock_path = documents._lock_path
    if lock_kind == "symlink":
        target = tmp_path / "outside-lock"
        target.write_bytes(b"outside-lock")
        lock_path.symlink_to(target)
    else:
        os.mkfifo(lock_path)

    with pytest.raises(DocumentIntentError, match="pair lock"):
        await documents.prepare_pair(expected, content)


@pytest.mark.asyncio
@pytest.mark.parametrize("special_kind", ["symlink", "fifo"])
async def test_document_read_rejects_special_file_without_blocking(
    tmp_path: Path,
    special_kind: str,
) -> None:
    expected, content = _seed_documents(tmp_path)
    documents = _documents(tmp_path, DomainEffectReceiptStore(tmp_path / "effects.sqlite"))
    context_path = tmp_path / PROACTIVE_CONTEXT
    context_path.unlink()
    if special_kind == "symlink":
        outside = tmp_path / "outside-document"
        outside.write_bytes(b"outside-document")
        context_path.symlink_to(outside)
    else:
        os.mkfifo(context_path)

    with pytest.raises(DocumentDriftError, match="document"):
        await documents.prepare_pair(expected, content)


@pytest.mark.asyncio
async def test_prepare_fsyncs_each_staged_file_and_intent_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected, content = _seed_documents(tmp_path)
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    original_fsync = documents_module.os.fsync
    calls = 0

    def record_fsync(descriptor: int) -> None:
        nonlocal calls
        calls += 1
        original_fsync(descriptor)

    monkeypatch.setattr(documents_module.os, "fsync", record_fsync)
    await documents.prepare_pair(expected, content)
    # old/new files (4), their directories (4), metadata (2), intent dir/root (2).
    assert calls >= 12


@pytest.mark.asyncio
async def test_pending_scan_stays_bound_to_invocation_identity(tmp_path: Path) -> None:
    expected, content = _seed_documents(tmp_path)
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    first = _documents(tmp_path, store, invocation_id="invocation-1")
    second = _documents(tmp_path, store, invocation_id="invocation-2")
    await first.prepare_pair(expected, content)
    await second.prepare_pair(expected, content)
    assert first.pending_intent_ids() == ("invocation-1",)
    assert second.pending_intent_ids() == ("invocation-2",)


@pytest.mark.asyncio
async def test_commit_after_requires_store_issued_exact_receipt_and_cleans_only_after_terminal(
    tmp_path: Path,
) -> None:
    expected, content = _seed_documents(tmp_path)
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    intent = await documents.prepare_pair(expected, content)

    forged = _receipt()
    with pytest.raises(ReceiptIdentityError, match="签发"):
        await documents.commit_after(intent, forged)
    assert intent.path.exists()
    assert (tmp_path / PROACTIVE_CONTEXT).read_bytes() == b"old-context\n"

    receipt = store.record(_receipt())
    terminal = await documents.commit_after(intent, receipt)
    assert terminal.status is DocumentReceiptStatus.COMMITTED
    assert (tmp_path / PROACTIVE_CONTEXT).read_bytes() == content.context
    assert (tmp_path / PROACTIVE_PENDING).read_bytes() == content.pending
    assert not intent.path.exists()
    assert documents.load_terminal_receipt() == terminal

    # A second exact call is idempotent, but a foreign receipt remains rejected.
    assert await documents.commit_after(intent, receipt) == terminal
    wrong = DomainEffectReceipt(
        effect_id="other.effect",
        idempotency_key="key-invocation-1",
        state="committed",
        result_digest="effect-result-digest",
        invocation_id="invocation-1",
        attempt=1,
    )
    with pytest.raises(ReceiptIdentityError):
        await documents.commit_after(intent, wrong)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["token", "effect", "expected", "new_digests"])
async def test_terminal_rejects_forged_immutable_intent_handle(
    tmp_path: Path,
    mutation: str,
) -> None:
    expected, content = _seed_documents(tmp_path)
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    intent = await documents.prepare_pair(expected, content)
    receipt = store.record(_receipt())
    await documents.commit_after(intent, receipt)

    if mutation == "token":
        forged = replace(intent, _token="forged-token")
    elif mutation == "effect":
        forged = replace(intent, effect_id="forged.effect")
    elif mutation == "expected":
        forged = replace(
            intent,
            expected=ProactiveDocumentDigests(
                context=_digest(b"forged-context"),
                pending=intent.expected.pending,
            ),
        )
    else:
        forged = replace(
            intent,
            new_digests=ProactiveDocumentDigests(
                context=_digest(b"forged-context"),
                pending=intent.new_digests.pending,
            ),
        )

    with pytest.raises(DocumentIntentError):
        await documents.commit_after(forged, receipt)


@pytest.mark.asyncio
async def test_terminal_rejects_invalid_mutable_phase_without_equating_restart_phase(
    tmp_path: Path,
) -> None:
    expected, content = _seed_documents(tmp_path)
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    intent = await documents.prepare_pair(expected, content)
    receipt = store.record(_receipt())
    await documents.commit_after(intent, receipt)

    forged = replace(intent, state="not-a-journal-phase")
    with pytest.raises(DocumentIntentError, match="state"):
        await documents.commit_after(forged, receipt)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("document_digest", "0" * 64),
        ("effect_id", "forged.effect"),
        ("status", "aborted"),
    ],
)
async def test_load_terminal_receipt_rejects_corrupt_semantics(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    expected, content = _seed_documents(tmp_path)
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    intent = await documents.prepare_pair(expected, content)
    receipt = store.record(_receipt())
    await documents.commit_after(intent, receipt)

    receipt_path = documents._receipt_path()
    payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    payload[field] = value
    receipt_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(DocumentIntentError, match="terminal document receipt"):
        documents.load_terminal_receipt()


@pytest.mark.asyncio
async def test_abort_prepared_requires_explicit_absent_and_restores_old_state(
    tmp_path: Path,
) -> None:
    expected, content = _seed_documents(tmp_path)
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    intent = await documents.prepare_pair(expected, content)

    await documents.abort_prepared(intent)
    terminal = documents.load_terminal_receipt()
    assert terminal is not None
    assert terminal.status is DocumentReceiptStatus.ABORTED
    assert (tmp_path / PROACTIVE_CONTEXT).read_bytes() == b"old-context\n"
    assert (tmp_path / PROACTIVE_PENDING).read_bytes() == b"old-pending\n"
    assert not intent.path.exists()
    assert documents.load_terminal_receipt() == terminal

    unavailable = _UnavailableLookup()
    blocked = ProactiveDocuments(
        tmp_path,
        "invocation-2",
        idempotency_key="key-invocation-2",
        effect_id="emotion.state",
        receipt_lookup=unavailable,
    )
    intent2 = await blocked.prepare_pair(expected, content)
    with pytest.raises(MissingDomainEffectReceipt):
        await blocked.abort_prepared(intent2)
    with pytest.raises(MissingDomainEffectReceipt):
        await blocked.commit_after(intent2, _receipt("invocation-2"))


@pytest.mark.asyncio
async def test_commit_recovery_finishes_partial_ordered_replace_after_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected, content = _seed_documents(tmp_path)
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    intent = await documents.prepare_pair(expected, content)
    receipt = store.record(_receipt())

    original_install = documents_module._atomic_install
    calls = 0

    def crash_before_second_install(
        target: Path,
        data: bytes,
        *,
        expected_state: object,
    ) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("forced crash before second replace")
        original_install(target, data, expected_state=expected_state)

    monkeypatch.setattr(documents_module, "_atomic_install", crash_before_second_install)
    with pytest.raises(RuntimeError, match="forced crash"):
        await documents.commit_after(intent, receipt)
    assert (tmp_path / PROACTIVE_CONTEXT).read_bytes() == content.context
    assert (tmp_path / PROACTIVE_PENDING).read_bytes() == b"old-pending\n"
    assert intent.path.exists()

    monkeypatch.setattr(documents_module, "_atomic_install", original_install)
    restarted = _documents(tmp_path, DomainEffectReceiptStore(tmp_path / "effects.sqlite"))
    recovered = await restarted.recover_pending()
    assert recovered[0].status is DocumentReceiptStatus.COMMITTED
    assert (tmp_path / PROACTIVE_CONTEXT).read_bytes() == content.context
    assert (tmp_path / PROACTIVE_PENDING).read_bytes() == content.pending
    assert not intent.path.exists()


@pytest.mark.asyncio
async def test_commit_recovery_cleans_intent_after_terminal_receipt_survives_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected, content = _seed_documents(tmp_path)
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    intent = await documents.prepare_pair(expected, content)
    receipt = store.record(_receipt())

    original_remove = documents._remove_intent_locked

    def crash_after_terminal_write(current: object) -> None:
        del current
        raise RuntimeError("forced crash after terminal receipt")

    monkeypatch.setattr(documents, "_remove_intent_locked", crash_after_terminal_write)
    with pytest.raises(RuntimeError, match="after terminal receipt"):
        await documents.commit_after(intent, receipt)
    assert documents.load_terminal_receipt() is not None
    assert intent.path.exists()

    monkeypatch.setattr(documents, "_remove_intent_locked", original_remove)
    restarted = _documents(tmp_path, DomainEffectReceiptStore(tmp_path / "effects.sqlite"))
    recovered = await restarted.recover_pending()
    assert recovered == (documents.load_terminal_receipt(),)
    assert not intent.path.exists()


@pytest.mark.asyncio
async def test_commit_exchange_cas_preserves_third_party_insertion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected, content = _seed_documents(tmp_path)
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    intent = await documents.prepare_pair(expected, content)
    receipt = store.record(_receipt())
    original_exchange = documents_module._rename_exchange
    injected = False

    def insert_before_exchange(left: Path, right: Path) -> None:
        nonlocal injected
        if not injected:
            injected = True
            left.write_bytes(b"third-party-before-exchange\n")
        original_exchange(left, right)

    monkeypatch.setattr(documents_module, "_rename_exchange", insert_before_exchange)
    with pytest.raises(DocumentDriftError, match="CAS|drift|fence"):
        await documents.commit_after(intent, receipt)
    assert (tmp_path / PROACTIVE_CONTEXT).read_bytes() == b"third-party-before-exchange\n"
    assert intent.path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("special_kind", ["symlink", "fifo"])
async def test_commit_exchange_window_restores_special_third_party_node(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    special_kind: str,
) -> None:
    expected, content = _seed_documents(tmp_path)
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    intent = await documents.prepare_pair(expected, content)
    receipt = store.record(_receipt())
    context_path = tmp_path / PROACTIVE_CONTEXT
    outside = tmp_path / "outside-special"
    outside.write_bytes(b"outside-special\n")
    original_exchange = documents_module._rename_exchange
    injected = False

    def insert_special_before_exchange(left: Path, right: Path) -> None:
        nonlocal injected
        if left == context_path and not injected:
            injected = True
            left.unlink()
            if special_kind == "symlink":
                left.symlink_to(outside)
            else:
                os.mkfifo(left)
        original_exchange(left, right)

    monkeypatch.setattr(documents_module, "_rename_exchange", insert_special_before_exchange)
    with pytest.raises(DocumentDriftError, match="document|drift|fence"):
        await documents.commit_after(intent, receipt)
    if special_kind == "symlink":
        assert context_path.is_symlink()
        assert outside.read_bytes() == b"outside-special\n"
    else:
        assert stat.S_ISFIFO(context_path.lstat().st_mode)
    assert intent.path.exists()


@pytest.mark.asyncio
async def test_abort_exchange_cas_preserves_third_party_insertion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected, content = _seed_documents(tmp_path)
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    intent = await documents.prepare_pair(expected, content)
    context_path = tmp_path / PROACTIVE_CONTEXT
    context_path.write_bytes(content.context)
    original_exchange = documents_module._rename_exchange
    injected = False

    def insert_before_exchange(left: Path, right: Path) -> None:
        nonlocal injected
        if not injected:
            injected = True
            left.write_bytes(b"third-party-before-abort\n")
        original_exchange(left, right)

    monkeypatch.setattr(documents_module, "_rename_exchange", insert_before_exchange)
    with pytest.raises(DocumentDriftError, match="CAS|drift|fence"):
        await documents.abort_prepared(intent)
    assert context_path.read_bytes() == b"third-party-before-abort\n"
    assert intent.path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("special_kind", ["symlink", "fifo"])
async def test_abort_exchange_window_restores_special_third_party_node(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    special_kind: str,
) -> None:
    pending = b"old-pending\n"
    (tmp_path / PROACTIVE_PENDING).write_bytes(pending)
    expected = ProactiveDocumentDigests(context=None, pending=_digest(pending))
    content = ProactiveDocumentPair(
        context=b"new-context\n",
        pending=b"new-pending\n",
    )
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    intent = await documents.prepare_pair(expected, content)
    context_path = tmp_path / PROACTIVE_CONTEXT
    context_path.write_bytes(content.context)
    outside = tmp_path / "outside-special"
    outside.write_bytes(b"outside-special\n")
    original_exchange = documents_module._rename_exchange
    injected = False

    def insert_special_before_exchange(left: Path, right: Path) -> None:
        nonlocal injected
        if left == context_path and not injected:
            injected = True
            left.unlink()
            if special_kind == "symlink":
                left.symlink_to(outside)
            else:
                os.mkfifo(left)
        original_exchange(left, right)

    monkeypatch.setattr(documents_module, "_rename_exchange", insert_special_before_exchange)
    with pytest.raises(DocumentDriftError, match="document|drift|fence"):
        await documents.abort_prepared(intent)
    if special_kind == "symlink":
        assert context_path.is_symlink()
        assert outside.read_bytes() == b"outside-special\n"
    else:
        assert stat.S_ISFIFO(context_path.lstat().st_mode)
    assert intent.path.exists()


@pytest.mark.asyncio
async def test_abort_remove_move_preserves_insertion_after_marker_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pending = b"old-pending\n"
    (tmp_path / PROACTIVE_PENDING).write_bytes(pending)
    expected = ProactiveDocumentDigests(context=None, pending=_digest(pending))
    content = ProactiveDocumentPair(
        context=b"new-context\n",
        pending=b"new-pending\n",
    )
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    intent = await documents.prepare_pair(expected, content)
    context_path = tmp_path / PROACTIVE_CONTEXT
    context_path.write_bytes(content.context)
    original_move = documents_module._rename_noreplace
    injected = False

    def insert_after_marker_check(source: Path, target: Path) -> None:
        nonlocal injected
        if source == context_path and not injected:
            injected = True
            context_path.write_bytes(b"third-party-after-marker-check\n")
        original_move(source, target)

    monkeypatch.setattr(documents_module, "_rename_noreplace", insert_after_marker_check)
    with pytest.raises(DocumentDriftError, match="CAS|drift|fence"):
        await documents.abort_prepared(intent)
    assert context_path.read_bytes() == b"third-party-after-marker-check\n"
    assert intent.path.exists()


@pytest.mark.asyncio
async def test_abort_absent_old_cas_preserves_third_party_insertion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pending = b"old-pending\n"
    (tmp_path / PROACTIVE_PENDING).write_bytes(pending)
    expected = ProactiveDocumentDigests(context=None, pending=_digest(pending))
    content = ProactiveDocumentPair(
        context=b"new-context\n",
        pending=b"new-pending\n",
    )
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    intent = await documents.prepare_pair(expected, content)
    context_path = tmp_path / PROACTIVE_CONTEXT
    context_path.write_bytes(content.context)
    original_exchange = documents_module._rename_exchange
    injected = False

    def insert_before_exchange(left: Path, right: Path) -> None:
        nonlocal injected
        if not injected:
            injected = True
            left.write_bytes(b"third-party-before-absent-remove\n")
        original_exchange(left, right)

    monkeypatch.setattr(documents_module, "_rename_exchange", insert_before_exchange)
    with pytest.raises(DocumentDriftError, match="CAS|drift|fence"):
        await documents.abort_prepared(intent)
    assert context_path.read_bytes() == b"third-party-before-absent-remove\n"
    assert intent.path.exists()


@pytest.mark.asyncio
async def test_third_party_drift_retains_intent_and_fails_loud(tmp_path: Path) -> None:
    expected, content = _seed_documents(tmp_path)
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    intent = await documents.prepare_pair(expected, content)
    receipt = store.record(_receipt())

    (tmp_path / PROACTIVE_PENDING).write_bytes(b"third-party-change\n")
    with pytest.raises(DocumentDriftError, match="drift|fence"):
        await documents.commit_after(intent, receipt)
    assert intent.path.exists()
    assert documents.load_terminal_receipt() is None
    assert (tmp_path / PROACTIVE_PENDING).read_bytes() == b"third-party-change\n"

    with pytest.raises(DocumentDriftError, match="drift|fence"):
        await documents.abort_prepared(intent)
    assert intent.path.exists()


class _GenericLookup:
    def __init__(self, receipt: DomainEffectReceipt) -> None:
        self.receipt = receipt

    def lookup(
        self,
        *,
        invocation_id: str,
        effect_id: str | None,
        idempotency_key: str,
    ) -> DomainEffectLookup:
        del invocation_id, effect_id, idempotency_key
        return DomainEffectLookup(ReceiptLookupState.FOUND, receipt=self.receipt)


@pytest.mark.asyncio
async def test_generic_lookup_requires_core_issued_receipt_capability(
    tmp_path: Path,
) -> None:
    expected, content = _seed_documents(tmp_path)
    capability = _receipt()
    lookup = _GenericLookup(capability)
    documents = ProactiveDocuments(
        tmp_path,
        "invocation-1",
        idempotency_key="key-invocation-1",
        effect_id="emotion.state",
        receipt_lookup=lookup,
    )
    intent = await documents.prepare_pair(expected, content)
    forged = replace(capability)
    with pytest.raises(ReceiptIdentityError, match="capability"):
        await documents.commit_after(intent, forged)
    terminal = await documents.commit_after(intent, capability)
    assert terminal.status is DocumentReceiptStatus.COMMITTED


class _UnavailableLookup:
    def lookup(
        self,
        *,
        invocation_id: str,
        effect_id: str | None,
        idempotency_key: str,
    ) -> DomainEffectLookup:
        del invocation_id, effect_id, idempotency_key
        return DomainEffectLookup(
            ReceiptLookupState.UNAVAILABLE,
            error="receipt database unavailable",
        )


class _NoneLookup:
    def lookup(
        self,
        *,
        invocation_id: str,
        effect_id: str | None,
        idempotency_key: str,
    ) -> None:
        del invocation_id, effect_id, idempotency_key
        return None


@pytest.mark.asyncio
async def test_none_lookup_result_is_protocol_error_not_absent(tmp_path: Path) -> None:
    expected, content = _seed_documents(tmp_path)
    documents = ProactiveDocuments(
        tmp_path,
        "invocation-1",
        idempotency_key="key-invocation-1",
        effect_id="emotion.state",
        receipt_lookup=_NoneLookup(),
    )
    intent = await documents.prepare_pair(expected, content)
    with pytest.raises(ReceiptIdentityError, match="不能返回 None"):
        await documents.abort_prepared(intent)


@pytest.mark.asyncio
async def test_same_invocation_prepare_is_idempotent_but_content_drift_fails(
    tmp_path: Path,
) -> None:
    expected, content = _seed_documents(tmp_path)
    store = DomainEffectReceiptStore(tmp_path / "effects.sqlite")
    documents = _documents(tmp_path, store)
    first = await documents.prepare_pair(expected, content)
    assert await documents.prepare_pair(expected, content) == first
    with pytest.raises(DocumentIntentError, match="漂移"):
        await documents.prepare_pair(
            expected,
            ProactiveDocumentPair(context=b"different", pending=content.pending),
        )
