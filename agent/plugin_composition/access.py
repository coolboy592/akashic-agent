from __future__ import annotations

import hashlib
import os
import re
import secrets
import stat
from dataclasses import dataclass
from pathlib import Path

from agent.plugin_composition.model import (
    ExternalEffectObservation,
    WriteObservation,
)


class CompositionAudit:
    """Record effects observed at Core-owned capability boundaries."""

    def __init__(self) -> None:
        self._writes: list[WriteObservation] = []
        self._external_effects: list[ExternalEffectObservation] = []

    @property
    def writes(self) -> tuple[WriteObservation, ...]:
        return tuple(self._writes)

    @property
    def external_effects(self) -> tuple[ExternalEffectObservation, ...]:
        return tuple(self._external_effects)

    def record_write(
        self,
        *,
        plugin_id: str,
        operation: str,
        relative_path: str,
        content: bytes,
    ) -> None:
        self._writes.append(
            WriteObservation(
                plugin_id=plugin_id,
                operation=operation,
                relative_path=relative_path,
                sha256=hashlib.sha256(content).hexdigest(),
            )
        )

    def record_external(self, *, kind: str, target: str, outcome: str) -> None:
        self._external_effects.append(
            ExternalEffectObservation(kind=kind, target=target, outcome=outcome)
        )


class ExternalEffectGate:
    """Deny external effects by default and expose every attempt to Core."""

    def __init__(self, audit: CompositionAudit) -> None:
        self._audit = audit

    def authorize(self, *, kind: str, target: str) -> None:
        self._audit.record_external(kind=kind, target=target, outcome="denied")
        raise PermissionError(f"候选插件禁止外部效果: {kind}:{target}")


class PluginDataAccess:
    """Allocate opaque plugin roots beneath one explicit workspace."""

    def __init__(self, workspace: Path, audit: CompositionAudit) -> None:
        self.workspace = workspace.resolve(strict=True)
        self._audit = audit
        self._root = self.workspace / "plugin-data"
        directory_fd = _open_directory_chain(self.workspace, ("plugin-data",))
        os.close(directory_fd)

    def for_plugin(self, plugin_id: str) -> ScopedPluginData:
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", plugin_id) is None:
            raise ValueError(f"插件 ID 不是安全路径段: {plugin_id!r}")
        root = self._root / plugin_id
        directory_fd = _open_directory_chain(
            self.workspace,
            ("plugin-data", plugin_id),
        )
        os.close(directory_fd)
        return ScopedPluginData(plugin_id, self.workspace, root, self._audit)


@dataclass(frozen=True, slots=True)
class ScopedPluginData:
    plugin_id: str
    workspace: Path
    root: Path
    _audit: CompositionAudit

    def read_text(self, relative_path: str) -> str:
        parts = self._relative_parts(relative_path)
        parent_fd = self._open_parent(parts[:-1])
        try:
            file_fd = os.open(
                parts[-1],
                os.O_RDONLY | os.O_NOFOLLOW,
                dir_fd=parent_fd,
            )
            with os.fdopen(file_fd, "r", encoding="utf-8") as stream:
                return stream.read()
        finally:
            os.close(parent_fd)

    def write_text(self, relative_path: str, content: str) -> Path:
        """Atomically write plugin-owned text through the scoped boundary."""

        parts = self._relative_parts(relative_path)
        parent_fd = self._open_parent(parts[:-1])
        try:
            operation = _atomic_write_at(parent_fd, parts[-1], content)
        finally:
            os.close(parent_fd)
        self._audit.record_write(
            plugin_id=self.plugin_id,
            operation=operation,
            relative_path=relative_path,
            content=content.encode("utf-8"),
        )
        return self.root.joinpath(*parts)

    def _relative_parts(self, relative_path: str) -> tuple[str, ...]:
        relative = Path(relative_path)
        if relative.is_absolute() or not relative.parts or ".." in relative.parts:
            raise ValueError(f"插件数据相对路径无效: {relative_path!r}")
        return relative.parts

    def _open_parent(self, relative_parts: tuple[str, ...]) -> int:
        return _open_directory_chain(
            self.workspace,
            ("plugin-data", self.plugin_id, *relative_parts),
        )


def _open_directory_chain(workspace: Path, parts: tuple[str, ...]) -> int:
    """Open or create a directory chain without following scoped symlinks."""

    directory_fd = os.open(workspace, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in parts:
            try:
                next_fd = os.open(
                    part,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=directory_fd,
                )
            except FileNotFoundError:
                os.mkdir(part, dir_fd=directory_fd)
                next_fd = os.open(
                    part,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=directory_fd,
                )
            os.close(directory_fd)
            directory_fd = next_fd
        return directory_fd
    except BaseException:
        os.close(directory_fd)
        raise


def _atomic_write_at(directory_fd: int, name: str, content: str) -> str:
    """Atomically replace one file relative to a bound directory descriptor."""

    try:
        target = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        target = None
    if target is not None and stat.S_ISLNK(target.st_mode):
        raise ValueError(f"插件数据目标不能是符号链接: {name}")

    temporary = f".{name}.{secrets.token_hex(16)}.tmp"
    file_fd = os.open(
        temporary,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW,
        0o666,
        dir_fd=directory_fd,
    )
    try:
        if target is not None:
            os.fchmod(file_fd, stat.S_IMODE(target.st_mode))
        with os.fdopen(file_fd, "w", encoding="utf-8", newline="") as stream:
            file_fd = -1
            _ = stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(
            temporary,
            name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        os.fsync(directory_fd)
    except BaseException:
        try:
            os.unlink(temporary, dir_fd=directory_fd)
        except FileNotFoundError:
            pass
        raise
    finally:
        if file_fd != -1:
            os.close(file_fd)
    return "replace" if target is not None else "create"
