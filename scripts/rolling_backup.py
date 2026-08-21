#!/usr/bin/env python3
"""为文件、目录和 SQLite 数据库创建可恢复的一致性滚动快照。"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import time
import tomllib
from collections.abc import Iterable
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, cast

DEFAULT_RETENTION = 14


@dataclass(frozen=True)
class BackupSource:
    """描述一个快照内名称、源路径和复制方式。"""

    name: str
    path: Path
    kind: str


@dataclass(frozen=True)
class DirectoryEntry:
    """目录快照中的一个相对路径及其不可变校验事实。"""

    relative_path: str
    kind: str
    mode: int
    size: int
    sha256: str | None = None


def _parse_source(raw: str, *, kind: str) -> BackupSource:
    name, separator, path = raw.partition("=")
    if not separator or not name.strip() or not path.strip():
        raise ValueError(f"源参数必须是 NAME=PATH: {raw!r}")
    source_path = Path(path).expanduser()
    return BackupSource(
        name=_validate_snapshot_name(name.strip()),
        path=(source_path.absolute() if kind == "directory" else source_path.resolve()),
        kind=kind,
    )


def _validate_snapshot_name(name: str) -> str:
    path = PurePosixPath(name)
    if path.is_absolute() or name in {"", "."} or ".." in path.parts:
        raise ValueError(f"快照内名称必须是相对安全路径: {name!r}")
    return str(path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="把文件、目录和 SQLite 数据库备份成滚动快照"
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="TOML 配置文件；使用配置时不再需要逐项传入源路径",
    )
    parser.add_argument(
        "--file",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="复制普通文件，可重复传入",
    )
    parser.add_argument(
        "--sqlite",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="使用 SQLite 在线备份 API 复制数据库，可重复传入",
    )
    parser.add_argument(
        "--directory",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="复制不含符号链接的目录树，可重复传入",
    )
    parser.add_argument("--destination", type=Path)
    parser.add_argument("--retention", type=int)
    return parser.parse_args()


def _config_string(config: dict[str, Any], key: str) -> str:
    value = config.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"备份配置 {key!r} 必须是非空字符串")
    return value.strip()


def _sources_from_config(raw_sources: object) -> list[BackupSource]:
    if not isinstance(raw_sources, list):
        raise ValueError("备份配置 sources 必须是数组")
    sources: list[BackupSource] = []
    for raw in raw_sources:
        if not isinstance(raw, dict):
            raise ValueError("备份配置中的 source 必须是对象")
        kind = _config_string(raw, "kind")
        if kind not in {"file", "sqlite", "directory"}:
            raise ValueError(f"备份配置 source.kind 无效: {kind!r}")
        source_path = Path(_config_string(raw, "path")).expanduser()
        sources.append(
            BackupSource(
                name=_validate_snapshot_name(_config_string(raw, "name")),
                path=(
                    source_path.absolute()
                    if kind == "directory"
                    else source_path.resolve()
                ),
                kind=kind,
            )
        )
    return sources


def _load_config(path: Path) -> tuple[Path, int, list[BackupSource]]:
    """读取通用 TOML 配置并转换成备份计划。"""

    # 1. 读取显式配置，缺失字段直接失败。
    with path.expanduser().open("rb") as handle:
        raw = tomllib.load(handle)
    if not isinstance(raw, dict):
        raise ValueError("备份配置根节点必须是对象")
    destination = Path(_config_string(raw, "destination")).expanduser()
    retention_value = raw.get("retention", DEFAULT_RETENTION)
    if not isinstance(retention_value, int):
        raise ValueError("备份配置 retention 必须是整数")

    # 2. 转换并校验源声明，源的语义完全由配置决定。
    sources = _sources_from_config(raw.get("sources"))
    return destination, retention_value, sources


def _path_is_relative_to(path: Path, parent: Path) -> bool:
    """判断 path 是否位于 parent 内且不越过目录边界。"""

    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _reject_symlink_components(path: Path, *, label: str) -> None:
    """拒绝路径自身及其已存在组件中的符号链接。"""

    absolute = path.absolute()
    current = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        current /= component
        if current.is_symlink():
            raise ValueError(f"拒绝 {label} 中的符号链接: {current}")


def _validate_directory_root(path: Path) -> Path:
    """校验目录源是稳定的真实目录，并返回不跟随链接的绝对路径。"""

    _reject_symlink_components(path, label="目录源")
    if not path.exists():
        raise FileNotFoundError(f"目录源不存在: {path}")
    metadata = path.stat(follow_symlinks=False)
    if not stat.S_ISDIR(metadata.st_mode):
        raise ValueError(f"目录源不是目录: {path}")
    return path.resolve(strict=True)


def _validate_source_name_collisions(sources: Iterable[BackupSource]) -> None:
    """拒绝快照内文件与目录之间的前缀冲突。"""

    paths = sorted(
        (PurePosixPath(source.name) for source in sources),
        key=lambda path: len(path.parts),
    )
    for index, parent in enumerate(paths):
        for child in paths[index + 1 :]:
            if (
                len(child.parts) > len(parent.parts)
                and child.parts[: len(parent.parts)] == parent.parts
            ):
                raise ValueError(
                    f"快照内名称存在文件/目录前缀冲突: {parent} 与 {child}"
                )


def _validate_sources(sources: Iterable[BackupSource]) -> list[BackupSource]:
    validated = list(sources)
    names = [source.name for source in validated]
    if len(names) != len(set(names)):
        raise ValueError("快照内名称不能重复")
    if not validated:
        raise ValueError("至少需要一个 --file、--sqlite 或 --directory 源")
    for source in validated:
        normalized_name = _validate_snapshot_name(source.name)
        if normalized_name != source.name:
            raise ValueError(f"快照源名称必须是规范路径: {source.name!r}")
        if source.name == "manifest.json" or source.name.startswith("manifest.json/"):
            raise ValueError("快照源不能覆盖保留的 manifest.json")
        if source.kind not in {"file", "sqlite", "directory"}:
            raise ValueError(f"未知备份类型: {source.kind}")
        if source.kind == "directory":
            _validate_directory_root(source.path)
            continue
        if not source.path.is_file():
            raise FileNotFoundError(
                f"备份源不存在或不是文件: {source.name} -> {source.path}"
            )
    _validate_source_name_collisions(validated)
    return validated


def _copy_sqlite(source: Path, destination: Path) -> None:
    """使用 SQLite 在线备份 API 生成一致性数据库快照。"""

    source_uri = f"file:{source}?mode=ro"
    with closing(sqlite3.connect(source_uri, uri=True)) as source_db:
        with closing(sqlite3.connect(destination)) as destination_db:
            # 1. 从运行中的数据库复制一致性快照，不直接复制主文件/WAL。
            source_db.backup(destination_db, pages=256, sleep=0.1)

            # 2. 备份完成后验证目标库，损坏时让调用方失败。
            result = destination_db.execute("PRAGMA integrity_check").fetchone()
            if result is None or result[0] != "ok":
                raise sqlite3.DatabaseError(
                    f"备份数据库完整性检查失败: {destination} ({result})"
                )
            destination_db.commit()


def _fsync_file(path: Path) -> None:
    """把一个已写入文件的内容刷入稳定存储。"""

    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    """把目录项更新刷入稳定存储。"""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _copy_regular_file(source: Path, destination: Path) -> tuple[int, str]:
    """复制普通文件并以源前后元数据和逐字节摘要证明复制结果。"""

    before = source.stat(follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode):
        raise ValueError(f"目录源包含非普通文件: {source}")
    shutil.copyfile(source, destination)
    after = source.stat(follow_symlinks=False)
    if (
        before.st_ino != after.st_ino
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or stat.S_IMODE(before.st_mode) != stat.S_IMODE(after.st_mode)
    ):
        raise RuntimeError(f"目录源文件在复制期间发生变化: {source}")
    digest = _sha256(destination)
    if digest != _sha256(source):
        raise RuntimeError(f"目录源文件复制摘要不一致: {source}")
    mode = stat.S_IMODE(after.st_mode)
    os.chmod(destination, mode)
    _fsync_file(destination)
    return after.st_size, digest


def _fsync_tree(root: Path) -> None:
    """Bottom-up fsync every directory entry in a completed tree."""

    directories = [root]
    for current, names, _files in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        for name in names:
            child = current_path / name
            if child.is_symlink():
                raise ValueError(f"拒绝 fsync tree 中的符号链接: {child}")
            directories.append(child)
    for directory in reversed(directories):
        _fsync_directory(directory)


def _scan_directory_entries(source: Path) -> tuple[int, list[DirectoryEntry]]:
    """扫描目录且拒绝符号链接、特殊文件和目录边界逃逸。"""

    root = _validate_directory_root(source)
    root_mode = stat.S_IMODE(root.stat(follow_symlinks=False).st_mode)
    entries: list[DirectoryEntry] = []

    def visit(directory: Path, parent: PurePosixPath) -> None:
        try:
            with os.scandir(directory) as iterator:
                children = sorted(iterator, key=lambda item: item.name)
        except FileNotFoundError as exc:
            raise RuntimeError(f"目录源在扫描期间消失: {directory}") from exc
        for child in children:
            relative = parent / child.name
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError(f"目录源相对路径越界: {relative}")
            item = Path(child.path)
            if child.is_symlink():
                raise ValueError(f"拒绝目录源中的符号链接: {item}")
            try:
                metadata = child.stat(follow_symlinks=False)
            except FileNotFoundError as exc:
                raise RuntimeError(f"目录源在扫描期间消失: {item}") from exc
            mode = stat.S_IMODE(metadata.st_mode)
            if stat.S_ISDIR(metadata.st_mode):
                entries.append(
                    DirectoryEntry(
                        relative_path=relative.as_posix(),
                        kind="directory",
                        mode=mode,
                        size=0,
                    )
                )
                visit(item, relative)
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError(f"目录源包含非普通文件: {item}")
            entries.append(
                DirectoryEntry(
                    relative_path=relative.as_posix(),
                    kind="file",
                    mode=mode,
                    size=metadata.st_size,
                )
            )

    visit(root, PurePosixPath())
    return root_mode, entries


def _directory_entries_equal(
    expected: Iterable[DirectoryEntry], actual: Iterable[DirectoryEntry]
) -> bool:
    """比较目录路径集合和不依赖内容摘要的稳定元数据。"""

    expected_rows = [
        (entry.relative_path, entry.kind, entry.mode, entry.size) for entry in expected
    ]
    actual_rows = [
        (entry.relative_path, entry.kind, entry.mode, entry.size) for entry in actual
    ]
    return expected_rows == actual_rows


def _copy_directory(
    source: BackupSource, snapshot: Path
) -> tuple[int, list[DirectoryEntry]]:
    """复制目录源并返回可恢复的相对路径 manifest 条目。"""

    root = _validate_directory_root(source.path)
    root_mode, scanned = _scan_directory_entries(root)
    destination = snapshot / source.name
    destination.mkdir(parents=True, exist_ok=False, mode=0o700)

    # 1. 按冻结的路径集合复制，目录先创建，文件逐项重新 hash。
    for entry_index, entry in enumerate(scanned):
        source_item = root / Path(entry.relative_path)
        target_item = destination / Path(entry.relative_path)
        if entry.kind == "directory":
            target_item.mkdir(parents=True, exist_ok=False, mode=0o700)
            continue
        target_item.parent.mkdir(parents=True, exist_ok=True)
        size, digest = _copy_regular_file(source_item, target_item)
        if size != entry.size:
            raise RuntimeError(f"目录源文件大小在复制期间发生变化: {source_item}")
        scanned[entry_index] = DirectoryEntry(
            relative_path=entry.relative_path,
            kind=entry.kind,
            mode=entry.mode,
            size=size,
            sha256=digest,
        )

    # 2. 重新扫描，拒绝复制窗口内新增、删除或类型变化的路径。
    final_root_mode, final = _scan_directory_entries(root)
    if final_root_mode != root_mode or not _directory_entries_equal(scanned, final):
        raise RuntimeError(f"目录源在快照期间发生路径或元数据变化: {root}")
    for entry in scanned:
        if entry.kind != "file" or entry.sha256 is None:
            continue
        source_item = root / Path(entry.relative_path)
        if _sha256(source_item) != entry.sha256:
            raise RuntimeError(f"目录源文件内容在快照期间发生变化: {source_item}")
    _apply_directory_modes(destination, root_mode, scanned)
    _fsync_tree(destination)
    return root_mode, scanned


def _apply_directory_modes(
    root: Path,
    root_mode: int,
    entries: Iterable[DirectoryEntry],
) -> None:
    """Apply final directory modes only after all child writes have completed."""

    directories = sorted(
        (entry for entry in entries if entry.kind == "directory"),
        key=lambda entry: len(PurePosixPath(entry.relative_path).parts),
        reverse=True,
    )
    for entry in directories:
        os.chmod(root / Path(entry.relative_path), entry.mode)
    os.chmod(root, root_mode)


def _copy_source(
    source: BackupSource, snapshot: Path
) -> tuple[int | None, list[DirectoryEntry] | None]:
    """复制一种备份源，并返回目录源的相对路径清单。"""

    destination = snapshot / source.name
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.kind == "sqlite":
        _copy_sqlite(source.path, destination)
        _fsync_file(destination)
        return None, None
    if source.kind == "directory":
        return _copy_directory(source, snapshot)
    shutil.copy2(source.path, destination)
    _fsync_file(destination)
    return None, None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _write_manifest(
    snapshot: Path,
    sources: Iterable[BackupSource],
    directory_records: dict[str, tuple[int, list[DirectoryEntry]]] | None = None,
) -> None:
    directory_records = {} if directory_records is None else directory_records
    manifest = {
        "schema_version": 2,
        "created_at": datetime.now().astimezone().isoformat(),
        "files": {
            source.name: {
                "kind": source.kind,
                "source": str(source.path),
                "size": (snapshot / source.name).stat().st_size,
                "mode": stat.S_IMODE((snapshot / source.name).stat().st_mode),
                "sha256": _sha256(snapshot / source.name),
            }
            for source in sources
            if source.kind != "directory"
        },
        "directories": {
            source.name: {
                "kind": source.kind,
                "source": str(source.path),
                "mode": root_mode,
                "entries": [
                    {
                        "relative_path": entry.relative_path,
                        "kind": entry.kind,
                        "mode": entry.mode,
                        "size": entry.size,
                        **(
                            {"sha256": entry.sha256} if entry.sha256 is not None else {}
                        ),
                    }
                    for entry in entries
                ],
            }
            for source in sources
            if source.kind == "directory"
            for root_mode, entries in [directory_records[source.name]]
        },
    }
    manifest_path = snapshot / "manifest.json"
    with manifest_path.open("w", encoding="utf-8") as handle:
        handle.write(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        )
        handle.flush()
    os.chmod(manifest_path, 0o600)
    _fsync_file(manifest_path)


def _load_manifest(snapshot: Path) -> dict[str, Any]:
    """读取并校验快照 manifest 的基本结构。"""

    manifest_path = snapshot / "manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError(f"快照缺少安全的 manifest.json: {snapshot}")
    with manifest_path.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    if not isinstance(manifest, dict):
        raise ValueError("快照 manifest 必须是对象")
    schema_version = manifest.get("schema_version")
    if schema_version is not None and schema_version != 2:
        raise ValueError(f"不支持的快照 manifest schema_version: {schema_version}")
    files = manifest.get("files", {})
    directories = manifest.get("directories", {})
    if not isinstance(files, dict) or not isinstance(directories, dict):
        raise ValueError("快照 manifest 的 files/directories 必须是对象")
    return manifest


def _manifest_mode(value: object, label: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= 0o7777
    ):
        raise ValueError(f"快照 manifest mode 无效: {label}")
    return value


def _manifest_file_records(
    manifest: dict[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    """展开普通文件和目录中的文件记录，保证每个备份路径唯一。"""

    records: list[tuple[str, dict[str, Any]]] = []
    names: set[str] = set()
    files = manifest.get("files", {})
    for name, raw_record in files.items():
        if not isinstance(name, str) or not isinstance(raw_record, dict):
            raise ValueError("快照 manifest 普通文件记录无效")
        _validate_snapshot_name(name)
        if name in names:
            raise ValueError(f"快照 manifest 路径重复: {name}")
        names.add(name)
        records.append((name, raw_record))

    directories = manifest.get("directories", {})
    for directory_name, raw_directory in directories.items():
        if not isinstance(directory_name, str) or not isinstance(raw_directory, dict):
            raise ValueError("快照 manifest 目录记录无效")
        _validate_snapshot_name(directory_name)
        if directory_name in names:
            raise ValueError(f"快照 manifest 路径重复: {directory_name}")
        names.add(directory_name)
        entries = raw_directory.get("entries")
        if not isinstance(entries, list):
            raise ValueError(f"快照 manifest 目录 entries 无效: {directory_name}")
        for raw_entry in entries:
            if not isinstance(raw_entry, dict):
                raise ValueError(f"快照 manifest 目录条目无效: {directory_name}")
            relative_path = raw_entry.get("relative_path")
            kind = raw_entry.get("kind")
            if not isinstance(relative_path, str) or not isinstance(kind, str):
                raise ValueError(
                    f"快照 manifest 目录条目缺少路径/类型: {directory_name}"
                )
            if (
                PurePosixPath(relative_path).is_absolute()
                or ".." in PurePosixPath(relative_path).parts
            ):
                raise ValueError(f"快照 manifest 目录相对路径越界: {relative_path}")
            if relative_path in {"", "."}:
                raise ValueError(f"快照 manifest 目录条目不能指向根: {directory_name}")
            path = f"{directory_name}/{relative_path}"
            if path in names:
                raise ValueError(f"快照 manifest 路径重复: {path}")
            names.add(path)
            if kind == "file":
                records.append((path, raw_entry))
            elif kind != "directory":
                raise ValueError(f"快照 manifest 目录条目类型无效: {kind}")
    return records


def _verify_manifest_file(
    root: Path, relative_path: str, record: dict[str, Any]
) -> None:
    """按 manifest 逐文件校验 regular、mode、size 和 SHA-256。"""

    path = root / Path(relative_path)
    _reject_symlink_components(path, label="快照文件")
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"快照文件不是安全的普通文件: {relative_path}")
    expected_size = record.get("size")
    expected_digest = record.get("sha256")
    if not isinstance(expected_size, int) or expected_size < 0:
        raise ValueError(f"快照文件 size 无效: {relative_path}")
    if not isinstance(expected_digest, str) or len(expected_digest) != 64:
        raise ValueError(f"快照文件 sha256 无效: {relative_path}")
    actual = path.stat(follow_symlinks=False)
    if actual.st_size != expected_size:
        raise ValueError(f"快照文件大小校验失败: {relative_path}")
    expected_mode = record.get("mode")
    if expected_mode is not None and (
        not isinstance(expected_mode, int)
        or stat.S_IMODE(actual.st_mode) != expected_mode
    ):
        raise ValueError(f"快照文件权限校验失败: {relative_path}")
    if _sha256(path) != expected_digest:
        raise ValueError(f"快照文件摘要校验失败: {relative_path}")


def verify_snapshot(snapshot: Path) -> None:
    """精确校验快照中的路径、类型、权限、大小和摘要。"""

    snapshot = snapshot.expanduser().resolve()
    manifest = _load_manifest(snapshot)
    _verify_manifest_tree(manifest, snapshot, include_manifest=True)
    for relative_path, record in _manifest_file_records(manifest):
        _verify_manifest_file(snapshot, relative_path, record)


def _manifest_expected_tree(
    manifest: dict[str, Any],
    *,
    include_manifest: bool,
) -> dict[str, tuple[str, int | None]]:
    """Build the exact relative path/type/mode set described by one manifest."""

    expected: dict[str, tuple[str, int | None]] = {}

    def add_ancestors(path: PurePosixPath) -> None:
        parts = path.parts[:-1]
        for index in range(1, len(parts) + 1):
            expected.setdefault("/".join(parts[:index]), ("directory", None))

    files = manifest.get("files", {})
    for name, record in files.items():
        if not isinstance(name, str) or not isinstance(record, dict):
            raise ValueError("快照 manifest 普通文件记录无效")
        if record.get("kind") not in {"file", "sqlite"}:
            raise ValueError(f"快照 manifest 普通文件 kind 无效: {name}")
        path = PurePosixPath(_validate_snapshot_name(name))
        add_ancestors(path)
        mode = record.get("mode")
        expected[name] = (
            "file",
            _manifest_mode(mode, name) if mode is not None else None,
        )

    directories = manifest.get("directories", {})
    for name, raw_directory in directories.items():
        if not isinstance(name, str) or not isinstance(raw_directory, dict):
            raise ValueError("快照 manifest 目录记录无效")
        root_path = PurePosixPath(_validate_snapshot_name(name))
        if raw_directory.get("kind") != "directory":
            raise ValueError(f"快照 manifest 目录 kind 无效: {name}")
        add_ancestors(root_path)
        root_mode = raw_directory.get("mode")
        expected[name] = (
            "directory",
            _manifest_mode(root_mode, name) if root_mode is not None else None,
        )
        entries = raw_directory.get("entries")
        if not isinstance(entries, list):
            raise ValueError(f"快照 manifest 目录 entries 无效: {name}")
        for raw_entry in entries:
            if not isinstance(raw_entry, dict):
                raise ValueError(f"快照 manifest 目录条目无效: {name}")
            relative = raw_entry.get("relative_path")
            kind = raw_entry.get("kind")
            if not isinstance(relative, str) or kind not in {"file", "directory"}:
                raise ValueError(f"快照 manifest 目录条目无效: {name}")
            path = root_path / PurePosixPath(relative)
            add_ancestors(path)
            mode = raw_entry.get("mode")
            expected[path.as_posix()] = (
                cast(str, kind),
                _manifest_mode(mode, path.as_posix()) if mode is not None else None,
            )
    if include_manifest:
        expected["manifest.json"] = ("file", None)
    return expected


def _verify_manifest_tree(
    manifest: dict[str, Any],
    root: Path,
    *,
    include_manifest: bool,
) -> None:
    """Reject missing, extra, symlink, special, type, and directory-mode drift."""

    expected = _manifest_expected_tree(manifest, include_manifest=include_manifest)
    actual: dict[str, tuple[str, int]] = {}
    for current, names, files in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        for name in sorted(names + files):
            path = current_path / name
            relative = path.relative_to(root).as_posix()
            metadata = path.stat(follow_symlinks=False)
            if path.is_symlink():
                raise ValueError(f"快照树包含符号链接: {relative}")
            if stat.S_ISDIR(metadata.st_mode):
                kind = "directory"
            elif stat.S_ISREG(metadata.st_mode):
                kind = "file"
            else:
                raise ValueError(f"快照树包含特殊文件: {relative}")
            actual[relative] = (kind, stat.S_IMODE(metadata.st_mode))
    if set(actual) != set(expected):
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        raise ValueError(f"快照树路径集合不一致: missing={missing} extra={extra}")
    for path, (expected_kind, expected_mode) in expected.items():
        actual_kind, actual_mode = actual[path]
        if actual_kind != expected_kind:
            raise ValueError(f"快照树类型校验失败: {path}")
        if expected_mode is not None and actual_mode != expected_mode:
            raise ValueError(f"快照树权限校验失败: {path}")


def _copy_restored_file(source: Path, destination: Path, mode: int) -> None:
    """复制恢复文件并固定其权限。"""

    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    os.chmod(destination, mode)
    _fsync_file(destination)


def _restore_file_sources(
    manifest: dict[str, Any], snapshot: Path, temporary: Path
) -> None:
    """恢复 manifest 中的普通文件源。"""

    files = manifest.get("files", {})
    if not isinstance(files, dict):
        raise ValueError("快照 manifest files 必须是对象")
    for name, record in files.items():
        if not isinstance(name, str) or not isinstance(record, dict):
            raise ValueError(f"快照 manifest 普通文件记录无效: {name}")
        mode = record.get("mode")
        source_path = snapshot / Path(name)
        _copy_restored_file(
            source_path,
            temporary / Path(name),
            mode if isinstance(mode, int) else stat.S_IMODE(source_path.stat().st_mode),
        )


def _restore_directory_sources(
    manifest: dict[str, Any], snapshot: Path, temporary: Path
) -> None:
    """恢复 manifest 中的目录源并保留每个目录的权限。"""

    directories = manifest.get("directories", {})
    if not isinstance(directories, dict):
        raise ValueError("快照 manifest directories 必须是对象")
    for directory_name, raw_directory in directories.items():
        if not isinstance(directory_name, str) or not isinstance(raw_directory, dict):
            raise ValueError(f"快照 manifest 目录记录无效: {directory_name}")
        root_target = temporary / Path(directory_name)
        root_target.mkdir(parents=True, exist_ok=False, mode=0o700)
        root_mode = raw_directory.get("mode")
        if not isinstance(root_mode, int):
            raise ValueError(f"快照 manifest 目录 mode 无效: {directory_name}")
        restored_entries: list[DirectoryEntry] = []
        entries = raw_directory.get("entries")
        if not isinstance(entries, list):
            raise ValueError(f"快照 manifest 目录 entries 无效: {directory_name}")
        for raw_entry in entries:
            if not isinstance(raw_entry, dict):
                raise ValueError(f"快照 manifest 目录条目无效: {directory_name}")
            relative_path = raw_entry.get("relative_path")
            kind = raw_entry.get("kind")
            mode = raw_entry.get("mode")
            if not isinstance(relative_path, str) or not isinstance(mode, int):
                raise ValueError(f"快照 manifest 目录条目字段无效: {directory_name}")
            target = root_target / Path(relative_path)
            if kind == "directory":
                target.mkdir(parents=True, exist_ok=False, mode=0o700)
                restored_entries.append(
                    DirectoryEntry(relative_path, "directory", mode, 0)
                )
            elif kind == "file":
                _copy_restored_file(
                    snapshot / Path(directory_name) / Path(relative_path),
                    target,
                    mode,
                )
            else:
                raise ValueError(f"快照 manifest 目录条目类型无效: {kind}")
        _apply_directory_modes(root_target, root_mode, restored_entries)


def restore_snapshot(snapshot: Path, destination: Path) -> Path:
    """将已验证快照原子恢复到一个不存在的目录并逐文件复核摘要。"""

    snapshot = snapshot.expanduser().resolve()
    destination = destination.expanduser().absolute()
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"恢复目标已存在，不覆盖既有目录: {destination}")
    if _path_is_relative_to(destination, snapshot):
        raise ValueError(f"恢复目标不能位于快照内部: {destination}")
    _reject_symlink_components(destination.parent, label="恢复目标父路径")
    manifest = _load_manifest(snapshot)
    verify_snapshot(snapshot)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = (
        destination.parent
        / f".{destination.name}.restore-{os.getpid()}-{time.time_ns()}"
    )
    committed = False
    try:
        temporary.mkdir(mode=0o700)
        _restore_file_sources(manifest, snapshot, temporary)
        _restore_directory_sources(manifest, snapshot, temporary)
        verify_snapshot_against_root(manifest, temporary)
        _fsync_tree(temporary)
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(f"恢复目标在发布前已存在: {destination}")
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
        committed = True
        return destination
    finally:
        if not committed and temporary.exists():
            shutil.rmtree(temporary)


def verify_snapshot_against_root(manifest: dict[str, Any], root: Path) -> None:
    """按同一 manifest 精确校验恢复目录。"""

    _verify_manifest_tree(manifest, root, include_manifest=False)
    for relative_path, record in _manifest_file_records(manifest):
        _verify_manifest_file(root, relative_path, record)


def _prune(destination: Path, retention: int) -> None:
    snapshots = sorted(
        (path for path in destination.glob("snapshot-*") if path.is_dir()),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )
    for path in snapshots[retention:]:
        shutil.rmtree(path)


def create_snapshot(
    *,
    sources: Iterable[BackupSource],
    destination: Path,
    retention: int = DEFAULT_RETENTION,
) -> Path:
    """创建所有源的一致性快照，并清理超出保留数的旧快照。"""

    # 1. 校验配置和所有源，避免生成半套快照。
    if retention < 1:
        raise ValueError("retention 必须大于等于 1")
    validated_sources = _validate_sources(sources)
    destination = destination.expanduser().resolve()
    for source in validated_sources:
        if source.kind != "directory":
            continue
        source_root = source.path.resolve(strict=True)
        if _path_is_relative_to(destination, source_root):
            raise ValueError(
                f"备份目标不能位于目录源内部，否则会把快照再次纳入源: {destination}"
            )
    destination.mkdir(parents=True, exist_ok=True)

    # 2. 用锁阻止并发备份写入同一个目标目录。
    lock_path = destination / ".backup.lock"
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"已有备份任务正在运行: {lock_path}") from exc

        timestamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
        temporary = destination / f".snapshot-{timestamp}-{os.getpid()}.tmp"
        snapshot = destination / f"snapshot-{timestamp}"
        if snapshot.exists() or snapshot.is_symlink():
            timestamp = f"{timestamp}-{time.time_ns() % 1_000_000:06d}"
            snapshot = destination / f"snapshot-{timestamp}"
        committed = False
        try:
            # 3. 先写临时目录，完成所有源和 manifest 后再原子发布。
            temporary.mkdir(mode=0o700)
            directory_records: dict[str, tuple[int, list[DirectoryEntry]]] = {}
            for source in validated_sources:
                root_mode, entries = _copy_source(source, temporary)
                if source.kind == "directory":
                    if root_mode is None or entries is None:
                        raise RuntimeError(
                            f"目录源复制没有生成 manifest 条目: {source.name}"
                        )
                    directory_records[source.name] = (root_mode, entries)
            _write_manifest(temporary, validated_sources, directory_records)
            _fsync_tree(temporary)
            if snapshot.exists() or snapshot.is_symlink():
                raise FileExistsError(f"快照目标已存在，不覆盖既有快照: {snapshot}")
            os.replace(temporary, snapshot)
            os.chmod(snapshot, 0o700)
            _fsync_directory(snapshot)
            _fsync_directory(destination)
            committed = True

            # 4. 只在新快照完整发布后滚动删除旧快照。
            _prune(destination, retention)
            return snapshot
        finally:
            if not committed and temporary.exists():
                shutil.rmtree(temporary)


def main() -> None:
    args = _parse_args()
    if args.config is not None:
        if (
            args.file
            or args.sqlite
            or args.directory
            or args.destination is not None
            or args.retention is not None
        ):
            raise ValueError(
                "--config 不能和 --file/--sqlite/--directory/--destination/--retention 同时使用"
            )
        destination, retention, sources = _load_config(args.config)
    else:
        if args.destination is None:
            raise ValueError("未提供 --config 或 --destination")
        sources = [
            *(_parse_source(raw, kind="file") for raw in args.file),
            *(_parse_source(raw, kind="sqlite") for raw in args.sqlite),
            *(_parse_source(raw, kind="directory") for raw in args.directory),
        ]
        destination = args.destination
        retention = DEFAULT_RETENTION if args.retention is None else args.retention
    snapshot = create_snapshot(
        sources=sources,
        destination=destination,
        retention=retention,
    )
    print(f"备份完成: {snapshot}")


if __name__ == "__main__":
    main()
