"""The only module that touches the filesystem. Keys are relative, prefix-shaped paths."""

from __future__ import annotations

import contextlib
import hashlib
import os
import tempfile
from collections.abc import Iterator
from pathlib import Path, PurePosixPath
from typing import Protocol


class ImmutabilityViolation(Exception):
    """A key that already exists was written again with different bytes."""


class BlobStore(Protocol):
    def put_bytes(self, key: str, data: bytes, *, overwrite: bool = False) -> None: ...

    def get_bytes(self, key: str) -> bytes: ...

    def exists(self, key: str) -> bool: ...

    def list(self, prefix: str) -> Iterator[str]: ...


class LocalBlobStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        p = PurePosixPath(key)
        if not key or p.is_absolute() or ".." in p.parts or "\\" in key:
            raise ValueError(f"invalid blob key: {key!r}")
        return self.root.joinpath(*p.parts)

    def put_bytes(self, key: str, data: bytes, *, overwrite: bool = False) -> None:
        """Atomically write `data`.

        An existing key with identical bytes is a no-op. Different bytes raise
        `ImmutabilityViolation` unless `overwrite=True` (only for uncommitted filing folders and
        append-style files the caller owns).
        """
        path = self._path(key)
        if path.exists() and not overwrite:
            if hashlib.sha256(path.read_bytes()).digest() == hashlib.sha256(data).digest():
                return
            raise ImmutabilityViolation(f"{key} exists with different content")
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-", suffix=".part")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise

    def get_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def list(self, prefix: str) -> Iterator[str]:
        base = self._path(prefix) if prefix else self.root
        if not base.exists():
            return
        for p in sorted(base.rglob("*")):
            if p.is_file() and not p.name.startswith(".tmp-"):
                yield p.relative_to(self.root).as_posix()

    def append_line(self, key: str, line: str) -> None:
        """Append one line to a log file (the one non-atomic, append-only exception)."""
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as f:
            f.write(line.encode("utf-8") + b"\n")
            f.flush()
            os.fsync(f.fileno())
