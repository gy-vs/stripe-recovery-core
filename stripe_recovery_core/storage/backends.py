"""In-memory and filesystem reference storage backends.

These are production-usable simple backends and also serve as the
reference semantics for the :class:`AsyncStorage` protocol.  The
filesystem backend emulates ``put_if_absent`` atomically via
``O_CREAT | O_EXCL`` and runs blocking file I/O in a thread so a slow disk
does not stall the event loop.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping

from ..errors import ConditionFailed, NotFound, StorageError


class MemoryStorage:
    """Single-process dict-backed storage; the reference implementation."""

    def __init__(self) -> None:
        self._data: dict[str, bytes] = {}
        self._lock = asyncio.Lock()
        self.get_count = 0
        self.put_count = 0
        self.cas_count = 0
        self.delete_count = 0

    def snapshot(self) -> Mapping[str, int]:
        return {
            "keys": len(self._data),
            "bytes": sum(len(v) for v in self._data.values()),
            "get": self.get_count,
            "put": self.put_count,
            "put_if_absent": self.cas_count,
            "delete": self.delete_count,
        }

    async def get(self, key: str) -> bytes:
        self.get_count += 1
        async with self._lock:
            if key not in self._data:
                raise NotFound(key=key)
            return self._data[key]

    async def put(self, key: str, value: bytes) -> None:
        self.put_count += 1
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise TypeError("storage values must be bytes-like")
        async with self._lock:
            self._data[key] = bytes(value)

    async def put_if_absent(self, key: str, value: bytes) -> bool:
        self.cas_count += 1
        async with self._lock:
            if key in self._data:
                return False
            self._data[key] = bytes(value)
            return True

    async def delete(self, key: str) -> None:
        self.delete_count += 1
        async with self._lock:
            self._data.pop(key, None)


def _fs_put_if_absent(path: str, value: bytes) -> bool:
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return False
    try:
        os.write(fd, value)
    finally:
        os.close(fd)
    return True


def _fs_put(path: str, value: bytes) -> None:
    tmp = f"{path}.tmp.{os.getpid()}.{id(value)}"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, value)
    finally:
        os.close(fd)
    os.replace(tmp, path)


class FileStorage:
    """Filesystem-backed storage rooted at *root*.

    Suitable for local durable verification and single-process use; keys
    map directly to relative paths.
    """

    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)
        self._loop = asyncio.get_event_loop

    def _path(self, key: str) -> str:
        if os.path.isabs(key) or ".." in key.split("/"):
            raise StorageError(f"unsafe storage key: {key!r}", key=key)
        path = os.path.join(self.root, *key.split("/"))
        if not os.path.abspath(path).startswith(self.root + os.sep):
            raise StorageError(f"storage key escapes root: {key!r}", key=key)
        return path

    async def get(self, key: str) -> bytes:
        path = self._path(key)
        loop = self._loop()
        try:
            return await loop.run_in_executor(None, _fs_read, path)
        except FileNotFoundError as exc:
            raise NotFound(key=key) from exc
        except OSError as exc:
            raise StorageError(str(exc), key=key, cause=exc) from exc

    async def put(self, key: str, value: bytes) -> None:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        loop = self._loop()
        try:
            await loop.run_in_executor(None, _fs_put, path, bytes(value))
        except OSError as exc:
            raise StorageError(str(exc), key=key, cause=exc) from exc

    async def put_if_absent(self, key: str, value: bytes) -> bool:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        loop = self._loop()
        try:
            return await loop.run_in_executor(
                None, _fs_put_if_absent, path, bytes(value))
        except OSError as exc:
            raise StorageError(str(exc), key=key, cause=exc) from exc

    async def delete(self, key: str) -> None:
        path = self._path(key)
        try:
            os.remove(path)
        except FileNotFoundError:
            return
        except OSError as exc:
            raise StorageError(str(exc), key=key, cause=exc) from exc


def _fs_read(path: str) -> bytes:
    with open(path, "rb") as fh:
        return fh.read()
