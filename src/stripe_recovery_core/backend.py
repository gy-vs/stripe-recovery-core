"""Caller-provided async storage.

The kernel never talks to a concrete storage product. The embedding program
supplies an object implementing the ``StorageBackend`` protocol — three async
methods over opaque string keys — and thereby keeps full ownership of where
bytes physically live (local disk, object storage, a database, ...).

Contract expected by the kernel:

* ``read(key)`` returns the stored bytes, or ``None`` when the key is absent.
  Raising means "storage failed" and surfaces to callers as ``StorageError`` —
  distinct from absence, which contributes to ``UnrecoverableError``.
* ``write(key, data)`` stores the bytes; once it returns, a subsequent
  ``read(key)`` of that key returns them (until overwritten or deleted).
* ``delete(key)`` removes the key; deleting an absent key is a no-op.
* Implementations must be safe to call concurrently from one event loop.

``InMemoryBackend`` is provided for tests, demos and single-process use.
"""

from __future__ import annotations

import asyncio
from typing import Protocol, runtime_checkable


@runtime_checkable
class StorageBackend(Protocol):
    async def read(self, key: str) -> bytes | None: ...
    async def write(self, key: str, data: bytes) -> None: ...
    async def delete(self, key: str) -> None: ...


class InMemoryBackend:
    """Dict-backed backend with operation counters (handy for assertions)."""

    def __init__(self, *, latency: float = 0.0) -> None:
        self._data: dict[str, bytes] = {}
        self.latency = latency
        self.read_count = 0
        self.write_count = 0
        self.delete_count = 0
        self.bytes_written = 0

    async def read(self, key: str) -> bytes | None:
        self.read_count += 1
        if self.latency:
            await asyncio.sleep(self.latency)
        data = self._data.get(key)
        return None if data is None else bytes(data)

    async def write(self, key: str, data: bytes) -> None:
        self.write_count += 1
        if self.latency:
            await asyncio.sleep(self.latency)
        blob = bytes(data)
        self._data[key] = blob
        self.bytes_written += len(blob)

    async def delete(self, key: str) -> None:
        self.delete_count += 1
        if self.latency:
            await asyncio.sleep(self.latency)
        self._data.pop(key, None)

    # Convenience helpers for tests and embeddings (not part of the protocol).
    def keys(self) -> list[str]:
        return list(self._data)

    def snapshot(self) -> dict[str, bytes]:
        return dict(self._data)

    def __len__(self) -> int:
        return len(self._data)
