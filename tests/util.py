"""Shared helpers for the verification suite."""

from __future__ import annotations

import asyncio

from stripe_recovery_core import InMemoryBackend, KernelConfig, StripeKernel
from stripe_recovery_core.kernel import _piece_key


def run(coro):
    """Drive a coroutine on a fresh event loop (no pytest-asyncio needed)."""
    return asyncio.run(coro)


def make_kernel(backend=None, **config_overrides):
    """Small-config kernel: 4-of-7, 64-byte shards => 256-byte stripes."""
    backend = backend if backend is not None else InMemoryBackend()
    config = KernelConfig(k=4, m=7, shard_size=64, read_chunk_size=50, **config_overrides)
    return StripeKernel(config, backend), backend


def pattern_bytes(n: int, seed: int = 0) -> bytes:
    """Deterministic, cheap-to-generate content."""
    base = bytes(((i + seed * 7) & 0xFF) for i in range(256))
    return (base * (n // 256 + 1))[:n]


def piece_key(kernel, name, generation, shard, stripe):
    return _piece_key(kernel.config.key_prefix, name, generation, shard, stripe)


class FaultBackend(InMemoryBackend):
    """In-memory backend with injectable failures and write gating."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.fail_reads = False
        self.fail_writes = False
        self.write_failure_predicate = None  # callable(key) -> bool
        self.write_hook = None  # callable(key) called before each write

    async def read(self, key):
        if self.fail_reads:
            raise RuntimeError("injected read failure")
        return await super().read(key)

    async def write(self, key, data):
        if self.write_hook is not None:
            self.write_hook(key)
        if self.fail_writes or (
            self.write_failure_predicate is not None
            and self.write_failure_predicate(key)
        ):
            raise RuntimeError("injected write failure")
        await super().write(key, data)


class DirectoryBackend:
    """File-backed storage (one file per key) for large-object tests.

    Shows what adapting a real storage system to the kernel looks like:
    three async methods over opaque keys, nothing else."""

    def __init__(self, root):
        import pathlib

        self.root = pathlib.Path(root)
        self.read_count = 0

    def _path(self, key):
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    async def read(self, key):
        self.read_count += 1

        def _read():
            try:
                return (self.root / key).read_bytes()
            except FileNotFoundError:
                return None

        return await asyncio.to_thread(_read)

    async def write(self, key, data):
        path = self._path(key)
        await asyncio.to_thread(path.write_bytes, data)

    async def delete(self, key):
        def _delete():
            try:
                (self.root / key).unlink()
            except FileNotFoundError:
                pass

        await asyncio.to_thread(_delete)
