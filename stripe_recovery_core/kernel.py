"""Kernel facade: the single object embedders instantiate.

Owns the things that must outlive individual operations: the zfec coder
cache, the background repair coordinator, and the registry of active
writers/readers/repairs that makes resource state checkable after every
operation.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .coding import Coder, CodingParams
from .descriptor import ObjectDescriptor, parse_descriptor
from .errors import ObjectNotFound, StorageError
from .errors import NotFound as _NotFound
from .reader import RangeReader
from .repair import RepairCoordinator, RepairHandle
from .results import (KernelResourceSnapshot, ReadReport, RepairReport,
                      RepairStats)
from .storage import keys
from .storage.base import AsyncStorage
from .writer import ObjectWriter

DEFAULT_BLOCK_SIZE = 64 * 1024
DEFAULT_INFLIGHT_BYTES = 4 * 1024 * 1024
DEFAULT_OUTPUT_BYTES = 2 * 1024 * 1024
DEFAULT_REPAIR_CONCURRENCY = 2
DEFAULT_UPLOAD_CONCURRENCY = 4


@dataclass
class KernelConfig:
    block_size: int = DEFAULT_BLOCK_SIZE
    """Coded share block size; memory and work granularity."""

    max_inflight_bytes: int = DEFAULT_INFLIGHT_BYTES
    """Writer-side bound on coded bytes uploaded but not yet completed."""

    max_output_bytes: int = DEFAULT_OUTPUT_BYTES
    """Reader-side bound on verified bytes buffered ahead of the consumer."""

    repair_concurrency: int = DEFAULT_REPAIR_CONCURRENCY
    """Max stripes repaired in parallel in the background."""

    explicit_repair_concurrency: int = DEFAULT_REPAIR_CONCURRENCY

    def validate(self) -> None:
        if self.block_size <= 0:
            raise ValueError("block_size must be positive")
        for attr in ("max_inflight_bytes", "max_output_bytes"):
            if getattr(self, attr) <= 0:
                raise ValueError(f"{attr} must be positive")
        if self.repair_concurrency <= 0:
            raise ValueError("repair_concurrency must be positive")


class Kernel:
    """Embeddable erasure-coded stripe kernel bound to one storage backend."""

    def __init__(self, storage: AsyncStorage,
                 config: KernelConfig | None = None) -> None:
        self._storage = storage
        self.config = config or KernelConfig()
        self.config.validate()
        self._coder = Coder()
        self._repairs = RepairCoordinator(
            storage, self._coder,
            max_concurrency=self.config.repair_concurrency)
        self._writers: set[ObjectWriter] = set()
        self._readers: set[RangeReader] = set()
        self._repair_handles: set[RepairHandle] = set()
        self._closed = False

    # ---- write ---------------------------------------------------------

    def new_writer(self, name: bytes | str, *, k: int = 4, m: int = 3,
                   block_size: int | None = None,
                   max_inflight_bytes: int | None = None) -> ObjectWriter:
        """Begin a new, not-yet-visible object generation."""
        if self._closed:
            raise RuntimeError("kernel is closed")
        params = CodingParams(k=k, m=m,
                              block_size=block_size or self.config.block_size)
        budget = max_inflight_bytes or self.config.max_inflight_bytes
        writer = ObjectWriter(
            storage=self._storage, coder=self._coder,
            name=_as_name(name), params=params,
            max_inflight_bytes=budget, on_close=self._writers.discard)
        self._writers.add(writer)
        return writer

    async def encode(self, name: bytes | str, data, *, k: int = 4,
                     m: int = 3, block_size: int | None = None,
                     chunk_size: int | None = None) -> ObjectDescriptor:
        """Convenience: stream *data* to a committed object.

        *data* may be bytes or an async iterable of bytes.  With an async
        iterable the chunks are fed as produced, so callers can encode
        without materializing the object.
        """
        writer = self.new_writer(name, k=k, m=m, block_size=block_size)
        try:
            if isinstance(data, (bytes, bytearray, memoryview)):
                view = memoryview(bytes(data))
                step = chunk_size or writer.params.stripe_data_size * 4
                for off in range(0, len(view), step):
                    await writer.write(bytes(view[off:off + step]))
            else:
                async for chunk in data:
                    await writer.write(chunk)
            return await writer.finish()
        except BaseException:
            await writer.aclose()
            raise

    # ---- read ----------------------------------------------------------

    async def resolve(self, name: bytes | str) -> ObjectDescriptor:
        """Return the committed descriptor currently published for *name*."""
        raw = await self._storage.get(keys.head_key(_as_name(name)))
        try:
            return parse_descriptor(raw)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"published descriptor is invalid: {exc}") \
                from exc

    async def open(self, name_or_descriptor, *,
                   at_generation: str | None = None) -> ObjectDescriptor:
        """Resolve an object name (or pass a descriptor) to a verified one.

        Opening by name always returns the latest *committed* descriptor;
        callers needing a fixed view retain the descriptor object (or its
        generation hex) and reopen that, so repeated writes under one name
        cannot change what an old descriptor resolves to.
        """
        if isinstance(name_or_descriptor, ObjectDescriptor):
            desc = name_or_descriptor
            desc.verify()
            if at_generation and desc.generation != at_generation:
                raise ObjectNotFound(
                    "descriptor does not match requested generation")
            return desc
        if at_generation is not None:
            try:
                raw = await self._storage.get(
                    f"gen/{at_generation}/desc")
            except _NotFound as exc:
                raise ObjectNotFound(
                    f"generation {at_generation} not found") from exc
            desc = parse_descriptor(raw)
            if desc.name != _as_name(name_or_descriptor):
                raise ObjectNotFound("generation belongs to another object")
            return desc
        try:
            return await self.resolve(name_or_descriptor)
        except _NotFound as exc:
            raise ObjectNotFound(
                f"no committed object named "
                f"{_as_name(name_or_descriptor)!r}") from exc

    def read_range(self, descriptor: ObjectDescriptor, *, start: int = 0,
                   end: int | None = None,
                   max_output_bytes: int | None = None,
                   repair_on_read: bool = False) -> RangeReader:
        """Begin a streaming range read against a committed descriptor."""
        descriptor.verify()
        if self._closed:
            raise RuntimeError("kernel is closed")
        reader = RangeReader(
            storage=self._storage, coder=self._coder, desc=descriptor,
            start=start, end=end,
            max_output_bytes=(max_output_bytes
                              or self.config.max_output_bytes),
            repair_coordinator=self._repairs,
            repair_on_read=repair_on_read, on_close=self._readers.discard)
        self._readers.add(reader)
        return reader

    async def read_all(self, descriptor: ObjectDescriptor, **kwargs) -> bytes:
        """Convenience for tests/small objects; prefer read_range streaming."""
        reader = self.read_range(descriptor, **kwargs)
        chunks: list[bytes] = []
        async for chunk in reader:
            chunks.append(chunk)
        report = await reader.report()
        if not report.status.ok:
            report.raise_for_status()
        return b"".join(chunks)

    # ---- repair --------------------------------------------------------

    def repair(self, descriptor: ObjectDescriptor, *,
               start_stripe: int | None = None,
               end_stripe: int | None = None,
               replace_corrupt: bool = False) -> RepairHandle:
        """Begin a standalone, independently cancellable repair scan."""
        descriptor.verify()
        handle = RepairHandle(
            self._storage, self._coder, descriptor,
            start_stripe=start_stripe, end_stripe=end_stripe,
            replace_corrupt=replace_corrupt,
            max_concurrency=self.config.explicit_repair_concurrency,
            on_close=self._repair_handles.discard)
        self._repair_handles.add(handle)
        return handle

    def spawn_background_repair(self, descriptor: ObjectDescriptor,
                                stripe_index: int) -> asyncio.Task:
        """Trigger one fire-and-forget background stripe repair."""
        descriptor.verify()
        return self._repairs.spawn_repair(descriptor, stripe_index)

    @property
    def background_repairs(self) -> int:
        return self._repairs.active

    def background_repair_stats(self) -> RepairStats:
        return self._repairs.stats

    # ---- lifecycle / resources ----------------------------------------

    def resources(self) -> KernelResourceSnapshot:
        """Checkable snapshot of resources the kernel currently owns."""
        return KernelResourceSnapshot(
            active_writers=len(self._writers),
            active_readers=len(self._readers),
            active_repairs=len(self._repair_handles) + self._repairs.active,
            inflight_storage_bytes=sum(
                getattr(w, "inflight_bytes", 0) for w in self._writers),
            inflight_tasks=len(asyncio.all_tasks()) - 1,
            coders_cached=len(self._coder._encoders) + len(  # noqa: SLF001
                self._coder._decoders))

    async def aclose(self) -> None:
        """Cancel background repairs; active handles should close themselves."""
        if self._closed:
            return
        self._closed = True
        await self._repairs.aclose()


def _as_name(name: bytes | str) -> bytes:
    if isinstance(name, str):
        return name.encode("utf-8")
    if isinstance(name, (bytes, bytearray, memoryview)):
        return bytes(name)
    raise TypeError("object name must be str or bytes")
