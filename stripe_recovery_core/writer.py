"""Streaming object writer with commit-gated visibility.

Each stripe is encoded as soon as ``k * block_size`` bytes are available.
Its n coded blocks go to content-addressed ``blob/<digest>`` keys, and its
manifest (the ordered list of content digests) goes under the
generation-scoped key -- but the generation is only known after the
descriptor digest is computed, which requires every stripe root.  So the
writer keeps, per finalized stripe, only its small manifest blob and root
in memory; the heavy share bytes live on storage from that moment on.

Commit (:meth:`finish`) order:

1. flush final partial stripe (zero-length objects get one empty stripe),
2. build the descriptor (digest = generation) and upload all manifest
   blobs to ``gen/<generation>/s/<i>/manifest``,
3. upload descriptor to ``gen/<generation>/desc`` (put-if-absent),
4. publish ``head/<name>`` last.  Before step 4 the new generation is
   unreadable as a completed object; aborting mid-way leaves only
   unreferenced blobs that are never resolvable through any descriptor.

Backpressure: a condition variable bounds the coded bytes represented by
in-flight stripe uploads (``max_inflight_bytes``).  A slow storage backend
makes :meth:`write` wait; only the current partial stripe is buffered,
never the whole object.
"""

from __future__ import annotations

import asyncio

from .coding import Coder, CodingParams
from .crypto import content_digest, manifest_blob, stripe_root
from .descriptor import ObjectDescriptor, build_descriptor
from .errors import ClosedError, StorageError
from .results import WriterStats
from .storage import keys
from .storage.base import AsyncStorage


class ObjectWriter:
    """Async, backpressured writer for one new object generation."""

    def __init__(self, *, storage: AsyncStorage, coder: Coder,
                 name: bytes, params: CodingParams,
                 max_inflight_bytes: int, on_close) -> None:
        params.validate()
        self._storage = storage
        self._coder = coder
        self._name = bytes(name)
        self._params = params
        self._on_close = on_close
        coded_stripe = params.stripe_data_size * params.n
        self._budget = max(coded_stripe, int(max_inflight_bytes))
        self._inflight = 0
        self._cond = asyncio.Condition()
        self._tasks: list[asyncio.Task] = []
        self._failures: list[BaseException] = []
        self._buffer = bytearray()
        self._stripe_records: list[tuple[bytes, bytes]] = []
        self._records_lock = asyncio.Lock()
        self._next_stripe = 0
        self.stats = WriterStats()
        self._closed = False
        self._committed = False
        self._aborted = False

    @property
    def params(self) -> CodingParams:
        return self._params

    @property
    def inflight_bytes(self) -> int:
        return self._inflight

    async def write(self, data: bytes) -> None:
        """Accept bytes with backpressure; buffers at most one stripe."""
        if self._closed:
            raise ClosedError("writer is closed")
        data = bytes(data)
        if not data:
            return
        self.stats.bytes_accepted += len(data)
        self._buffer.extend(data)
        size = self._params.stripe_data_size
        while len(self._buffer) >= size:
            chunk = bytes(self._buffer[:size])
            del self._buffer[:size]
            await self._launch_stripe(chunk)
        if self._failures:
            raise StorageError("background upload failed",
                               cause=self._failures[0])

    async def _reserve(self, coded_size: int) -> None:
        async with self._cond:
            await self._cond.wait_for(
                lambda: self._inflight + coded_size <= self._budget
                or bool(self._failures))
            if self._failures:
                raise StorageError("background upload failed",
                                   cause=self._failures[0])
            self._inflight += coded_size

    def _release(self, coded_size: int) -> None:
        async def _drop() -> None:
            async with self._cond:
                self._inflight = max(0, self._inflight - coded_size)
                self._cond.notify_all()
        asyncio.ensure_future(_drop())

    async def _launch_stripe(self, stripe_bytes: bytes) -> None:
        coded_size = self._params.stripe_data_size * self._params.n
        await self._reserve(coded_size)
        si = self._next_stripe
        self._next_stripe += 1
        task = asyncio.ensure_future(self._encode_upload_record(si,
                                                                stripe_bytes))
        self._tasks.append(task)
        task.add_done_callback(lambda t: self._task_done(t))

    def _task_done(self, task: asyncio.Task) -> None:
        exc = task.exception()
        if exc is not None:
            self._failures.append(exc)
            asyncio.ensure_future(self._notify_failure())

    async def _notify_failure(self) -> None:
        async with self._cond:
            self._cond.notify_all()

    async def _encode_upload_record(self, si: int,
                                    stripe_bytes: bytes) -> None:
        coded_size = self._params.stripe_data_size * self._params.n
        try:
            blocks = self._params.split_stripe(stripe_bytes)
            shares = self._coder.encode(self._params, blocks)
            leaves = [content_digest(s) for s in shares]
            await asyncio.gather(*(
                self._storage.put(keys.blob_key(shares[j]), shares[j])
                for j in range(self._params.n)))
            root = stripe_root(si, leaves)
            async with self._records_lock:
                # si is monotonically assigned; extend to keep ordering.
                while len(self._stripe_records) <= si:
                    self._stripe_records.append(None)  # type: ignore[arg-type]
                self._stripe_records[si] = (root, manifest_blob(leaves))
            self.stats.shares_uploaded += self._params.n
            self.stats.bytes_uploaded += coded_size
            self.stats.stripes_encoded += 1
        finally:
            self._release(coded_size)

    async def _drain(self) -> None:
        if self._tasks:
            results = await asyncio.gather(*self._tasks,
                                           return_exceptions=True)
        else:
            results = []
        for r in results:
            if isinstance(r, BaseException) and not self._failures:
                self._failures.append(r)
        self._tasks = []
        if self._failures:
            raise StorageError("stripe upload failed",
                               cause=self._failures[0])

    async def finish(self) -> ObjectDescriptor:
        """Commit the object and publish its descriptor.

        Returns the committed descriptor; no other caller can observe the
        object until this method returns successfully.
        """
        if self._closed:
            raise ClosedError("writer is closed")
        # Final (possibly empty) stripe.
        tail = bytes(self._buffer)
        del self._buffer[:]
        if tail or self._next_stripe == 0:
            await self._launch_stripe(tail)
        await self._drain()
        if any(rec is None for rec in self._stripe_records):
            raise StorageError("incomplete stripe records at commit")

        length = self.stats.bytes_accepted
        roots = [rec[0] for rec in self._stripe_records]
        desc = build_descriptor(self._name, self._params, length, roots)

        # Generation-scoped manifests first ...
        await asyncio.gather(*(
            self._storage.put(keys.manifest_key(desc, si), rec[1])
            for si, rec in enumerate(self._stripe_records)))
        self.stats.manifests_uploaded = len(self._stripe_records)
        # ... then descriptor ...
        await self._storage.put_if_absent(
            keys.descriptor_key(desc), desc.to_bytes())
        # ... and only then the head pointer: the commit point.
        await self._storage.put(keys.head_key(self._name), desc.to_bytes())

        self._committed = True
        await self._mark_closed()
        return desc

    async def aclose(self) -> None:
        """Abandon an uncommitted write and release resources.

        Content-addressed blobs already uploaded are unreferenced by any
        descriptor and therefore unobservable as object content; nothing
        under ``head/`` is touched.
        """
        if self._closed:
            return
        self._aborted = True
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        await self._mark_closed()

    async def _mark_closed(self) -> None:
        self._closed = True
        self.stats.closed = True
        self.stats.committed = self._committed
        self.stats.aborted = self._aborted
        self.stats.inflight_bytes = self._inflight
        if self._on_close is not None:
            result = self._on_close(self)
            if asyncio.iscoroutine(result):
                await result
