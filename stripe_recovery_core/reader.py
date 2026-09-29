"""Streaming, integrity-checked range reader.

The reader is an async iterator over verified byte chunks.  A producer
task walks only the stripes intersecting the requested range; recovered
chunks pass through a byte-bounded queue, so a slow consumer makes the
producer wait and the kernel's buffered output stays at
``max_output_bytes`` regardless of object size.

Cancellation is split:

* :meth:`aclose` cancels the *read* (the producer).  Chunks already
  delivered to a consumer are bytes that already passed verification and
  are never "withdrawn".
* Background repair tasks are separate, owned by the repair coordinator
  and tracked independently; cancelling a read does not cancel a repair,
  and a failed repair never alters the read's confirmed bytes or the
  object descriptor.

Nothing is yielded past the first stripe that cannot be fully verified;
``confirmed_end`` in the final report is the boundary up to which all
returned bytes are authentic.
"""

from __future__ import annotations

import asyncio

from .coding import CodingParams
from .descriptor import ObjectDescriptor
from .errors import ClosedError
from .results import (ReadReport, ReadStatus, ReaderStats, ShareStatus,
                      StripeResult)
from .stripe_ops import plan_stripe_read
from .storage.base import AsyncStorage

_SENTINEL = object()


class RangeReader:
    """Async iterator delivering one verified byte range of one object."""

    def __init__(self, *, storage: AsyncStorage, coder,
                 desc: ObjectDescriptor, start: int, end: int | None,
                 max_output_bytes: int, repair_coordinator,
                 repair_on_read: bool, on_close) -> None:
        if start < 0:
            raise ValueError("start must be >= 0")
        end = desc.length if end is None else min(end, desc.length)
        if end < start:
            raise ValueError("end must be >= start")
        self._storage = storage
        self._coder = coder
        self._desc = desc
        self._start = start
        self._end = end
        # Queue budget: at least one full stripe's data.
        budget = max(desc.params.stripe_data_size, int(max_output_bytes))
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max(1, budget //
                                                               max(1, desc.block_size)))
        self._budget = budget
        self._repair = repair_coordinator
        self._repair_on_read = repair_on_read
        self._on_close = on_close
        self.stats = ReaderStats(bytes_requested=end - start)
        self._producer: asyncio.Task | None = None
        self._closed = False
        self._done = asyncio.Event()
        self._produced_bytes = 0
        self._report: ReadReport | None = None
        self._producer_started = False

    @property
    def descriptor(self) -> ObjectDescriptor:
        return self._desc

    @property
    def start(self) -> int:
        return self._start

    @property
    def end(self) -> int:
        return self._end

    @property
    def confirmed_end(self) -> int:
        if self._report is not None:
            return self._report.confirmed_end
        # While streaming, returned bytes are confirmed as delivered.
        return self._start + self.stats.bytes_returned

    def _ensure_started(self) -> None:
        if not self._producer_started:
            self._producer_started = True
            self._producer = asyncio.ensure_future(self._produce())

    def __aiter__(self) -> "RangeReader":
        self._ensure_started()
        return self

    async def __anext__(self) -> bytes:
        self._ensure_started()
        item = await self._queue.get()
        if item is _SENTINEL:
            self._closed = True
            self.stats.closed = True
            result = self._on_close(self)
            if asyncio.iscoroutine(result):
                await result
            raise StopAsyncIteration
        (chunk,) = item
        self.stats.bytes_returned += len(chunk)
        return chunk

    async def _enqueue_bytes(self, data: bytes) -> None:
        # Chunk at block granularity or smaller; the queue item count also
        # bounds memory, and put() applies backpressure.
        bs = self._desc.block_size
        for off in range(0, len(data), bs):
            await self._queue.put((data[off:off + bs],))
        self._produced_bytes += len(data)

    async def _produce(self) -> None:
        params: CodingParams = self._desc.params
        stripe_results: list[StripeResult] = []
        confirmed_end = self._start
        status = ReadStatus.CONFIRMED
        error = None
        try:
            if self._end > self._start:
                first = self._start // params.stripe_data_size
                last_stripe = (self._end - 1) // params.stripe_data_size
                for si in range(first, last_stripe + 1):
                    s_abs = si * params.stripe_data_size
                    byte_lo = max(self._start, s_abs) - s_abs
                    byte_hi = min(self._end, s_abs + params.stripe_data_size) \
                        - s_abs
                    block_lo = byte_lo // params.block_size
                    block_hi = (byte_hi + params.block_size - 1) \
                        // params.block_size
                    needed = set(range(block_lo, block_hi))

                    counter = {"fetches": 0, "bytes": 0}
                    plan = await plan_stripe_read(
                        self._storage, self._coder, self._desc, si,
                        needed_data_blocks=needed, counter=counter)
                    self.stats.shares_fetched += counter["fetches"]
                    self.stats.share_bytes_fetched += counter["bytes"]
                    self.stats.stripes_processed += 1
                    stripe_results.append(plan)

                    if not plan.status.ok:
                        status = plan.status
                        break

                    parts = [plan.data_blocks[b] for b in sorted(needed)]
                    stripe_bytes = b"".join(parts)
                    out_start = byte_lo - block_lo * params.block_size
                    out_end = byte_hi - block_lo * params.block_size
                    await self._enqueue_bytes(stripe_bytes[out_start:out_end])
                    confirmed_end = s_abs + byte_hi
                    if self._repair_on_read:
                        self._repair.spawn_repair(
                            self._desc, si, replace_corrupt=False)
                        self.stats.repairs_spawned += 1
        except asyncio.CancelledError:
            status = ReadStatus.CANCELLED
            error = "read cancelled"
            raise
        except Exception as exc:  # noqa: BLE001 - storage boundary
            status = ReadStatus.STORAGE_FAILURE
            error = f"{type(exc).__name__}: {exc}"
        finally:
            self._build_report(status, confirmed_end, stripe_results, error)
            await self._queue.put(_SENTINEL)
            self._done.set()

    def _build_report(self, status: ReadStatus, confirmed_end: int,
                      stripes: list[StripeResult],
                      error: str | None) -> ReadReport:
        used: list[tuple[int, int]] = []
        missing: list[tuple[int, int]] = []
        corrupt: list[tuple[int, int]] = []
        fetch_failed: list[tuple[int, int]] = []
        manifests: list[int] = []
        for sr in stripes:
            for j, st in sr.share_statuses.items():
                if st is ShareStatus.USED:
                    used.append((sr.stripe_index, j))
                elif st is ShareStatus.MISSING:
                    missing.append((sr.stripe_index, j))
                elif st is ShareStatus.CORRUPT:
                    corrupt.append((sr.stripe_index, j))
                elif st is ShareStatus.FETCH_FAILED:
                    fetch_failed.append((sr.stripe_index, j))
            if getattr(sr, "manifest_regenerated", False):
                manifests.append(sr.stripe_index)
            if sr.error and error is None:
                error = sr.error
        # On a completed read every produced chunk is verified; bytes_returned
        # reflects delivered+verifiable bytes.  On cancellation, count only
        # what the consumer actually took.
        delivered = (self._produced_bytes if status is ReadStatus.CONFIRMED
                     else self.stats.bytes_returned)
        report = ReadReport(
            status=status, start=self._start, requested_end=self._end,
            confirmed_end=confirmed_end,
            bytes_returned=delivered,
            stripes=tuple(stripes), used_shares=tuple(used),
            missing_shares=tuple(missing), corrupt_shares=tuple(corrupt),
            fetch_failed_shares=tuple(fetch_failed),
            manifest_regenerated=tuple(manifests), error=error)
        self._report = report
        return report

    async def aclose(self) -> None:
        """Cancel the read producer and release its resources.

        Chunks already delivered to the consumer stay valid: they passed
        verification before being enqueued.  Background repairs are owned
        by the repair coordinator and are not cancelled here.
        """
        if self._closed:
            return
        self._closed = True
        self.stats.closed = True
        if self._producer is not None and not self._producer.done():
            self._producer.cancel()
            try:
                await self._producer
            except BaseException:  # noqa: BLE001
                pass
        if self._report is None:
            confirmed = self._start + self.stats.bytes_returned
            self._build_report(ReadStatus.CANCELLED,
                               min(confirmed, self._end), [],
                               "reader closed before completion")
        result = self._on_close(self)
        if asyncio.iscoroutine(result):
            await result

    async def report(self) -> ReadReport:
        """Block until streaming completes and return the final report.

        Safe to call after (or instead of) consuming the async iterator;
        it never drains already-delivered bytes again.
        """
        self._ensure_started()
        await self._done.wait()
        assert self._report is not None
        return self._report
