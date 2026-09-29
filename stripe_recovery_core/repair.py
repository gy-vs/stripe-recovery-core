"""Repair coordinator and standalone cancellable repair scans.

Responsibilities distinct from reading:

* bounded concurrency with its own semaphore and task set,
* independent cancellation -- a read closing never cancels repair, and
  repair cancellation never revokes bytes a reader already delivered,
* only generation-scoped, put-if-absent writes, so repair cannot change
  object meaning or overwrite newer content,
* failures are recorded per stripe in a :class:`RepairReport`; they are
  never represented as content changes (descriptors and heads are never
  rewritten by repair).
"""

from __future__ import annotations

import asyncio

from .descriptor import ObjectDescriptor
from .results import RepairReport, RepairStats, RepairStatus, RepairStripeResult
from .stripe_ops import repair_stripe
from .storage.base import AsyncStorage


class RepairCoordinator:
    """Owns background (read-triggered) repair tasks for one kernel."""

    def __init__(self, storage: AsyncStorage, coder, *,
                 max_concurrency: int) -> None:
        self._storage = storage
        self._coder = coder
        self._sem = asyncio.Semaphore(max(1, max_concurrency))
        self._tasks: set[asyncio.Task] = set()
        self.stats = RepairStats()
        self.results: dict[tuple[str, int], RepairStripeResult] = {}

    @property
    def active(self) -> int:
        return len(self._tasks)

    def spawn_repair(self, desc: ObjectDescriptor, stripe_index: int,
                     *, replace_corrupt: bool = False) -> asyncio.Task:
        """Fire-and-track one stripe repair; duplicate (gen, stripe) merged."""
        key = (desc.generation, stripe_index)
        for t in self._tasks:
            if getattr(t, "repair_key", None) == key and not t.done():
                return t
        task = asyncio.ensure_future(
            self._run(desc, stripe_index, replace_corrupt))
        task.repair_key = key  # type: ignore[attr-defined]
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _run(self, desc: ObjectDescriptor, stripe_index: int,
                   replace_corrupt: bool) -> RepairStripeResult:
        async with self._sem:
            result = await repair_stripe(
                self._storage, self._coder, desc, stripe_index,
                replace_corrupt=replace_corrupt, repair_stats=self.stats)
            self.results[(desc.generation, stripe_index)] = result
            if result.regenerated_shares:
                self.stats.shares_regenerated += len(result.regenerated_shares)
            if result.manifest_regenerated:
                self.stats.manifests_regenerated += 1
            self.stats.stripes_scanned += 1
            return result

    async def aclose(self) -> None:
        """Cancel and await all background repairs (kernel shutdown)."""
        for t in list(self._tasks):
            t.cancel()
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        self._tasks.clear()
        self.stats.closed = True


class RepairHandle:
    """A standalone, independently cancellable repair scan."""

    def __init__(self, storage: AsyncStorage, coder, desc: ObjectDescriptor,
                 *, start_stripe: int | None, end_stripe: int | None,
                 replace_corrupt: bool, max_concurrency: int,
                 on_close) -> None:
        self._storage = storage
        self._coder = coder
        self._desc = desc
        self._start = start_stripe or 0
        self._end = desc.stripe_count if end_stripe is None else end_stripe
        self._replace_corrupt = replace_corrupt
        self.max_concurrency = max(1, max_concurrency)
        self._on_close = on_close
        self._cancel = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._closed = False
        self.stats = RepairStats()

    async def run(self) -> RepairReport:
        """Scan stripes, reconstruct missing shares; return full report."""
        results: list[RepairStripeResult] = []
        status = RepairStatus.REPAIRED
        error = None
        width = min(self.max_concurrency,
                    max(1, self._end - self._start))
        try:
            for base in range(self._start, self._end, width):
                if self._cancel.is_set():
                    status = RepairStatus.CANCELLED
                    error = "repair cancelled"
                    break
                batch = [
                    asyncio.ensure_future(self._one(si))
                    for si in range(base, min(base + width, self._end))]
                batch_results = await asyncio.gather(
                    *batch, return_exceptions=True)
                for br in batch_results:
                    if isinstance(br, BaseException):
                        status = RepairStatus.STORAGE_FAILURE
                        error = f"{type(br).__name__}: {br}"
                    else:
                        results.append(br)
        except asyncio.CancelledError:
            status = RepairStatus.CANCELLED
            error = "repair cancelled"
        results.sort(key=lambda r: r.stripe_index)
        for r in results:
            if r.status is RepairStatus.STORAGE_FAILURE:
                status = RepairStatus.STORAGE_FAILURE
                error = error or r.error
            elif r.status is RepairStatus.UNRECOVERABLE and \
                    status is RepairStatus.REPAIRED:
                status = RepairStatus.UNRECOVERABLE
                error = error or r.error
            elif r.status is RepairStatus.UNVERIFIABLE and \
                    status in (RepairStatus.REPAIRED, RepairStatus.UNRECOVERABLE):
                status = RepairStatus.UNVERIFIABLE
                error = error or r.error
        if self._cancel.is_set() and status is RepairStatus.REPAIRED:
            status = RepairStatus.CANCELLED
        report = RepairReport(
            status=status, stripes=tuple(results),
            shares_regenerated=sum(len(r.regenerated_shares) for r in results),
            manifests_regenerated=sum(1 for r in results
                                      if r.manifest_regenerated),
            error=error)
        await self._mark_closed()
        return report

    async def _one(self, si: int) -> RepairStripeResult:
        r = await repair_stripe(
            self._storage, self._coder, self._desc, si,
            replace_corrupt=self._replace_corrupt,
            repair_stats=self.stats)
        self.stats.stripes_scanned += 1
        self.stats.shares_regenerated += len(r.regenerated_shares)
        return r

    async def cancel(self) -> None:
        """Cancel this repair only; other reads/repairs are unaffected."""
        self._cancel.set()
        if self._task is not None:
            self._task.cancel()

    async def aclose(self) -> None:
        await self.cancel()
        await self._mark_closed()

    async def _mark_closed(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.stats.closed = True
        result = self._on_close(self)
        if asyncio.iscoroutine(result):
            await result
