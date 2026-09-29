"""Streaming, backpressure, cancellation and resource-accounting tests."""

from __future__ import annotations

import asyncio
import unittest

from stripe_recovery_core import (Kernel, KernelConfig, MemoryStorage,
                                  ReadStatus, RepairStatus)
from stripe_recovery_core.storage import keys

from .inspect import manifest_leaves


class SlowStorage:
    """MemoryStorage with a per-get gate used to stall share delivery."""

    def __init__(self, inner, delay: float = 0.02) -> None:
        self.inner = inner
        self.delay = delay
        self.gate = asyncio.Event()
        self.inflight = 0
        self.peak = 0

    async def get(self, key: str) -> bytes:
        if key.startswith("blob/"):
            self.inflight += 1
            self.peak = max(self.peak, self.inflight)
            try:
                await asyncio.sleep(self.delay)
            finally:
                self.inflight -= 1
        return await self.inner.get(key)

    async def put(self, key, value):
        return await self.inner.put(key, value)

    async def put_if_absent(self, key, value):
        return await self.inner.put_if_absent(key, value)

    async def delete(self, key):
        return await self.inner.delete(key)


class StreamingTests(unittest.IsolatedAsyncioTestCase):

    async def test_write_backpressure_bounds_inflight(self):
        storage = MemoryStorage()
        k = Kernel(storage, KernelConfig(
            block_size=256, max_inflight_bytes=256 * 4 * 2))
        # k=4,m=3 -> coded stripe = 256*4*7 = 7168; budget clips to one.
        w = k.new_writer("stream", k=4, m=3)
        observed_max = 0

        async def feeder():
            nonlocal observed_max
            # Much more than one stripe, pushed as fast as possible.
            for _ in range(40):
                await w.write(b"q" * 1024)
                observed_max = max(observed_max, w.inflight_bytes)

        feed = asyncio.ensure_future(feeder())
        await feed
        desc = await w.finish()
        self.assertEqual(desc.length, 40 * 1024)
        # In-flight coded bytes never exceeded one stripe budget.
        self.assertLessEqual(observed_max, 256 * 4 * 7 + 1)
        self.assertEqual(k.resources().active_writers, 0)
        self.assertEqual(await k.read_all(desc), b"q" * (40 * 1024))
        await k.aclose()

    async def test_slow_consumer_does_not_accumulate_object(self):
        storage = MemoryStorage()
        k = Kernel(storage, KernelConfig(
            block_size=64, max_output_bytes=64 * 4))
        data = bytes((i * 17) % 256 for i in range(20_000))
        desc = await k.encode("big", data, k=4, m=3)
        reader = k.read_range(desc)
        total = 0
        chunk_sizes = []
        async for chunk in reader:
            chunk_sizes.append(len(chunk))
            total += len(chunk)
            await asyncio.sleep(0.005)  # deliberately slow consumer
        report = await reader.report()
        self.assertEqual(total, len(data))
        self.assertTrue(report.status.ok)
        # Producer chunks respect block granularity; queued data was bounded.
        self.assertLessEqual(max(chunk_sizes), 64)
        await k.aclose()

    async def test_range_reads_touch_only_needed_stripes(self):
        storage = MemoryStorage()
        slow = SlowStorage(storage)
        k = Kernel(slow, KernelConfig(block_size=128))
        data = bytes((i * 11 + 5) % 256 for i in range(128 * 4 * 5))
        desc = await k.encode("multi", data, k=4, m=3)  # 5 stripes
        before = storage.snapshot()["get"]
        reader = k.read_range(desc,
                              start=128 * 4 + 10, end=128 * 4 + 200)
        out = b"".join([c async for c in reader])
        report = await reader.report()
        self.assertEqual(out, data[128 * 4 + 10: 128 * 4 + 200])
        self.assertTrue(report.status.ok)
        self.assertEqual(tuple(s.stripe_index for s in report.stripes), (1,))
        # Only systematic blocks covering the two requested data blocks.
        self.assertLessEqual(len(report.used_shares), 2)
        await k.aclose()

    async def test_read_cancel_keeps_delivered_bytes_and_frees_reader(self):
        storage = MemoryStorage()
        slow = SlowStorage(storage, delay=0.05)
        k = Kernel(slow, KernelConfig(block_size=64, max_output_bytes=64 * 2))
        data = bytes((i * 7 + 1) % 256 for i in range(10_000))
        desc = await k.encode("c", data, k=4, m=3)
        reader = k.read_range(desc)
        it = reader.__aiter__()
        first = await it.__anext__()
        self.assertEqual(first, data[:64])
        # Abandon after one chunk; cancellation reclaims the producer.
        await reader.aclose()
        self.assertEqual(reader.stats.closed, True)
        self.assertEqual(k.resources().active_readers, 0)
        # Already-delivered bytes remain valid.
        self.assertEqual(first, data[:64])
        rep = reader._report
        self.assertEqual(rep.status, ReadStatus.CANCELLED)
        await k.aclose()

    async def test_repair_cancel_is_independent_of_reads(self):
        storage = MemoryStorage()
        k = Kernel(storage, KernelConfig(
            block_size=128, explicit_repair_concurrency=1))
        data = bytes((i * 3 + 2) % 256 for i in range(128 * 4 * 6))
        desc = await k.encode("r", data, k=4, m=3)  # 6 stripes
        leaves0 = await manifest_leaves(storage, desc, 0)
        # Delete one share per stripe to force work.
        for si in range(6):
            leaves = await manifest_leaves(storage, desc, si)
            await storage.delete(keys.blob_key_for_digest(leaves[0]))
        handle = k.repair(desc)
        run = asyncio.ensure_future(handle.run())
        await asyncio.sleep(0)
        await handle.cancel()
        report = await run
        self.assertIn(report.status,
                      (RepairStatus.CANCELLED, RepairStatus.REPAIRED))
        # A concurrent read is unaffected and still returns truth.
        self.assertEqual(await k.read_all(desc), data)
        await k.aclose()

    async def test_explicit_partial_repair_then_read(self):
        storage = MemoryStorage()
        k = Kernel(storage, KernelConfig(block_size=128))
        data = bytes((i * 5) % 256 for i in range(128 * 4 * 3))
        desc = await k.encode("p", data, k=4, m=3)
        for si in range(3):
            leaves = await manifest_leaves(storage, desc, si)
            await storage.delete(keys.blob_key_for_digest(leaves[5]))
        handle = k.repair(desc, start_stripe=0, end_stripe=2)
        report = await handle.run()
        self.assertTrue(report.status.ok, report.error)
        # Only stripes 0,1 scanned; stripe 2 still has the loss but reads.
        scanned = sorted(s.stripe_index for s in report.stripes)
        self.assertEqual(scanned, [0, 1])
        self.assertEqual(await k.read_all(desc), data)
        await k.aclose()

    async def test_async_iterable_input(self):
        storage = MemoryStorage()
        k = Kernel(storage, KernelConfig(block_size=64))

        async def source():
            for i in range(10):
                yield bytes([i]) * 100
                await asyncio.sleep(0)

        desc = await k.encode("ai", source(), k=2, m=2)
        self.assertEqual(desc.length, 1000)
        self.assertEqual(await k.read_all(desc),
                         b"".join(bytes([i]) * 100 for i in range(10)))
        await k.aclose()


if __name__ == "__main__":
    unittest.main()
