"""Read-repair coupling and storage-failure classification tests."""

from __future__ import annotations

import asyncio
import unittest

from stripe_recovery_core import (Kernel, KernelConfig, MemoryStorage,
                                  ReadStatus)
from stripe_recovery_core.errors import NotFound
from stripe_recovery_core.storage import keys

from .fault_storage import FaultStorage
from .inspect import manifest_leaves


class FlakyStorage(FaultStorage):
    """Fails gets for configured share positions a bounded number of times."""

    def __init__(self, inner):
        super().__init__(inner)
        self.transient_fail_keys: dict[str, int] = {}

    async def get(self, key: str) -> bytes:
        remaining = self.transient_fail_keys.get(key)
        if remaining is not None:
            if remaining > 0:
                self.transient_fail_keys[key] = remaining - 1
                raise ConnectionError("transient backend error")
        return await super().get(key)


class RepairOnReadTests(unittest.IsolatedAsyncioTestCase):

    async def test_read_with_repair_refills_missing_share(self):
        storage = MemoryStorage()
        k = Kernel(storage, KernelConfig(block_size=128))
        data = bytes((i * 9 + 1) % 256 for i in range(128 * 4 * 2))
        desc = await k.encode("o", data, k=4, m=3)
        leaves1 = await manifest_leaves(storage, desc, 1)
        missing_key = keys.blob_key_for_digest(leaves1[0])
        await storage.delete(missing_key)

        reader = k.read_range(desc, repair_on_read=True)
        out = b"".join([c async for c in reader])
        report = await reader.report()
        self.assertTrue(report.status.ok)
        self.assertEqual(out, data)
        self.assertIn((1, 0), report.missing_shares)
        # Background repair eventually restores the blob.
        await asyncio.wait_for(_drain_background(k), timeout=5)
        restored = await storage.get(missing_key)
        self.assertEqual(len(restored), desc.block_size)
        # A subsequent read sees the share as available, content unchanged.
        self.assertEqual(await k.read_all(desc), data)
        await k.aclose()

    async def test_failed_repair_does_not_change_reported_content(self):
        flaky = FlakyStorage(MemoryStorage())
        k = Kernel(flaky, KernelConfig(block_size=128))
        data = bytes((i * 5 + 4) % 256 for i in range(128 * 4))
        desc = await k.encode("o", data, k=4, m=3)
        leaves = await manifest_leaves(flaky.inner, desc, 0)
        # Force the repaired blob's put_if_absent to fail.
        flaky.cas_fail.add("blob/")
        reader = k.read_range(desc, repair_on_read=True,
                              start=10, end=20)
        # Delete a parity share so repair has something to write.
        await flaky.inner.delete(keys.blob_key_for_digest(leaves[5]))
        out = b"".join([c async for c in reader])
        report = await reader.report()
        self.assertTrue(report.status.ok)
        self.assertEqual(out, data[10:20])
        await asyncio.wait_for(_drain_background(k), timeout=5)
        # Content path remains intact; repair failure surfaced only in
        # coordinator results, never as "content changed".
        self.assertEqual(await k.read_all(desc), data)
        await k.aclose()

    async def test_storage_failure_distinct_from_unrecoverable(self):
        flaky = FlakyStorage(MemoryStorage())
        k = Kernel(flaky, KernelConfig(block_size=128))
        data = bytes((i * 3) % 256 for i in range(128 * 4))
        desc = await k.encode("o", data, k=4, m=3)
        leaves = await manifest_leaves(flaky.inner, desc, 0)
        share_keys = [keys.blob_key_for_digest(leaves[j])
                      for j in range(desc.n)]
        # Hard error on every share fetch and manifest -> storage_failure.
        flaky.get_fail.add("blob/")
        flaky.get_fail.add("/manifest")
        reader = k.read_range(desc)
        _ = [c async for c in reader]
        report = await reader.report()
        self.assertEqual(report.status, ReadStatus.STORAGE_FAILURE)
        flaky.get_fail.clear()

        # Missing-but-clean (NotFound) with fewer than k authentic shares
        # is UNRECOVERABLE, not a storage failure.
        for bk in share_keys[:5]:
            await flaky.inner.delete(bk)
        # Remove manifest too would be unrecoverable; keep manifest present.
        reader = k.read_range(desc)
        _ = [c async for c in reader]
        report = await reader.report()
        self.assertEqual(report.status, ReadStatus.UNRECOVERABLE)
        self.assertFalse(any(t for t in report.fetch_failed_shares))
        await k.aclose()

    async def test_parallel_readers_and_repairs_converge(self):
        storage = MemoryStorage()
        k = Kernel(storage, KernelConfig(block_size=256))
        data = bytes(((i * 131 + 7) ^ (i >> 2)) % 256
                     for i in range(256 * 4 * 4))
        desc = await k.encode("o", data, k=4, m=3)
        for si in range(4):
            leaves = await manifest_leaves(storage, desc, si)
            for j in (0, 6):
                await storage.delete(keys.blob_key_for_digest(leaves[j]))

        async def one_read():
            reader = k.read_range(desc, repair_on_read=True)
            out = b"".join([c async for c in reader])
            report = await reader.report()
            return out, report

        results = await asyncio.gather(*(one_read() for _ in range(4)))
        for out, report in results:
            self.assertTrue(report.status.ok, report.error)
            self.assertEqual(out, data)
        # Let all repairs finish and explicitly repair any stragglers.
        await asyncio.wait_for(_drain_background(k), timeout=10)
        handle = k.repair(desc)
        final = await handle.run()
        self.assertTrue(final.status.ok, final.error)
        self.assertEqual(final.shares_regenerated, 0)
        self.assertEqual(await k.read_all(desc), data)
        await k.aclose()


async def _drain_background(kernel: Kernel) -> None:
    while kernel.background_repairs:
        await asyncio.sleep(0.01)


if __name__ == "__main__":
    unittest.main()
