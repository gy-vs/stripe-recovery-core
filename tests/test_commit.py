"""Commit visibility and generation isolation tests.

* a writer that never finishes is invisible to open();
* committing twice under one name moves the head, but the *old descriptor*
  still resolves and reads the OLD bytes;
* a late repair of the old generation cannot overwrite the new content.
"""

from __future__ import annotations

import unittest

from stripe_recovery_core import (Kernel, KernelConfig, MemoryStorage,
                                  ObjectNotFound)
from stripe_recovery_core.storage import keys

from .inspect import manifest_leaves


class CommitVisibilityTests(unittest.IsolatedAsyncioTestCase):

    def kernel(self, block_size=128):
        self.storage = MemoryStorage()
        return Kernel(self.storage,
                      KernelConfig(block_size=block_size,
                                   max_inflight_bytes=4096))

    async def test_uncommitted_writer_is_invisible(self):
        k = self.kernel()
        w = k.new_writer("ghost", k=2, m=1)
        await w.write(b"x" * 200)
        with self.assertRaises(ObjectNotFound):
            await k.open("ghost")
        await w.aclose()
        with self.assertRaises(ObjectNotFound):
            await k.open("ghost")
        await k.aclose()

    async def test_repeated_commit_keeps_old_generation_readable(self):
        k = self.kernel()
        a = bytes(range(256)) + b"AAAA"
        b = bytes(range(256)) + b"BBBB"
        desc_a = await k.encode("doc", a, k=2, m=2)
        desc_b = await k.encode("doc", b, k=2, m=2)
        self.assertNotEqual(desc_a.generation, desc_b.generation)

        head = await k.open("doc")
        self.assertEqual(head.generation, desc_b.generation)
        self.assertEqual(await k.read_all(desc_b), b)
        # Retaining the old descriptor pins the old content.
        self.assertEqual(await k.read_all(desc_a), a)
        # And explicit generation pinning works too.
        pinned = await k.open("doc", at_generation=desc_a.generation)
        self.assertEqual(pinned.generation, desc_a.generation)
        with self.assertRaises(ObjectNotFound):
            await k.open("doc", at_generation="ab" * 32)
        await k.aclose()

    async def test_late_old_generation_repair_cannot_touch_new_content(self):
        k = self.kernel()
        a = (bytes(range(256)) * 2)[:300]
        b = bytes((i + 9) % 256 for i in range(300))
        desc_a = await k.encode("doc", a, k=2, m=2)
        desc_b = await k.encode("doc", b, k=2, m=2)

        # Damage generation A: delete one data share blob.
        leaves_a = await manifest_leaves(self.storage, desc_a, 0)
        target = keys.blob_key_for_digest(leaves_a[0])
        await self.storage.delete(target)
        # Generation B content is independently stored.
        self.assertEqual(await k.read_all(desc_b), b)

        # Run a repair on the *old* descriptor.
        handle = k.repair(desc_a)
        report = await handle.run()
        self.assertTrue(report.status.ok, report.error)

        # Old generation restored, new generation byte-identical, and head
        # still points at B.
        self.assertEqual(await k.read_all(desc_a), a)
        self.assertEqual(await k.read_all(desc_b), b)
        head_raw = await self.storage.get(keys.head_key(b"doc"))
        self.assertEqual(head_raw, desc_b.to_bytes())
        await k.aclose()

    async def test_storage_failure_aborts_without_publishing(self):
        from .fault_storage import FaultStorage
        from stripe_recovery_core import StorageError
        inner = MemoryStorage()
        faults = FaultStorage(inner)
        k = Kernel(faults, KernelConfig(block_size=128,
                                        max_inflight_bytes=4096))
        faults.put_fail.add("blob/")
        with self.assertRaises(StorageError):
            await k.encode("boom", b"y" * 500, k=2, m=1)
        # Nothing committed under the name.
        with self.assertRaises(ObjectNotFound):
            await k.open("boom")
        await k.aclose()

    async def test_zero_length_and_subblock_objects(self):
        k = self.kernel(block_size=64)
        for payload in (b"", b"1", b"x" * 63, b"z" * 65):
            desc = await k.encode("z", payload, k=3, m=2)
            self.assertEqual(desc.length, len(payload))
            self.assertEqual(await k.read_all(desc), payload)
        await k.aclose()


if __name__ == "__main__":
    unittest.main()
