"""Integrity tests: the two failure modes the zfec experiments exposed.

* shares from a *different encoding* of the same object name must never be
  accepted as content of this descriptor;
* a single-bit change to one share must never surface as verified bytes.

Mathematical decoding succeeding is treated as meaningless until the
output is authenticated against the descriptor.
"""

from __future__ import annotations

import asyncio
import unittest

from stripe_recovery_core import (Kernel, KernelConfig, MemoryStorage,
                                  ReadStatus, UnrecoverableError)
from stripe_recovery_core.crypto import content_digest
from stripe_recovery_core.storage import keys

from .fault_storage import FaultStorage
from .inspect import manifest_leaves


def data1003(seed: int = 3) -> bytes:
    # Distinct, non-repeating content so coded blocks do not collapse onto
    # shared content-addressed blobs by accident.
    return bytes(((i * 131 + seed * 17) ^ (i >> 3)) % 256
                 for i in range(1003))


class IntegrityTests(unittest.IsolatedAsyncioTestCase):

    def make_kernel(self, block_size=256):
        self.storage = MemoryStorage()
        self.faults = FaultStorage(self.storage)
        return Kernel(self.faults, KernelConfig(block_size=block_size))

    async def test_1003_four_plus_three_roundtrip(self):
        k = self.make_kernel()
        data = data1003()
        desc = await k.encode("obj", data, k=4, m=3)
        self.assertEqual(desc.length, 1003)
        self.assertEqual(desc.stripe_count, 1)
        back = await k.read_all(desc)
        self.assertEqual(back, data)
        # Regenerated shares from a fresh decode are byte-identical: any 4
        # of the 7 reconstruct the same coded blocks; k=4 tolerates exactly
        # 3 losses.  Each round deletes a different 3-share pattern.
        for missing in [(0, 1, 2), (4, 5, 6), (0, 2, 6)]:
            leaves = await manifest_leaves(self.storage, desc, 0)
            for j in missing:
                await self.storage.delete(
                    keys.blob_key_for_digest(leaves[j]))
            self.assertEqual(await k.read_all(desc), data)
            handle = k.repair(desc)
            report = await handle.run()
            self.assertTrue(report.status.ok, report.error)
            self.assertEqual(set(report.stripes[0].regenerated_shares),
                             set(missing))
            # And a fourth loss beyond tolerance is honestly unrecoverable.
        leaves = await manifest_leaves(self.storage, desc, 0)
        for j in (0, 2, 4, 6):
            await self.storage.delete(keys.blob_key_for_digest(leaves[j]))
        with self.assertRaises(UnrecoverableError):
            await k.read_all(desc)
        await k.aclose()

    async def test_mixed_generation_shares_rejected(self):
        """Two encodings of the same name: cross-generation mix must fail."""
        k = self.make_kernel()
        data_a = data1003(seed=3)
        data_b = data1003(seed=99)
        desc_a = await k.encode("obj", data_a, k=4, m=3)
        desc_b = await k.encode("obj", data_b, k=4, m=3)
        # head now points at generation B
        head = await self.storage.get(keys.head_key(b"obj"))
        self.assertEqual(head, desc_b.to_bytes())
        self.assertNotEqual(desc_a.generation, desc_b.generation)

        # Attack: splice generation A's blob for a data position of B's
        # manifest.  B's manifest names B's content digest at that slot, so
        # the foreign blob simply does not satisfy it.
        leaves_a = await manifest_leaves(self.storage, desc_a, 0)
        leaves_b = await manifest_leaves(self.storage, desc_b, 0)
        foreign_blob = await self.storage.get(
            keys.blob_key_for_digest(leaves_a[0]))
        # Make enough B shares disappear that the kernel must rely on the
        # position we poisoned.  Delete B's own blob for share 0 and write
        # A's bytes at B's expected content key.
        await self.storage.delete(keys.blob_key_for_digest(leaves_b[0]))
        await self.storage.put(keys.blob_key_for_digest(leaves_b[0]),
                               foreign_blob)
        # Sanity: content digests differ, so the planted bytes mismatch.
        self.assertNotEqual(content_digest(foreign_blob), leaves_b[0])

        # With 3 other shares deleted, fewer than k authentic remain and
        # the read must report UNRECOVERABLE -- never foreign bytes.
        for j in (1, 2, 3):
            await self.storage.delete(keys.blob_key_for_digest(leaves_b[j]))
        reader = k.read_range(desc_b)
        chunks = [c async for c in reader]
        report = await reader.report()
        self.assertEqual(report.status, ReadStatus.UNRECOVERABLE)
        self.assertEqual(b"".join(chunks), b"")
        # Old descriptor still reads the OLD content.
        self.assertEqual(await k.read_all(desc_a), data_a)
        await k.aclose()

    async def test_single_bit_flip_is_not_returned_as_content(self):
        """One flipped byte in one share: detect it, decode around it."""
        k = self.make_kernel()
        data = data1003()
        desc = await k.encode("obj", data, k=4, m=3)
        leaves = await manifest_leaves(self.storage, desc, 0)

        # Flip a bit in share 0 (a systematic data share) on physical media.
        bkey = keys.blob_key_for_digest(leaves[0])
        original = await self.storage.get(bkey)
        mutated = bytearray(original)
        mutated[10] ^= 0xFF
        await self.storage.put(bkey, bytes(mutated))

        # All 7 positions still *present*; share 0 is corrupt.  Parity lets
        # the read succeed CONFIRMED while explicitly naming the corrupt
        # share -- it is never silently used.
        reader = k.read_range(desc)
        out = b"".join([c async for c in reader])
        report = await reader.report()
        self.assertEqual(report.status, ReadStatus.CONFIRMED)
        self.assertEqual(out, data)
        self.assertIn((0, 0), report.corrupt_shares)
        self.assertNotIn((0, 0), report.used_shares)

        # Now push past the tolerance: corrupt one and remove k others so
        # only k-1 authentic shares exist.
        for j in (4, 5, 6):
            await self.storage.delete(keys.blob_key_for_digest(leaves[j]))
        await self.storage.delete(keys.blob_key_for_digest(leaves[1]))
        reader = k.read_range(desc)
        chunks = [c async for c in reader]
        report = await reader.report()
        self.assertEqual(report.status, ReadStatus.UNRECOVERABLE)
        self.assertEqual(b"".join(chunks), b"")
        self.assertIn((0, 0), report.corrupt_shares)
        await k.aclose()

    async def test_corrupt_manifest_is_unverifiable_not_missing(self):
        k = self.make_kernel(block_size=64)
        desc = await k.encode("m", bytes(range(200)), k=2, m=2)
        mkey = keys.manifest_key(desc, 0)
        raw = bytearray(await self.storage.get(mkey))
        # Manifest prefix is 19 bytes; flip inside share-0's digest.
        raw[19 + 5] ^= 0x01
        await self.storage.put(mkey, bytes(raw))
        reader = k.read_range(desc)
        chunks = [c async for c in reader]
        report = await reader.report()
        self.assertEqual(report.status, ReadStatus.UNVERIFIABLE)
        self.assertEqual(b"".join(chunks), b"")
        await k.aclose()

    async def test_descriptor_tampering_detected(self):
        k = self.make_kernel()
        desc = await k.encode("obj", data1003(), k=4, m=3)
        # Flip inside the serialized stripe root (starts after the 32-byte
        # header + 3-byte name "obj"); either root or digest flip invalidates.
        blob = bytearray(desc.to_bytes())
        self.assertGreater(len(blob), 32 + 3 + 10)
        blob[32 + 3 + 10] ^= 0x01
        from stripe_recovery_core.descriptor import parse_descriptor
        from stripe_recovery_core import DescriptorError
        with self.assertRaises(DescriptorError):
            parse_descriptor(bytes(blob))
        await k.aclose()


if __name__ == "__main__":
    unittest.main()
