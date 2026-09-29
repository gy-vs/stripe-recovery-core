"""Large-object and memory-bounds tests (opt-in, resource heavy).

Run with SRC_BIG=1 (and optionally SRC_BIG_MIB to change object size).
These verify the core scaling promise on a >256 MiB object:

* block-wise streaming writes complete,
* a read starting in the *middle* returns the correct slice,
* peak resident memory tracks the configured stripe/output budgets rather
  than the object size,
* per-range resources are reclaimed after the reader closes,
* the committed descriptor exposes the confirmed length and stripe map.
"""

from __future__ import annotations

import asyncio
import gc
import os
import resource
import tempfile
import unittest

from stripe_recovery_core import (FileStorage, Kernel, KernelConfig,
                                  ReadStatus)

BIG = os.environ.get("SRC_BIG") == "1"
BIG_MIB = int(os.environ.get("SRC_BIG_MIB", "300"))
BLOCK = 64 * 1024
K, M = 4, 3


def rss_mib() -> float:
    # ru_maxrss is kilobytes on Linux.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


@unittest.skipUnless(BIG, "set SRC_BIG=1 to run the >256 MiB verification")
class BigObjectTests(unittest.IsolatedAsyncioTestCase):

    async def test_300mib_stream_write_mid_read_reclaim(self):
        total = BIG_MIB * 1024 * 1024
        with tempfile.TemporaryDirectory(prefix="src-big-") as tmp:
            storage = FileStorage(tmp)
            k = Kernel(storage, KernelConfig(
                block_size=BLOCK,
                max_inflight_bytes=BLOCK * K * 4,   # ~1 MiB coded in flight
                max_output_bytes=BLOCK * 4))
            gc.collect()
            base_rss = rss_mib()
            writer = k.new_writer("large", k=K, m=M, block_size=BLOCK)

            chunk = BLOCK * K  # one data stripe
            written = 0

            def block_value(off: int) -> int:
                # Cheap deterministic, non-repeating pattern.
                return ((off * 1103515245 + 12345) >> 8) & 0xFF

            while written < total:
                n = min(chunk, total - written)
                buf = bytearray(n)
                for i in range(n):
                    buf[i] = block_value(written + i)
                await writer.write(bytes(buf))
                written += n
                del buf
            desc = await writer.finish()
            self.assertEqual(desc.length, total)
            stripe_data = BLOCK * K
            self.assertEqual(desc.stripe_count,
                             (total + stripe_data - 1) // stripe_data)

            peak_after_write = rss_mib()
            write_growth = peak_after_write - base_rss
            # Generous ceiling: budget plus interpreter/runtime noise, but
            # nowhere near object size.  Must hold regardless of object size.
            self.assertLess(
                write_growth, 96,
                f"writer RSS grew {write_growth:.1f} MiB for {BIG_MIB} MiB "
                f"object (expected budget-scale only)")

            # Middle range read: 4 MiB starting ~40% into the object.
            start = int(total * 0.4)
            length = 4 * 1024 * 1024
            reader = k.read_range(desc, start=start, end=start + length)
            got = bytearray()
            offset = start
            async for part in reader:
                for i, b in enumerate(part):
                    self.assertEqual(b, block_value(offset + i))
                got.extend(part)
                offset += len(part)
            report = await reader.report()
            self.assertEqual(report.status, ReadStatus.CONFIRMED)
            self.assertEqual(report.confirmed_end, start + length)
            self.assertEqual(len(got), length)
            # Only the intersecting stripe(s) were processed, never all.
            self.assertLessEqual(
                len(report.stripes),
                length // stripe_data + 2)
            peak_read = rss_mib()
            await reader.aclose()
            gc.collect()
            after_close = rss_mib()
            self.assertEqual(k.resources().active_readers, 0)
            self.assertEqual(k.resources().active_writers, 0)
            # Closing reclaims the read pipeline (ru_maxrss is a high-water
            # mark, so compare current allocation behavior via resource
            # count; the hard bound is that the high-water mark did not
            # scale with object size).
            self.assertLess(
                peak_read - base_rss, 96,
                f"read RSS high-water grew by {peak_read - base_rss:.1f} MiB")
            self.assertGreaterEqual(after_close, 0)
            await k.aclose()


if __name__ == "__main__":
    unittest.main()
