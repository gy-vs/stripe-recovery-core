#!/usr/bin/env python3
"""Runnable verification for stripe-recovery-core.

Scenarios (all use the public async API, no test-only shortcuts):

  1. 1003 bytes, k=4/m=3: any 4 of 7 recover; regenerated shares match the
     original coded bytes byte-for-byte.
  2. Shares from a different encoding of the same name mixed in -> the
     kernel returns UNRECOVERABLE/UNVERIFIABLE, never foreign bytes.
  3. One flipped byte in one share -> detected; verified content still
     served when redundancy allows, otherwise a typed non-OK status.
  4. Repeated commits under one name: old descriptor still reads old bytes;
     a late repair of the old generation cannot touch the new generation.
  5. Streaming middle-range read, per-share accounting, backpressure.
  6. Read-cancel vs repair-cancel independence; background repair failure
     never masquerades as changed content.
  7. --big: a >256 MiB object written block-wise, read from the middle,
     RSS measured against stripe-scale budgets, resources reclaimed.

Usage:
    python verify.py                # functional scenarios 1-6
    python verify.py --big [--mib N]
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import os
import resource
import sys
import tempfile
import time

from stripe_recovery_core import (FileStorage, Kernel, KernelConfig,
                                  MemoryStorage, ReadStatus,
                                  RepairStatus, StorageError,
                                  UnrecoverableError, UnverifiableError)
from stripe_recovery_core.crypto import content_digest
from stripe_recovery_core.storage import keys

PASS, FAIL = "PASS", "FAIL"
_results: list[tuple[str, bool, str]] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    _results.append((name, bool(condition), detail))
    print(f"  [{PASS if condition else FAIL}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))


def rss_mib() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def distinct(n: int, seed: int = 1) -> bytes:
    return bytes(((i * 131 + seed * 17) ^ (i >> 2)) % 256 for i in range(n))


async def leaves_of(storage, desc, si: int):
    from stripe_recovery_core.crypto import parse_manifest
    raw = await storage.get(keys.manifest_key(desc, si))
    return parse_manifest(raw, desc.n)


# ---------------------------------------------------------------- scenario 1

async def scenario_roundtrip() -> None:
    print("\n[1] 1003 bytes, k=4, m=3: any four shares recover, exact bytes")
    storage = MemoryStorage()
    kernel = Kernel(storage, KernelConfig(block_size=256))
    data = distinct(1003)
    desc = await kernel.encode("obj", data, k=4, m=3)
    check("descriptor length is 1003", desc.length == 1003)
    check("one padded stripe", desc.stripe_count == 1)
    check("full read matches original",
          await kernel.read_all(desc) == data)

    leaves = await leaves_of(storage, desc, 0)
    # Re-encode independently and compare every regenerated share.
    import zfec
    blocks = desc.params.split_stripe(data)
    expected = zfec.Encoder(4, 7).encode(blocks)
    stored = [await storage.get(keys.blob_key_for_digest(leaves[j]))
              for j in range(7)]
    check("all 7 stored shares byte-equal to a fresh zfec encode",
          stored == list(expected))

    for missing in ((0, 1, 2), (4, 5, 6), (0, 2, 6)):
        for j in missing:
            await storage.delete(keys.blob_key_for_digest(leaves[j]))
        got = await kernel.read_all(desc)
        check(f"recover after deleting shares {missing}", got == data)
        repair = await (kernel.repair(desc)).run()
        check(f"repair refills {missing}",
              repair.status is RepairStatus.REPAIRED
              and set(repair.stripes[0].regenerated_shares) == set(missing),
              repair.error or repair.status.value)
    await kernel.aclose()


async def _repair(kernel, desc):
    h = kernel.repair(desc)
    return await h.run()


# ---------------------------------------------------------------- scenario 2

async def scenario_mixed_generations() -> None:
    print("\n[2] Mixing shares from different encodings of the same name")
    storage = MemoryStorage()
    kernel = Kernel(storage, KernelConfig(block_size=256))
    data_a, data_b = distinct(1003, 1), distinct(1003, 9)
    desc_a = await kernel.encode("obj", data_a, k=4, m=3)
    desc_b = await kernel.encode("obj", data_b, k=4, m=3)
    check("generations differ", desc_a.generation != desc_b.generation)

    leaves_a = await leaves_of(storage, desc_a, 0)
    leaves_b = await leaves_of(storage, desc_b, 0)
    foreign = await storage.get(keys.blob_key_for_digest(leaves_a[0]))
    # Poison B's position 0 with A's block; remove B shares 1..3.
    await storage.delete(keys.blob_key_for_digest(leaves_b[0]))
    await storage.put(keys.blob_key_for_digest(leaves_b[0]), foreign)
    for j in (1, 2, 3):
        await storage.delete(keys.blob_key_for_digest(leaves_b[j]))

    reader = kernel.read_range(desc_b)
    chunks = [c async for c in reader]
    report = await reader.report()
    check("poisoned read is not CONFIRMED",
          not report.status.ok, f"got {report.status.value}")
    check("status is UNRECOVERABLE",
          report.status is ReadStatus.UNRECOVERABLE, report.status.value)
    check("no unverified bytes returned", b"".join(chunks) == b"")
    check("old descriptor still yields OLD content",
          await kernel.read_all(desc_a) == data_a)
    await kernel.aclose()


# ---------------------------------------------------------------- scenario 3

async def scenario_bit_flip() -> None:
    print("\n[3] A single flipped byte in one share")
    storage = MemoryStorage()
    kernel = Kernel(storage, KernelConfig(block_size=256))
    data = distinct(1003)
    desc = await kernel.encode("obj", data, k=4, m=3)
    leaves = await leaves_of(storage, desc, 0)
    bkey = keys.blob_key_for_digest(leaves[0])
    raw = bytearray(await storage.get(bkey))
    raw[10] ^= 0x01
    await storage.put(bkey, bytes(raw))

    reader = kernel.read_range(desc)
    out = b"".join([c async for c in reader])
    report = await reader.report()
    check("read still CONFIRMED using redundancy", report.status.ok)
    check("returned bytes are exactly the original", out == data)
    check("corrupt share named in the report",
          (0, 0) in report.corrupt_shares)
    check("corrupt share excluded from used set",
          (0, 0) not in report.used_shares)

    for j in (1, 4, 5, 6):
        await storage.delete(keys.blob_key_for_digest(leaves[j]))
    reader = kernel.read_range(desc)
    _ = [c async for c in reader]
    report = await reader.report()
    check("beyond tolerance -> typed UNRECOVERABLE (never garbage bytes)",
          report.status is ReadStatus.UNRECOVERABLE,
          report.status.value)
    await kernel.aclose()


# ---------------------------------------------------------------- scenario 4

async def scenario_commits_and_late_repair() -> None:
    print("\n[4] Repeated commits; old generation immutable; late repair safe")
    storage = MemoryStorage()
    kernel = Kernel(storage, KernelConfig(block_size=128))
    a = distinct(600, 2)
    b = distinct(600, 8)
    desc_a = await kernel.encode("doc", a, k=3, m=2)
    desc_b = await kernel.encode("doc", b, k=3, m=2)
    head = await kernel.open("doc")
    check("name resolves to latest commit", head.generation == desc_b.generation)
    check("old descriptor still reads OLD content",
          await kernel.read_all(desc_a) == a)

    leaves_a = await leaves_of(storage, desc_a, 0)
    await storage.delete(keys.blob_key_for_digest(leaves_a[0]))
    repair = await (kernel.repair(desc_a)).run()
    check("late repair of old generation succeeds", repair.status.ok,
          repair.error or "")
    check("old content restored", await kernel.read_all(desc_a) == a)
    head_raw = await storage.get(keys.head_key(b"doc"))
    check("head still points at new generation", head_raw == desc_b.to_bytes())
    check("new content untouched", await kernel.read_all(desc_b) == b)
    await kernel.aclose()


# ---------------------------------------------------------------- scenario 5

async def scenario_streaming_and_accounting() -> None:
    print("\n[5] Streaming middle-range read with per-share accounting")
    storage = MemoryStorage()
    kernel = Kernel(storage, KernelConfig(
        block_size=128, max_output_bytes=512))
    data = distinct(128 * 4 * 5)
    desc = await kernel.encode("multi", data, k=4, m=3)
    start, end = 128 * 4 + 10, 128 * 4 + 200
    reader = kernel.read_range(desc, start=start, end=end)
    out = b"".join([c async for c in reader])
    report = await reader.report()
    check("middle slice bytes correct", out == data[start:end])
    check("status CONFIRMED", report.status is ReadStatus.CONFIRMED)
    check("confirmed_end marks the boundary", report.confirmed_end == end)
    check("only one stripe processed",
          tuple(s.stripe_index for s in report.stripes) == (1,))
    check("only 2 systematic shares used", len(report.used_shares) == 2,
          f"{len(report.used_shares)} used")
    check("fetch accounting is concrete (2 used + 5 probed = 7 blocks)",
          reader.stats.shares_fetched == 7
          and reader.stats.share_bytes_fetched == 7 * 128,
          f"{reader.stats.shares_fetched} shares / "
          f"{reader.stats.share_bytes_fetched} bytes")
    check("zero missing shares reported",
          len(report.missing_shares) == 0)
    await kernel.aclose()


# ---------------------------------------------------------------- scenario 6

async def scenario_cancellation() -> None:
    print("\n[6] Read/repair cancellation independence")
    storage = MemoryStorage()
    kernel = Kernel(storage, KernelConfig(block_size=64))
    data = distinct(64 * 4 * 20)
    desc = await kernel.encode("c", data, k=4, m=3)
    reader = kernel.read_range(desc)
    it = reader.__aiter__()
    first = await it.__anext__()
    await reader.aclose()
    check("cancelled read reports CANCELLED",
          reader._report.status is ReadStatus.CANCELLED)
    check("delivered bytes stay valid", first == data[:64])
    check("reader resource released", kernel.resources().active_readers == 0)

    for si in range(20):
        lv = await leaves_of(storage, desc, si)
        await storage.delete(keys.blob_key_for_digest(lv[0]))
    handle = kernel.repair(desc)
    task = asyncio.ensure_future(handle.run())
    await asyncio.sleep(0)
    await handle.cancel()
    r = await task
    check("repair cancellation is a typed status",
          r.status in (RepairStatus.CANCELLED, RepairStatus.REPAIRED),
          r.status.value)
    check("content still fully readable after repair cancel",
          await kernel.read_all(desc) == data)
    await kernel.aclose()


# ---------------------------------------------------------------- big object

async def scenario_big(mib: int) -> int:
    print(f"\n[7] Big object: {mib} MiB, block-wise write + middle read + RSS")
    with tempfile.TemporaryDirectory(prefix="src-big-") as tmp:
        storage = FileStorage(tmp)
        block = 64 * 1024
        k, m = 4, 3
        kernel = Kernel(storage, KernelConfig(
            block_size=block, max_inflight_bytes=block * k * 4,
            max_output_bytes=block * 4))
        total = mib * 1024 * 1024
        gc.collect()
        base = rss_mib()
        t0 = time.monotonic()
        writer = kernel.new_writer("large", k=k, m=m)
        written = 0
        chunk = block * k
        pattern = bytearray((i * 31 + 7) & 0xFF for i in range(256))
        while written < total:
            n = min(chunk, total - written)
            buf = (pattern * (n // 256 + 1))[:n]
            # Vary per stripe so blocks are distinct.
            stripe_idx = written // (block * k)
            buf = bytearray(buf)
            for i in range(0, len(buf), 256):
                buf[i] = (buf[i] + stripe_idx) & 0xFF
            await writer.write(bytes(buf))
            written += n
        desc = await writer.finish()
        write_dt = time.monotonic() - t0
        peak_write = rss_mib()
        check(f"committed {mib} MiB in {write_dt:.1f}s",
              desc.length == total)
        check(f"writer RSS growth {peak_write - base:.1f} MiB "
              f"(budget-scaled, not object-scaled)",
              peak_write - base < 128)
        check("stripe count matches length/k*block",
              desc.stripe_count ==
              (total + block * k - 1) // (block * k))

        start = int(total * 0.4)
        length = 4 * 1024 * 1024
        t0 = time.monotonic()
        reader = kernel.read_range(desc, start=start, end=start + length)
        got = b"".join([c async for c in reader])
        report = await reader.report()
        read_dt = time.monotonic() - t0
        check(f"middle 4 MiB read in {read_dt:.2f}s",
              report.status is ReadStatus.CONFIRMED and len(got) == length)
        check("read touched only intersecting stripes",
              len(report.stripes) <= length // (block * k) + 2,
              f"{len(report.stripes)} stripes")
        check("used-shares accounting populated",
              len(report.used_shares) > 0)
        await reader.aclose()
        check("resources zeroed after close",
              kernel.resources().active_readers == 0
              and kernel.resources().active_writers == 0)
        await kernel.aclose()
    return 0


async def amain(args) -> int:
    await scenario_roundtrip()
    await scenario_mixed_generations()
    await scenario_bit_flip()
    await scenario_commits_and_late_repair()
    await scenario_streaming_and_accounting()
    await scenario_cancellation()
    if args.big:
        await scenario_big(args.mib)

    print("\n" + "=" * 64)
    failed = [n for n, ok, _ in _results if not ok]
    print(f"{len(_results) - len(failed)}/{len(_results)} checks passed")
    if failed:
        print("FAILED:")
        for n in failed:
            print("  -", n)
        return 1
    print("ALL VERIFICATIONS PASSED")
    return 0


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--big", action="store_true",
                   help="also run the >256 MiB object verification")
    p.add_argument("--mib", type=int, default=300)
    args = p.parse_args()
    sys.exit(asyncio.run(amain(args)))


if __name__ == "__main__":
    main()
