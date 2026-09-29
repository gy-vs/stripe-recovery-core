"""Minimal embedding example: write, publish, range-read, and repair.

Run:  python examples/embedding_example.py
"""

from __future__ import annotations

import asyncio

from stripe_recovery_core import (Kernel, KernelConfig, MemoryStorage,
                                  ReadStatus)


async def main() -> None:
    # Caller-supplied storage (any backend implementing the 4 async ops).
    storage = MemoryStorage()
    kernel = Kernel(storage, KernelConfig(block_size=4096))

    # 1) Stream content in; until finish() the generation is invisible.
    writer = kernel.new_writer("report-2026-09", k=4, m=3)
    payload = b"quarterly figures: " + bytes(range(256)) * 64
    step = 4096
    for off in range(0, len(payload), step):
        await writer.write(payload[off:off + step])
        await asyncio.sleep(0)
    descriptor = await writer.finish()
    print(f"committed generation {descriptor.generation[:16]}... "
          f"({descriptor.length} bytes, {descriptor.stripe_count} stripes)")

    # 2) Another process/caller resolves the committed name.
    head = await kernel.open("report-2026-09")

    # 3) Stream a middle range -- no whole-object buffering.
    reader = kernel.read_range(head, start=100, end=500)
    chunks = []
    async for chunk in reader:
        chunks.append(chunk)
    report = await reader.report()
    assert report.status is ReadStatus.CONFIRMED
    print(f"read {report.bytes_returned} bytes using shares "
          f"{report.used_shares}; missing={report.missing_shares} "
          f"corrupt={report.corrupt_shares}")

    # 4) Independently cancellable repair over the whole object.
    repair_handle = kernel.repair(head)
    repair_report = await repair_handle.run()
    print(f"repair status={repair_report.status.value} "
          f"shares_regenerated={repair_report.shares_regenerated}")
    print("resources:", kernel.resources())
    await kernel.aclose()


if __name__ == "__main__":
    asyncio.run(main())
