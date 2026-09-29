"""Streaming behaviour: real backpressure on both ends, memory bounded by
configuration (not object size), mid-object access, resource reclamation."""

import asyncio
import os
import tracemalloc

import pytest

from stripe_recovery_core import InMemoryBackend, KernelConfig, StripeKernel
from util import DirectoryBackend, make_kernel, pattern_bytes, run


def test_slow_consumer_does_not_force_buffering():
    """Pull a single chunk from a 16-stripe object: the kernel must not run
    ahead and recover content nobody asked for."""

    async def go():
        kernel, backend = make_kernel()
        data = pattern_bytes(256 * 16)
        desc = (await kernel.put("obj", data)).descriptor
        backend.read_count = 0

        session = kernel.read(desc)
        first = await session.__anext__()
        assert first == data[:50]  # read_chunk_size = 50
        # one stripe decoded so far: exactly k piece reads, nothing more
        assert backend.read_count <= 4
        await session.aclose()
        rr = kernel.resource_report()
        assert rr.active_reads == 0 and rr.stripe_buffers_held == 0
    run(go())


def test_slow_producer_paces_storage_writes():
    """The kernel consumes the source as stripes fill; it must not drain the
    whole source before writing the first stripe."""

    async def go():
        kernel, backend = make_kernel()
        data = pattern_bytes(2000)
        produced = {"bytes": 0}
        produced_at_first_write = {"bytes": None}

        async def source():
            for i in range(0, len(data), 100):
                produced["bytes"] += 100
                await asyncio.sleep(0)
                yield data[i : i + 100]

        real_write = backend.write

        async def watching_write(key, blob):
            if produced_at_first_write["bytes"] is None and "/shards/" in key:
                produced_at_first_write["bytes"] = produced["bytes"]
            await real_write(key, blob)

        backend.write = watching_write  # type: ignore[assignment]
        await kernel.put("obj", source())
        # first shard write happened right after the first 256-byte stripe
        # was assembled from 100-byte chunks: at most 300 source bytes in
        assert produced_at_first_write["bytes"] is not None
        assert produced_at_first_write["bytes"] <= 300
    run(go())


def test_consumer_cancellation_reclaims_resources():
    async def go():
        kernel, backend = make_kernel()
        data = pattern_bytes(256 * 8)
        desc = (await kernel.put("obj", data)).descriptor

        session = kernel.read(desc)
        await session.__anext__()
        session.cancel()
        try:
            await session.__anext__()
            raised = None
        except Exception as e:  # OperationCancelled
            raised = e
        assert raised is not None and raised.kind == "cancelled"
        rr = kernel.resource_report()
        assert rr.active_reads == 0 and rr.in_flight_piece_ops == 0
        assert kernel.stats.read_sessions_cancelled == 1
    run(go())


class _NullBackend:
    """Discarding backend: isolates the kernel's own buffering from whatever
    the storage layer itself retains."""

    async def read(self, key):
        return None

    async def write(self, key, data):
        pass

    async def delete(self, key):
        pass


def test_large_object_chunked_write_memory_bounded():
    """16 MiB object, 1 MiB stripes: the kernel's write-path buffers track the
    stripe/shard configuration, not the object size."""

    async def go():
        config = KernelConfig(k=4, m=7, shard_size=256 * 1024)
        kernel = StripeKernel(config, _NullBackend())
        n = 16 * 1024 * 1024
        data = pattern_bytes(n)

        async def source():
            for i in range(0, n, 256 * 1024):
                await asyncio.sleep(0)
                yield data[i : i + 256 * 1024]

        tracemalloc.start()
        desc = (await kernel.put("big", source())).descriptor
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert desc.stripe_count == 16
        # one stripe plaintext + m pieces in flight (~2.75 MiB), nowhere near
        # the 16 MiB object
        assert peak < 8 * 1024 * 1024, f"put peak {peak}"
    run(go())


def test_large_object_mid_read_memory_bounded():
    """Mid-object range read of a 16 MiB object touches only its own stripes
    and holds ~one stripe of buffers at a time."""

    async def go():
        backend = InMemoryBackend()
        config = KernelConfig(k=4, m=7, shard_size=256 * 1024, read_chunk_size=64 * 1024)
        kernel = StripeKernel(config, backend)
        n = 16 * 1024 * 1024
        data = pattern_bytes(n)

        async def source():
            for i in range(0, n, 256 * 1024):
                await asyncio.sleep(0)
                yield data[i : i + 256 * 1024]

        desc = (await kernel.put("big", source())).descriptor
        assert desc.stripe_count == 16

        backend.read_count = 0
        offset, length = 8 * 1024 * 1024 + 123, 3 * 1024 * 1024
        tracemalloc.start()
        async with kernel.read(desc, offset=offset, length=length) as session:
            got = b"".join([c async for c in session])
        _, peak_read = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert got == data[offset : offset + length]
        assert session.report.confirmed_ranges == [(offset, offset + length)]
        # range spans stripes 8..11: only their pieces were fetched
        assert backend.read_count <= 4 * 7
        # read path holds ~one stripe at a time, not the object
        assert peak_read < 8 * 1024 * 1024, f"read peak {peak_read}"
        rr = kernel.resource_report()
        assert rr.active_reads == 0 and rr.stripe_buffers_held == 0
    run(go())


def test_read_session_is_single_pass_stream():
    """Chunks arrive incrementally (a real stream, not a preassembled blob)."""

    async def go():
        kernel, _ = make_kernel()
        data = pattern_bytes(1000)
        desc = (await kernel.put("obj", data)).descriptor
        seen = []
        async with kernel.read(desc) as session:
            async for chunk in session:
                seen.append(len(chunk))
        # 1000 bytes in read_chunk_size=50 chunks; chunk boundaries restart
        # per 256-byte stripe (256 = 5*50 + 6, final stripe 232 = 4*50 + 32)
        assert seen == [50, 50, 50, 50, 50, 6] * 3 + [50, 50, 50, 50, 32]
        assert sum(seen) == 1000
    run(go())


@pytest.mark.skipif(
    os.environ.get("SRC_RUN_HUGE") != "1",
    reason=">256 MiB end-to-end run; enable with SRC_RUN_HUGE=1",
)
def test_huge_object_over_256mib_end_to_end(tmp_path):
    """A 300 MiB object on a file-backed store: chunked write, mid-object
    range read, missing-shard recovery, resource reclamation — with memory
    still bounded by the stripe configuration."""

    async def go():
        backend = DirectoryBackend(tmp_path / "store")
        config = KernelConfig(k=4, m=7, shard_size=1024 * 1024, read_chunk_size=256 * 1024)
        kernel = StripeKernel(config, backend)
        n = 300 * 1024 * 1024
        block = pattern_bytes(1024 * 1024)

        async def source():
            for _ in range(n // len(block)):
                await asyncio.sleep(0)
                yield block

        tracemalloc.start()
        desc = (await kernel.put("huge", source(), length=n)).descriptor
        _, peak_put = tracemalloc.get_traced_memory()
        assert desc.length == n
        assert desc.stripe_count == 75  # 4 MiB stripes

        # knock out two shards of a stripe in the middle, then read a range
        # that crosses it: recovery must kick in, bounded as ever
        from util import piece_key

        for shard in (1, 4):
            await backend.delete(piece_key(kernel, "huge", desc.generation, shard, 40))
        backend.read_count = 0
        offset, length = 160 * 1024 * 1024 + 7, 8 * 1024 * 1024
        async with kernel.read(desc, offset=offset, length=length) as session:
            got = b"".join([c async for c in session])
        _, peak_read = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        expected = (block * (n // len(block)))[offset : offset + length]
        assert got == expected
        assert session.report.shards_unavailable == {1: "missing", 4: "missing"}
        assert session.report.fully_confirmed
        # only the stripes covering the range were touched
        assert backend.read_count <= 3 * 7
        # ~stripe-scale buffers (4 MiB stripes), never object-scale
        assert peak_put < 64 * 1024 * 1024, f"put peak {peak_put}"
        assert peak_read < 64 * 1024 * 1024, f"read peak {peak_read}"
        rr = kernel.resource_report()
        assert rr.active_reads == 0 and rr.stripe_buffers_held == 0
        assert kernel.stats.bytes_encoded == n
    run(go())
