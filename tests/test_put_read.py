"""Write/commit semantics, streaming reads, range reads, stats and resources."""

import asyncio
import hashlib

import pytest

from stripe_recovery_core import (
    ErrorKind,
    InMemoryBackend,
    KernelConfig,
    ObjectDescriptor,
    StorageError,
    StripeKernel,
    UsageError,
)
from util import FaultBackend, make_kernel, pattern_bytes, piece_key, run


def test_roundtrip_1003_bytes_with_full_confirmation():
    """The original zfec experiment shape: 1003 bytes, 4-of-7, and this time
    the result is verified content, not just decoder output."""

    async def go():
        kernel, backend = make_kernel()
        data = pattern_bytes(1003)
        result = await kernel.put("object-1003", data)
        desc = result.descriptor
        assert desc.length == 1003
        assert desc.k == 4 and desc.m == 7
        assert desc.stripe_count == 4  # 256+256+256+235
        assert desc.content_sha256 == hashlib.sha256(data).hexdigest()
        assert result.shards_written == 4 * 7

        async with kernel.read(desc) as session:
            chunks = [c async for c in session]
        assert b"".join(chunks) == data
        report = session.report
        assert report.fully_confirmed
        assert report.content_verified is True
        assert report.stripes_decoded == 4
        assert report.shards_used == {0, 1, 2, 3}  # first k shards sufficed
        assert report.shards_unavailable == {}
        assert report.confirmed_ranges == [(0, 1003)]
        # resources reclaimed after the session
        rr = kernel.resource_report()
        assert rr.active_reads == 0 and rr.stripe_buffers_held == 0

    run(go())


def test_regenerated_shards_are_byte_identical():
    """Same content encoded twice -> byte-identical shard pieces (deterministic
    codec), even though generations differ."""

    async def go():
        kernel, backend = make_kernel()
        data = pattern_bytes(1003)
        d1 = (await kernel.put("a", data)).descriptor
        d2 = (await kernel.put("b", data)).descriptor
        assert d1.generation != d2.generation
        for shard in range(7):
            for stripe in range(4):
                k1 = piece_key(kernel, "a", d1.generation, shard, stripe)
                k2 = piece_key(kernel, "b", d2.generation, shard, stripe)
                assert backend._data[k1] == backend._data[k2]

    run(go())


def test_put_accepts_async_streaming_source():
    async def go():
        kernel, _ = make_kernel()
        data = pattern_bytes(1000)
        produced = {"n": 0}

        async def source():
            for i in range(0, len(data), 100):
                produced["n"] += 100
                await asyncio.sleep(0)  # genuinely asynchronous producer
                yield data[i : i + 100]

        result = await kernel.put("streamed", source(), length=len(data))
        assert result.descriptor.length == 1000
        assert await kernel.read_bytes(result.descriptor) == data

    run(go())


def test_declared_length_mismatch_aborts_without_commit():
    async def go():
        kernel, backend = make_kernel()
        with pytest.raises(UsageError):
            await kernel.put("bad", pattern_bytes(1000), length=999)
        assert await kernel.load_descriptor("bad") is None
        assert len(backend) == 0  # staged shards cleaned up

    run(go())


def test_failing_source_leaves_nothing_visible():
    async def go():
        kernel, backend = make_kernel()

        async def flaky_source():
            yield pattern_bytes(300)
            yield pattern_bytes(300)
            raise RuntimeError("caller source died")

        with pytest.raises(RuntimeError):
            await kernel.put("died", flaky_source())
        assert await kernel.load_descriptor("died") is None
        assert len(backend) == 0

    run(go())


def test_storage_failure_during_put_is_storage_error():
    async def go():
        backend = FaultBackend()
        kernel, _ = make_kernel(backend)
        backend.fail_writes = True
        with pytest.raises(StorageError) as exc:
            await kernel.put("nospace", pattern_bytes(500))
        assert exc.value.kind == ErrorKind.STORAGE
        assert await kernel.load_descriptor("nospace") is None

    run(go())


def test_uncommitted_content_is_never_visible():
    """A put that has not committed must not be readable as a complete object,
    even though some shard bytes already sit in storage."""

    async def go():
        backend = InMemoryBackend()
        kernel, _ = make_kernel(backend)
        commit_blocked = asyncio.Event()

        real_write = backend.write

        async def gated_write(key, data):
            if key.endswith("/descriptor"):
                await commit_blocked.wait()
            await real_write(key, data)

        backend.write = gated_write  # type: ignore[assignment]

        put_task = asyncio.create_task(kernel.put("pending", pattern_bytes(800)))
        await asyncio.sleep(0.05)
        # shards are staged, but no descriptor is committed yet
        assert await kernel.load_descriptor("pending") is None
        assert any("/shards/" in k for k in backend.keys())
        commit_blocked.set()
        result = await put_task
        committed = await kernel.load_descriptor("pending")
        assert committed == result.descriptor
        assert await kernel.read_bytes(committed) == pattern_bytes(800)

    run(go())


def test_empty_object_roundtrip():
    async def go():
        kernel, _ = make_kernel()
        result = await kernel.put("empty", b"")
        desc = result.descriptor
        assert desc.length == 0 and desc.stripe_count == 0
        async with kernel.read(desc) as session:
            assert [c async for c in session] == []
        assert session.report.content_verified is True
        assert session.report.fully_confirmed

    run(go())


def test_read_range_from_middle():
    async def go():
        kernel, backend = make_kernel()
        data = pattern_bytes(3000)
        desc = (await kernel.put("ranged", data)).descriptor
        backend.read_count = 0

        async with kernel.read(desc, offset=100, length=1000) as session:
            got = b"".join([c async for c in session])
        assert got == data[100:1100]
        report = session.report
        assert report.confirmed_ranges == [(100, 1100)]
        assert report.content_verified is None  # partial read
        assert report.fully_confirmed
        # range [100,1100) touches stripes 0..4; only k=4 pieces per stripe
        # were fetched, never the whole shard set, never whole-object buffers
        assert backend.read_count <= 5 * 4

    run(go())


def test_read_range_at_object_end_and_single_byte():
    async def go():
        kernel, _ = make_kernel()
        data = pattern_bytes(1003)
        desc = (await kernel.put("edges", data)).descriptor
        assert await kernel.read_bytes(desc, offset=1002, length=1) == data[1002:]
        assert await kernel.read_bytes(desc, offset=1003, length=0) == b""
        assert await kernel.read_bytes(desc, offset=256, length=256) == data[256:512]

    run(go())


def test_read_out_of_range_rejected():
    async def go():
        kernel, _ = make_kernel()
        desc = (await kernel.put("bounds", pattern_bytes(100))).descriptor
        with pytest.raises(UsageError):
            kernel.read(desc, offset=101)
        with pytest.raises(UsageError):
            kernel.read(desc, offset=50, length=51)
        with pytest.raises(UsageError):
            kernel.read(desc, offset=-1)

    run(go())


def test_stats_and_resources_are_verifiable():
    async def go():
        kernel, _ = make_kernel()
        before = kernel.resource_report()
        assert before.active_puts == 0 and before.active_reads == 0

        desc = (await kernel.put("statobj", pattern_bytes(1200))).descriptor
        mid = kernel.resource_report()
        assert mid.active_puts == 0  # put finished, nothing lingers

        got = await kernel.read_bytes(desc)
        assert got == pattern_bytes(1200)
        stats = kernel.stats
        assert stats.objects_written == 1
        assert stats.bytes_encoded == 1200
        assert stats.bytes_stored == 5 * 7 * 64  # 5 stripes x 7 shards x 64B
        assert stats.read_sessions_completed == 1
        assert stats.bytes_delivered == 1200
        assert stats.stripes_decoded == 5
        after = kernel.resource_report()
        assert after == before  # all resources reclaimed

    run(go())


def test_descriptor_is_persistable_and_reusable_across_kernels():
    """A descriptor persisted after commit lets a *different* kernel instance
    (another caller) read the object from the same storage."""

    async def go():
        backend = InMemoryBackend()
        kernel_a, _ = make_kernel(backend)
        data = pattern_bytes(1500)
        desc = (await kernel_a.put("shared", data)).descriptor
        persisted = desc.to_bytes()

        kernel_b, _ = make_kernel(backend)  # another caller, same storage
        revived = ObjectDescriptor.from_bytes(persisted)
        assert await kernel_b.read_bytes(revived) == data
        # and via the name-based latest pointer
        latest = await kernel_b.load_descriptor("shared")
        assert latest == desc

    run(go())
