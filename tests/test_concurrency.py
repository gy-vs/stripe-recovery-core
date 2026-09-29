"""Concurrent readers + repair + rewrites: results, descriptors and storage
stay consistent; one operation's cancellation never revokes another's."""

import asyncio

from stripe_recovery_core import OperationCancelled, RepairStatus
from util import FaultBackend, make_kernel, pattern_bytes, piece_key, run


def test_concurrent_readers_and_repair_end_consistent():
    async def go():
        backend = FaultBackend(latency=0.001)
        kernel, _ = make_kernel(backend)
        data = pattern_bytes(3000)
        desc = (await kernel.put("obj", data)).descriptor
        # damage: one piece missing, one corrupted
        backend._data.pop(piece_key(kernel, "obj", desc.generation, 6, 0))
        bad = bytearray(backend._data[piece_key(kernel, "obj", desc.generation, 5, 1)])
        bad[0] ^= 0xFF
        backend._data[piece_key(kernel, "obj", desc.generation, 5, 1)] = bytes(bad)

        async def read_range(offset, length):
            async with kernel.read(desc, offset=offset, length=length) as s:
                return b"".join([c async for c in s])

        readers = [
            read_range(0, 1000),
            read_range(500, 1500),
            read_range(1000, 2000),
            read_range(0, 3000),
        ]
        repair = kernel.repair(desc)
        results = await asyncio.gather(*readers, repair.run())
        *blobs, repair_result = results

        assert blobs[0] == data[0:1000]
        assert blobs[1] == data[500:2000]
        assert blobs[2] == data[1000:3000]
        assert blobs[3] == data
        assert repair_result.status == RepairStatus.COMPLETED
        assert sorted(repair_result.pieces_repaired) == [(0, 6), (1, 5)]

        # storage now fully consistent with the descriptor
        for stripe in desc.stripes:
            for shard, ph in enumerate(stripe.piece_hashes):
                import hashlib

                key = piece_key(kernel, "obj", desc.generation, shard, stripe.index)
                assert hashlib.sha256(backend._data[key]).hexdigest() == ph
        # stats and resources reconcile
        stats = kernel.stats
        assert stats.read_sessions_completed == 4
        assert stats.repair_runs == 1
        rr = kernel.resource_report()
        assert rr.active_reads == 0 and rr.active_repairs == 0
    run(go())


def test_one_readers_cancellation_does_not_revoke_anothers():
    async def go():
        backend = FaultBackend(latency=0.002)
        kernel, _ = make_kernel(backend)
        data = pattern_bytes(2000)
        desc = (await kernel.put("obj", data)).descriptor

        session_a = kernel.read(desc)
        first = await session_a.__anext__()
        assert first == data[:50]

        async def full_read_b():
            async with kernel.read(desc) as s:
                return b"".join([c async for c in s])

        task_b = asyncio.create_task(full_read_b())
        session_a.cancel()
        try:
            await session_a.__anext__()
            raised = None
        except OperationCancelled as e:
            raised = e
        assert raised is not None
        # A's already-delivered bytes remain valid content
        assert first == data[:50]
        # B completes undisturbed
        assert await task_b == data
        assert kernel.stats.read_sessions_cancelled == 1
        assert kernel.stats.read_sessions_completed == 1
    run(go())


def test_repair_cancellation_does_not_affect_concurrent_reader():
    async def go():
        backend = FaultBackend(latency=0.002)
        kernel, _ = make_kernel(backend)
        data = pattern_bytes(2500)
        desc = (await kernel.put("obj", data)).descriptor
        backend._data.pop(piece_key(kernel, "obj", desc.generation, 6, 2))

        op = kernel.repair(desc)
        repair_task = asyncio.create_task(op.run())

        async def full_read():
            async with kernel.read(desc) as s:
                return b"".join([c async for c in s])

        read_task = asyncio.create_task(full_read())
        await asyncio.sleep(0.01)
        op.cancel()
        repair_result = await repair_task
        assert repair_result.status == RepairStatus.CANCELLED
        # reader unaffected by the repair's cancellation; content intact
        assert await read_task == data
    run(go())


def test_concurrent_same_name_puts_keep_generations_isolated():
    async def go():
        backend = FaultBackend(latency=0.001)
        kernel, _ = make_kernel(backend)
        data_a = pattern_bytes(1200, seed=3)
        data_b = pattern_bytes(1200, seed=9)

        result_a, result_b = await asyncio.gather(
            kernel.put("obj", data_a), kernel.put("obj", data_b)
        )
        da, db = result_a.descriptor, result_b.descriptor
        assert da.generation != db.generation
        # each descriptor reads its own content regardless of commit order
        assert await kernel.read_bytes(da) == data_a
        assert await kernel.read_bytes(db) == data_b
        # latest pointer is one of them and is readable
        latest = await kernel.load_descriptor("obj")
        assert latest in (da, db)
    run(go())
