"""Repair: restores shards byte-identically, never changes object meaning,
and a late repair of an old generation cannot touch newer content."""

import asyncio

import pytest

from stripe_recovery_core import RepairStatus
from util import FaultBackend, make_kernel, pattern_bytes, piece_key, run


def _delete_piece(backend, kernel, name, generation, shard, stripe):
    key = piece_key(kernel, name, generation, shard, stripe)
    backend._data.pop(key)
    return key


def test_repair_restores_missing_pieces_byte_identical():
    async def go():
        kernel, backend = make_kernel()
        data = pattern_bytes(1003)
        desc = (await kernel.put("obj", data)).descriptor
        snapshot = backend.snapshot()

        removed = [
            _delete_piece(backend, kernel, "obj", desc.generation, 5, 0),
            _delete_piece(backend, kernel, "obj", desc.generation, 6, 3),
        ]
        result = await kernel.repair(desc).run()
        assert result.status == RepairStatus.COMPLETED
        assert sorted(result.pieces_repaired) == [(0, 5), (3, 6)]
        assert result.unrecoverable_stripes == []
        # regenerated pieces are byte-identical to the originals
        for key in removed:
            assert backend._data[key] == snapshot[key]
        # descriptor untouched, content intact
        assert await kernel.load_descriptor("obj") == desc
        assert await kernel.read_bytes(desc) == data
        assert kernel.stats.pieces_repaired == 2
    run(go())


def test_repair_rewrites_corrupted_piece():
    async def go():
        kernel, backend = make_kernel()
        data = pattern_bytes(800)
        desc = (await kernel.put("obj", data)).descriptor
        key = piece_key(kernel, "obj", desc.generation, 2, 1)
        original = backend._data[key]
        corrupted = bytearray(original)
        corrupted[10] ^= 0xFF
        backend._data[key] = bytes(corrupted)

        result = await kernel.repair(desc).run()
        assert result.status == RepairStatus.COMPLETED
        assert result.pieces_repaired == [(1, 2)]
        assert backend._data[key] == original
        assert await kernel.read_bytes(desc) == data
    run(go())


def test_repair_reports_unrecoverable_stripes_as_partial():
    async def go():
        kernel, backend = make_kernel()
        data = pattern_bytes(1003)
        desc = (await kernel.put("obj", data)).descriptor
        # stripe 0 loses 4 shards: beyond parity, cannot be repaired
        for shard in (0, 1, 2, 3):
            _delete_piece(backend, kernel, "obj", desc.generation, shard, 0)

        result = await kernel.repair(desc).run()
        assert result.status == RepairStatus.PARTIAL
        assert result.unrecoverable_stripes == [0]
        assert result.stripes_processed == desc.stripe_count
        # other stripes were still verified
        assert result.pieces_verified == (desc.stripe_count - 1) * 7 + 3
        # reading the damaged region fails loudly, the rest reads fine
        from stripe_recovery_core import UnrecoverableError

        with pytest.raises(UnrecoverableError):
            await kernel.read_bytes(desc, offset=0, length=256)
        assert await kernel.read_bytes(desc, offset=256) == data[256:]
    run(go())


def test_repair_write_failure_is_recorded_not_reported_as_content_change():
    async def go():
        backend = FaultBackend()
        kernel, _ = make_kernel(backend)
        data = pattern_bytes(600)
        desc = (await kernel.put("obj", data)).descriptor
        _delete_piece(backend, kernel, "obj", desc.generation, 6, 0)

        backend.write_failure_predicate = lambda key: "/shards/006/" in key
        result = await kernel.repair(desc).run()
        assert result.status == RepairStatus.PARTIAL
        assert result.write_failures and result.write_failures[0][1] == 6
        # the object as committed is unchanged: descriptor identical, content
        # still fully readable from the remaining shards
        assert await kernel.load_descriptor("obj") == desc
        assert await kernel.read_bytes(desc) == data

        # and once storage behaves, a re-repair completes
        backend.write_failure_predicate = None
        again = await kernel.repair(desc).run()
        assert again.status == RepairStatus.COMPLETED
    run(go())


def test_late_repair_cannot_replace_newer_content():
    """Repair of generation 1 runs to completion only after generation 2 was
    committed under the same name. Generation 2 must remain byte-identical."""

    async def go():
        kernel, backend = make_kernel()
        data_v1 = pattern_bytes(1003, seed=1)
        data_v2 = pattern_bytes(1003, seed=2)
        d1 = (await kernel.put("obj", data_v1)).descriptor
        _delete_piece(backend, kernel, "obj", d1.generation, 4, 2)

        # new content committed under the same name while repair is pending
        d2 = (await kernel.put("obj", data_v2)).descriptor
        v2_snapshot = {
            k: v for k, v in backend.snapshot().items() if d2.generation in k
        }

        # the late repair of the old generation now runs
        result = await kernel.repair(d1).run()
        assert result.status == RepairStatus.COMPLETED

        # generation 2 storage untouched, byte for byte
        for key, value in v2_snapshot.items():
            assert backend._data[key] == value
        # both generations read their own content
        assert await kernel.read_bytes(d1) == data_v1
        assert await kernel.read_bytes(d2) == data_v2
        assert await kernel.load_descriptor("obj") == d2
    run(go())


def test_repair_concurrent_with_rewrite_of_same_name():
    async def go():
        kernel, backend = make_kernel()
        data_v1 = pattern_bytes(2000, seed=1)
        data_v2 = pattern_bytes(1500, seed=2)
        d1 = (await kernel.put("obj", data_v1)).descriptor
        for shard in (5, 6):
            for stripe in range(3):
                _delete_piece(backend, kernel, "obj", d1.generation, shard, stripe)

        repair_task = asyncio.create_task(kernel.repair(d1).run())
        put_task = asyncio.create_task(kernel.put("obj", data_v2))
        repair_result, put2 = await asyncio.gather(repair_task, put_task)

        assert repair_result.status == RepairStatus.COMPLETED
        assert len(repair_result.pieces_repaired) == 6
        assert await kernel.read_bytes(d1) == data_v1
        assert await kernel.read_bytes(put2.descriptor) == data_v2
        assert await kernel.load_descriptor("obj") == put2.descriptor
    run(go())


def test_repair_is_cancellable_and_cancellation_changes_nothing():
    async def go():
        backend = FaultBackend(latency=0.002)
        kernel, _ = make_kernel(backend)
        data = pattern_bytes(5000)
        desc = (await kernel.put("obj", data)).descriptor
        _delete_piece(backend, kernel, "obj", desc.generation, 6, 5)

        op = kernel.repair(desc)
        task = asyncio.create_task(op.run())
        await asyncio.sleep(0.01)
        op.cancel()
        result = await task
        assert result.status == RepairStatus.CANCELLED
        assert result.stripes_processed < result.stripes_total

        # content and descriptor unaffected by the cancelled repair
        assert await kernel.load_descriptor("obj") == desc
        assert await kernel.read_bytes(desc) == data
        # a fresh repair still completes
        final = await kernel.repair(desc).run()
        assert final.status == RepairStatus.COMPLETED
    run(go())


def test_repair_run_twice_rejected():
    async def go():
        kernel, _ = make_kernel()
        desc = (await kernel.put("obj", pattern_bytes(100))).descriptor
        op = kernel.repair(desc)
        await op.run()
        from stripe_recovery_core import UsageError

        with pytest.raises(UsageError):
            await op.run()
    run(go())
